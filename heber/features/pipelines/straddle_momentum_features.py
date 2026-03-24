"""Straddle Momentum Pipeline — tracks ATM straddle returns for vol dynamics.

Captures implied-vs-realized volatility dynamics by tracking the trailing return
of at-the-money (ATM) straddle positions.

Produces per-ticker daily Gold features:
  - straddle_return_1m   (20-trading-day trailing return of ATM straddle)
  - straddle_return_3m   (60-trading-day trailing return of ATM straddle)

Usage:
    uv run python -m heber.features.pipelines.straddle_momentum_features --start 2026-01-01 --end 2026-02-28
    uv run python -m heber.features.pipelines.straddle_momentum_features --start 2026-01-01 --end 2026-02-28 --dry-run
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 70  # Calendar days for 3-month (~60 trading day) lookback
STRADDLE_WINDOW_1M = 20  # Trading days for 1-month straddle return
STRADDLE_WINDOW_3M = 60  # Trading days for 3-month straddle return


def _ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ts_available column for zero-leakage Gold writes."""
    if "ts_available" not in df.columns:
        df["ts_available"] = datetime.now(UTC)
    return df


def _expand_chain_json(option_chain_snapshots: pd.DataFrame) -> pd.DataFrame:
    """Flatten option_chain_snapshot rows with ``chain_json`` into per-contract rows.

    Silver ``option_chain_snapshot`` stores contract data inside a JSON blob
    (``chain_json``).  Each JSON object contains ``data.contracts`` — an array of
    dicts with keys like ``bid``, ``ask``, ``strike``, ``option_type``, ``delta``.

    This function explodes that JSON into one row per contract, carrying over
    ``underlying`` and ``ts_event`` from the snapshot row.
    """
    if "chain_json" not in option_chain_snapshots.columns:
        return option_chain_snapshots  # already flat

    rows: list[dict] = []
    for _, snap in option_chain_snapshots.iterrows():
        try:
            blob = json.loads(snap["chain_json"]) if isinstance(snap["chain_json"], str) else snap["chain_json"]
        except (json.JSONDecodeError, TypeError):
            continue

        contracts = blob.get("data", {}).get("contracts", []) if isinstance(blob, dict) else blob
        if not isinstance(contracts, list):
            continue

        for c in contracts:
            if not isinstance(c, dict):
                continue
            rows.append(
                {
                    "underlying": snap.get("underlying", c.get("underlying", "")),
                    "ts_event": snap["ts_event"],
                    "strike": c.get("strike"),
                    "bid": c.get("bid"),
                    "ask": c.get("ask"),
                    "last": c.get("last"),
                    "delta": c.get("delta"),
                    "put_call": (
                        {"call": "C", "put": "P"}.get(str(c.get("option_type", "")).lower(), c.get("option_type"))
                    ),
                    "instrument_key": c.get("contract_symbol", ""),
                }
            )

    if not rows:
        cols = ["underlying", "ts_event", "strike", "bid", "ask", "last", "delta", "put_call", "instrument_key"]
        return pd.DataFrame(columns=cols)

    return pd.DataFrame(rows)


def select_atm_options(
    option_chain: pd.DataFrame,
    date_col: str = "date",
) -> pd.DataFrame:
    """Select ATM call and put for each (underlying, date).

    ATM is defined as the strike where |delta| is closest to 0.5.
    If delta is not available, falls back to nearest strike to the last
    known spot price.

    Args:
        option_chain: DataFrame with columns including: underlying, strike,
                      delta, bid, ask, ts_event, instrument_key.
                      Must have a ``put_call`` or similar indicator.
        date_col: Column name for the date grouping.

    Returns:
        DataFrame with columns: underlying, date, atm_call_mid, atm_put_mid,
        straddle_value. One row per (underlying, date).
    """
    if option_chain.empty:
        return pd.DataFrame(columns=["underlying", "date", "atm_call_mid", "atm_put_mid", "straddle_value"])

    df = option_chain.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

    # Derive date column for grouping
    if date_col not in df.columns:
        df[date_col] = df["ts_event"].dt.date

    # Ensure numeric columns
    for col in ("bid", "ask", "strike", "delta"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute mid price
    if "bid" in df.columns and "ask" in df.columns:
        df["mid"] = (df["bid"] + df["ask"]) / 2
    elif "last" in df.columns:
        df["mid"] = pd.to_numeric(df["last"], errors="coerce")
    else:
        logger.warning("No bid/ask or last column in option chain")
        return pd.DataFrame(columns=["underlying", "date", "atm_call_mid", "atm_put_mid", "straddle_value"])

    # Determine call/put from instrument_key or explicit column
    if "put_call" not in df.columns:
        # Try to infer from instrument_key (OCC format: last char before strike is C or P)
        if "instrument_key" in df.columns:
            df["put_call"] = df["instrument_key"].str.extract(r"([CP])\d+$", expand=False)
        else:
            logger.warning("Cannot determine put_call type")
            return pd.DataFrame(columns=["underlying", "date", "atm_call_mid", "atm_put_mid", "straddle_value"])

    df["put_call"] = df["put_call"].str.upper()

    # Filter valid rows
    df = df.dropna(subset=["mid", "strike"])
    df = df[df["mid"] > 0]

    results = []
    for (underlying, date), group in df.groupby(["underlying", date_col]):
        calls = group[group["put_call"] == "C"]
        puts = group[group["put_call"] == "P"]

        if calls.empty or puts.empty:
            continue

        # Find ATM using |delta| closest to 0.5 if delta available
        if "delta" in group.columns and group["delta"].notna().any():
            calls_with_delta = calls.dropna(subset=["delta"])
            puts_with_delta = puts.dropna(subset=["delta"])

            if not calls_with_delta.empty and not puts_with_delta.empty:
                atm_call_idx = (calls_with_delta["delta"].abs() - 0.5).abs().idxmin()
                atm_put_idx = (puts_with_delta["delta"].abs() - 0.5).abs().idxmin()
                atm_call_mid = calls_with_delta.loc[atm_call_idx, "mid"]
                atm_put_mid = puts_with_delta.loc[atm_put_idx, "mid"]
            else:
                # Fallback: use strike midpoint
                atm_call_mid, atm_put_mid = _select_atm_by_strike(calls, puts)
        else:
            atm_call_mid, atm_put_mid = _select_atm_by_strike(calls, puts)

        if np.isnan(atm_call_mid) or np.isnan(atm_put_mid):
            continue

        straddle_value = atm_call_mid + atm_put_mid
        if straddle_value <= 0:
            continue

        results.append(
            {
                "underlying": underlying,
                "date": date,
                "atm_call_mid": atm_call_mid,
                "atm_put_mid": atm_put_mid,
                "straddle_value": straddle_value,
            }
        )

    if not results:
        return pd.DataFrame(columns=["underlying", "date", "atm_call_mid", "atm_put_mid", "straddle_value"])

    return pd.DataFrame(results)


def _select_atm_by_strike(calls: pd.DataFrame, puts: pd.DataFrame) -> tuple[float, float]:
    """Select ATM options by nearest strike overlap between calls and puts.

    Finds the strike that appears in both calls and puts, selecting the
    strike closest to the median strike as a proxy for ATM.

    Returns:
        Tuple (atm_call_mid, atm_put_mid).
    """
    common_strikes = set(calls["strike"].dropna().unique()) & set(puts["strike"].dropna().unique())
    if not common_strikes:
        # No common strikes — pick median strike from each
        call_mid_strike = calls["strike"].median()
        put_mid_strike = puts["strike"].median()
        atm_call = calls.iloc[(calls["strike"] - call_mid_strike).abs().argsort().iloc[0]]
        atm_put = puts.iloc[(puts["strike"] - put_mid_strike).abs().argsort().iloc[0]]
        return (atm_call["mid"], atm_put["mid"])

    # Pick common strike closest to median of all strikes
    median_strike = calls["strike"].median()
    best_strike = min(common_strikes, key=lambda s: abs(s - median_strike))

    atm_call = calls[calls["strike"] == best_strike].iloc[0]
    atm_put = puts[puts["strike"] == best_strike].iloc[0]
    return (atm_call["mid"], atm_put["mid"])


def compute_straddle_returns(
    straddle_values: pd.DataFrame,
    window_1m: int = STRADDLE_WINDOW_1M,
    window_3m: int = STRADDLE_WINDOW_3M,
) -> pd.DataFrame:
    """Compute trailing straddle returns from daily ATM straddle values.

    Args:
        straddle_values: DataFrame with columns: underlying, date, straddle_value.
                         One row per (underlying, date), sorted chronologically.
        window_1m: Trading day window for 1-month return (default 20).
        window_3m: Trading day window for 3-month return (default 60).

    Returns:
        DataFrame with columns: instrument_key, ts_event, straddle_return_1m,
        straddle_return_3m.
    """
    if straddle_values.empty:
        return pd.DataFrame(columns=["instrument_key", "ts_event", "straddle_return_1m", "straddle_return_3m"])

    df = straddle_values.copy()
    df = df.sort_values(["underlying", "date"])

    all_results = []
    for underlying, group in df.groupby("underlying"):
        group = group.sort_values("date").reset_index(drop=True)
        values = group["straddle_value"].values
        dates = group["date"].values
        n = len(values)

        for i in range(n):
            ret_1m = np.nan
            ret_3m = np.nan

            if i >= window_1m and values[i - window_1m] > 0:
                ret_1m = (values[i] - values[i - window_1m]) / values[i - window_1m]

            if i >= window_3m and values[i - window_3m] > 0:
                ret_3m = (values[i] - values[i - window_3m]) / values[i - window_3m]

            if np.isnan(ret_1m) and np.isnan(ret_3m):
                continue

            all_results.append(
                {
                    "instrument_key": underlying if ":" in str(underlying) else f"equity:{underlying}",
                    "ts_event": dates[i],
                    "straddle_return_1m": ret_1m,
                    "straddle_return_3m": ret_3m,
                }
            )

    if not all_results:
        return pd.DataFrame(columns=["instrument_key", "ts_event", "straddle_return_1m", "straddle_return_3m"])

    result = pd.DataFrame(all_results)
    result["ts_event"] = pd.to_datetime(result["ts_event"], utc=True)
    return result


class StraddleMomentumPipeline:
    """Pipeline to compute straddle momentum Gold features from Silver option chain data."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "quant",
        version: str = "v1",
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version

    def run(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the straddle momentum pipeline.

        Args:
            start_date: Start of feature date range.
            end_date: End of feature date range.
            dry_run: If True, compute but don't write.

        Returns:
            Dict with pipeline stats.
        """
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # When end_date is midnight (date-only input), extend to end of day
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        # Need lookback for trailing returns
        bar_start = start_date - timedelta(days=LOOKBACK_DAYS)

        logger.info(
            "Starting straddle momentum pipeline",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            lookback_start=bar_start.isoformat(),
        )

        # Load option chain snapshots from Silver
        logger.info("Loading Silver option_chain_snapshot", start=bar_start.isoformat(), end=end_date.isoformat())
        option_chain = self.reader.read_silver(
            "option_chain_snapshot",
            time_range=(bar_start, end_date),
        )
        logger.info("Loaded option chain data", rows=len(option_chain))

        if option_chain.empty:
            logger.warning("No option chain data found")
            return {"status": "no_data", "rows": 0}

        # Expand chain_json blobs into flat per-contract rows (Silver schema)
        if "chain_json" in option_chain.columns:
            logger.info("Expanding chain_json into per-contract rows")
            option_chain = _expand_chain_json(option_chain)
            logger.info("Expanded option chain", rows=len(option_chain))

        # Select ATM options and compute daily straddle values
        straddle_values = select_atm_options(option_chain)
        logger.info("Computed daily straddle values", rows=len(straddle_values))

        if straddle_values.empty:
            logger.warning("No ATM straddle values computed")
            return {"status": "no_data", "rows": 0}

        # Compute trailing returns
        features = compute_straddle_returns(straddle_values)
        logger.info("Computed straddle returns", rows=len(features))

        if features.empty:
            logger.warning("No straddle returns computed")
            return {"status": "no_data", "rows": 0}

        # Filter to requested date range
        features["ts_event"] = pd.to_datetime(features["ts_event"], utc=True)
        features = features[features["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        features = features[features["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if features.empty:
            logger.warning("No features within date range after filtering")
            return {"status": "no_data", "rows": 0}

        # Add Gold schema columns
        features = _ensure_ts_available(features)

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="straddle_momentum_features",
                df=features,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote straddle_momentum_features to Gold", rows=len(features), path=str(output_path))
        else:
            logger.info("Dry run — would write straddle_momentum_features", rows=len(features))
            output_path = None

        stats: dict[str, Any] = {
            "status": "success",
            "rows": len(features),
            "tickers": int(features["instrument_key"].nunique()),
            "mean_return_1m": (
                float(features["straddle_return_1m"].mean()) if features["straddle_return_1m"].notna().any() else None
            ),
            "mean_return_3m": (
                float(features["straddle_return_3m"].mean()) if features["straddle_return_3m"].notna().any() else None
            ),
            "path": str(output_path) if output_path else None,
        }

        logger.info("Straddle momentum pipeline complete", **stats)
        return stats


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute straddle momentum Gold features")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="quant", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = StraddleMomentumPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("STRADDLE MOMENTUM PIPELINE RESULTS")
    print("=" * 60)
    print(f"  Status: {stats.get('status', 'unknown')}")
    print(f"  Rows: {stats.get('rows', 0)}")
    print(f"  Tickers: {stats.get('tickers', 0)}")
    if stats.get("mean_return_1m") is not None:
        print(f"  Mean 1M return: {stats['mean_return_1m']:.4f}")
    if stats.get("mean_return_3m") is not None:
        print(f"  Mean 3M return: {stats['mean_return_3m']:.4f}")
    if stats.get("path"):
        print(f"  Output: {stats['path']}")
    print()


if __name__ == "__main__":
    main()
