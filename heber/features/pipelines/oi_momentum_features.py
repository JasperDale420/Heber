"""OI change momentum feature pipeline -- compute Gold datasets from Silver oi_change.

Produces per-ticker daily OI momentum metrics:
  - oi_buildup_ratio    (net OI change / average OI across strikes)
  - new_position_signal (binary: fresh informed positioning detected)
  - oi_change_momentum_5d (5-day rolling sum of oi_buildup_ratio)

These features capture institutional open interest dynamics — new position
building, position unwinding, and sustained directional conviction.

Usage:
    uv run python -m heber.features.pipelines.oi_momentum_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.oi_momentum_features --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 10  # Need 5 trading days + buffer for 5d rolling window

# Threshold: volume / prev_oi must exceed this for new_position_signal
NEW_POSITION_VOLUME_RATIO_THRESHOLD = 1.5


def _ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ts_available column for zero-leakage Gold writes."""
    if "ts_available" not in df.columns:
        df["ts_available"] = datetime.now(UTC)
    return df


def _ensure_instrument_key(df: pd.DataFrame, symbol_col: str = "symbol") -> pd.DataFrame:
    """Add instrument_key if missing."""
    if "instrument_key" not in df.columns and symbol_col in df.columns:
        df["instrument_key"] = df[symbol_col].apply(lambda s: s if ":" in str(s) else f"equity:{s}")
    return df


def compute_oi_momentum(oi_change_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-ticker daily OI momentum features.

    Input: Silver oi_change with columns:
        symbol, ts_event, call_oi, put_oi, call_oi_change, put_oi_change,
        avg_price, prev_oi, volume, trades

    Output: One row per (instrument_key, date) with:
      - oi_buildup_ratio: total_oi_change / avg_oi across all strikes
      - new_position_signal: 1.0 if buildup > 0 and volume/prev_oi > 1.5
      - oi_change_momentum_5d: 5-day rolling sum of oi_buildup_ratio
    """
    if oi_change_df.empty:
        return pd.DataFrame()

    df = oi_change_df.copy()

    # --- Step 1: Parse timestamps and extract date ---
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    # --- Step 2: Coerce numeric columns ---
    numeric_cols = ["call_oi", "put_oi", "call_oi_change", "put_oi_change", "volume", "prev_oi"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # --- Step 3: Group by (symbol, date) and aggregate across all strikes ---
    daily = (
        df.groupby(["symbol", "date"])
        .agg(
            total_call_oi_change=("call_oi_change", "sum"),
            total_put_oi_change=("put_oi_change", "sum"),
            avg_call_oi=("call_oi", "mean"),
            avg_put_oi=("put_oi", "mean"),
            total_volume=("volume", "sum"),
            total_prev_oi=("prev_oi", "sum"),
        )
        .reset_index()
    )

    # --- Step 4: Compute oi_buildup_ratio ---
    daily["total_oi_change"] = daily["total_call_oi_change"] + daily["total_put_oi_change"]
    daily["avg_oi"] = daily["avg_call_oi"] + daily["avg_put_oi"]

    # Avoid division by zero: when avg_oi is 0, ratio is 0
    daily["oi_buildup_ratio"] = daily.apply(
        lambda row: row["total_oi_change"] / row["avg_oi"] if row["avg_oi"] != 0 else 0.0,
        axis=1,
    )

    # --- Step 5: Compute new_position_signal ---
    daily["volume_prev_oi_ratio"] = daily.apply(
        lambda row: row["total_volume"] / row["total_prev_oi"] if row["total_prev_oi"] != 0 else 0.0,
        axis=1,
    )
    daily["new_position_signal"] = (
        (daily["oi_buildup_ratio"] > 0) & (daily["volume_prev_oi_ratio"] > NEW_POSITION_VOLUME_RATIO_THRESHOLD)
    ).astype(float)

    # --- Step 6: Sort and compute 5-day rolling sum per symbol ---
    daily = daily.sort_values(["symbol", "date"])

    rolling_parts: list[pd.DataFrame] = []
    for _ticker, group in daily.groupby("symbol"):
        g = group.copy()
        g["oi_change_momentum_5d"] = g["oi_buildup_ratio"].rolling(window=5, min_periods=5).sum()
        rolling_parts.append(g)

    if not rolling_parts:
        return pd.DataFrame()

    result = pd.concat(rolling_parts, ignore_index=True)

    # --- Step 7: Drop NaN rows (first 4 days per ticker won't have enough data for 5d window) ---
    result = result.dropna(subset=["oi_change_momentum_5d"])

    if result.empty:
        return pd.DataFrame()

    # --- Step 8: Add Gold metadata columns ---
    result["ts_event"] = pd.to_datetime(result["date"], utc=True)

    result = _ensure_instrument_key(result)
    result = _ensure_ts_available(result)

    # Select final columns
    output_cols = [
        "instrument_key",
        "symbol",
        "ts_event",
        "ts_available",
        "oi_buildup_ratio",
        "new_position_signal",
        "oi_change_momentum_5d",
    ]
    return result[output_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class OiMomentumPipeline:
    """Pipeline to compute OI momentum Gold features from Silver oi_change."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "watch",
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
        """Run the OI momentum pipeline.

        Args:
            start_date: Start of date range
            end_date: End of date range
            dry_run: If True, compute but don't write

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

        gold_name = "oi_momentum_features"

        # Read Silver oi_change with lookback for rolling window
        read_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info(
            "Loading Silver oi_change",
            read_start=read_start.isoformat(),
            end=end_date.isoformat(),
        )
        oi_change = self.reader.read_silver(
            "oi_change",
            time_range=(read_start, end_date),
        )
        logger.info("Loaded oi_change", rows=len(oi_change))

        # Compute features
        logger.info("Computing OI momentum features")
        df = compute_oi_momentum(oi_change)

        if df.empty:
            logger.warning("No OI momentum features computed")
            return {gold_name: {"status": "no_data", "rows": 0}}

        # Filter output to requested date range (exclude lookback period)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if df.empty:
            logger.warning("No data in requested date range after filtering")
            return {gold_name: {"status": "no_data", "rows": 0}}

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset=gold_name,
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote to Gold", dataset=gold_name, rows=len(df), path=str(output_path))
        else:
            logger.info("Dry run -- would write", dataset=gold_name, rows=len(df))
            output_path = None

        return {
            gold_name: {
                "status": "success",
                "rows": len(df),
                "path": str(output_path) if output_path else None,
            }
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute OI momentum Gold features from Silver oi_change")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = OiMomentumPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("OI MOMENTUM PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
