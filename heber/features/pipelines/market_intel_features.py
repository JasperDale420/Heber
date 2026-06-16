"""Market intelligence feature pipelines — compute Gold datasets from Silver darkpool/greek/iv/ftd/tide feeds.

Produces:
  - darkpool_features          (daily dark pool aggregates per symbol)
  - greek_exposure_features    (daily greek exposure per symbol)
  - options_sentiment_features (IV rank + market/sector tide per symbol)
  - ftd_features               (failures-to-deliver per symbol)

Usage:
    uv run python -m heber.features.pipelines.market_intel_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.market_intel_features --start 2026-01-01 --end 2026-03-10 --dry-run
    uv run python -m heber.features.pipelines.market_intel_features \
        --start 2026-01-01 --end 2026-03-10 --datasets darkpool ftd
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)


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


# ---------------------------------------------------------------------------
# Dark Pool Features
# ---------------------------------------------------------------------------


def compute_darkpool_features(darkpool: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily dark pool statistics per symbol.

    Input: Silver darkpool with columns: instrument_key, symbol, ts_event, ts_available,
           price, size, notional, nbbo_bid, nbbo_ask
    Output: One row per (symbol, date) with aggregated dark pool metrics.
    """
    if darkpool.empty:
        return pd.DataFrame()

    df = darkpool.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    for col in ("price", "size", "notional", "nbbo_bid", "nbbo_ask"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Classify trades relative to NBBO
    if "nbbo_ask" in df.columns and "nbbo_bid" in df.columns:
        df["above_nbbo"] = df["price"] > df["nbbo_ask"]
        df["below_nbbo"] = df["price"] < df["nbbo_bid"]
    else:
        df["above_nbbo"] = False
        df["below_nbbo"] = False

    groups = df.groupby(["instrument_key", "date"])

    agg = groups.agg(
        total_volume=("size", "sum"),
        total_notional=("notional", "sum"),
        avg_price=("price", "mean"),
        trade_count=("price", "count"),
        avg_trade_size=("size", "mean"),
    ).reset_index()

    # Percentage above/below NBBO
    above_counts = df[df["above_nbbo"]].groupby(["instrument_key", "date"]).size().reset_index(name="_above")
    below_counts = df[df["below_nbbo"]].groupby(["instrument_key", "date"]).size().reset_index(name="_below")
    total_counts = groups.size().reset_index(name="_total")

    agg = agg.merge(above_counts, on=["instrument_key", "date"], how="left")
    agg = agg.merge(below_counts, on=["instrument_key", "date"], how="left")
    agg = agg.merge(total_counts, on=["instrument_key", "date"], how="left")
    agg["_above"] = agg["_above"].fillna(0)
    agg["_below"] = agg["_below"].fillna(0)

    agg["pct_above_nbbo"] = np.where(agg["_total"] > 0, agg["_above"] / agg["_total"], 0.0)
    agg["pct_below_nbbo"] = np.where(agg["_total"] > 0, agg["_below"] / agg["_total"], 0.0)

    agg = agg.drop(columns=["_above", "_below", "_total"])

    # Rename date -> ts_event
    agg = agg.rename(columns={"date": "ts_event"})
    agg["ts_event"] = pd.to_datetime(agg["ts_event"], utc=True)

    return _ensure_ts_available(agg)


# ---------------------------------------------------------------------------
# Greek Exposure Features
# ---------------------------------------------------------------------------


def compute_greek_exposure_features(greek: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily greek exposure per underlying symbol.

    Input: Silver greek_exposure — handles both aggregate (v1) and per-leg (v2/v3) schemas.
      v1 columns: instrument_key, symbol, ts_event, gamma_exposure, delta_exposure
      v2/v3 columns: instrument_key, symbol, ts_event, call_gamma, put_gamma,
                      call_delta, put_delta, strike, dte
    Output: One row per (symbol, date) with net greek exposures.
    """
    if greek.empty:
        return pd.DataFrame()

    df = greek.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    for col in (
        "gamma_exposure",
        "delta_exposure",
        "call_gamma",
        "put_gamma",
        "call_delta",
        "put_delta",
        "strike",
        "dte",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize schema: compute net gamma/delta from per-leg columns if not present
    has_aggregate = "gamma_exposure" in df.columns and "delta_exposure" in df.columns
    has_per_leg = "call_gamma" in df.columns and "put_gamma" in df.columns

    if has_per_leg and not has_aggregate:
        df["gamma_exposure"] = df["call_gamma"].fillna(0) + df["put_gamma"].fillna(0)
        df["delta_exposure"] = df.get("call_delta", pd.Series(0, index=df.index)).fillna(0) + df.get(
            "put_delta", pd.Series(0, index=df.index)
        ).fillna(0)

    groups = df.groupby(["instrument_key", "date"])

    agg = groups.agg(
        net_gamma_exposure=("gamma_exposure", "sum"),
        net_delta_exposure=("delta_exposure", "sum"),
    ).reset_index()

    # Put/call gamma ratio
    if has_per_leg:
        leg_agg = groups.agg(
            _total_put_gamma=("put_gamma", lambda x: x.fillna(0).abs().sum()),
            _total_call_gamma=("call_gamma", lambda x: x.fillna(0).abs().sum()),
        ).reset_index()
        leg_agg["put_call_gamma_ratio"] = np.where(
            leg_agg["_total_call_gamma"] > 0,
            leg_agg["_total_put_gamma"] / leg_agg["_total_call_gamma"],
            np.nan,
        )
        agg = agg.merge(
            leg_agg[["instrument_key", "date", "put_call_gamma_ratio"]],
            on=["instrument_key", "date"],
            how="left",
        )
    else:
        agg["put_call_gamma_ratio"] = np.nan

    # Max gamma strike
    if "strike" in df.columns:
        idx = df.groupby(["instrument_key", "date"])["gamma_exposure"].transform("idxmax")
        max_strike = df.loc[idx.dropna().astype(int).unique(), ["instrument_key", "date", "strike"]].drop_duplicates(
            subset=["instrument_key", "date"]
        )
        max_strike = max_strike.rename(columns={"strike": "max_gamma_strike"})
        agg = agg.merge(max_strike, on=["instrument_key", "date"], how="left")
    else:
        agg["max_gamma_strike"] = np.nan

    # Weighted average DTE
    if "dte" in df.columns:
        df["_gamma_abs"] = df["gamma_exposure"].abs()
        df["_weighted_dte"] = df["dte"] * df["_gamma_abs"]
        dte_agg = groups.agg(
            _total_weight=("_gamma_abs", "sum"),
            _total_weighted_dte=("_weighted_dte", "sum"),
        ).reset_index()
        dte_agg["weighted_avg_dte"] = np.where(
            dte_agg["_total_weight"] > 0,
            dte_agg["_total_weighted_dte"] / dte_agg["_total_weight"],
            np.nan,
        )
        agg = agg.merge(
            dte_agg[["instrument_key", "date", "weighted_avg_dte"]],
            on=["instrument_key", "date"],
            how="left",
        )
    else:
        agg["weighted_avg_dte"] = np.nan

    agg = agg.rename(columns={"date": "ts_event"})
    agg["ts_event"] = pd.to_datetime(agg["ts_event"], utc=True)

    return _ensure_ts_available(agg)


# ---------------------------------------------------------------------------
# Options Sentiment Features
# ---------------------------------------------------------------------------


def compute_options_sentiment_features(
    iv_rank_df: pd.DataFrame,
    market_tide_df: pd.DataFrame,
    sector_tide_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine IV rank, market tide, and sector tide into per-symbol sentiment features.

    Input:
      - iv_rank: instrument_key, symbol, ts_event, iv_rank, current_iv, iv_percentile
      - market_tide: instrument_key, ts_event, call_put_ratio, net_volume, sentiment
      - sector_tide: instrument_key, ts_event, sector, sentiment, call_put_ratio

    Output: One row per (symbol, date) with sentiment metrics.
    """
    parts: list[pd.DataFrame] = []

    # --- IV Rank features (per-symbol) ---
    if not iv_rank_df.empty:
        iv = iv_rank_df.copy()
        iv["ts_event"] = pd.to_datetime(iv["ts_event"], utc=True)
        iv["date"] = iv["ts_event"].dt.date
        for col in ("iv_rank", "current_iv", "iv_percentile"):
            if col in iv.columns:
                iv[col] = pd.to_numeric(iv[col], errors="coerce")

        iv_agg = (
            iv.groupby(["instrument_key", "date"])
            .agg(
                iv_rank=("iv_rank", "last"),
                current_iv=("current_iv", "last"),
                iv_percentile=("iv_percentile", "last"),
            )
            .reset_index()
        )
        parts.append(iv_agg)
        logger.info("IV rank features computed", rows=len(iv_agg))

    # --- Market Tide features (market-wide, broadcast to all symbols) ---
    market_part = pd.DataFrame()
    if not market_tide_df.empty:
        mt = market_tide_df.copy()
        mt["ts_event"] = pd.to_datetime(mt["ts_event"], utc=True)
        mt["date"] = mt["ts_event"].dt.date
        for col in ("call_put_ratio", "net_volume"):
            if col in mt.columns:
                mt[col] = pd.to_numeric(mt[col], errors="coerce")

        # Encode sentiment as numeric
        sentiment_map = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}
        if "sentiment" in mt.columns:
            mt["market_sentiment"] = mt["sentiment"].str.lower().map(sentiment_map).fillna(0.0)
        else:
            mt["market_sentiment"] = 0.0

        market_agg = (
            mt.groupby("date")
            .agg(
                market_call_put_ratio=("call_put_ratio", "last"),
                market_net_volume=("net_volume", "last"),
                market_sentiment=("market_sentiment", "last"),
            )
            .reset_index()
        )
        market_part = market_agg
        logger.info("Market tide features computed", rows=len(market_agg))

    # --- Sector Tide features ---
    sector_part = pd.DataFrame()
    if not sector_tide_df.empty:
        st = sector_tide_df.copy()
        st["ts_event"] = pd.to_datetime(st["ts_event"], utc=True)
        st["date"] = st["ts_event"].dt.date
        for col in ("call_put_ratio", "net_volume"):
            if col in st.columns:
                st[col] = pd.to_numeric(st[col], errors="coerce")

        sentiment_map = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}
        if "sentiment" in st.columns:
            st["sector_sentiment"] = st["sentiment"].str.lower().map(sentiment_map).fillna(0.0)
        else:
            st["sector_sentiment"] = 0.0

        sector_agg = (
            st.groupby(["date", "sector"])
            .agg(
                sector_sentiment=("sector_sentiment", "last"),
                sector_call_put_ratio=("call_put_ratio", "last"),
                sector_net_volume=("net_volume", "last"),
            )
            .reset_index()
        )
        sector_part = sector_agg
        logger.info("Sector tide features computed", rows=len(sector_agg))

    # --- Merge ---
    if not parts:
        # No IV rank data; create empty frame
        return pd.DataFrame()

    # Start with IV rank as the base (per-symbol)
    result = parts[0]

    # Cross-join market tide (market-wide metrics apply to all symbols on that date)
    if not market_part.empty:
        result = result.merge(market_part, on="date", how="left")

    # Sector tide is sector-level; we won't join it here unless we have a symbol->sector map.
    # Store sector-level features as a separate output row set keyed by sector.
    # For simplicity, include sector features if available via a best-effort join.
    # In practice, downstream models consume sector_tide independently.
    if not sector_part.empty:
        # Store sector features alongside per-symbol features as additional rows
        # keyed by instrument_key = "sector:{sector_name}"
        sector_out = sector_part.copy()
        sector_out["instrument_key"] = "sector:" + sector_out["sector"].astype(str)
        sector_out = sector_out.rename(columns={"date": "ts_event"})
        sector_out["ts_event"] = pd.to_datetime(sector_out["ts_event"], utc=True)
        sector_out = _ensure_ts_available(sector_out)
        # We'll append these after the main result

    # Finalize per-symbol result
    result = result.rename(columns={"date": "ts_event"})
    result["ts_event"] = pd.to_datetime(result["ts_event"], utc=True)
    result = _ensure_ts_available(result)

    # Append sector rows if present
    if not sector_part.empty:
        result = pd.concat([result, sector_out], ignore_index=True)

    return result


# ---------------------------------------------------------------------------
# FTD Features
# ---------------------------------------------------------------------------


def compute_ftd_features(ftd: pd.DataFrame) -> pd.DataFrame:
    """Compute failures-to-deliver features per symbol.

    Input: Silver ftd with columns: instrument_key, symbol, ts_event, ts_available,
           quantity, price, value, ftd_date
    Output: One row per (symbol, date) with FTD metrics.
    """
    if ftd.empty:
        return pd.DataFrame()

    df = ftd.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    for col in ("quantity", "price", "value"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    groups = df.groupby(["instrument_key", "date"])

    agg = groups.agg(
        ftd_quantity=("quantity", "sum"),
        ftd_value=("value", "sum"),
        ftd_trade_count=("quantity", "count"),
        ftd_avg_price=("price", "mean"),
    ).reset_index()

    # Days outstanding: count distinct ftd_dates per (symbol, date) as proxy
    if "ftd_date" in df.columns:
        days_out = groups["ftd_date"].nunique().reset_index(name="ftd_days_outstanding")
        agg = agg.merge(days_out, on=["instrument_key", "date"], how="left")
    else:
        agg["ftd_days_outstanding"] = 1

    agg = agg.rename(columns={"date": "ts_event"})
    agg["ts_event"] = pd.to_datetime(agg["ts_event"], utc=True)

    return _ensure_ts_available(agg)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

ALL_DATASETS = ("darkpool", "greek_exposure", "options_sentiment", "ftd")

DATASET_TO_GOLD_NAME = {
    "darkpool": "darkpool_features",
    "greek_exposure": "greek_exposure_features",
    "options_sentiment": "options_sentiment_features",
    "ftd": "ftd_features",
}


class MarketIntelPipeline:
    """Pipeline to compute market intelligence Gold features from Silver data."""

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
        datasets: tuple[str, ...] = ALL_DATASETS,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the pipeline for specified datasets.

        Args:
            start_date: Start of date range
            end_date: End of date range
            datasets: Which datasets to compute
            dry_run: If True, compute but don't write

        Returns:
            Dict with pipeline stats per dataset.
        """
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # When end_date is midnight (date-only input), extend to end of day
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        stats: dict[str, Any] = {}
        time_range = (start_date, end_date)

        # Pre-load Silver data for requested datasets
        darkpool_data = None
        greek_data = None
        iv_rank_data = None
        market_tide_data = None
        sector_tide_data = None
        ftd_data = None

        if "darkpool" in datasets:
            logger.info("Loading Silver darkpool", start=start_date.isoformat(), end=end_date.isoformat())
            darkpool_data = self.reader.read_silver("darkpool", time_range=time_range)
            logger.info("Loaded darkpool", rows=len(darkpool_data))

        if "greek_exposure" in datasets:
            logger.info("Loading Silver greek_exposure")
            greek_data = self.reader.read_silver("greek_exposure", time_range=time_range)
            logger.info("Loaded greek_exposure", rows=len(greek_data))

        if "options_sentiment" in datasets:
            logger.info("Loading Silver iv_rank, market_tide, sector_tide")
            iv_rank_data = self.reader.read_silver("iv_rank", time_range=time_range)
            market_tide_data = self.reader.read_silver("market_tide", time_range=time_range)
            sector_tide_data = self.reader.read_silver("sector_tide", time_range=time_range)
            logger.info(
                "Loaded sentiment sources",
                iv_rank_rows=len(iv_rank_data),
                market_tide_rows=len(market_tide_data),
                sector_tide_rows=len(sector_tide_data),
            )

        if "ftd" in datasets:
            logger.info("Loading Silver ftd")
            ftd_data = self.reader.read_silver("ftd", time_range=time_range)
            logger.info("Loaded ftd", rows=len(ftd_data))

        # Compute each dataset
        for ds_name in datasets:
            gold_name = DATASET_TO_GOLD_NAME[ds_name]
            logger.info("Computing dataset", dataset=gold_name)

            try:
                if ds_name == "darkpool":
                    df = compute_darkpool_features(darkpool_data)
                elif ds_name == "greek_exposure":
                    df = compute_greek_exposure_features(greek_data)
                elif ds_name == "options_sentiment":
                    df = compute_options_sentiment_features(iv_rank_data, market_tide_data, sector_tide_data)
                elif ds_name == "ftd":
                    df = compute_ftd_features(ftd_data)
                else:
                    logger.warning("Unknown dataset", dataset=ds_name)
                    continue

                if df.empty:
                    logger.warning("No data computed", dataset=gold_name)
                    stats[gold_name] = {"status": "no_data", "rows": 0}
                    continue

                # Ensure instrument_key for all rows
                df = _ensure_instrument_key(df)

                # Filter to requested date range only
                if "ts_event" in df.columns:
                    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
                    df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
                    df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

                if not dry_run:
                    output_path = self.reader.write_gold(
                        dataset=gold_name,
                        df=df,
                        project=self.project,
                        version=self.version,
                    )
                    logger.info("Wrote to Gold", dataset=gold_name, rows=len(df), path=str(output_path))
                else:
                    logger.info("Dry run — would write", dataset=gold_name, rows=len(df))
                    output_path = None

                stats[gold_name] = {
                    "status": "success",
                    "rows": len(df),
                    "path": str(output_path) if output_path else None,
                }

            except Exception as exc:
                logger.error("Failed to compute dataset", dataset=gold_name, error=str(exc), exc_info=True)
                stats[gold_name] = {"status": "error", "error": str(exc)}

        return stats


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute market intelligence Gold features from Silver data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--datasets",
        nargs="*",
        choices=list(ALL_DATASETS),
        default=list(ALL_DATASETS),
        help="Which datasets to compute (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = MarketIntelPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        datasets=tuple(args.datasets),
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("MARKET INTEL PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
