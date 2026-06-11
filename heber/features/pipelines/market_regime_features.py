"""Market regime feature pipelines — compute Gold datasets from Silver bars/quotes and Gold momentum.

Produces market-level (one row per date) regime features:
  - dispersion            (cross-sectional return SD / 90d rolling median)
  - vol_of_vol            (20d rolling SD of VIX log returns)
  - breadth_proxy         (fraction of tickers with positive momentum_1d)
  - yield_curve_slope     (10Y - 2Y treasury yield)

Usage:
    uv run python -m heber.features.pipelines.market_regime_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.market_regime_features --start 2026-01-01 --end 2026-03-10 --dry-run
    uv run python -m heber.features.pipelines.market_regime_features \
        --start 2026-01-01 --end 2026-03-10 --datasets dispersion breadth_proxy
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.features.pipelines.base import ensure_market_instrument_key, ensure_ts_available
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 120  # Extra history for 90d rolling median + daily returns
INSTRUMENT_KEY = "market:regime"


# ---------------------------------------------------------------------------
# Dispersion
# ---------------------------------------------------------------------------


def compute_dispersion(daily_bars: pd.DataFrame, rolling_window: int = 90) -> pd.DataFrame:
    """Cross-sectional standard deviation of daily returns, normalized by 90-day rolling median.

    Args:
        daily_bars: DataFrame with columns: instrument_key, ts_event, close.
                    One row per (ticker, date) with daily close prices.
        rolling_window: Window for rolling median normalization (default 90 trading days).

    Returns:
        DataFrame with columns: ts_event, dispersion.
        One row per date.
    """
    if daily_bars.empty:
        return pd.DataFrame(columns=["ts_event", "dispersion"])

    df = daily_bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.sort_values(["instrument_key", "ts_event"])

    # Compute daily returns per ticker
    df["daily_return"] = df.groupby("instrument_key")["close"].pct_change(fill_method=None)

    # Cross-sectional SD of daily returns per date
    cross_sd = df.groupby("ts_event")["daily_return"].std().reset_index()
    cross_sd.columns = ["ts_event", "cross_sd"]
    cross_sd = cross_sd.sort_values("ts_event")

    # 90-day rolling median of that SD series
    cross_sd["rolling_median"] = cross_sd["cross_sd"].rolling(rolling_window, min_periods=1).median()

    # Dispersion = cross_sd / rolling_median
    cross_sd["dispersion"] = np.where(
        cross_sd["rolling_median"] > 0,
        cross_sd["cross_sd"] / cross_sd["rolling_median"],
        np.nan,
    )

    return cross_sd[["ts_event", "dispersion"]].copy()


# ---------------------------------------------------------------------------
# Vol-of-Vol
# ---------------------------------------------------------------------------


def compute_vol_of_vol(vix_prices: pd.DataFrame, rolling_window: int = 20) -> pd.DataFrame:
    """Rolling 20-day standard deviation of VIX daily log returns.

    Args:
        vix_prices: DataFrame with columns: ts_event, close.
                    Daily close prices for VIX proxy (UVXY or VIX).
        rolling_window: Window for rolling SD (default 20 trading days).

    Returns:
        DataFrame with columns: ts_event, vol_of_vol.
        One row per date.
    """
    if vix_prices.empty:
        return pd.DataFrame(columns=["ts_event", "vol_of_vol"])

    df = vix_prices.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # If multiple rows per date (intraday quotes), take last close per date
    df["date"] = df["ts_event"].dt.date
    df = df.sort_values("ts_event").groupby("date").last().reset_index()
    df["ts_event"] = pd.to_datetime(df["date"], utc=True)

    df = df.sort_values("ts_event")

    # Daily log returns
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # 20-day rolling SD of log returns
    df["vol_of_vol"] = df["log_return"].rolling(rolling_window, min_periods=rolling_window).std()

    return df[["ts_event", "vol_of_vol"]].copy()


# ---------------------------------------------------------------------------
# Breadth Proxy
# ---------------------------------------------------------------------------


def compute_breadth_proxy(momentum_features: pd.DataFrame) -> pd.DataFrame:
    """Fraction of equity universe with positive momentum_1d on each date.

    Args:
        momentum_features: Gold momentum features DataFrame with columns:
                           ts_event, instrument_key, momentum_1d.

    Returns:
        DataFrame with columns: ts_event, breadth_proxy.
        One row per date. Values range 0-1.
    """
    if momentum_features.empty:
        return pd.DataFrame(columns=["ts_event", "breadth_proxy"])

    df = momentum_features.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["momentum_1d"] = pd.to_numeric(df["momentum_1d"], errors="coerce")

    # Drop rows where momentum_1d is NaN
    df = df.dropna(subset=["momentum_1d"])

    if df.empty:
        return pd.DataFrame(columns=["ts_event", "breadth_proxy"])

    # Count positive vs total per date
    grouped = df.groupby("ts_event")["momentum_1d"]
    positive_count = grouped.apply(lambda x: (x > 0).sum())
    total_count = grouped.count()

    breadth = (positive_count / total_count).reset_index()
    breadth.columns = ["ts_event", "breadth_proxy"]

    return breadth


# ---------------------------------------------------------------------------
# Yield Curve Slope
# ---------------------------------------------------------------------------


def compute_yield_curve_slope(treasury_yields: pd.DataFrame) -> pd.DataFrame:
    """10-year Treasury yield minus 2-year Treasury yield.

    Args:
        treasury_yields: Silver treasury_yields DataFrame with columns:
                         ts_event, maturity, yield_pct.
                         maturity values include "2Y" and "10Y".

    Returns:
        DataFrame with columns: ts_event, yield_curve_slope.
        One row per date.
    """
    if treasury_yields.empty:
        return pd.DataFrame(columns=["ts_event", "yield_curve_slope"])

    df = treasury_yields.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["yield_pct"] = pd.to_numeric(df["yield_pct"], errors="coerce")
    df["date"] = df["ts_event"].dt.date

    # Pivot to get 2Y and 10Y columns per date
    pivot = df.pivot_table(index="date", columns="maturity", values="yield_pct", aggfunc="last")

    if "10Y" not in pivot.columns or "2Y" not in pivot.columns:
        logger.warning("treasury_yields_missing_maturities", available=list(pivot.columns))
        return pd.DataFrame(columns=["ts_event", "yield_curve_slope"])

    result = pd.DataFrame(
        {
            "ts_event": pd.to_datetime(pivot.index, utc=True),
            "yield_curve_slope": (pivot["10Y"] - pivot["2Y"]).values,
        }
    )

    return result


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

ALL_DATASETS = ("dispersion", "vol_of_vol", "breadth_proxy", "yield_curve_slope")

DATASET_TO_GOLD_COL = {
    "dispersion": "dispersion",
    "vol_of_vol": "vol_of_vol",
    "breadth_proxy": "breadth_proxy",
    "yield_curve_slope": "yield_curve_slope",
}


class MarketRegimePipeline:
    """Pipeline to compute market-level regime Gold features from Silver/Gold data."""

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
            start_date: Start of date range.
            end_date: End of date range.
            datasets: Which features to compute (default: all).
            dry_run: If True, compute but don't write.

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
        feature_frames: dict[str, pd.DataFrame] = {}

        # Compute each requested feature
        for ds_name in datasets:
            logger.info("Computing regime feature", feature=ds_name)

            try:
                if ds_name == "dispersion":
                    feature_frames[ds_name] = self._compute_dispersion(start_date, end_date)
                elif ds_name == "vol_of_vol":
                    feature_frames[ds_name] = self._compute_vol_of_vol(start_date, end_date)
                elif ds_name == "breadth_proxy":
                    feature_frames[ds_name] = self._compute_breadth_proxy(start_date, end_date)
                elif ds_name == "yield_curve_slope":
                    feature_frames[ds_name] = self._compute_yield_curve_slope(start_date, end_date)
                else:
                    logger.warning("Unknown dataset", dataset=ds_name)
                    continue

            except Exception as exc:
                logger.error("Failed to compute feature", feature=ds_name, error=str(exc), exc_info=True)
                stats[ds_name] = {"status": "error", "error": str(exc)}

        # Merge all feature frames on ts_event into a single Gold output
        merged = self._merge_features(feature_frames)

        if merged.empty:
            logger.warning("No regime features computed")
            for ds_name in datasets:
                if ds_name not in stats:
                    stats[ds_name] = {"status": "no_data", "rows": 0}
            return stats

        # Filter to requested date range
        merged["ts_event"] = pd.to_datetime(merged["ts_event"], utc=True)
        merged = merged[merged["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        merged = merged[merged["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        # Add Gold schema columns
        merged = ensure_ts_available(merged)
        merged = ensure_market_instrument_key(merged, INSTRUMENT_KEY)

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="market_regime_features",
                df=merged,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote market_regime_features to Gold", rows=len(merged), path=str(output_path))
        else:
            logger.info("Dry run — would write market_regime_features", rows=len(merged))
            output_path = None

        for ds_name in datasets:
            if ds_name in stats:
                continue  # Already has an error entry
            has_data = ds_name in feature_frames and not feature_frames[ds_name].empty
            stats[ds_name] = {
                "status": "success" if has_data else "no_data",
                "rows": len(feature_frames.get(ds_name, pd.DataFrame())),
            }

        stats["_merged"] = {
            "status": "success",
            "rows": len(merged),
            "path": str(output_path) if output_path else None,
        }

        return stats

    def _compute_dispersion(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load Silver bars and compute dispersion."""
        bar_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info("Loading Silver bars for dispersion", start=bar_start.isoformat(), end=end_date.isoformat())
        bars = self.reader.read_silver(
            "bars",
            instrument_type="equity",
            time_range=(bar_start, end_date),
            columns=["instrument_key", "symbol", "ts_event", "ts_available", "timeframe", "close"],
            batch_size=500_000,
        )
        logger.info("Loaded bars for dispersion", rows=len(bars))

        if bars.empty:
            return pd.DataFrame()

        # Drop intraday rows early — _to_daily_close only needs close prices
        # and prefers 1Day bars.  Loading millions of intraday rows causes OOM.
        if "timeframe" in bars.columns and len(bars) > 1_000_000:
            daily_only = bars[bars["timeframe"] == "1Day"]
            if not daily_only.empty:
                logger.info("Dropped intraday bars", before=len(bars), after=len(daily_only))
                bars = daily_only

        # Resample to daily if needed — take last close per (instrument_key, date)
        bars = self._to_daily_close(bars)
        return compute_dispersion(bars)

    def _compute_vol_of_vol(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load Silver quotes for UVXY and compute vol_of_vol."""
        vix_start = start_date - timedelta(days=60)  # Extra lookback for 20d rolling
        logger.info("Loading Silver quotes for UVXY", start=vix_start.isoformat(), end=end_date.isoformat())
        quotes = self.reader.read_silver(
            "quotes",
            instrument_keys=["equity:UVXY"],
            time_range=(vix_start, end_date),
        )
        logger.info("Loaded UVXY quotes", rows=len(quotes))

        if quotes.empty:
            # Fallback: try bars for UVXY
            logger.info("No quotes for UVXY, trying bars")
            quotes = self.reader.read_silver(
                "bars",
                instrument_keys=["equity:UVXY"],
                time_range=(vix_start, end_date),
            )
            logger.info("Loaded UVXY bars", rows=len(quotes))

        if quotes.empty:
            return pd.DataFrame()

        # Ensure close column exists (quotes may have bid/ask but not close)
        if "close" not in quotes.columns:
            for col in ("ask_price", "ask_px", "bid_price", "bid_px"):
                if col in quotes.columns:
                    quotes["close"] = pd.to_numeric(quotes[col], errors="coerce")
                    break

        if "close" not in quotes.columns:
            logger.warning("No close/price column in UVXY data")
            return pd.DataFrame()

        return compute_vol_of_vol(quotes)

    def _compute_breadth_proxy(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load Gold momentum_features and compute breadth_proxy."""
        logger.info("Loading Gold momentum_features for breadth_proxy")
        momentum = self.reader.read_gold(
            "momentum_features",
            project=self.project,
            time_range=(start_date, end_date),
        )
        logger.info("Loaded momentum_features", rows=len(momentum))

        if momentum.empty:
            return pd.DataFrame()

        return compute_breadth_proxy(momentum)

    def _compute_yield_curve_slope(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Load Silver treasury_yields and compute yield_curve_slope."""
        logger.info("Loading Silver treasury_yields")
        yields_df = self.reader.read_silver("treasury_yields", time_range=(start_date, end_date))
        logger.info("Loaded treasury_yields", rows=len(yields_df))

        if yields_df.empty:
            return pd.DataFrame()

        return compute_yield_curve_slope(yields_df)

    @staticmethod
    def _to_daily_close(bars: pd.DataFrame) -> pd.DataFrame:
        """Reduce bars to one daily close per instrument_key."""
        if bars.empty:
            return bars

        df = bars.copy()
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["date"] = df["ts_event"].dt.date

        # Prefer pre-aggregated daily bars
        if "timeframe" in df.columns:
            daily_mask = df["timeframe"] == "1Day"
            if daily_mask.any():
                daily = df[daily_mask].copy()
                daily_keys = set(zip(daily["instrument_key"], daily["date"], strict=False))
                intraday = df[~daily_mask].copy()
                if not intraday.empty:
                    intraday["_key"] = list(zip(intraday["instrument_key"], intraday["date"], strict=False))
                    intraday = intraday[~intraday["_key"].isin(daily_keys)].drop(columns=["_key"])
                df = pd.concat([daily, intraday], ignore_index=True)

        # Take last close per (instrument_key, date)
        df = df.sort_values(["instrument_key", "ts_event"])
        daily = df.groupby(["instrument_key", "date"]).agg(close=("close", "last")).reset_index()
        daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)

        return daily[["instrument_key", "ts_event", "close"]]

    @staticmethod
    def _merge_features(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Outer-merge all feature DataFrames on ts_event."""
        merged: pd.DataFrame | None = None

        for _name, df in frames.items():
            if df.empty:
                continue

            df = df.copy()
            df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

            if merged is None:
                merged = df
            else:
                merged = merged.merge(df, on="ts_event", how="outer")

        return merged if merged is not None else pd.DataFrame()


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute market regime Gold features")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--datasets",
        nargs="*",
        choices=list(ALL_DATASETS),
        default=list(ALL_DATASETS),
        help="Which features to compute (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = MarketRegimePipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        datasets=tuple(args.datasets),
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("MARKET REGIME PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
