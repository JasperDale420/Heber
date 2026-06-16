"""Trend-Scanning Labels Pipeline — implements Lopez de Prado AFML Ch 3.5.

Instead of fixed-horizon labels, this finds the OPTIMAL lookforward horizon for
each (ticker, date) observation by fitting OLS regressions across multiple
candidate horizons and selecting the one with the highest |t-statistic|.

Produces per-ticker daily Gold features:
  - trend_scan_horizon   (optimal horizon in days, integer 1-20)
  - trend_scan_t_value   (t-stat of slope at optimal horizon; continuous label)

Usage:
    uv run python -m heber.features.pipelines.trend_scan_features --start 2026-01-01 --end 2026-02-28
    uv run python -m heber.features.pipelines.trend_scan_features --start 2026-01-01 --end 2026-02-28 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

# Candidate lookforward horizons (trading days)
CANDIDATE_HORIZONS = [1, 2, 3, 5, 10, 15, 20]

# Extra days of price data needed beyond end_date for forward-looking labels
FORWARD_BUFFER_DAYS = 30  # calendar days (~20 trading days)

LOOKBACK_DAYS = 0  # No backward lookback; purely forward-looking


def _ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ts_available column for zero-leakage Gold writes.

    Trend scan labels are forward-looking — each label requires price data
    through FORWARD_BUFFER_DAYS after ts_event, so ts_available must reflect
    that to prevent look-ahead bias.
    """
    if "ts_available" not in df.columns:
        df["ts_available"] = pd.to_datetime(df["ts_event"], utc=True) + timedelta(days=FORWARD_BUFFER_DAYS)
    return df


def compute_ols_t_stat(prices: np.ndarray) -> tuple[float, float]:
    """Compute OLS slope t-statistic for a price series against time index.

    Fits: price = alpha + beta * t + epsilon
    Returns (beta, t_stat) where t_stat = beta / SE(beta).

    Args:
        prices: 1-D array of prices (length >= 2).

    Returns:
        Tuple of (slope, t_statistic). Returns (NaN, NaN) if regression
        is degenerate.
    """
    n = len(prices)
    if n < 2:
        return (np.nan, np.nan)

    t = np.arange(n, dtype=np.float64)
    y = prices.astype(np.float64)

    # Check for constant or all-NaN
    if np.all(np.isnan(y)) or np.nanstd(y) == 0:
        return (np.nan, np.nan)

    # OLS via normal equations: y = alpha + beta * t
    t_mean = t.mean()
    y_mean = y.mean()

    ss_tt = np.sum((t - t_mean) ** 2)
    if ss_tt == 0:
        return (np.nan, np.nan)

    beta = np.sum((t - t_mean) * (y - y_mean)) / ss_tt
    alpha = y_mean - beta * t_mean

    # Residuals and standard error
    residuals = y - (alpha + beta * t)
    ssr = np.sum(residuals**2)
    mse = ssr / (n - 2) if n > 2 else 0.0

    if mse <= 0 or ss_tt <= 0:
        return (beta, np.nan)

    se_beta = np.sqrt(mse / ss_tt)
    if se_beta == 0:
        return (beta, np.nan)

    t_stat = beta / se_beta
    return (beta, t_stat)


def compute_trend_scan_for_series(
    close_prices: pd.Series,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    """Compute trend-scanning labels for a single ticker's close price series.

    For each observation, fits OLS over each candidate horizon and picks the
    horizon with the maximum |t-statistic|.

    Args:
        close_prices: Series indexed by datetime with daily close prices,
                      sorted chronologically.
        horizons: Candidate lookforward horizons in trading days.

    Returns:
        DataFrame with columns: ts_event, trend_scan_horizon, trend_scan_t_value.
        Rows where no valid horizon could be computed are dropped.
    """
    if horizons is None:
        horizons = CANDIDATE_HORIZONS

    if close_prices.empty:
        return pd.DataFrame(columns=["ts_event", "trend_scan_horizon", "trend_scan_t_value"])

    prices = close_prices.values.astype(np.float64)
    dates = close_prices.index
    n = len(prices)

    results = []
    for i in range(n):
        best_h = np.nan
        best_t = np.nan
        best_abs_t = -1.0

        for h in horizons:
            end_idx = i + h
            if end_idx >= n:
                continue

            # Price window from observation i to i+h (inclusive)
            window = prices[i : end_idx + 1]
            if np.any(np.isnan(window)):
                continue

            _beta, t_stat = compute_ols_t_stat(window)
            if np.isnan(t_stat):
                continue

            abs_t = abs(t_stat)
            if abs_t > best_abs_t:
                best_abs_t = abs_t
                best_h = h
                best_t = t_stat

        if not np.isnan(best_h):
            results.append(
                {
                    "ts_event": dates[i],
                    "trend_scan_horizon": int(best_h),
                    "trend_scan_t_value": best_t,
                }
            )

    if not results:
        return pd.DataFrame(columns=["ts_event", "trend_scan_horizon", "trend_scan_t_value"])

    return pd.DataFrame(results)


def compute_trend_scan_labels(
    daily_bars: pd.DataFrame,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    """Compute trend-scanning labels for all tickers in a daily bars DataFrame.

    Args:
        daily_bars: DataFrame with columns: instrument_key, ts_event, close.
                    One row per (ticker, date).
        horizons: Candidate lookforward horizons in trading days.

    Returns:
        DataFrame with columns: instrument_key, ts_event, trend_scan_horizon,
        trend_scan_t_value. One row per (ticker, date) where a valid label
        could be computed.
    """
    if daily_bars.empty:
        return pd.DataFrame(columns=["instrument_key", "ts_event", "trend_scan_horizon", "trend_scan_t_value"])

    df = daily_bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.sort_values(["instrument_key", "ts_event"])

    all_results = []
    for instrument_key, group in df.groupby("instrument_key"):
        series = group.set_index("ts_event")["close"].dropna()
        if series.empty:
            continue

        ticker_labels = compute_trend_scan_for_series(series, horizons=horizons)
        if ticker_labels.empty:
            continue

        ticker_labels["instrument_key"] = instrument_key
        all_results.append(ticker_labels)

    if not all_results:
        return pd.DataFrame(columns=["instrument_key", "ts_event", "trend_scan_horizon", "trend_scan_t_value"])

    result = pd.concat(all_results, ignore_index=True)
    return result[["instrument_key", "ts_event", "trend_scan_horizon", "trend_scan_t_value"]]


class TrendScanPipeline:
    """Pipeline to compute trend-scanning Gold labels from Silver bars."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "quant",
        version: str = "v1",
        horizons: list[int] | None = None,
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version
        self.horizons = horizons or CANDIDATE_HORIZONS

    def run(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the trend-scanning label pipeline.

        Args:
            start_date: Start of label date range.
            end_date: End of label date range.
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

        logger.info(
            "Starting trend scan pipeline",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            horizons=self.horizons,
        )

        # Need extra price data after end_date for forward-looking labels
        bar_end = end_date + timedelta(days=FORWARD_BUFFER_DAYS)

        logger.info("Loading Silver bars for trend scan", start=start_date.isoformat(), end=bar_end.isoformat())
        bars = self.reader.read_silver(
            "bars",
            instrument_type="equity",
            time_range=(start_date, bar_end),
        )
        logger.info("Loaded bars", rows=len(bars))

        if bars.empty:
            logger.warning("No bars found for trend scan")
            return {"status": "no_data", "rows": 0}

        # Reduce to daily close
        daily = self._to_daily_close(bars)
        logger.info("Daily close records", rows=len(daily))

        # Compute labels
        labels = compute_trend_scan_labels(daily, horizons=self.horizons)
        logger.info("Computed raw trend scan labels", rows=len(labels))

        if labels.empty:
            logger.warning("No trend scan labels computed")
            return {"status": "no_data", "rows": 0}

        # Filter to requested date range (labels computed within range)
        labels["ts_event"] = pd.to_datetime(labels["ts_event"], utc=True)
        labels = labels[labels["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        labels = labels[labels["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if labels.empty:
            logger.warning("No labels within date range after filtering")
            return {"status": "no_data", "rows": 0}

        # Add Gold schema columns
        labels = _ensure_ts_available(labels)

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="trend_scan_features",
                df=labels,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote trend_scan_features to Gold", rows=len(labels), path=str(output_path))
        else:
            logger.info("Dry run — would write trend_scan_features", rows=len(labels))
            output_path = None

        stats: dict[str, Any] = {
            "status": "success",
            "rows": len(labels),
            "tickers": int(labels["instrument_key"].nunique()),
            "horizon_distribution": labels["trend_scan_horizon"].value_counts().to_dict(),
            "mean_abs_t_value": float(labels["trend_scan_t_value"].abs().mean()),
            "path": str(output_path) if output_path else None,
        }

        logger.info("Trend scan pipeline complete", **stats)
        return stats

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


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute trend-scanning Gold labels")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="quant", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = TrendScanPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("TREND SCAN PIPELINE RESULTS")
    print("=" * 60)
    print(f"  Status: {stats.get('status', 'unknown')}")
    print(f"  Rows: {stats.get('rows', 0)}")
    print(f"  Tickers: {stats.get('tickers', 0)}")
    if stats.get("mean_abs_t_value") is not None:
        print(f"  Mean |t-value|: {stats['mean_abs_t_value']:.4f}")
    if stats.get("horizon_distribution"):
        print(f"  Horizon distribution: {stats['horizon_distribution']}")
    if stats.get("path"):
        print(f"  Output: {stats['path']}")
    print()


if __name__ == "__main__":
    main()
