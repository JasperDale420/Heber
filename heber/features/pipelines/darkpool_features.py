"""Darkpool Confirmation pipeline -- per-ticker daily darkpool activity metrics.

Produces daily darkpool confirmation signals by combining darkpool print data
with options flow to identify institutional cross-market activity.

Features:
  - darkpool_notional_1d      Total darkpool notional traded for this ticker today
  - darkpool_premium_ratio    Darkpool notional / total options premium (same ticker, same day)
  - darkpool_activity_zscore  Z-score of today's notional vs 20-day rolling stats

Usage:
    uv run python -m heber.features.pipelines.darkpool_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.darkpool_features --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 30  # Need 20 trading days + buffer for z-score rolling window

# Output columns for empty DataFrame
OUTPUT_COLUMNS = [
    "instrument_key",
    "symbol",
    "date",
    "ts_event",
    "ts_available",
    "darkpool_notional_1d",
    "darkpool_premium_ratio",
    "darkpool_activity_zscore",
]


def compute_darkpool_features(
    darkpool: pd.DataFrame,
    flow_alerts: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-ticker daily darkpool confirmation features.

    Input:
        darkpool: Silver darkpool with columns: underlying, notional, ts_event
        flow_alerts: Silver flow_alerts with columns: underlying, premium, ts_event

    Output:
        One row per (instrument_key, date) with:
          - darkpool_notional_1d: total darkpool notional for this ticker on this day
          - darkpool_premium_ratio: darkpool notional / options premium (same ticker, same day)
          - darkpool_activity_zscore: z-score of today's notional vs 20-day rolling stats
    """
    if darkpool.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    dp = darkpool.copy()
    dp["ts_event"] = pd.to_datetime(dp["ts_event"], utc=True)

    # Extract underlying ticker -- darkpool Silver uses 'underlying' column
    if "underlying" not in dp.columns and "instrument_key" in dp.columns:
        dp["underlying"] = dp["instrument_key"]

    dp["notional"] = pd.to_numeric(dp.get("notional", 0), errors="coerce").fillna(0.0)
    dp["date"] = dp["ts_event"].dt.date

    # Step 1: Aggregate darkpool notional by (underlying, date)
    dp_daily = (
        dp.groupby(["underlying", "date"])
        .agg(
            darkpool_notional_1d=("notional", "sum"),
        )
        .reset_index()
    )

    # Step 2: Aggregate options premium by (underlying, date) from flow_alerts
    if not flow_alerts.empty:
        fa = flow_alerts.copy()
        fa["ts_event"] = pd.to_datetime(fa["ts_event"], utc=True)
        fa["premium"] = pd.to_numeric(fa.get("premium", 0), errors="coerce").fillna(0.0)
        fa["date"] = fa["ts_event"].dt.date

        fa_daily = (
            fa.groupby(["underlying", "date"])
            .agg(
                daily_premium=("premium", "sum"),
            )
            .reset_index()
        )
    else:
        fa_daily = pd.DataFrame(columns=["underlying", "date", "daily_premium"])

    # Step 3: Merge darkpool daily with options premium daily
    merged = dp_daily.merge(fa_daily, on=["underlying", "date"], how="left")
    merged["daily_premium"] = pd.to_numeric(merged["daily_premium"], errors="coerce").fillna(0.0)

    # Step 4: Compute darkpool_premium_ratio
    # Avoid division by zero -- if no premium, ratio is NaN
    merged["darkpool_premium_ratio"] = np.where(
        merged["daily_premium"] > 0,
        merged["darkpool_notional_1d"] / merged["daily_premium"],
        np.nan,
    )

    # Step 5: Compute 20-day rolling z-score per ticker
    merged = merged.sort_values(["underlying", "date"])

    zscore_parts: list[pd.DataFrame] = []
    for _ticker, group in merged.groupby("underlying"):
        g = group.copy()
        rolling_mean = g["darkpool_notional_1d"].rolling(window=20, min_periods=20).mean()
        rolling_std = g["darkpool_notional_1d"].rolling(window=20, min_periods=20).std()

        g["darkpool_activity_zscore"] = np.where(
            rolling_std > 0,
            (g["darkpool_notional_1d"] - rolling_mean) / rolling_std,
            0.0,
        )

        # Mark rows where we don't have enough history as NaN
        insufficient_mask = rolling_mean.isna()
        g.loc[insufficient_mask, "darkpool_activity_zscore"] = np.nan

        zscore_parts.append(g)

    if not zscore_parts:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    result = pd.concat(zscore_parts, ignore_index=True)

    # Step 6: Add Gold metadata columns
    result["symbol"] = result["underlying"]
    result["instrument_key"] = result["underlying"].apply(lambda s: s if ":" in str(s) else f"equity:{s}")
    result["ts_event"] = pd.to_datetime(result["date"], utc=True)
    result["ts_available"] = datetime.now(UTC)

    # Select final columns
    output_cols = [
        "instrument_key",
        "symbol",
        "date",
        "ts_event",
        "ts_available",
        "darkpool_notional_1d",
        "darkpool_premium_ratio",
        "darkpool_activity_zscore",
    ]
    return result[output_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class DarkpoolPipeline:
    """Pipeline to compute per-ticker daily darkpool confirmation features."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "watch",
        version: str = "v1",
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version

    def _read_daily_source_aggregates(
        self,
        dataset: str,
        metric: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Read one Silver date partition at a time and retain daily ticker totals.

        The 20-day z-score only consumes daily per-underlying values. Reducing
        each date immediately prevents a high-volume source frame from staying
        resident for the full lookback window.
        """
        daily_frames: list[pd.DataFrame] = []
        current_day = start.date()
        final_day = end.date()

        while current_day <= final_day:
            day_start = datetime(current_day.year, current_day.month, current_day.day, tzinfo=UTC)
            day_end = day_start + timedelta(days=1) - timedelta(microseconds=1)
            frame = self.reader.read_silver(
                dataset,
                time_range=(day_start, day_end),
                columns=["underlying", metric, "ts_event"],
                prune_by_dt=True,
            )
            if not frame.empty:
                frame = frame.copy()
                frame["ts_event"] = pd.to_datetime(frame["ts_event"], utc=True)
                frame[metric] = pd.to_numeric(frame[metric], errors="coerce").fillna(0.0)
                frame["date"] = frame["ts_event"].dt.date
                daily = frame.groupby(["underlying", "date"], as_index=False)[metric].sum()
                daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)
                daily_frames.append(daily[["underlying", metric, "ts_event"]])
            current_day += timedelta(days=1)

        if not daily_frames:
            return pd.DataFrame(columns=["underlying", metric, "ts_event"])
        return pd.concat(daily_frames, ignore_index=True)

    def run(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the darkpool confirmation pipeline.

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

        gold_name = "darkpool_features"

        # Read Silver darkpool with lookback for rolling window
        read_start = start_date - timedelta(days=LOOKBACK_DAYS)

        logger.info(
            "Loading Silver darkpool",
            read_start=read_start.isoformat(),
            end=end_date.isoformat(),
        )
        darkpool = self._read_daily_source_aggregates("darkpool", "notional", read_start, end_date)
        logger.info("Loaded darkpool daily aggregates", rows=len(darkpool))

        logger.info(
            "Loading Silver flow_alerts for premium data",
            read_start=read_start.isoformat(),
            end=end_date.isoformat(),
        )
        flow_alerts = self._read_daily_source_aggregates("flow_alerts", "premium", read_start, end_date)
        logger.info("Loaded flow_alerts daily aggregates", rows=len(flow_alerts))

        if darkpool.empty:
            logger.warning("No darkpool data found")
            return {gold_name: {"status": "no_data", "rows": 0}}

        # Compute features
        logger.info("Computing darkpool features")
        df = compute_darkpool_features(darkpool, flow_alerts)

        if df.empty:
            logger.warning("No darkpool features computed")
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
    parser = argparse.ArgumentParser(description="Compute per-ticker daily darkpool confirmation features")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = DarkpoolPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("DARKPOOL FEATURES PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
