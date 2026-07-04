"""Flow toxicity (VPIN) feature pipeline -- compute Gold datasets from Silver flow_alerts.

Produces per-ticker daily flow toxicity metrics:
  - flow_toxicity_1d   (VPIN approximation: |ask_prem - bid_prem| / (ask_prem + bid_prem))
  - toxicity_acceleration  (today's toxicity minus 5-day rolling mean of toxicity)

VPIN (Volume-Synchronized Probability of Informed Trading) measures order-flow
imbalance.  Values near 1 indicate one-sided flow (likely informed traders);
values near 0 indicate balanced flow.

Usage:
    uv run python -m heber.features.pipelines.flow_toxicity_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.flow_toxicity_features --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.features.pipelines.base import ensure_instrument_key, ensure_ts_available
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 10  # Need 5 trading days for acceleration rolling window + buffer


def compute_vpin(ask_premium: float, bid_premium: float) -> float:
    """Compute VPIN for a single (ticker, date) observation.

    VPIN = |ask_premium - bid_premium| / (ask_premium + bid_premium)

    Returns NaN when both premiums are zero (no flow to measure).
    """
    total = ask_premium + bid_premium
    if total == 0:
        return float("nan")
    return abs(ask_premium - bid_premium) / total


def compute_flow_toxicity_features(flow_alerts: pd.DataFrame) -> pd.DataFrame:
    """Compute per-ticker daily flow toxicity (VPIN) metrics.

    Input: Silver flow_alerts with columns: underlying, ts_event,
           total_ask_side_prem, total_bid_side_prem
    Output: One row per (instrument_key, date) with:
      - flow_toxicity_1d: VPIN approximation (0-1 range)
      - toxicity_acceleration: today's toxicity minus 5-day rolling mean
    """
    if flow_alerts.empty:
        return pd.DataFrame()

    df = flow_alerts.copy()

    # --- Step 1: Parse timestamps and extract date ---
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    # --- Step 2: Coerce numeric columns ---
    df["total_ask_side_prem"] = pd.to_numeric(df.get("total_ask_side_prem", 0), errors="coerce").fillna(0)
    df["total_bid_side_prem"] = pd.to_numeric(df.get("total_bid_side_prem", 0), errors="coerce").fillna(0)

    # --- Step 3: Group by (underlying, date) and sum premiums ---
    daily = (
        df.groupby(["underlying", "date"])
        .agg(
            ask_sum=("total_ask_side_prem", "sum"),
            bid_sum=("total_bid_side_prem", "sum"),
        )
        .reset_index()
    )

    # --- Step 4: Compute VPIN per (ticker, date) ---
    total_flow = daily["ask_sum"] + daily["bid_sum"]
    daily["flow_toxicity_1d"] = np.where(
        total_flow > 0,
        (daily["ask_sum"] - daily["bid_sum"]).abs() / total_flow,
        np.nan,
    )

    # --- Step 5: Sort and compute toxicity_acceleration per underlying ---
    daily = daily.sort_values(["underlying", "date"])

    accel_parts: list[pd.DataFrame] = []
    for _ticker, group in daily.groupby("underlying"):
        g = group.copy()
        rolling_mean = g["flow_toxicity_1d"].rolling(window=5, min_periods=5).mean()
        g["toxicity_acceleration"] = g["flow_toxicity_1d"] - rolling_mean
        accel_parts.append(g)

    if not accel_parts:
        return pd.DataFrame()

    result = pd.concat(accel_parts, ignore_index=True)

    # --- Step 6: Rename and add Gold metadata columns ---
    result = result.rename(columns={"underlying": "symbol"})
    result["ts_event"] = pd.to_datetime(result["date"], utc=True)

    result = ensure_instrument_key(result)
    result = ensure_ts_available(result)

    # Select final columns
    output_cols = [
        "instrument_key",
        "symbol",
        "ts_event",
        "ts_available",
        "flow_toxicity_1d",
        "toxicity_acceleration",
    ]
    return result[output_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class FlowToxicityPipeline:
    """Pipeline to compute flow toxicity (VPIN) Gold features from Silver flow_alerts."""

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
        """Run the flow toxicity pipeline.

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

        gold_name = "flow_toxicity_features"

        # Read Silver flow_alerts with lookback for rolling window
        read_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info(
            "Loading Silver flow_alerts",
            read_start=read_start.isoformat(),
            end=end_date.isoformat(),
        )
        flow_alerts = self.reader.read_silver(
            "flow_alerts",
            instrument_type="option",
            time_range=(read_start, end_date),
        )
        logger.info("Loaded flow_alerts", rows=len(flow_alerts))

        # Compute features
        logger.info("Computing flow toxicity features")
        df = compute_flow_toxicity_features(flow_alerts)

        if df.empty:
            logger.warning("No flow toxicity features computed")
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
    parser = argparse.ArgumentParser(description="Compute flow toxicity (VPIN) Gold features from Silver flow_alerts")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = FlowToxicityPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("FLOW TOXICITY PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
