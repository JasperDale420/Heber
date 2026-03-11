"""Flow Context Features pipeline — compute per-alert clustering/agreement signals.

For each flow alert, measures whether the alert is part of a cluster of similar activity
on the same ticker, which is a strong signal of informed trading.

Features (all backward-looking, no leakage):
  - same_ticker_alerts_1h      Count of other alerts on the same underlying in (t-1h, t)
  - directional_agreement_4h   Fraction of same-ticker alerts in (t-4h, t) agreeing in put_call
  - repeat_ticker_days_5d      Distinct trading days in (t-5d, t) with at least one same-ticker alert

Usage:
    uv run python -m heber.features.pipelines.flow_context_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.flow_context_features --start 2026-01-01 --end 2026-03-10 --dry-run
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

LOOKBACK_DAYS = 5  # Extra history needed for 5-day repeat ticker window

# Output columns for empty DataFrame
OUTPUT_COLUMNS = [
    "alert_id",
    "instrument_key",
    "ts_event",
    "ts_available",
    "same_ticker_alerts_1h",
    "directional_agreement_4h",
    "repeat_ticker_days_5d",
]


def compute_flow_context_features(flow_alerts: pd.DataFrame) -> pd.DataFrame:
    """Compute per-alert flow context features.

    For each alert, looks backward at recent alerts on the same ticker to measure
    clustering, directional agreement, and repeat activity.

    Input: Silver flow_alerts sorted by ts_event, with: event_id, underlying, ts_event, put_call
    Output: One row per alert with: alert_id, instrument_key, ts_event, ts_available,
            same_ticker_alerts_1h, directional_agreement_4h, repeat_ticker_days_5d
    """
    if flow_alerts.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = flow_alerts.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df = df.sort_values("ts_event").reset_index(drop=True)

    # Group by underlying for efficient per-ticker computation
    # We process each ticker group independently — alerts for AAPL never affect MSFT.
    grouped = df.groupby("underlying", sort=False)

    # Build results per ticker, then assemble
    ticker_results: dict[str, tuple[list[int], list[float], list[int]]] = {}

    for ticker, group in grouped:
        group = group.sort_values("ts_event").reset_index(drop=True)
        ts_events = group["ts_event"].values  # numpy datetime64
        put_calls = group["put_call"].values
        n = len(group)

        t_same_1h: list[int] = []
        t_dir_4h: list[float] = []
        t_repeat_5d: list[int] = []

        for i in range(n):
            t_i = ts_events[i]

            # --- same_ticker_alerts_1h ---
            # Count alerts in open interval (t_i - 1h, t_i) exclusive on both ends
            # We use strictly less than t_i (no self-count) and strictly greater than t_i - 1h
            count_1h = 0
            cutoff_1h = t_i - np.timedelta64(1, "h")
            j = i - 1
            while j >= 0 and ts_events[j] > cutoff_1h:
                # ts_events[j] is in (cutoff_1h, t_i) — strictly between
                if ts_events[j] < t_i:
                    count_1h += 1
                j -= 1
            t_same_1h.append(count_1h)

            # --- directional_agreement_4h ---
            # Among alerts in open interval (t_i - 4h, t_i), fraction matching put_call of alert i
            cutoff_4h = t_i - np.timedelta64(4, "h")
            current_pc = put_calls[i]
            agree = 0
            total_4h = 0
            j = i - 1
            while j >= 0 and ts_events[j] > cutoff_4h:
                if ts_events[j] < t_i:
                    total_4h += 1
                    if put_calls[j] == current_pc:
                        agree += 1
                j -= 1
            if total_4h == 0:
                t_dir_4h.append(0.5)  # default when no prior alerts
            else:
                t_dir_4h.append(agree / total_4h)

            # --- repeat_ticker_days_5d ---
            # Count distinct calendar dates in open interval (t_i - 5d, t_i) for same underlying
            cutoff_5d = t_i - np.timedelta64(5, "D")
            seen_dates: set[object] = set()
            j = i - 1
            while j >= 0 and ts_events[j] > cutoff_5d:
                if ts_events[j] < t_i:
                    # Extract date from numpy datetime64
                    day = pd.Timestamp(ts_events[j]).date()
                    seen_dates.add(day)
                j -= 1
            t_repeat_5d.append(len(seen_dates))

        ticker_results[ticker] = (t_same_1h, t_dir_4h, t_repeat_5d)

    # Reassemble results in original order
    # We need to map each row in df to its ticker group result
    result_same_1h = np.empty(len(df), dtype=np.int64)
    result_dir_4h = np.empty(len(df), dtype=np.float64)
    result_repeat_5d = np.empty(len(df), dtype=np.int64)

    for ticker, group in grouped:
        group_sorted = group.sort_values("ts_event").reset_index()
        original_indices = group_sorted["index"].values
        t_same_1h, t_dir_4h, t_repeat_5d = ticker_results[ticker]
        for local_i, global_i in enumerate(original_indices):
            result_same_1h[global_i] = t_same_1h[local_i]
            result_dir_4h[global_i] = t_dir_4h[local_i]
            result_repeat_5d[global_i] = t_repeat_5d[local_i]

    result = pd.DataFrame(
        {
            "alert_id": df["event_id"].values,
            "instrument_key": df["underlying"].values,
            "ts_event": df["ts_event"].values,
            "ts_available": df["ts_event"].values,
            "same_ticker_alerts_1h": result_same_1h,
            "directional_agreement_4h": result_dir_4h,
            "repeat_ticker_days_5d": result_repeat_5d,
        }
    )

    # Ensure proper types
    result["ts_event"] = pd.to_datetime(result["ts_event"], utc=True)
    result["ts_available"] = pd.to_datetime(result["ts_available"], utc=True)
    result["same_ticker_alerts_1h"] = result["same_ticker_alerts_1h"].astype(np.int64)
    result["repeat_ticker_days_5d"] = result["repeat_ticker_days_5d"].astype(np.int64)

    return result


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class FlowContextPipeline:
    """Pipeline to compute per-alert flow context features from Silver flow_alerts."""

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
        """Run the pipeline.

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

        # Load with lookback for 5-day window
        load_start = start_date - timedelta(days=LOOKBACK_DAYS)

        logger.info(
            "Loading Silver flow_alerts",
            load_start=load_start.isoformat(),
            end=end_date.isoformat(),
        )
        flow_alerts = self.reader.read_silver(
            "flow_alerts",
            time_range=(load_start, end_date),
        )
        logger.info("Loaded flow_alerts", rows=len(flow_alerts))

        if flow_alerts.empty:
            logger.warning("No flow alerts found")
            return {"status": "no_data", "rows": 0}

        # Compute features (includes lookback data for context)
        logger.info("Computing flow context features")
        df = compute_flow_context_features(flow_alerts)

        if df.empty:
            logger.warning("No features computed")
            return {"status": "no_data", "rows": 0}

        # Filter to requested date range only (exclude lookback rows)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df = df[df["ts_event"] >= pd.Timestamp(start_date, tz="UTC")]
        df = df[df["ts_event"] <= pd.Timestamp(end_date, tz="UTC")]

        if df.empty:
            logger.warning("No features in requested date range after filtering")
            return {"status": "no_data", "rows": 0}

        logger.info("Features computed", rows=len(df))

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="flow_context_features",
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote to Gold", dataset="flow_context_features", rows=len(df), path=str(output_path))
        else:
            logger.info("Dry run — would write", dataset="flow_context_features", rows=len(df))
            output_path = None

        return {
            "status": "success",
            "rows": len(df),
            "path": str(output_path) if output_path else None,
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute per-alert flow context features from Silver flow_alerts")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = FlowContextPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("FLOW CONTEXT FEATURES PIPELINE RESULTS")
    print("=" * 60)
    status = stats.get("status", "unknown")
    rows = stats.get("rows", 0)
    print(f"  flow_context_features: {status} ({rows} rows)")
    if stats.get("path"):
        print(f"  Output: {stats['path']}")
    print()


if __name__ == "__main__":
    main()
