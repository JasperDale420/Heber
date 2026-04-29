"""Ticker Base Rates Pipeline — compute per-ticker daily base rate statistics from historical alert labels.

Creates a feedback loop: past label outcomes inform current predictions by capturing
which tickers are historically predictable.

Produces:
  - ticker_base_rates (per ticker per day: win rate, alert frequency, flow predictability)

Usage:
    uv run python -m heber.features.pipelines.ticker_base_rates --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.ticker_base_rates --start 2026-01-01 --end 2026-03-10 --dry-run
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

LOOKBACK_DAYS = 90  # Rolling window for base rate computation

EXPECTED_OUTPUT_COLUMNS = [
    "instrument_key",
    "ts_event",
    "ts_available",
    "ticker_win_rate_90d",
    "ticker_alert_frequency",
    "ticker_flow_predictability",
]


def _normalize_instrument_key(value: Any) -> str:
    """Convert raw ticker symbols to canonical equity instrument keys."""
    s = str(value).strip()
    if ":" in s:
        return s
    return f"equity:{s}"


def _extract_ticker_column(df: pd.DataFrame) -> pd.Series:
    """Extract and normalize the ticker identifier from the labels DataFrame.

    Ticker base rates are per-underlying statistics (e.g., "AAPL's 90-day win
    rate"), so we want one identifier per underlying. The labels source
    (``labels_alert_barriers``) keys ``instrument_key`` to the specific
    option contract that fired the alert (``option:OCC:AAPL260515C00190000``),
    not the underlying — so preferring ``instrument_key`` produced one row
    per option contract, which (a) duplicated stats across thousands of
    strike/expiry combinations and (b) made downstream consumers (Orion ML
    scorer's ``EQUITY_TICKER_RATES_FEATURES``) unable to find the rates
    when filtering by ``equity:AAPL``.

    Prefer ``underlying`` (the ticker symbol) and only fall back to
    ``instrument_key`` when ``underlying`` is missing entirely.
    """
    has_ik = "instrument_key" in df.columns
    has_ul = "underlying" in df.columns

    if has_ul and has_ik:
        # Coalesce: prefer underlying, fall back to instrument_key for any null underlying.
        raw = df["underlying"].fillna(df["instrument_key"])
    elif has_ul:
        raw = df["underlying"]
    elif has_ik:
        raw = df["instrument_key"]
    else:
        raise ValueError("Labels DataFrame must contain 'instrument_key' or 'underlying' column")

    return raw.map(_normalize_instrument_key)


def compute_ticker_base_rates(
    labels: pd.DataFrame,
    window_days: int = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Compute per-ticker base rate statistics from historical alert labels.

    For each (instrument_key, date) pair in the input, computes rolling statistics
    over the trailing ``window_days`` calendar days:

    - ``ticker_win_rate_90d``: fraction of alerts where ``hit_tp_first == 1``
    - ``ticker_alert_frequency``: count of alerts in the lookback window
    - ``ticker_flow_predictability``: ``abs(win_rate - 0.5) * 2``, scaled to [0,1]

    Parameters
    ----------
    labels:
        Gold ``labels_alert_barriers`` DataFrame. Must contain:
        - ``underlying`` or ``instrument_key`` (ticker identifier)
        - ``ts_event`` (alert timestamp)
        - ``hit_tp_first`` (0 or 1 label)
    window_days:
        Number of calendar days for the rolling lookback window (default: 90).

    Returns
    -------
    pd.DataFrame
        One row per (instrument_key, date) with base rate features.
        Empty DataFrame with correct schema when input is empty.
    """
    empty_result = pd.DataFrame(columns=EXPECTED_OUTPUT_COLUMNS)

    if labels.empty:
        return empty_result

    # Validate required columns
    if "ts_event" not in labels.columns or "hit_tp_first" not in labels.columns:
        logger.warning("ticker_base_rates_missing_columns", columns=list(labels.columns))
        return empty_result

    df = labels.copy()

    # Normalize ticker column
    df["instrument_key"] = _extract_ticker_column(df)

    # Parse timestamps and extract date
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["hit_tp_first"] = pd.to_numeric(df["hit_tp_first"], errors="coerce")

    # Drop rows with missing labels
    df = df.dropna(subset=["hit_tp_first"])
    if df.empty:
        return empty_result

    # Get unique (instrument_key, ts_event) combinations to compute features for
    # We need to compute rolling stats for each ticker on each date it has an alert
    df = df.sort_values(["instrument_key", "ts_event"]).reset_index(drop=True)

    # Compute rolling stats per ticker using a lookback window
    results = []
    for ticker, group in df.groupby("instrument_key"):
        group = group.sort_values("ts_event").reset_index(drop=True)

        # For each row, compute stats using all alerts within the lookback window
        for _idx, row in group.iterrows():
            current_ts = row["ts_event"]
            window_start = current_ts - timedelta(days=window_days)

            # All alerts for this ticker strictly before the current alert (no self-leakage)
            mask = (group["ts_event"] >= window_start) & (group["ts_event"] < current_ts)
            window_data = group[mask]

            count = len(window_data)
            if count == 0:
                win_rate = np.nan
                predictability = np.nan
            else:
                win_rate = window_data["hit_tp_first"].mean()
                predictability = abs(win_rate - 0.5) * 2

            results.append(
                {
                    "instrument_key": ticker,
                    "ts_event": current_ts,
                    "ticker_win_rate_90d": win_rate,
                    "ticker_alert_frequency": count,
                    "ticker_flow_predictability": predictability,
                }
            )

    if not results:
        return empty_result

    out = pd.DataFrame(results)

    # Deduplicate: if multiple alerts on the same (ticker, date), keep only the
    # last computed row (which includes all alerts up to that point on that date).
    out = out.sort_values(["instrument_key", "ts_event"])
    out = out.drop_duplicates(subset=["instrument_key", "ts_event"], keep="last")

    # Ensure integer type for frequency
    out["ticker_alert_frequency"] = out["ticker_alert_frequency"].astype(np.int64)

    # Add ts_available (zero-leakage: available after the event date)
    out["ts_available"] = datetime.now(UTC)

    out = out[EXPECTED_OUTPUT_COLUMNS].reset_index(drop=True)

    logger.info(
        "ticker_base_rates_computed",
        rows=len(out),
        tickers=out["instrument_key"].nunique(),
    )
    return out


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class TickerBaseRatesPipeline:
    """Pipeline to compute ticker base rate Gold features from Gold alert labels."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "watch",
        version: str = "v1",
        labels_dataset: str = "labels_alert_barriers",
        labels_project: str = "watch",
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version
        self.labels_dataset = labels_dataset
        self.labels_project = labels_project

    def run(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the pipeline for the specified date range.

        Args:
            start_date: Start of output date range.
            end_date: End of output date range.
            dry_run: If True, compute but do not write.

        Returns:
            Dict with pipeline stats.
        """
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # Extend to end of day if midnight
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        logger.info(
            "ticker_base_rates_pipeline_start",
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )

        # Load labels with extra lookback for the rolling window
        label_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info("Loading Gold alert labels", label_start=label_start.isoformat(), end=end_date.isoformat())

        labels = self.reader.read_gold(
            dataset=self.labels_dataset,
            project=self.labels_project,
            time_range=(label_start, end_date),
        )

        if labels.empty:
            logger.warning("No alert labels found in date range")
            return {"status": "no_data", "rows": 0}

        logger.info("Loaded alert labels", rows=len(labels))

        # Compute base rates
        df = compute_ticker_base_rates(labels, window_days=LOOKBACK_DAYS)

        if df.empty:
            logger.warning("No base rates computed")
            return {"status": "no_data", "rows": 0}

        # Filter to requested date range only (exclude lookback)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if df.empty:
            logger.warning("No base rates in requested date range after filtering")
            return {"status": "no_data", "rows": 0}

        # Write to Gold
        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="ticker_base_rates",
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote ticker base rates to Gold", rows=len(df), path=str(output_path))
        else:
            logger.info("Dry run — would write ticker base rates", rows=len(df))
            output_path = None

        return {
            "status": "success",
            "rows": len(df),
            "tickers": int(df["instrument_key"].nunique()),
            "path": str(output_path) if output_path else None,
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute ticker base rate Gold features from alert labels")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")
    parser.add_argument("--labels-dataset", default="labels_alert_barriers", help="Source labels dataset")
    parser.add_argument("--labels-project", default="watch", help="Source labels project")

    args = parser.parse_args()

    pipeline = TickerBaseRatesPipeline(
        project=args.project,
        version=args.version,
        labels_dataset=args.labels_dataset,
        labels_project=args.labels_project,
    )
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("TICKER BASE RATES PIPELINE RESULTS")
    print("=" * 60)
    print(f"  Status: {stats.get('status', 'unknown')}")
    print(f"  Rows:   {stats.get('rows', 0)}")
    print(f"  Tickers: {stats.get('tickers', 0)}")
    if stats.get("path"):
        print(f"  Output: {stats['path']}")
    print()


if __name__ == "__main__":
    main()
