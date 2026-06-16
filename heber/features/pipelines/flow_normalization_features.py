"""Flow normalization feature pipeline -- compute Gold datasets from Silver flow_alerts.

Produces per-ticker daily flow normalization metrics:
  - adv_premium_20d   (20-day average daily total premium)
  - adv_volume_20d    (20-day average daily total option volume)
  - adv_oi_20d        (20-day average daily total open interest)

These rolling averages allow normalizing individual alert premiums/volumes by the
ticker's typical activity level, distinguishing genuinely unusual flow from normally
active tickers.

Usage:
    uv run python -m heber.features.pipelines.flow_normalization_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.flow_normalization_features --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 30  # Need 20 trading days + buffer for rolling window


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


def compute_flow_normalization_features(flow_alerts: pd.DataFrame) -> pd.DataFrame:
    """Compute per-ticker daily flow normalization metrics.

    Input: Silver flow_alerts with columns: underlying, ts_event, premium, total_size, open_interest
    Output: One row per (instrument_key, date) with:
      - adv_premium_20d: 20-day rolling avg daily premium
      - adv_volume_20d: 20-day rolling avg daily option volume (total_size)
      - adv_oi_20d: 20-day rolling avg daily open interest
    """
    if flow_alerts.empty:
        return pd.DataFrame()

    df = flow_alerts.copy()

    # --- Step 1: Parse timestamps and extract date ---
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    # --- Step 2: Coerce numeric columns ---
    df["premium"] = pd.to_numeric(df.get("premium", 0), errors="coerce").fillna(0)
    df["total_size"] = pd.to_numeric(df.get("total_size", 0), errors="coerce").fillna(0)

    has_oi = "open_interest" in df.columns
    if has_oi:
        df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)

    # --- Step 3: Group by (underlying, date) and sum to get daily totals ---
    agg_spec: dict[str, tuple[str, str]] = {
        "daily_premium": ("premium", "sum"),
        "daily_volume": ("total_size", "sum"),
    }
    if has_oi:
        agg_spec["daily_oi"] = ("open_interest", "sum")

    daily = df.groupby(["underlying", "date"]).agg(**agg_spec).reset_index()

    if not has_oi:
        daily["daily_oi"] = float("nan")

    # --- Step 4: Sort and compute 20-day rolling mean per underlying ---
    daily = daily.sort_values(["underlying", "date"])

    rolling_parts: list[pd.DataFrame] = []
    for _ticker, group in daily.groupby("underlying"):
        g = group.copy()
        g["adv_premium_20d"] = g["daily_premium"].rolling(window=20, min_periods=20).mean()
        g["adv_volume_20d"] = g["daily_volume"].rolling(window=20, min_periods=20).mean()
        if has_oi:
            g["adv_oi_20d"] = g["daily_oi"].rolling(window=20, min_periods=20).mean()
        else:
            g["adv_oi_20d"] = float("nan")
        rolling_parts.append(g)

    if not rolling_parts:
        return pd.DataFrame()

    result = pd.concat(rolling_parts, ignore_index=True)

    # --- Step 5: Drop NaN rows (first 19 days per ticker won't have enough data) ---
    result = result.dropna(subset=["adv_premium_20d", "adv_volume_20d"])

    if result.empty:
        return pd.DataFrame()

    # --- Step 6: Rename and add Gold metadata columns ---
    result = result.rename(columns={"underlying": "symbol"})
    result["ts_event"] = pd.to_datetime(result["date"], utc=True)

    result = _ensure_instrument_key(result)
    result = _ensure_ts_available(result)

    # Select final columns
    output_cols = [
        "instrument_key",
        "symbol",
        "ts_event",
        "ts_available",
        "adv_premium_20d",
        "adv_volume_20d",
        "adv_oi_20d",
    ]
    return result[output_cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class FlowNormalizationPipeline:
    """Pipeline to compute flow normalization Gold features from Silver flow_alerts."""

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
        """Run the flow normalization pipeline.

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

        gold_name = "flow_normalization_features"

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
        logger.info("Computing flow normalization features")
        df = compute_flow_normalization_features(flow_alerts)

        if df.empty:
            logger.warning("No flow normalization features computed")
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
    parser = argparse.ArgumentParser(description="Compute flow normalization Gold features from Silver flow_alerts")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = FlowNormalizationPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("FLOW NORMALIZATION PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
