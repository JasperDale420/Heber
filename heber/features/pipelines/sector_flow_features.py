"""Sector Flow Context pipeline -- per-alert sector alignment enrichment.

For each flow alert, looks up the sector sentiment at the time of the alert and
computes how well the alert's direction agrees with the broader sector flow.

Features (all backward-looking, no leakage):
  - sector_flow_alignment    Float -1 to 1: bullish alert + positive sector => 1.0,
                             opposite => -1.0, neutral sector => 0.0
  - sector_call_put_ratio    The sector's call/put ratio at the time of the alert

Usage:
    uv run python -m heber.features.pipelines.sector_flow_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.sector_flow_features --start 2026-01-01 --end 2026-03-10 --dry-run
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

LOOKBACK_DAYS = 5  # Extra history for sector_tide snapshots preceding alerts

# Output columns for empty DataFrame
OUTPUT_COLUMNS = [
    "alert_id",
    "instrument_key",
    "ts_event",
    "ts_available",
    "sector_flow_alignment",
    "sector_call_put_ratio",
]

# ---------------------------------------------------------------------------
# Ticker -> GICS Sector mapping (top ~50 tickers)
# ---------------------------------------------------------------------------

SECTOR_MAP: dict[str, str] = {
    # Technology
    "AAPL": "Information Technology",
    "MSFT": "Information Technology",
    "NVDA": "Information Technology",
    "AVGO": "Information Technology",
    "AMD": "Information Technology",
    "INTC": "Information Technology",
    "CRM": "Information Technology",
    "ORCL": "Information Technology",
    "ADBE": "Information Technology",
    "CSCO": "Information Technology",
    "MU": "Information Technology",
    "QCOM": "Information Technology",
    # Communication Services
    "GOOGL": "Communication Services",
    "GOOG": "Communication Services",
    "META": "Communication Services",
    "NFLX": "Communication Services",
    "DIS": "Communication Services",
    # Consumer Discretionary
    "AMZN": "Consumer Discretionary",
    "TSLA": "Consumer Discretionary",
    "HD": "Consumer Discretionary",
    "NKE": "Consumer Discretionary",
    "SBUX": "Consumer Discretionary",
    "MCD": "Consumer Discretionary",
    # Financials
    "JPM": "Financials",
    "BAC": "Financials",
    "GS": "Financials",
    "MS": "Financials",
    "WFC": "Financials",
    "C": "Financials",
    "V": "Financials",
    "MA": "Financials",
    # Health Care
    "UNH": "Health Care",
    "JNJ": "Health Care",
    "PFE": "Health Care",
    "ABBV": "Health Care",
    "MRK": "Health Care",
    "LLY": "Health Care",
    # Energy
    "XOM": "Energy",
    "CVX": "Energy",
    "COP": "Energy",
    "SLB": "Energy",
    # Industrials
    "BA": "Industrials",
    "CAT": "Industrials",
    "UPS": "Industrials",
    "RTX": "Industrials",
    # Consumer Staples
    "PG": "Consumer Staples",
    "KO": "Consumer Staples",
    "PEP": "Consumer Staples",
    "WMT": "Consumer Staples",
    "COST": "Consumer Staples",
    # ETFs mapped to broad market
    "SPY": "Broad Market",
    "QQQ": "Information Technology",
    "IWM": "Broad Market",
}


def get_sector(ticker: str) -> str:
    """Return the GICS sector for a ticker, or 'Unknown' if not mapped."""
    return SECTOR_MAP.get(ticker, "Unknown")


def compute_sector_flow_features(
    flow_alerts: pd.DataFrame,
    sector_tide: pd.DataFrame,
) -> pd.DataFrame:
    """Compute per-alert sector flow alignment features.

    For each alert, finds the most recent sector_tide snapshot for that ticker's
    sector BEFORE the alert time, then computes alignment between the alert
    direction and the sector sentiment.

    Input:
        flow_alerts: Silver flow_alerts with columns: event_id, underlying, ts_event, put_call
        sector_tide: Silver sector_tide with columns: sector, sentiment, call_put_ratio, ts_event

    Output:
        One row per alert with: alert_id, instrument_key, ts_event, ts_available,
        sector_flow_alignment, sector_call_put_ratio
    """
    if flow_alerts.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    alerts = flow_alerts.copy()
    alerts["ts_event"] = pd.to_datetime(alerts["ts_event"], utc=True)
    alerts = alerts.sort_values("ts_event").reset_index(drop=True)

    # Map each alert's underlying to its sector
    alerts["sector"] = alerts["underlying"].map(get_sector)

    # Prepare sector_tide lookup
    if sector_tide.empty:
        # No sector data -- all features are neutral/NaN
        return _build_neutral_result(alerts)

    tide = sector_tide.copy()
    tide["ts_event"] = pd.to_datetime(tide["ts_event"], utc=True)
    tide = tide.sort_values("ts_event").reset_index(drop=True)

    # Convert sentiment: string labels → numeric (+1 bullish, -1 bearish, 0 neutral)
    sentiment_raw = tide.get("sentiment")
    if sentiment_raw is not None and sentiment_raw.dtype == object:
        sentiment_map = {"bullish": 1.0, "bearish": -1.0, "neutral": 0.0}
        tide["sentiment"] = sentiment_raw.str.lower().str.strip().map(sentiment_map).fillna(0.0)
    else:
        tide["sentiment"] = pd.to_numeric(tide.get("sentiment", 0), errors="coerce").fillna(0.0)
    tide["call_put_ratio"] = pd.to_numeric(tide.get("call_put_ratio", 0), errors="coerce").fillna(0.0)

    # For each alert, find the most recent sector_tide snapshot BEFORE the alert time
    # using merge_asof (efficient sorted-merge approach)
    # We need to match on sector and find the latest ts_event <= alert ts_event
    alignment_values = np.zeros(len(alerts), dtype=np.float64)
    cpr_values = np.full(len(alerts), np.nan, dtype=np.float64)

    for sector_name, sector_group in alerts.groupby("sector", sort=False):
        if sector_name == "Unknown":
            # Unknown sector: leave as neutral (0.0 alignment, NaN ratio)
            continue

        sector_tide_rows = tide[tide["sector"] == sector_name].sort_values("ts_event")
        if sector_tide_rows.empty:
            continue

        sector_ts = sector_tide_rows["ts_event"].values.astype("datetime64[ns]")
        sector_sentiments = sector_tide_rows["sentiment"].values
        sector_cprs = sector_tide_rows["call_put_ratio"].values

        # Pre-convert alert timestamps to numpy datetime64 for searchsorted compatibility
        alert_ts_np = alerts.loc[sector_group.index, "ts_event"].values.astype("datetime64[ns]")
        alert_ts_map = dict(zip(sector_group.index, alert_ts_np, strict=True))

        for idx in sector_group.index:
            alert_ts = alert_ts_map[idx]
            put_call = alerts.at[idx, "put_call"]

            # Binary search for latest sector_tide before alert time
            pos = np.searchsorted(sector_ts, alert_ts, side="right") - 1
            if pos < 0:
                # No sector data before this alert
                continue

            sentiment = float(sector_sentiments[pos])
            cpr = float(sector_cprs[pos])

            # Determine alert direction: CALL/C = bullish (+1), PUT/P = bearish (-1)
            is_bullish = str(put_call).upper() in ("C", "CALL")

            # Compute alignment
            alignment = _compute_alignment(is_bullish, sentiment)

            alignment_values[idx] = alignment
            cpr_values[idx] = cpr

    result = pd.DataFrame(
        {
            "alert_id": alerts["event_id"].values,
            "instrument_key": alerts["underlying"].values,
            "ts_event": alerts["ts_event"].values,
            "ts_available": alerts["ts_event"].values,
            "sector_flow_alignment": alignment_values,
            "sector_call_put_ratio": cpr_values,
        }
    )

    result["ts_event"] = pd.to_datetime(result["ts_event"], utc=True)
    result["ts_available"] = pd.to_datetime(result["ts_available"], utc=True)

    return result


def _compute_alignment(is_bullish: bool, sentiment: float) -> float:
    """Compute alignment score between alert direction and sector sentiment.

    Returns:
        Float in [-1, 1]:
          +1.0 if alert direction fully agrees with sector sentiment
          -1.0 if alert direction fully opposes sector sentiment
           0.0 if sector sentiment is neutral (zero)
    """
    if sentiment == 0.0:
        return 0.0

    sector_is_positive = sentiment > 0
    if is_bullish and sector_is_positive:
        return 1.0
    elif not is_bullish and not sector_is_positive:
        return 1.0
    else:
        return -1.0


def _build_neutral_result(alerts: pd.DataFrame) -> pd.DataFrame:
    """Build result with neutral alignment when no sector data is available."""
    return pd.DataFrame(
        {
            "alert_id": alerts["event_id"].values,
            "instrument_key": alerts["underlying"].values,
            "ts_event": alerts["ts_event"].values,
            "ts_available": alerts["ts_event"].values,
            "sector_flow_alignment": np.zeros(len(alerts), dtype=np.float64),
            "sector_call_put_ratio": np.full(len(alerts), np.nan, dtype=np.float64),
        }
    )


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class SectorFlowPipeline:
    """Pipeline to compute per-alert sector flow alignment from Silver data."""

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

        # Load with lookback for sector tide context
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

        logger.info(
            "Loading Silver sector_tide",
            load_start=load_start.isoformat(),
            end=end_date.isoformat(),
        )
        sector_tide = self.reader.read_silver(
            "sector_tide",
            time_range=(load_start, end_date),
        )
        logger.info("Loaded sector_tide", rows=len(sector_tide))

        if flow_alerts.empty:
            logger.warning("No flow alerts found")
            return {"sector_flow_features": {"status": "no_data", "rows": 0}}

        # Compute features
        logger.info("Computing sector flow features")
        df = compute_sector_flow_features(flow_alerts, sector_tide)

        if df.empty:
            logger.warning("No features computed")
            return {"sector_flow_features": {"status": "no_data", "rows": 0}}

        # Filter to requested date range only (exclude lookback rows)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if df.empty:
            logger.warning("No features in requested date range after filtering")
            return {"sector_flow_features": {"status": "no_data", "rows": 0}}

        logger.info("Features computed", rows=len(df))

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="sector_flow_features",
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote to Gold", dataset="sector_flow_features", rows=len(df), path=str(output_path))
        else:
            logger.info("Dry run -- would write", dataset="sector_flow_features", rows=len(df))
            output_path = None

        return {
            "sector_flow_features": {
                "status": "success",
                "rows": len(df),
                "path": str(output_path) if output_path else None,
            }
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute per-alert sector flow alignment features")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = SectorFlowPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("SECTOR FLOW FEATURES PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
        if info.get("path"):
            print(f"  Output: {info['path']}")
    print()


if __name__ == "__main__":
    main()
