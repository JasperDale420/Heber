"""Market tide context feature pipelines -- compute Gold datasets from Silver market_tide data.

Produces market-level (one row per date) sentiment features:
  - market_sentiment_score   (net premium normalized: call-put / call+put, range -1 to 1)
  - market_premium_momentum  (5-day rate of change of sentiment score)

Usage:
    uv run python -m heber.features.pipelines.market_tide_context_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.market_tide_context_features --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import structlog

from heber.features.pipelines.base import ensure_market_instrument_key, ensure_ts_available, gold_dataset_result
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 10
INSTRUMENT_KEY = "market:tide_context"
MOMENTUM_WINDOW = 5
OUTPUT_DATASET = "market_tide_context_features"


# ---------------------------------------------------------------------------
# Market Sentiment Score
# ---------------------------------------------------------------------------


def compute_market_sentiment_score(market_tide_df: pd.DataFrame) -> pd.DataFrame:
    """Net premium normalized: (total_call_premium - total_put_premium) / (total_call_premium + total_put_premium).

    Market tide data arrives as hourly snapshots. This function:
    1. Resamples to daily by taking the LAST observation per date.
    2. Computes the sentiment score from call/put premiums.

    Args:
        market_tide_df: DataFrame with columns: ts_event, total_call_premium, total_put_premium.
                        One row per observation (hourly snapshots).

    Returns:
        DataFrame with columns: ts_event, market_sentiment_score.
        One row per date. Values range -1 to 1.
    """
    if market_tide_df.empty:
        return pd.DataFrame(columns=["ts_event", "market_sentiment_score"])

    df = market_tide_df.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["total_call_premium"] = pd.to_numeric(df["total_call_premium"], errors="coerce")
    df["total_put_premium"] = pd.to_numeric(df["total_put_premium"], errors="coerce")

    # Resample to daily: take last observation per date
    df["date"] = df["ts_event"].dt.date
    df = df.sort_values("ts_event")
    daily = df.groupby("date").last().reset_index()
    daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)

    # Compute sentiment score: (call - put) / (call + put)
    total = daily["total_call_premium"] + daily["total_put_premium"]
    net = daily["total_call_premium"] - daily["total_put_premium"]

    # Handle zero total premium: result is 0.0 (neutral)
    daily["market_sentiment_score"] = net / total
    daily.loc[total.isna() | (total == 0), "market_sentiment_score"] = 0.0

    return daily[["ts_event", "market_sentiment_score"]].copy()


def compute_market_premium_momentum(sentiment_series: pd.DataFrame, window: int = MOMENTUM_WINDOW) -> pd.DataFrame:
    """5-day rate of change of market_sentiment_score.

    Momentum = today's score - 5-day rolling mean of score.
    Captures shifting market sentiment direction.

    Args:
        sentiment_series: DataFrame with columns: ts_event, market_sentiment_score.
                          One row per date, sorted by date.
        window: Rolling window size (default 5 trading days).

    Returns:
        DataFrame with columns: ts_event, market_premium_momentum.
        One row per date.
    """
    if sentiment_series.empty:
        return pd.DataFrame(columns=["ts_event", "market_premium_momentum"])

    df = sentiment_series.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df = df.sort_values("ts_event")

    rolling_mean = df["market_sentiment_score"].rolling(window, min_periods=window).mean()
    df["market_premium_momentum"] = df["market_sentiment_score"] - rolling_mean

    return df[["ts_event", "market_premium_momentum"]].copy()


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class MarketTideContextPipeline:
    """Pipeline to compute market-level tide context Gold features from Silver market_tide data."""

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
        """Run the pipeline to compute market tide context features.

        Args:
            start_date: Start of date range.
            end_date: End of date range.
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

        component_stats: dict[str, dict[str, Any]] = {}

        # Load Silver market_tide with lookback for rolling windows
        read_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info(
            "Loading Silver market_tide",
            start=read_start.isoformat(),
            end=end_date.isoformat(),
        )

        try:
            market_tide = self.reader.read_silver("market_tide", time_range=(read_start, end_date))
        except Exception as exc:
            logger.error("Failed to load Silver market_tide", error=str(exc), exc_info=True)
            component_stats["market_sentiment_score"] = {"status": "error", "rows": 0, "error": str(exc)}
            component_stats["market_premium_momentum"] = {"status": "error", "rows": 0, "error": str(exc)}
            return {
                OUTPUT_DATASET: gold_dataset_result(
                    status="error",
                    rows=0,
                    error=str(exc),
                    components=component_stats,
                )
            }

        logger.info("Loaded Silver market_tide", rows=len(market_tide))

        if market_tide.empty:
            logger.warning("No market_tide data found")
            component_stats["market_sentiment_score"] = {"status": "no_data", "rows": 0}
            component_stats["market_premium_momentum"] = {"status": "no_data", "rows": 0}
            return {
                OUTPUT_DATASET: gold_dataset_result(
                    status="no_data",
                    rows=0,
                    components=component_stats,
                )
            }

        # Compute sentiment score (daily)
        try:
            sentiment = compute_market_sentiment_score(market_tide)
            logger.info("Computed market_sentiment_score", rows=len(sentiment))
            component_stats["market_sentiment_score"] = {
                "status": "success" if not sentiment.empty else "no_data",
                "rows": len(sentiment),
            }
        except Exception as exc:
            logger.error("Failed to compute market_sentiment_score", error=str(exc), exc_info=True)
            component_stats["market_sentiment_score"] = {"status": "error", "rows": 0, "error": str(exc)}
            sentiment = pd.DataFrame(columns=["ts_event", "market_sentiment_score"])

        # Compute momentum from sentiment series
        try:
            momentum = compute_market_premium_momentum(sentiment)
            logger.info("Computed market_premium_momentum", rows=len(momentum))
            component_stats["market_premium_momentum"] = {
                "status": "success" if not momentum.empty else "no_data",
                "rows": len(momentum),
            }
        except Exception as exc:
            logger.error("Failed to compute market_premium_momentum", error=str(exc), exc_info=True)
            component_stats["market_premium_momentum"] = {"status": "error", "rows": 0, "error": str(exc)}
            momentum = pd.DataFrame(columns=["ts_event", "market_premium_momentum"])

        # Merge sentiment + momentum on ts_event
        if sentiment.empty and momentum.empty:
            logger.warning("No tide context features computed")
            status = "error" if any(s["status"] == "error" for s in component_stats.values()) else "no_data"
            errors = [
                f"{name}: {info['error']}"
                for name, info in component_stats.items()
                if info.get("status") == "error" and info.get("error")
            ]
            return {
                OUTPUT_DATASET: gold_dataset_result(
                    status=status,
                    rows=0,
                    error="; ".join(errors) if errors else None,
                    components=component_stats,
                )
            }

        merged = sentiment
        if not momentum.empty and not sentiment.empty:
            merged = sentiment.merge(momentum, on="ts_event", how="outer")
        elif not momentum.empty:
            merged = momentum

        # Filter to requested date range
        merged["ts_event"] = pd.to_datetime(merged["ts_event"], utc=True)
        merged = merged[merged["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        merged = merged[merged["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        # Add Gold schema columns
        merged = ensure_ts_available(merged)
        merged = ensure_market_instrument_key(merged, INSTRUMENT_KEY)

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset=OUTPUT_DATASET,
                df=merged,
                project=self.project,
                version=self.version,
            )
            logger.info(
                "Wrote market_tide_context_features to Gold",
                rows=len(merged),
                path=str(output_path),
            )
        else:
            logger.info("Dry run -- would write market_tide_context_features", rows=len(merged))
            output_path = None

        status = "error" if any(s["status"] == "error" for s in component_stats.values()) else "success"
        errors = [
            f"{name}: {info['error']}"
            for name, info in component_stats.items()
            if info.get("status") == "error" and info.get("error")
        ]
        return {
            OUTPUT_DATASET: gold_dataset_result(
                status=status,
                rows=len(merged),
                path=str(output_path) if output_path else None,
                error="; ".join(errors) if errors else None,
                components=component_stats,
            )
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute market tide context Gold features")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = MarketTideContextPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("MARKET TIDE CONTEXT PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
