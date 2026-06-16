"""GEX regime feature pipeline — compute Gold datasets from Silver greek_exposure.

Produces market-level (one row per date) GEX regime features:
  - net_gex              (mean net gamma exposure across all strikes/tickers per day)
  - gex_regime           (categorical: 1.0 positive, -1.0 negative, 0.0 neutral)
  - gex_flip_distance    (distance of net_gex from zero, normalized by 20d rolling std)

Usage:
    uv run python -m heber.features.pipelines.gex_regime_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.gex_regime_features --start 2026-01-01 --end 2026-03-10 --dry-run
    uv run python -m heber.features.pipelines.gex_regime_features \
        --start 2026-01-01 --end 2026-03-10 --datasets net_gex gex_regime
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

LOOKBACK_DAYS = 60  # Extra history for 20d rolling std
INSTRUMENT_KEY = "market:gex_regime"


def _ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ts_available column for zero-leakage Gold writes."""
    if "ts_available" not in df.columns:
        df["ts_available"] = datetime.now(UTC)
    return df


def _ensure_market_instrument_key(df: pd.DataFrame) -> pd.DataFrame:
    """Set instrument_key to the market-level GEX regime key."""
    df["instrument_key"] = INSTRUMENT_KEY
    return df


# ---------------------------------------------------------------------------
# Net GEX
# ---------------------------------------------------------------------------


def compute_net_gex(greek_exposure: pd.DataFrame) -> pd.DataFrame:
    """Compute daily net gamma exposure from greek_exposure Silver data.

    Supports two Silver schemas:
      - Granular: ``call_gamma`` and ``put_gamma`` columns → net = call - put.
      - Aggregate: ``gamma_exposure`` column only → use directly as net GEX.

    Args:
        greek_exposure: DataFrame with ``ts_event`` and either
                        (``call_gamma``, ``put_gamma``) or ``gamma_exposure``.

    Returns:
        DataFrame with columns: ts_event, net_gex.
        One row per date.
    """
    if greek_exposure.empty:
        return pd.DataFrame(columns=["ts_event", "net_gex"])

    df = greek_exposure.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

    # Support both granular (call/put split) and aggregate schemas.
    if "call_gamma" in df.columns and "put_gamma" in df.columns:
        df["call_gamma"] = pd.to_numeric(df["call_gamma"], errors="coerce")
        df["put_gamma"] = pd.to_numeric(df["put_gamma"], errors="coerce")
        df["gamma_diff"] = df["call_gamma"] - df["put_gamma"]
    elif "gamma_exposure" in df.columns:
        df["gamma_diff"] = pd.to_numeric(df["gamma_exposure"], errors="coerce")
    else:
        logger.warning("greek_exposure missing call_gamma/put_gamma and gamma_exposure columns")
        return pd.DataFrame(columns=["ts_event", "net_gex"])

    # Drop rows where gamma_diff is NaN (e.g. schema-merge artifacts)
    df = df.dropna(subset=["gamma_diff"])

    # Aggregate to daily: mean net gamma across all strikes/tickers
    df["date"] = df["ts_event"].dt.date
    daily = df.groupby("date")["gamma_diff"].mean().reset_index()
    daily.columns = ["date", "net_gex"]
    daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)

    return daily[["ts_event", "net_gex"]].copy()


# ---------------------------------------------------------------------------
# GEX Regime
# ---------------------------------------------------------------------------


def compute_gex_regime(net_gex_df: pd.DataFrame, rolling_window: int = 20) -> pd.DataFrame:
    """Classify GEX regime as positive (1.0), negative (-1.0), or neutral (0.0).

    The neutral zone is defined as |net_gex| < 0.1 * rolling 20-day std of net_gex.
    This adaptive threshold adjusts to the prevailing volatility of gamma flows.

    Args:
        net_gex_df: DataFrame with columns: ts_event, net_gex.
                    One row per date (output of compute_net_gex).
        rolling_window: Window for rolling std threshold (default 20 trading days).

    Returns:
        DataFrame with columns: ts_event, gex_regime.
        One row per date. Values: 1.0, -1.0, or 0.0.
    """
    if net_gex_df.empty:
        return pd.DataFrame(columns=["ts_event", "gex_regime"])

    df = net_gex_df.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["net_gex"] = pd.to_numeric(df["net_gex"], errors="coerce")
    df = df.sort_values("ts_event")

    # Rolling std with min_periods=1 so early rows still get a value
    rolling_std = df["net_gex"].rolling(rolling_window, min_periods=1).std()
    threshold = 0.1 * rolling_std

    # Classify regime
    df["gex_regime"] = np.where(
        df["net_gex"].abs() < threshold,
        0.0,
        np.where(df["net_gex"] > 0, 1.0, -1.0),
    )

    return df[["ts_event", "gex_regime"]].copy()


# ---------------------------------------------------------------------------
# GEX Flip Distance
# ---------------------------------------------------------------------------


def compute_gex_flip_distance(net_gex_df: pd.DataFrame, rolling_window: int = 20) -> pd.DataFrame:
    """Distance of net_gex from the GEX flip point (zero), normalized by 20-day rolling std.

    A value of 2.0 means net_gex is 2 standard deviations away from zero. Positive
    values indicate distance into positive GEX territory, negative into negative.

    Args:
        net_gex_df: DataFrame with columns: ts_event, net_gex.
                    One row per date (output of compute_net_gex).
        rolling_window: Window for rolling std normalization (default 20 trading days).

    Returns:
        DataFrame with columns: ts_event, gex_flip_distance.
        One row per date.
    """
    if net_gex_df.empty:
        return pd.DataFrame(columns=["ts_event", "gex_flip_distance"])

    df = net_gex_df.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["net_gex"] = pd.to_numeric(df["net_gex"], errors="coerce")
    df = df.sort_values("ts_event")

    # Rolling std of net_gex
    rolling_std = df["net_gex"].rolling(rolling_window, min_periods=1).std()

    # Normalize distance from zero by rolling std
    df["gex_flip_distance"] = np.where(
        rolling_std > 0,
        df["net_gex"] / rolling_std,
        np.nan,
    )

    return df[["ts_event", "gex_flip_distance"]].copy()


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

ALL_DATASETS = ("net_gex", "gex_regime", "gex_flip_distance")

DATASET_TO_GOLD_COL = {
    "net_gex": "net_gex",
    "gex_regime": "gex_regime",
    "gex_flip_distance": "gex_flip_distance",
}


class GexRegimePipeline:
    """Pipeline to compute market-level GEX regime Gold features from Silver greek_exposure."""

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

        # Load Silver greek_exposure data with lookback for rolling computations
        greek_start = start_date - timedelta(days=LOOKBACK_DAYS)
        logger.info(
            "Loading Silver greek_exposure",
            start=greek_start.isoformat(),
            end=end_date.isoformat(),
        )

        try:
            greek_exposure = self.reader.read_silver(
                "greek_exposure",
                time_range=(greek_start, end_date),
            )
            logger.info("Loaded greek_exposure", rows=len(greek_exposure))
        except Exception as exc:
            logger.error("Failed to load greek_exposure", error=str(exc), exc_info=True)
            for ds_name in datasets:
                stats[ds_name] = {"status": "error", "error": str(exc)}
            return stats

        if greek_exposure.empty:
            logger.warning("No greek_exposure data found")
            for ds_name in datasets:
                stats[ds_name] = {"status": "no_data", "rows": 0}
            return stats

        # Compute net_gex first (needed by all downstream features)
        net_gex_df = compute_net_gex(greek_exposure)

        if net_gex_df.empty:
            logger.warning("net_gex computation returned empty")
            for ds_name in datasets:
                stats[ds_name] = {"status": "no_data", "rows": 0}
            return stats

        # Compute each requested feature
        for ds_name in datasets:
            logger.info("Computing GEX feature", feature=ds_name)

            try:
                if ds_name == "net_gex":
                    feature_frames[ds_name] = net_gex_df
                elif ds_name == "gex_regime":
                    feature_frames[ds_name] = compute_gex_regime(net_gex_df)
                elif ds_name == "gex_flip_distance":
                    feature_frames[ds_name] = compute_gex_flip_distance(net_gex_df)
                else:
                    logger.warning("Unknown dataset", dataset=ds_name)
                    continue

            except Exception as exc:
                logger.error("Failed to compute feature", feature=ds_name, error=str(exc), exc_info=True)
                stats[ds_name] = {"status": "error", "error": str(exc)}

        # Merge all feature frames on ts_event into a single Gold output
        merged = self._merge_features(feature_frames)

        if merged.empty:
            logger.warning("No GEX regime features computed")
            for ds_name in datasets:
                if ds_name not in stats:
                    stats[ds_name] = {"status": "no_data", "rows": 0}
            return stats

        # Filter to requested date range
        merged["ts_event"] = pd.to_datetime(merged["ts_event"], utc=True)
        merged = merged[merged["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        merged = merged[merged["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        # Add Gold schema columns
        merged = _ensure_ts_available(merged)
        merged = _ensure_market_instrument_key(merged)

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="gex_regime_features",
                df=merged,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote gex_regime_features to Gold", rows=len(merged), path=str(output_path))
        else:
            logger.info("Dry run - would write gex_regime_features", rows=len(merged))
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
    parser = argparse.ArgumentParser(description="Compute GEX regime Gold features")
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

    pipeline = GexRegimePipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        datasets=tuple(args.datasets),
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("GEX REGIME PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
