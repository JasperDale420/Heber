"""IV surface feature pipeline — compute Gold datasets from Silver iv_term_structure.

Produces:
  - iv_surface_features  (daily IV surface metrics per symbol)
    - put_call_iv_skew       (put-call IV differential at nearest expiry)
    - term_structure_slope   (far vs near IV term structure shape)
    - iv_change_1d           (day-over-day change in near-term IV)

Usage:
    uv run python -m heber.features.pipelines.iv_surface_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.iv_surface_features --start 2026-01-01 --end 2026-03-10 --dry-run
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

LOOKBACK_DAYS = 2  # Extra history for computing iv_change_1d


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


def compute_iv_surface_features(iv_data: pd.DataFrame) -> pd.DataFrame:
    """Compute IV surface features from Silver iv_term_structure data.

    Input: Silver iv_term_structure with columns:
        instrument_key, symbol, ts_event, expiry, dte, iv, call_iv, put_iv

    Output: One row per (instrument_key, date) with:
        put_call_iv_skew, term_structure_slope, iv_change_1d

    Logic:
        1. For each (symbol, date), sort by dte ascending.
        2. put_call_iv_skew = (put_iv - call_iv) / call_iv for the row with
           smallest dte > 0.  Returns NaN when call_iv == 0.
        3. term_structure_slope = (second_expiry_iv - first_expiry_iv) /
           first_expiry_iv.  Requires at least 2 distinct expiries.
           Returns NaN when first_expiry_iv == 0 or only 1 expiry.
        4. iv_change_1d = day-over-day diff of near-term IV per symbol.
    """
    if iv_data.empty:
        return pd.DataFrame()

    df = iv_data.copy()

    # Coerce types
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.normalize()
    for col in ("iv", "call_iv", "put_iv", "dte"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter out rows with dte <= 0 (expired or same-day expiry)
    df = df[df["dte"] > 0].copy()

    if df.empty:
        return pd.DataFrame()

    # Sort by instrument_key, date, dte
    df = df.sort_values(["instrument_key", "date", "dte"])

    # --- Per (instrument_key, date) aggregation ---
    results: list[dict[str, Any]] = []

    for (inst_key, date), group in df.groupby(["instrument_key", "date"]):
        group = group.sort_values("dte")
        nearest = group.iloc[0]

        # put_call_iv_skew: (put_iv - call_iv) / call_iv for nearest expiry
        call_iv_val = nearest["call_iv"]
        put_iv_val = nearest["put_iv"]
        if pd.notna(call_iv_val) and call_iv_val != 0:
            skew = (put_iv_val - call_iv_val) / call_iv_val
        else:
            skew = np.nan

        # term_structure_slope: (far_iv - near_iv) / near_iv
        near_iv = nearest["iv"]
        if len(group) >= 2:
            second = group.iloc[1]
            far_iv = second["iv"]
            if pd.notna(near_iv) and near_iv != 0:
                slope = (far_iv - near_iv) / near_iv
            else:
                slope = np.nan
        else:
            slope = np.nan

        # Extract symbol from group
        symbol = nearest.get("symbol", inst_key)

        results.append(
            {
                "instrument_key": inst_key,
                "symbol": symbol,
                "ts_event": date,
                "near_iv": near_iv,
                "put_call_iv_skew": skew,
                "term_structure_slope": slope,
            }
        )

    if not results:
        return pd.DataFrame()

    out = pd.DataFrame(results)

    # --- iv_change_1d: day-over-day diff of near-term IV per symbol ---
    out = out.sort_values(["instrument_key", "ts_event"])
    out["iv_change_1d"] = out.groupby("instrument_key")["near_iv"].diff()

    # Drop helper column
    out = out.drop(columns=["near_iv"])

    # Ensure required columns
    out = _ensure_instrument_key(out)
    out = _ensure_ts_available(out)

    logger.info(
        "iv_surface_features_computed",
        rows=len(out),
        symbols=out["instrument_key"].nunique(),
    )

    return out


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class IVSurfacePipeline:
    """Pipeline to compute IV surface Gold features from Silver iv_term_structure data."""

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
        """Run the IV surface feature pipeline.

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

        # Extend end_date to end of day when midnight (date-only input)
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        # Read with lookback for iv_change_1d
        read_start = start_date - timedelta(days=LOOKBACK_DAYS)

        logger.info(
            "iv_surface_pipeline_start",
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            read_start=read_start.isoformat(),
        )

        iv_data = self.reader.read_silver("iv_term_structure", time_range=(read_start, end_date))
        logger.info("iv_surface_loaded_silver", rows=len(iv_data))

        if iv_data.empty:
            logger.warning("iv_surface_no_data")
            return {"iv_surface_features": {"status": "no_data", "rows": 0}}

        df = compute_iv_surface_features(iv_data)

        if df.empty:
            logger.warning("iv_surface_empty_result")
            return {"iv_surface_features": {"status": "no_data", "rows": 0}}

        # Filter to requested date range only (exclude lookback rows)
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
        df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
        df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if df.empty:
            logger.warning("iv_surface_empty_after_filter")
            return {"iv_surface_features": {"status": "no_data", "rows": 0}}

        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="iv_surface_features",
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("iv_surface_wrote_gold", rows=len(df), path=str(output_path))
        else:
            logger.info("iv_surface_dry_run", rows=len(df))
            output_path = None

        return {
            "iv_surface_features": {
                "status": "success",
                "rows": len(df),
                "path": str(output_path) if output_path else None,
            }
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute IV surface Gold features from Silver iv_term_structure")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = IVSurfacePipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("IV SURFACE FEATURES PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
