"""Excursion Analytics Pipeline — aggregate excursion profiles from all trading system ledgers.

Reads LedgerTrade records from each system's ledger.db, extracts excursion fields
(MFE, MAE, capture efficiency, excursion velocity, holding time), and writes a
unified Parquet dataset to the Gold layer for cross-system analysis.

Produces:
  - excursion_analytics (one row per closed trade with excursion metrics, across all systems)

Usage:
    uv run python -m heber.features.pipelines.excursion_analytics --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.excursion_analytics --start 2026-01-01 --end 2026-03-10 --dry-run
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

# Ledger DB paths relative to the monorepo root
SYSTEM_LEDGER_PATHS = {
    "3Roses": "3Roses/runs/ledger.db",
    "Kairos": "Kairos/.kairos_data/ledger.db",
    "Cerberus": "Cerberus/ledger.db",
    "Orion": "Orion/ledger.db",
    "whalehunter": "whalehunter/ledger.db",
    "trading-bot": "trading-bot/ledger.db",
    "options-bot": "options-bot/ledger.db",
}

# Columns extracted from each ledger_trades table
LEDGER_QUERY = """
SELECT
    system,
    strategy,
    symbol,
    side,
    mfe_r,
    mae_r,
    time_to_mfe_seconds,
    time_to_mae_seconds,
    mfe_mae_ratio,
    capture_efficiency,
    excursion_velocity,
    pnl_r,
    regime_entry,
    holding_seconds,
    entry_time,
    exit_time
FROM ledger_trades
WHERE exit_time IS NOT NULL
  AND entry_time >= :start_date
  AND entry_time <= :end_date
"""

EXPECTED_OUTPUT_COLUMNS = [
    "system",
    "strategy",
    "symbol",
    "side",
    "mfe_r",
    "mae_r",
    "time_to_mfe_seconds",
    "time_to_mae_seconds",
    "mfe_mae_ratio",
    "capture_efficiency",
    "excursion_velocity",
    "pnl_r",
    "regime_entry",
    "holding_seconds",
    "instrument_key",
    "ts_event",
    "ts_available",
]


def _find_monorepo_root() -> Path:
    """Walk up from this file to find the Empire monorepo root."""
    candidate = Path(__file__).resolve()
    for _ in range(10):
        candidate = candidate.parent
        if (candidate / "ruff-base.toml").exists():
            return candidate
    return Path("/Users/jacobmcmillan/Empire")


def _read_ledger(system: str, db_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    """Read excursion data from a single system's ledger.db.

    Returns an empty DataFrame if the file does not exist or the table is missing.
    """
    if not db_path.exists():
        logger.debug("ledger_not_found", system=system, path=str(db_path))
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(str(db_path))
        try:
            # Verify table exists
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ledger_trades'")
            if cursor.fetchone() is None:
                logger.debug("ledger_trades_table_missing", system=system, path=str(db_path))
                return pd.DataFrame()

            df = pd.read_sql_query(
                LEDGER_QUERY,
                conn,
                params={"start_date": start_date, "end_date": end_date},
            )
            logger.info("ledger_read", system=system, rows=len(df))
            return df
        finally:
            conn.close()
    except Exception as exc:
        logger.error("ledger_read_failed", system=system, path=str(db_path), error=str(exc))
        return pd.DataFrame()


def compute_excursion_analytics(
    monorepo_root: Path,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Read all system ledgers and produce a unified excursion analytics DataFrame.

    Parameters
    ----------
    monorepo_root:
        Path to the Empire monorepo root directory.
    start_date:
        ISO date string for the start of the date range (inclusive).
    end_date:
        ISO date string for the end of the date range (inclusive).

    Returns
    -------
    pd.DataFrame
        Unified excursion analytics with one row per closed trade.
    """
    frames: list[pd.DataFrame] = []

    for system, rel_path in SYSTEM_LEDGER_PATHS.items():
        db_path = monorepo_root / rel_path
        df = _read_ledger(system, db_path, start_date, end_date)
        if not df.empty:
            # Ensure system column is set (some ledgers may not populate it)
            df["system"] = df["system"].fillna(system)
            frames.append(df)

    if not frames:
        logger.warning("no_ledger_data_found", start=start_date, end=end_date)
        return pd.DataFrame(columns=EXPECTED_OUTPUT_COLUMNS)

    out = pd.concat(frames, ignore_index=True)

    # Normalize numeric columns
    numeric_cols = [
        "mfe_r",
        "mae_r",
        "time_to_mfe_seconds",
        "time_to_mae_seconds",
        "mfe_mae_ratio",
        "capture_efficiency",
        "excursion_velocity",
        "pnl_r",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "holding_seconds" in out.columns:
        out["holding_seconds"] = pd.to_numeric(out["holding_seconds"], errors="coerce").astype("Int64")

    # Build instrument_key from symbol
    if "symbol" in out.columns:
        out["instrument_key"] = out["symbol"].apply(lambda s: s if ":" in str(s) else f"equity:{s}")

    # Use entry_time as ts_event
    if "entry_time" in out.columns:
        out["ts_event"] = pd.to_datetime(out["entry_time"], utc=True)
    else:
        out["ts_event"] = pd.NaT

    # Zero-leakage: ts_available = now
    out["ts_available"] = datetime.now(UTC)

    # Drop intermediate columns
    out = out.drop(columns=["entry_time", "exit_time"], errors="ignore")

    # Ensure all expected columns exist
    for col in EXPECTED_OUTPUT_COLUMNS:
        if col not in out.columns:
            out[col] = None

    logger.info(
        "excursion_analytics_computed",
        rows=len(out),
        systems=out["system"].nunique(),
        strategies=out["strategy"].nunique() if "strategy" in out.columns else 0,
    )

    return out


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------


class ExcursionAnalyticsPipeline:
    """Pipeline to aggregate excursion profiles from all trading system ledgers."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "watch",
        version: str = "v1",
        monorepo_root: Path | None = None,
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version
        self.monorepo_root = monorepo_root or _find_monorepo_root()

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
            "excursion_analytics_pipeline_start",
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )

        df = compute_excursion_analytics(
            monorepo_root=self.monorepo_root,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

        if df.empty:
            logger.warning("No excursion data computed")
            return {"excursion_analytics": {"status": "no_data", "rows": 0}}

        # Filter to requested date range
        if "ts_event" in df.columns:
            df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
            df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
            df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

        if df.empty:
            logger.warning("No excursion data in requested date range after filtering")
            return {"excursion_analytics": {"status": "no_data", "rows": 0}}

        # Write to Gold
        if not dry_run:
            output_path = self.reader.write_gold(
                dataset="excursion_analytics",
                df=df,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote excursion analytics to Gold", rows=len(df), path=str(output_path))
        else:
            logger.info("Dry run — would write excursion analytics", rows=len(df))
            output_path = None

        return {
            "excursion_analytics": {
                "status": "success",
                "rows": len(df),
                "systems": int(df["system"].nunique()),
                "path": str(output_path) if output_path else None,
            }
        }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Aggregate excursion profiles from all trading system ledgers")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = ExcursionAnalyticsPipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("EXCURSION ANALYTICS PIPELINE RESULTS")
    print("=" * 60)
    info = stats.get("excursion_analytics", {})
    print(f"  Status:  {info.get('status', 'unknown')}")
    print(f"  Rows:    {info.get('rows', 0)}")
    print(f"  Systems: {info.get('systems', 0)}")
    if info.get("path"):
        print(f"  Output:  {info['path']}")
    print()


if __name__ == "__main__":
    main()
