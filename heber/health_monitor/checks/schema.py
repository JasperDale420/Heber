"""Tier 3 — Schema Drift Detection.

Compares today's Parquet schema fingerprints against stored baselines
to detect column additions, removals, and type changes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq
import structlog

from heber.health_monitor.metrics import health_schema_changes_total
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import BRONZE_ONLY_SILVER_DATASETS

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Use a dedicated baseline key to separate schema baselines from volume baselines.
SCHEMA_BASELINE_KEY = "schema"


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


def _silver_feeds() -> list[str]:
    """Return canonical Silver feed names (those with Arrow schemas, minus bronze-only)."""
    return sorted(name for name in SILVER_SCHEMAS if name not in BRONZE_ONLY_SILVER_DATASETS)


def _fingerprint(schema: list[tuple[str, str]]) -> str:
    """Hash a sorted list of (column_name, column_type) tuples."""
    columns_json = json.dumps(schema)
    return hashlib.sha256(columns_json.encode()).hexdigest()


def _read_parquet_schema(partition_dir: Path) -> list[tuple[str, str]] | None:
    """Read the Arrow schema from the first Parquet file found in a partition."""
    for pf in partition_dir.rglob("*.parquet"):
        try:
            meta = pq.read_metadata(pf)
            arrow_schema = meta.schema.to_arrow_schema()
            return sorted((f.name, str(f.type)) for f in arrow_schema)
        except Exception:
            logger.warning("parquet_schema_read_error", path=str(pf), exc_info=True)
            continue
    return None


def _diff_schemas(
    old_columns: list[tuple[str, str]],
    new_columns: list[tuple[str, str]],
) -> tuple[list[str], list[str], list[tuple[str, str, str]]]:
    """Compare old and new column lists.

    Returns (added_cols, removed_cols, type_changed as [(name, old_type, new_type)]).
    """
    old_map = dict(old_columns)
    new_map = dict(new_columns)

    old_names = set(old_map.keys())
    new_names = set(new_map.keys())

    added = sorted(new_names - old_names)
    removed = sorted(old_names - new_names)

    type_changed = []
    for name in sorted(old_names & new_names):
        if old_map[name] != new_map[name]:
            type_changed.append((name, old_map[name], new_map[name]))

    return added, removed, type_changed


async def run_schema_checks(ctx: CheckContext, check_date: date | None = None) -> list[CheckResult]:
    """Run Tier 3 schema drift detection for all Silver feeds with data today."""
    now = _now_et()
    today = check_date or now.date()

    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    silver_root = ctx.settings.silver_path

    # Load previous schema baselines.
    # Schema baselines are stored with feed, schema_hash, columns_json columns.
    yesterday = today - timedelta(days=1)
    baselines_df = ctx.store.read_baselines(yesterday, yesterday, baseline_key="schema")
    prev_schemas: dict[str, list[tuple[str, str]]] = {}
    if not baselines_df.empty and "feed" in baselines_df.columns and "columns_json" in baselines_df.columns:
        for _, row in baselines_df.iterrows():
            try:
                prev_schemas[row["feed"]] = [tuple(c) for c in json.loads(row["columns_json"])]
            except (json.JSONDecodeError, TypeError):
                continue

    new_baselines: list[dict] = []

    for feed in _silver_feeds():
        dt_dir = silver_root / f"feed={feed}" / f"dt={today.isoformat()}"

        if not dt_dir.exists():
            continue

        current_columns = _read_parquet_schema(dt_dir)
        if current_columns is None:
            continue

        current_hash = _fingerprint(current_columns)
        columns_json = json.dumps(current_columns)

        new_baselines.append(
            {
                "feed": feed,
                "schema_hash": current_hash,
                "columns_json": columns_json,
            }
        )

        prev_columns = prev_schemas.get(feed)

        if prev_columns is None:
            # First run — store and PASS
            results.append(
                CheckResult(
                    check_name="schema_drift",
                    feed=feed,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"Initial schema baseline recorded for feed={feed}",
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "schema_hash": current_hash,
                        "column_count": len(current_columns),
                    },
                    ts_checked=now,
                )
            )
            continue

        prev_hash = _fingerprint(prev_columns)
        if current_hash == prev_hash:
            results.append(
                CheckResult(
                    check_name="schema_drift",
                    feed=feed,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"No schema drift detected for feed={feed}",
                    details={"feed": feed, "date": today.isoformat(), "schema_hash": current_hash},
                    ts_checked=now,
                )
            )
            continue

        # Schema changed — determine severity
        added, removed, type_changed = _diff_schemas(prev_columns, current_columns)

        # Increment Prometheus counter
        try:
            health_schema_changes_total.labels(feed=feed).inc()
        except Exception:
            pass

        if removed or type_changed:
            # Breaking change — FAIL P0
            parts = []
            if removed:
                parts.append(f"columns removed: {removed}")
            if type_changed:
                type_desc = [f"{n}: {ot}->{nt}" for n, ot, nt in type_changed]
                parts.append(f"type changes: {type_desc}")
            if added:
                parts.append(f"columns added: {added}")

            severity = ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now)
            results.append(
                CheckResult(
                    check_name="schema_drift",
                    feed=feed,
                    severity=severity,
                    status=Status.FAIL,
                    message=f"Breaking schema change for feed={feed}: {'; '.join(parts)}",
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "added": added,
                        "removed": removed,
                        "type_changed": [{"column": n, "old_type": ot, "new_type": nt} for n, ot, nt in type_changed],
                        "old_hash": prev_hash,
                        "new_hash": current_hash,
                    },
                    ts_checked=now,
                )
            )
        elif added:
            # Additive change — WARN P1
            severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
            results.append(
                CheckResult(
                    check_name="schema_drift",
                    feed=feed,
                    severity=severity,
                    status=Status.WARN,
                    message=f"Schema evolution for feed={feed}: columns added: {added}",
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "added": added,
                        "old_hash": prev_hash,
                        "new_hash": current_hash,
                    },
                    ts_checked=now,
                )
            )

    # Write new schema baselines
    if new_baselines:
        baseline_df = pd.DataFrame(new_baselines)
        ctx.store.write_baseline(baseline_df, today, baseline_key="schema")

    return results
