"""Tier 2 — Partition Completeness Check.

Verifies expected Silver partitions exist and are not empty.
Runs every ~15 min during market hours.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import structlog

from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import BRONZE_ONLY_SILVER_DATASETS

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")

# Feeds that produce hour-level sub-partitions during market hours.
HOURLY_FEEDS: frozenset[str] = frozenset({"bars", "trades", "quotes"})


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


def _silver_feeds() -> list[str]:
    """Return canonical Silver feed names (those with Arrow schemas, minus bronze-only)."""
    return sorted(name for name in SILVER_SCHEMAS if name not in BRONZE_ONLY_SILVER_DATASETS)


def _partition_has_rows(partition_dir: Path) -> bool:
    """Check if a partition directory (or its subdirectories) contains Parquet files with >0 rows."""
    for pf in partition_dir.rglob("*.parquet"):
        try:
            meta = pq.read_metadata(pf)
            if meta.num_rows > 0:
                return True
        except Exception:
            continue
    return False


async def run_partition_checks(ctx: CheckContext, check_date: date | None = None) -> list[CheckResult]:
    """Run Tier 2 partition completeness checks for all Silver feeds."""
    now = _now_et()
    today = check_date or now.date()

    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    silver_root = ctx.settings.silver_path

    for feed in _silver_feeds():
        dt_str = today.isoformat()
        # Silver partitions may use different instrument_type subdirectories;
        # check for the feed partition at the top level.
        feed_dir = silver_root / f"feed={feed}"
        dt_dir = feed_dir / f"dt={dt_str}"

        if not dt_dir.exists():
            severity = ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now)
            results.append(
                CheckResult(
                    check_name="partition_exists",
                    feed=feed,
                    severity=severity,
                    status=Status.FAIL,
                    message=f"Missing dt={dt_str} partition for feed={feed}",
                    details={"feed": feed, "date": dt_str, "expected_path": str(dt_dir)},
                    ts_checked=now,
                )
            )
            continue

        # Check if partition has data
        if not _partition_has_rows(dt_dir):
            severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
            results.append(
                CheckResult(
                    check_name="partition_nonempty",
                    feed=feed,
                    severity=severity,
                    status=Status.WARN,
                    message=f"Empty partition dt={dt_str} for feed={feed}",
                    details={"feed": feed, "date": dt_str, "path": str(dt_dir)},
                    ts_checked=now,
                )
            )
            continue

        # For hourly feeds, check hour= subdirectories
        if feed in HOURLY_FEEDS:
            elapsed = ctx.calendar.elapsed_hours(now)
            missing_hours: list[int] = []
            for hour in elapsed:
                hour_dir = dt_dir / f"hour={hour:02d}"
                if not hour_dir.exists():
                    missing_hours.append(hour)

            if missing_hours:
                severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
                results.append(
                    CheckResult(
                        check_name="partition_hourly",
                        feed=feed,
                        severity=severity,
                        status=Status.WARN,
                        message=f"Missing hour partitions for feed={feed}: {missing_hours}",
                        details={
                            "feed": feed,
                            "date": dt_str,
                            "missing_hours": missing_hours,
                            "elapsed_hours": elapsed,
                        },
                        ts_checked=now,
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_name="partition_hourly",
                        feed=feed,
                        severity=ctx.calendar.adjust_severity(Severity.P2_INFO, now),
                        status=Status.PASS,
                        message=f"All hour partitions present for feed={feed}",
                        details={"feed": feed, "date": dt_str, "elapsed_hours": elapsed},
                        ts_checked=now,
                    )
                )
        else:
            results.append(
                CheckResult(
                    check_name="partition_exists",
                    feed=feed,
                    severity=ctx.calendar.adjust_severity(Severity.P2_INFO, now),
                    status=Status.PASS,
                    message=f"Partition dt={dt_str} present for feed={feed}",
                    details={"feed": feed, "date": dt_str},
                    ts_checked=now,
                )
            )

    return results
