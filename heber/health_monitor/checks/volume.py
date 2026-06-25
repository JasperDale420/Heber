"""Tier 2 — Volume Trending Check.

Compares today's Silver partition row counts against a trailing baseline
to detect anomalous volume drops.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq
import structlog

from heber.health_monitor.metrics import health_volume_ratio
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import BRONZE_ONLY_SILVER_DATASETS

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")


def _now_et() -> datetime:
    """Return the current time in US/Eastern.  Patchable in tests."""
    return datetime.now(tz=ET)


def _silver_feeds() -> list[str]:
    """Return canonical Silver feed names (those with Arrow schemas, minus bronze-only)."""
    return sorted(name for name in SILVER_SCHEMAS if name not in BRONZE_ONLY_SILVER_DATASETS)


def _count_rows(partition_dir: Path) -> int:
    """Sum row counts across all Parquet files in a partition using metadata only."""
    total = 0
    for pf in partition_dir.rglob("*.parquet"):
        try:
            meta = pq.read_metadata(pf)
            total += meta.num_rows
        except Exception:
            logger.warning("parquet_metadata_read_error", path=str(pf), exc_info=True)
            continue
    return total


async def run_volume_checks(ctx: CheckContext, check_date: date | None = None) -> list[CheckResult]:
    """Run Tier 2 volume trending checks for all Silver feeds."""
    now = _now_et()
    today = check_date or now.date()

    if not ctx.calendar.is_trading_day(today):
        return []

    results: list[CheckResult] = []
    silver_root = ctx.settings.silver_path
    today_counts: dict[str, int] = {}

    # Read trailing baselines
    baseline_days = ctx.settings.health_volume_baseline_days
    baseline_end = today - timedelta(days=1)
    baseline_start = baseline_end - timedelta(days=baseline_days - 1)
    baselines_df = ctx.store.read_baselines(baseline_start, baseline_end, baseline_key="volume")

    # Compute median per feed from baselines
    feed_medians: dict[str, float] = {}
    if not baselines_df.empty and "feed" in baselines_df.columns and "row_count" in baselines_df.columns:
        for feed, group in baselines_df.groupby("feed"):
            feed_medians[str(feed)] = float(group["row_count"].median())

    warn_ratio = ctx.settings.health_volume_warn_ratio
    critical_ratio = ctx.settings.health_volume_critical_ratio

    for feed in _silver_feeds():
        feed_dir = silver_root / f"feed={feed}"
        dt_dirs = list(feed_dir.glob(f"instrument_type=*/dt={today.isoformat()}")) if feed_dir.exists() else []

        if not dt_dirs:
            continue

        row_count = sum(_count_rows(d) for d in dt_dirs)
        today_counts[feed] = row_count

        median = feed_medians.get(feed)

        if median is None or median == 0:
            # No baseline — skip with INFO
            results.append(
                CheckResult(
                    check_name="volume_trending",
                    feed=feed,
                    severity=Severity.P2_INFO,
                    status=Status.PASS,
                    message=f"No baseline available for feed={feed}, skipping volume comparison",
                    details={"feed": feed, "date": today.isoformat(), "row_count": row_count},
                    ts_checked=now,
                )
            )
            continue

        ratio = row_count / median

        # Update Prometheus gauge
        try:
            health_volume_ratio.labels(feed=feed).set(ratio)
        except Exception:
            pass

        if ratio < critical_ratio:
            severity = ctx.calendar.adjust_severity(Severity.P0_CRITICAL, now)
            results.append(
                CheckResult(
                    check_name="volume_trending",
                    feed=feed,
                    severity=severity,
                    status=Status.FAIL,
                    message=(
                        f"Volume critically low for feed={feed}: "
                        f"{row_count} rows ({ratio:.1%} of baseline median {median:.0f})"
                    ),
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "row_count": row_count,
                        "baseline_median": median,
                        "ratio": round(ratio, 4),
                    },
                    ts_checked=now,
                )
            )
        elif ratio < warn_ratio:
            severity = ctx.calendar.adjust_severity(Severity.P1_WARNING, now)
            results.append(
                CheckResult(
                    check_name="volume_trending",
                    feed=feed,
                    severity=severity,
                    status=Status.WARN,
                    message=(
                        f"Volume below baseline for feed={feed}: "
                        f"{row_count} rows ({ratio:.1%} of baseline median {median:.0f})"
                    ),
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "row_count": row_count,
                        "baseline_median": median,
                        "ratio": round(ratio, 4),
                    },
                    ts_checked=now,
                )
            )
        else:
            severity = ctx.calendar.adjust_severity(Severity.P2_INFO, now)
            results.append(
                CheckResult(
                    check_name="volume_trending",
                    feed=feed,
                    severity=severity,
                    status=Status.PASS,
                    message=(
                        f"Volume OK for feed={feed}: {row_count} rows ({ratio:.1%} of baseline median {median:.0f})"
                    ),
                    details={
                        "feed": feed,
                        "date": today.isoformat(),
                        "row_count": row_count,
                        "baseline_median": median,
                        "ratio": round(ratio, 4),
                    },
                    ts_checked=now,
                )
            )

    return results


async def write_volume_baseline(ctx: CheckContext, check_date: date) -> None:
    """Write EOD volume baseline for all Silver feeds. Called from Tier 3 only."""
    silver_root = ctx.settings.silver_path
    today_counts: dict[str, int] = {}
    for feed in _silver_feeds():
        feed_dir = silver_root / f"feed={feed}"
        dt_dirs = list(feed_dir.glob(f"instrument_type=*/dt={check_date.isoformat()}")) if feed_dir.exists() else []
        if dt_dirs:
            today_counts[feed] = sum(_count_rows(d) for d in dt_dirs)
    if today_counts:
        baseline_rows = [{"feed": feed, "row_count": count} for feed, count in today_counts.items()]
        ctx.store.write_baseline(pd.DataFrame(baseline_rows), check_date, baseline_key="volume")
        logger.info("volume_baseline_written", date=check_date.isoformat(), feeds=len(today_counts))
