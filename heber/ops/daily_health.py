"""Daily end-of-day health report for Heber data lakehouse.

Runs 3 checks after market close to verify that the day's data landed correctly:
1. Cross-feed completeness — instrument overlap across feeds
2. Silver invariants — value-level consistency checks on the day's Silver data
3. Gold freshness — Gold dt= partitions exist for expected datasets

NOTE: Partition freshness, fill rate, and DLQ status checks were
removed in favour of heber.health_monitor, which provides richer alerting and
persistence for those checks.  See checks/partition.py, checks/volume.py,
checks/ml_readiness.py, and checks/stream_health.py.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as pa_ds
import structlog

from heber.calendar import MarketCalendar
from heber.config import Settings, get_settings
from heber.quality.silver_invariants import (
    FEED_INVARIANTS,
    ColumnFill,
    accumulate_fill,
    check_frame,
    completeness_violations,
    duplicate_row_count,
    split_by_tolerance,
)
from heber.reader.core import _collect_parquet_files

logger = structlog.get_logger(__name__)

# Exact cross-partition de-duplication holds every distinct event_id for a
# feed-day in memory. Event ids are 32-char hashes, so this cap is roughly a
# few hundred MB in the worst case — high enough that no real feed-day reaches
# it, low enough that a runaway one degrades the check instead of the box.
_MAX_TRACKED_EVENT_IDS = 5_000_000


def _check_result(
    check_id: str,
    *,
    status: str,
    severity: str,
    observed: dict[str, Any],
    threshold: dict[str, Any],
    message: str,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "status": status,
        "severity": severity,
        "observed": observed,
        "threshold": threshold,
        "message": message,
    }


def _check_cross_feed_completeness(
    report_date: date,
    settings: Settings,
) -> dict[str, Any]:
    """Compare instrument sets across feeds for the date."""
    dt_str = report_date.isoformat()
    feed_symbols: dict[str, set[str]] = {}

    for feed in ("bars", "quotes", "trades"):
        # The writer nests dt under instrument_type= (feed={feed}/instrument_type=
        # {type}/dt={dt}); the bare feed/dt path never exists.
        feed_dir = settings.silver_path / f"feed={feed}"
        part_dirs = list(feed_dir.glob(f"instrument_type=*/dt={dt_str}")) if feed_dir.exists() else []
        if not part_dirs:
            continue
        symbols: set[str] = set()
        for partition in part_dirs:
            if not list(partition.rglob("*.parquet")):
                continue
            try:
                df = pd.read_parquet(partition)
            except Exception:
                logger.warning("cross_feed_read_failed", feed=feed, date=dt_str, exc_info=True)
                continue
            if "instrument_key" in df.columns:
                symbols |= set(df["instrument_key"].dropna().unique())
            elif "symbol" in df.columns:
                symbols |= set(df["symbol"].dropna().unique())
        if symbols:
            feed_symbols[feed] = symbols

    if len(feed_symbols) < 2:
        return _check_result(
            "cross_feed_completeness",
            status="warn",
            severity="warning",
            observed={"feeds_found": list(feed_symbols.keys())},
            threshold={"min_feeds": 2},
            message=f"Only {len(feed_symbols)} feed(s) have data; cannot compare.",
        )

    all_symbols = set().union(*feed_symbols.values())
    per_feed_coverage = {f: len(s) / len(all_symbols) if all_symbols else 1.0 for f, s in feed_symbols.items()}
    min_coverage = min(per_feed_coverage.values()) if per_feed_coverage else 0.0

    return _check_result(
        "cross_feed_completeness",
        status="ok" if min_coverage >= 0.5 else "warn",
        severity="warning",
        observed={
            "feed_symbol_counts": {f: len(s) for f, s in feed_symbols.items()},
            "min_coverage": round(min_coverage, 3),
        },
        threshold={"min_coverage": 0.5},
        message=f"Cross-feed coverage: {min_coverage:.1%} minimum overlap across {len(feed_symbols)} feeds.",
    )


def _check_silver_invariants(
    report_date: date,
    settings: Settings,
) -> dict[str, Any]:
    """Check that the day's Silver values are internally consistent.

    Scoped to the report date's partitions, not the whole feed — the retired
    Soda scan pointed DuckDB at every Parquet file a feed had ever written,
    which on this volume could not finish.

    Anything that stops a check from actually running — an unreadable
    partition, a column that has been renamed away — makes this non-passing.
    A scan that silently covered less than it claims is what let the previous
    integration sit broken.
    """
    dt_str = report_date.isoformat()
    violations: dict[str, dict[str, int]] = {}
    warnings: dict[str, dict[str, int]] = {}
    not_run: dict[str, list[str]] = {}
    rows_checked = 0
    feeds_checked: list[str] = []
    read_failures: list[str] = []

    for feed, spec in FEED_INVARIANTS.items():
        feed_dir = settings.silver_path / f"feed={feed}"
        part_dirs = list(feed_dir.glob(f"instrument_type=*/dt={dt_str}")) if feed_dir.exists() else []
        if not part_dirs:
            continue
        feed_violations: dict[str, int] = {}
        feed_not_run: set[str] = set()
        fill_tally: dict[str, ColumnFill] = {}
        # event_id uniqueness spans every instrument_type partition of a feed,
        # so ids are tallied across all of them rather than per frame.
        event_ids: Counter[str] | None = Counter()
        feed_rows = 0
        read_any = False

        for partition in part_dirs:
            # Go through the canonical collector, not pd.read_parquet on the
            # directory: `._*` AppleDouble sidecars and `part-*.parquet.tmp`
            # partial writes are ordinary noise on this volume, and letting
            # pyarrow auto-discover them turns a healthy partition into a
            # failed critical check.
            files = _collect_parquet_files(partition)
            if not files:
                continue
            try:
                # One dataset over the collected files rather than per-file
                # frames concatenated, so fragments are unified the way the
                # canonical reader unifies them. Detecting *schema drift* is
                # not this check's job — health_monitor/checks/schema.py
                # fingerprints each feed's schema against a stored baseline
                # and reports column and type changes. Duplicating a weaker
                # copy of that here would only disagree with it.
                df = pa_ds.dataset(files, format="parquet").to_table().to_pandas()
            except Exception:
                logger.warning("silver_invariants_read_failed", feed=feed, date=dt_str, exc_info=True)
                read_failures.append(f"{feed}/{partition.parent.name}")
                continue
            read_any = True
            outcome = check_frame(df, spec)
            feed_rows += outcome.rows
            feed_not_run.update(outcome.not_run)
            for violation in outcome.violations:
                feed_violations[violation.name] = feed_violations.get(violation.name, 0) + violation.rows
            accumulate_fill(df, spec, fill_tally)
            if "event_id" in df.columns and event_ids is not None:
                # Add ids one at a time and stop at the first that would
                # actually push the distinct set past the cap. Counting the
                # frame's distinct ids up front both allocates that set anyway
                # and double-counts ids already tracked, so a frame of pure
                # repeats could abandon a tally that never grew.
                overflowed = False
                for event_id in df["event_id"].dropna().astype(str):
                    if event_id in event_ids:
                        event_ids[event_id] += 1
                        continue
                    if len(event_ids) >= _MAX_TRACKED_EVENT_IDS:
                        overflowed = True
                        break
                    event_ids[event_id] = 1
                if overflowed:
                    # Exact daily de-duplication needs every id in memory. Past
                    # this many, stop rather than risk exhausting the box and
                    # losing the whole report — and say the check did not run
                    # instead of reporting a clean pass on a partial tally.
                    logger.warning(
                        "silver_invariants_duplicate_scan_abandoned",
                        feed=feed,
                        distinct_ids=len(event_ids),
                        cap=_MAX_TRACKED_EVENT_IDS,
                    )
                    feed_not_run.add("event_id_unique")
                    event_ids = None

        feed_violations.update(completeness_violations(spec, fill_tally))
        feed_violations, feed_warnings = split_by_tolerance(spec, feed_violations, feed_rows)
        if feed_warnings:
            warnings[feed] = feed_warnings
        if event_ids is not None:
            duplicates = duplicate_row_count(event_ids)
            if duplicates:
                feed_violations["event_id_unique"] = duplicates
        if read_any:
            feeds_checked.append(feed)
            rows_checked += feed_rows
        if feed_violations:
            violations[feed] = feed_violations
        if feed_not_run:
            not_run[feed] = sorted(feed_not_run)

    total_violations = sum(sum(v.values()) for v in violations.values())
    observed = {
        "feeds_checked": feeds_checked,
        "rows_checked": rows_checked,
        "violations": violations,
        "warnings": warnings,
        "not_run": not_run,
        "read_failures": read_failures,
        "scan_complete": not read_failures and not not_run,
    }

    if not feeds_checked:
        # Nothing checked is not the same as everything passing. Saying "ok"
        # here is how the retired Soda check went unnoticed for so long. And
        # "nothing was there" is a different problem from "everything there
        # was corrupt", so only the former is a warning.
        return _check_result(
            "silver_invariants",
            status="fail" if read_failures else "warn",
            severity="critical",
            observed=observed,
            threshold={"max_violations": 0},
            message=(
                f"No Silver partition could be read for {dt_str}; {len(read_failures)} unreadable, nothing was checked."
                if read_failures
                else f"No Silver partitions found for {dt_str}; nothing was checked."
            ),
        )

    if total_violations or read_failures:
        status = "fail"
    elif not_run or warnings:
        # Tolerated-invariant breaches and checks that could not run are worth
        # surfacing but are not the same class of problem as a negative price.
        status = "warn"
    else:
        status = "ok"

    parts = [f"{total_violations} violating row(s) across {len(violations)} feed(s)"]
    if read_failures:
        parts.append(f"{len(read_failures)} unreadable partition(s)")
    if warnings:
        parts.append(f"{sum(sum(v.values()) for v in warnings.values())} tolerated-threshold row(s)")
    if not_run:
        parts.append(f"{sum(len(v) for v in not_run.values())} check(s) could not run")
    return _check_result(
        "silver_invariants",
        status=status,
        severity="critical",
        observed=observed,
        threshold={"max_violations": 0},
        message=f"Silver invariants: {', '.join(parts)}; {rows_checked} rows checked.",
    )


def _check_gold_freshness(
    report_date: date,
    settings: Settings,
) -> dict[str, Any]:
    """Check that Gold dt= partitions exist for expected datasets."""
    dt_str = report_date.isoformat()
    gold_datasets = ["meta_label_features", "labels_alert_barriers"]
    found: list[str] = []
    missing: list[str] = []

    for dataset in gold_datasets:
        matches = list(settings.gold_path.glob(f"dataset={dataset}/project=*/version=*/dt={dt_str}"))
        if matches:
            found.append(dataset)
        else:
            missing.append(dataset)

    return _check_result(
        "gold_freshness",
        status="ok" if not missing else "warn",
        severity="warning",
        observed={"found": found, "missing": missing},
        threshold={"expected_datasets": gold_datasets},
        message=f"Gold: {len(found)}/{len(gold_datasets)} datasets have partitions for {dt_str}.",
    )


def _overall_status(summary: dict[str, int]) -> str:
    if summary["fail"] > 0:
        return "fail"
    if summary["warn"] > 0:
        return "warn"
    return "ok"


def generate_daily_health_report(
    report_date: date | None = None,
    settings: Settings | None = None,
    *,
    force: bool = False,
    skip_invariants: bool = False,
) -> dict[str, Any]:
    """Generate a daily health report for the given date.

    Args:
        report_date: Date to report on (default: today).
        settings: Heber settings (default: from env).
        force: Run even on non-trading days.
        skip_invariants: Skip the Silver invariant scan (for tests with no lake on disk).

    Returns:
        Report dict with checks and overall status.
    """
    active_settings = settings or get_settings()
    effective_date = report_date or date.today()
    now_utc = datetime.now(UTC)

    cal = MarketCalendar()
    check_dt = datetime(effective_date.year, effective_date.month, effective_date.day, 12, 0, tzinfo=UTC)
    is_trading = cal.is_trading_day(check_dt)

    if not is_trading and not force:
        return {
            "ts_utc": now_utc.isoformat(),
            "report_date": effective_date.isoformat(),
            "overall_status": "skipped",
            "is_trading_day": False,
            "checks": [],
            "summary": {"ok": 0, "warn": 0, "fail": 0, "skipped": 0},
            "message": f"{effective_date} is not a trading day. Use --force to run anyway.",
        }

    checks: list[dict[str, Any]] = []

    # 1. Cross-feed completeness
    checks.append(_check_cross_feed_completeness(effective_date, active_settings))

    # 2. Silver value invariants
    if not skip_invariants:
        checks.append(_check_silver_invariants(effective_date, active_settings))

    # 3. Gold freshness
    checks.append(_check_gold_freshness(effective_date, active_settings))

    summary = {"ok": 0, "warn": 0, "fail": 0, "skipped": 0}
    for check in checks:
        summary[check["status"]] += 1

    report = {
        "ts_utc": now_utc.isoformat(),
        "report_date": effective_date.isoformat(),
        "overall_status": _overall_status(summary),
        "is_trading_day": is_trading,
        "checks": checks,
        "summary": summary,
    }

    logger.info(
        "daily_health_report_generated",
        report_date=effective_date.isoformat(),
        overall_status=report["overall_status"],
        summary=summary,
    )

    return report


def write_daily_report(report: dict[str, Any], report_dir: Path) -> Path:
    """Write report JSON to the report directory."""
    report_dir.mkdir(parents=True, exist_ok=True)
    report_date = report.get("report_date", date.today().isoformat())
    output = report_dir / f"{report_date}.json"
    output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return output


def run_daily_health(
    report_date: date | None = None,
    *,
    force: bool = False,
    verbose: bool = False,
    settings: Settings | None = None,
) -> dict[str, Any]:
    """Run daily health check and write report.

    Called from CLI. Generates report, writes to disk, prints summary.
    """
    active_settings = settings or get_settings()
    report = generate_daily_health_report(
        report_date=report_date,
        settings=active_settings,
        force=force,
    )

    try:
        path = write_daily_report(report, active_settings.daily_health_report_dir)
        logger.info("daily_health_report_written", path=str(path))
    except OSError as exc:
        logger.warning("daily_health_report_write_failed", error=str(exc))

    return report
