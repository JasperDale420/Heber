"""Daily end-of-day health report for Heber data lakehouse.

Runs 3 checks after market close to verify that the day's data landed correctly:
1. Cross-feed completeness — instrument overlap across feeds
2. Soda quality — SodaCL checks on Silver data
3. Gold freshness — Gold dt= partitions exist for expected datasets

NOTE: Partition freshness, fill rate, zero-leakage, and DLQ status checks were
removed in favour of heber.health_monitor, which provides richer alerting and
persistence for those checks.  See checks/partition.py, checks/volume.py,
checks/ml_readiness.py, and checks/stream_health.py.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from heber.calendar import MarketCalendar
from heber.config import Settings, get_settings

logger = structlog.get_logger(__name__)


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
        partition = settings.silver_path / f"feed={feed}" / f"dt={dt_str}"
        if not partition.exists():
            continue
        parquet_files = list(partition.glob("*.parquet"))
        if not parquet_files:
            continue
        try:
            df = pd.read_parquet(partition)
            if "instrument_key" in df.columns:
                feed_symbols[feed] = set(df["instrument_key"].dropna().unique())
            elif "symbol" in df.columns:
                feed_symbols[feed] = set(df["symbol"].dropna().unique())
        except Exception:
            logger.warning("cross_feed_read_failed", feed=feed, date=dt_str, exc_info=True)

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


def _check_soda_quality(settings: Settings) -> dict[str, Any]:
    """Run Soda quality scans for all datasets with check files."""
    try:
        from heber.quality.soda_scanner import SodaQualityScanner

        scanner = SodaQualityScanner()
        results = scanner.scan_all()

        total_passed = sum(r.passed for r in results.values())
        total_failed = sum(r.failed for r in results.values())
        total_errors = sum(r.errors for r in results.values())

        status = "ok"
        if total_errors > 0 or total_failed > 0:
            status = "fail"

        return _check_result(
            "soda_quality",
            status=status,
            severity="critical",
            observed={
                "datasets_scanned": list(results.keys()),
                "total_passed": total_passed,
                "total_failed": total_failed,
                "total_errors": total_errors,
            },
            threshold={"max_failures": 0},
            message=(
                f"Soda: {total_passed} passed, {total_failed} failed, "
                f"{total_errors} errors across {len(results)} datasets."
            ),
        )
    except Exception as exc:
        return _check_result(
            "soda_quality",
            status="warn",
            severity="critical",
            observed={"error": str(exc)},
            threshold={"max_failures": 0},
            message=f"Soda scan could not run: {exc}",
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
    skip_soda: bool = False,
) -> dict[str, Any]:
    """Generate a daily health report for the given date.

    Args:
        report_date: Date to report on (default: today).
        settings: Heber settings (default: from env).
        force: Run even on non-trading days.
        skip_soda: Skip Soda quality scan (for tests without Soda installed).

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

    # 2. Soda quality
    if not skip_soda:
        checks.append(_check_soda_quality(active_settings))

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
