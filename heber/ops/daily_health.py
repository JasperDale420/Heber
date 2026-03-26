"""Daily end-of-day health report for Heber data lakehouse.

Runs 7 checks after market close to verify that the day's data landed correctly:
1. Partition freshness — Silver dt= dirs exist for expected feeds
2. Cross-feed completeness — instrument overlap across feeds
3. Soda quality — SodaCL checks on Silver data
4. Fill rate — distinct symbol count vs expected
5. Zero-leakage audit — ts_available <= ts_event sampling
6. DLQ status — Redis dead-letter queue depth
7. Gold freshness — Gold dt= partitions exist for expected datasets
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import redis
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


def _check_partition_freshness(
    report_date: date,
    settings: Settings,
) -> list[dict[str, Any]]:
    """Check that Silver dt= partitions exist for each expected feed."""
    dt_str = report_date.isoformat()
    checks: list[dict[str, Any]] = []

    for feed in settings.daily_health_expected_feeds:
        partition = settings.silver_path / f"feed={feed}" / f"dt={dt_str}"
        exists = partition.exists() and any(partition.iterdir()) if partition.exists() else False

        checks.append(
            _check_result(
                f"partition_freshness_{feed}",
                status="ok" if exists else "fail",
                severity="critical" if feed in ("bars", "quotes") else "warning",
                observed={"partition": str(partition), "exists": exists},
                threshold={"required": True},
                message=f"{feed} partition for {dt_str} {'found' if exists else 'MISSING'}.",
            )
        )

    return checks


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


def _check_fill_rate(
    report_date: date,
    settings: Settings,
) -> dict[str, Any]:
    """Count distinct symbols in bars partition vs expected count."""
    dt_str = report_date.isoformat()
    partition = settings.silver_path / "feed=bars" / f"dt={dt_str}"

    if not partition.exists():
        return _check_result(
            "fill_rate",
            status="fail",
            severity="warning",
            observed={"distinct_symbols": 0},
            threshold={"expected_min": settings.daily_health_expected_symbol_count},
            message=f"No bars partition for {dt_str}.",
        )

    try:
        df = pd.read_parquet(partition)
        col = "instrument_key" if "instrument_key" in df.columns else "symbol"
        distinct = df[col].nunique() if col in df.columns else 0
    except Exception:
        logger.warning("fill_rate_partition_read_failed", feed="bars", date=dt_str, exc_info=True)
        distinct = 0

    expected = settings.daily_health_expected_symbol_count
    status = "ok" if distinct >= expected else "warn"

    return _check_result(
        "fill_rate",
        status=status,
        severity="warning",
        observed={"distinct_symbols": distinct},
        threshold={"expected_min": expected},
        message=f"{distinct} distinct symbols vs {expected} expected.",
    )


def _check_zero_leakage(
    report_date: date,
    settings: Settings,
    sample_size: int = 1000,
) -> dict[str, Any]:
    """Sample rows and verify ts_available >= ts_event for all."""
    dt_str = report_date.isoformat()
    violations = 0
    sampled = 0

    for feed in ("bars", "quotes"):
        partition = settings.silver_path / f"feed={feed}" / f"dt={dt_str}"
        if not partition.exists():
            continue
        try:
            df = pd.read_parquet(partition)
            if "ts_available" not in df.columns or "ts_event" not in df.columns:
                continue
            sample = df.sample(n=min(sample_size, len(df)), random_state=42) if len(df) > sample_size else df
            sampled += len(sample)
            violations += int((sample["ts_available"] < sample["ts_event"]).sum())
        except Exception:
            logger.warning("zero_leakage_read_failed", feed=feed, date=dt_str, exc_info=True)

    if sampled == 0:
        return _check_result(
            "zero_leakage",
            status="warn",
            severity="critical",
            observed={"sampled": 0, "violations": 0},
            threshold={"max_violations": 0},
            message="No data available to audit for zero-leakage.",
        )

    return _check_result(
        "zero_leakage",
        status="ok" if violations == 0 else "fail",
        severity="critical",
        observed={"sampled": sampled, "violations": violations},
        threshold={"max_violations": 0},
        message=f"Zero-leakage: {violations} violations in {sampled} sampled rows.",
    )


def _check_dlq_status(settings: Settings) -> dict[str, Any]:
    """Check Redis dead-letter queue depth."""
    try:
        client = redis.Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=False,
        )
        try:
            length = int(client.xlen(settings.redis_dlq_stream_name))  # type: ignore[arg-type]
        except redis.ResponseError:
            length = 0
        finally:
            client.close()

        return _check_result(
            "dlq_status",
            status="ok" if length == 0 else "warn",
            severity="warning",
            observed={"dlq_length": length},
            threshold={"max_length": 0},
            message=f"DLQ has {length} message(s)." if length > 0 else "DLQ is empty.",
        )
    except Exception as exc:
        return _check_result(
            "dlq_status",
            status="warn",
            severity="warning",
            observed={"error": str(exc)},
            threshold={"max_length": 0},
            message=f"Could not check DLQ: {exc}",
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
    skip_dlq: bool = False,
) -> dict[str, Any]:
    """Generate a daily health report for the given date.

    Args:
        report_date: Date to report on (default: today).
        settings: Heber settings (default: from env).
        force: Run even on non-trading days.
        skip_soda: Skip Soda quality scan (for tests without Soda installed).
        skip_dlq: Skip DLQ check (for tests without Redis).

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

    # 1. Partition freshness
    checks.extend(_check_partition_freshness(effective_date, active_settings))

    # 2. Cross-feed completeness
    checks.append(_check_cross_feed_completeness(effective_date, active_settings))

    # 3. Soda quality
    if not skip_soda:
        checks.append(_check_soda_quality(active_settings))

    # 4. Fill rate
    checks.append(_check_fill_rate(effective_date, active_settings))

    # 5. Zero-leakage
    checks.append(_check_zero_leakage(effective_date, active_settings))

    # 6. DLQ status
    if not skip_dlq:
        checks.append(_check_dlq_status(active_settings))

    # 7. Gold freshness
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
