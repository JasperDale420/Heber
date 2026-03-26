"""Prometheus metrics for the data health monitor."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

from heber.ops.metrics import _get_or_create

health_check_status = _get_or_create(
    Gauge,
    "heber_health_check_status",
    "Current health check status (1=pass, 0=fail)",
    ["check_name", "feed"],
)
health_check_duration_seconds = _get_or_create(
    Histogram,
    "heber_health_check_duration_seconds",
    "Health check execution duration",
    ["check_name"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
)
health_checks_total = _get_or_create(
    Counter,
    "heber_health_checks_total",
    "Total health checks executed",
    ["check_name", "status"],
)
health_gap_hours = _get_or_create(
    Gauge,
    "heber_health_gap_hours",
    "Longest detected data gap in hours",
    ["feed"],
)
health_volume_ratio = _get_or_create(
    Gauge,
    "heber_health_volume_ratio",
    "Today vs baseline row count ratio",
    ["feed"],
)
health_null_rate = _get_or_create(
    Gauge,
    "heber_health_null_rate",
    "Current null percentage per column",
    ["feed", "column"],
)
health_schema_changes_total = _get_or_create(
    Counter,
    "heber_health_schema_changes_total",
    "Schema change events detected",
    ["feed"],
)
health_leakage_violations = _get_or_create(
    Gauge,
    "heber_health_leakage_violations",
    "Count of ts_available < ts_event violations",
    ["dataset"],
)


def record_check(check_name: str, feed: str, status: str, duration: float) -> None:
    """Record a health check execution with status and duration."""
    status_val = 1.0 if status == "pass" else 0.0
    health_check_status.labels(check_name=check_name, feed=feed or "").set(status_val)
    health_check_duration_seconds.labels(check_name=check_name).observe(duration)
    health_checks_total.labels(check_name=check_name, status=status).inc()
