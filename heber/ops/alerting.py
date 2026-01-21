"""Alerting rules for Heber per PRD §12.5.4.

Provides Prometheus alerting rules as YAML and Python config.
"""

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    """Alert severity levels."""

    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AlertRule:
    """Prometheus alerting rule definition."""

    name: str
    expr: str
    for_duration: str
    severity: Severity
    summary: str
    description: str

    def to_yaml_dict(self) -> dict:
        """Convert to Prometheus rules YAML format."""
        return {
            "alert": self.name,
            "expr": self.expr,
            "for": self.for_duration,
            "labels": {"severity": self.severity.value},
            "annotations": {
                "summary": self.summary,
                "description": self.description,
            },
        }


# Per PRD §12.5.4 Alerting Thresholds
ALERTING_RULES = [
    AlertRule(
        name="HeberConsumerLagHigh",
        expr="heber_consumer_lag_seconds > 60",
        for_duration="5m",
        severity=Severity.WARNING,
        summary="Heber consumer lag is high",
        description="Consumer {{ $labels.stream }} has lag > 60s for 5 minutes. Current: {{ $value }}s",
    ),
    AlertRule(
        name="HeberConsumerLagCritical",
        expr="heber_consumer_lag_seconds > 300",
        for_duration="5m",
        severity=Severity.CRITICAL,
        summary="Heber consumer lag is critical",
        description="Consumer {{ $labels.stream }} has lag > 300s for 5 minutes. Current: {{ $value }}s",
    ),
    AlertRule(
        name="HeberWriteErrorRateHigh",
        expr="rate(heber_writer_errors_total[5m]) > 0.01",
        for_duration="5m",
        severity=Severity.WARNING,
        summary="Heber write error rate is high",
        description="Write errors in {{ $labels.layer }} exceeding 1% for 5 minutes",
    ),
    AlertRule(
        name="HeberHotStoreLagHigh",
        expr="heber_hotstore_lag_seconds > 300",
        for_duration="5m",
        severity=Severity.WARNING,
        summary="Hot Store sync lag is high",
        description="Hot Store {{ $labels.dataset }} has sync lag > 300s for 5 minutes. Current: {{ $value }}s",
    ),
    AlertRule(
        name="HeberAvailabilityLagSpike",
        expr="histogram_quantile(0.99, rate(heber_availability_lag_seconds_bucket[5m])) > 30",
        for_duration="5m",
        severity=Severity.WARNING,
        summary="Availability lag p99 is elevated",
        description="P99 availability lag exceeds 30s, potential data freshness issue",
    ),
    AlertRule(
        name="HeberDLQGrowing",
        expr="rate(heber_dlq_events_total[5m]) > 0",
        for_duration="10m",
        severity=Severity.WARNING,
        summary="Dead-letter queue is growing",
        description="DLQ events are accumulating for {{ $labels.feed }}. Check for processing errors.",
    ),
    AlertRule(
        name="HeberCatalogDown",
        expr='up{job="heber-catalog"} == 0',
        for_duration="1m",
        severity=Severity.CRITICAL,
        summary="Heber Catalog API is down",
        description="Catalog service is not responding. SDK lookups will fail.",
    ),
    AlertRule(
        name="HeberCompactionFailed",
        expr='heber_compactor_runs_total{status="error"} > 0',
        for_duration="5m",
        severity=Severity.WARNING,
        summary="Compaction job failed",
        description="Compaction for {{ $labels.dataset }} has failed. Small files may accumulate.",
    ),
]


def generate_prometheus_rules_yaml() -> str:
    """Generate Prometheus alerting rules YAML file content."""
    import yaml

    rules_file = {
        "groups": [
            {
                "name": "heber.rules",
                "rules": [rule.to_yaml_dict() for rule in ALERTING_RULES],
            }
        ]
    }
    return yaml.dump(rules_file, default_flow_style=False, sort_keys=False)


def get_alerting_rules() -> list[AlertRule]:
    """Get all alerting rules."""
    return ALERTING_RULES


# Dashboard specifications (PRD §12.5.7)
DASHBOARDS = {
    "heber-overview": {
        "title": "Heber Overview Dashboard",
        "description": "High-level view of Heber Data Lakehouse health",
        "panels": [
            {
                "title": "Consumer Lag (all streams)",
                "query": "heber_consumer_lag_seconds",
                "type": "gauge",
            },
            {
                "title": "Events Processed Rate (by feed)",
                "query": "rate(heber_consumer_events_processed_total{status='success'}[5m])",
                "type": "graph",
            },
            {
                "title": "Write Throughput (rows/sec)",
                "query": "rate(heber_writer_rows_written_total[5m])",
                "type": "graph",
            },
            {
                "title": "Write Throughput (bytes/sec)",
                "query": "rate(heber_writer_bytes_written_total[5m])",
                "type": "graph",
            },
            {
                "title": "Error Rate (by type)",
                "query": "rate(heber_writer_errors_total[5m])",
                "type": "graph",
            },
            {
                "title": "Hot Store Sync Lag",
                "query": "heber_hotstore_lag_seconds",
                "type": "gauge",
            },
        ],
    },
    "heber-latency": {
        "title": "Heber Latency Dashboard",
        "description": "Latency metrics for anti-leakage monitoring",
        "panels": [
            {
                "title": "Ingest Lag (p50/p95/p99)",
                "query": "histogram_quantile(0.XX, rate(heber_ingest_lag_seconds_bucket[5m]))",
                "type": "heatmap",
            },
            {
                "title": "Availability Lag (p50/p95/p99)",
                "query": "histogram_quantile(0.XX, rate(heber_availability_lag_seconds_bucket[5m]))",
                "type": "heatmap",
            },
            {
                "title": "Write Duration (p50/p95/p99)",
                "query": "histogram_quantile(0.XX, rate(heber_writer_flush_duration_seconds_bucket[5m]))",
                "type": "heatmap",
            },
            {
                "title": "API Response Time (p50/p95/p99)",
                "query": "histogram_quantile(0.XX, rate(heber_catalog_request_duration_seconds_bucket[5m]))",
                "type": "heatmap",
            },
        ],
    },
    "heber-health": {
        "title": "Heber Health Dashboard",
        "description": "Service health and operational status",
        "panels": [
            {
                "title": "Service Health (up/down)",
                "query": 'up{job=~"heber-.*"}',
                "type": "stat",
            },
            {
                "title": "DLQ Growth Rate",
                "query": "rate(heber_dlq_events_total[5m])",
                "type": "graph",
            },
            {
                "title": "Compaction Status",
                "query": 'heber_compactor_runs_total{status="success"}',
                "type": "stat",
            },
            {
                "title": "Catalog Connection Pool",
                "query": "heber_catalog_db_connections_active",
                "type": "gauge",
            },
        ],
    },
}


def get_dashboard_specs() -> dict:
    """Get dashboard specifications for Grafana import."""
    return DASHBOARDS
