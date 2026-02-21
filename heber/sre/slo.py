"""SLO Framework (PRD §37).

Service Level Objectives, Indicators, and Burn Rate management.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class SLOWindow(str, Enum):
    """SLO measurement windows."""

    WINDOW_1H = "1h"
    WINDOW_6H = "6h"
    WINDOW_1D = "1d"
    WINDOW_3D = "3d"
    WINDOW_7D = "7d"
    WINDOW_30D = "30d"

    def to_timedelta(self) -> timedelta:
        mapping = {
            "1h": timedelta(hours=1),
            "6h": timedelta(hours=6),
            "1d": timedelta(days=1),
            "3d": timedelta(days=3),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30),
        }
        return mapping[self.value]


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class SLI:
    """Service Level Indicator (PRD §37.1).

    Attributes:
        name: Indicator name (e.g., "ingestion_availability")
        metric_query: PromQL-style query for the metric
        description: Human-readable description
    """

    name: str
    metric_query: str
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "metric_query": self.metric_query,
            "description": self.description,
        }


@dataclass
class SLO:
    """Service Level Objective (PRD §37.1).

    Attributes:
        name: SLO name (e.g., "Ingestion Availability")
        sli: The indicator being measured
        target: Target percentage (e.g., 0.999 for 99.9%)
        window: Measurement window
    """

    name: str
    sli: SLI
    target: float  # 0.999 = 99.9%
    window: SLOWindow

    @property
    def target_percentage(self) -> float:
        """Return target as percentage."""
        return self.target * 100

    @property
    def error_budget_ratio(self) -> float:
        """Return allowed error ratio (1 - target)."""
        return 1 - self.target

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "sli": self.sli.to_dict(),
            "target": self.target,
            "target_percentage": f"{self.target_percentage:.2f}%",
            "error_budget": f"{self.error_budget_ratio * 100:.3f}%",
            "window": self.window.value,
        }


@dataclass
class BurnRateAlert:
    """Burn rate alert configuration (PRD §37.4).

    Burn rate = actual error rate / allowed error rate
    14x burn rate means consuming 14 hours of budget per hour.
    """

    burn_rate: float  # e.g., 14 for 14x
    window: SLOWindow
    severity: AlertSeverity
    action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "burn_rate": f"{self.burn_rate}x",
            "window": self.window.value,
            "severity": self.severity.value,
            "action": self.action,
        }

    def to_prometheus_rule(self, slo: SLO) -> dict[str, Any]:
        """Generate Prometheus alerting rule."""
        threshold = self.burn_rate * slo.error_budget_ratio
        return {
            "alert": f"Heber{slo.name.replace(' ', '')}BurnRate{self.severity.value.title()}",
            "expr": f"({slo.sli.metric_query}) > {threshold}",
            "for": "5m",
            "labels": {"severity": self.severity.value},
            "annotations": {
                "summary": f"{slo.name} SLO burning at {self.burn_rate}x rate",
                "description": f"Current burn rate exceeds {self.burn_rate}x threshold over {self.window.value}",
            },
        }


@dataclass
class SLOStatus:
    """Current status of an SLO."""

    slo: SLO
    current_value: float  # Current SLI value (0-1)
    budget_remaining: float  # Remaining error budget (0-1)
    burn_rate: float  # Current burn rate (1x = normal)
    is_healthy: bool
    measured_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "slo_name": self.slo.name,
            "target": f"{self.slo.target_percentage:.2f}%",
            "current": f"{self.current_value * 100:.2f}%",
            "budget_remaining": f"{self.budget_remaining * 100:.1f}%",
            "burn_rate": f"{self.burn_rate:.1f}x",
            "is_healthy": self.is_healthy,
            "measured_at": self.measured_at.isoformat(),
        }


# Default SLO definitions from PRD §37.1
DEFAULT_SLIS: dict[str, SLI] = {
    "ingestion_availability": SLI(
        name="ingestion_availability",
        metric_query=(
            'sum(rate(heber_consumer_events_processed_total{status="success"}[30d])) '
            "/ sum(rate(heber_consumer_events_processed_total[30d]))"
        ),
        description="Ratio of successfully processed events to total events",
    ),
    "write_success_rate": SLI(
        name="write_success_rate",
        metric_query="heber_writer_rows_written_total / heber_writer_rows_attempted_total",
        description="Ratio of successfully written rows to attempted rows",
    ),
    "read_latency_p99": SLI(
        name="read_latency_p99",
        metric_query='heber_sdk_read_latency_seconds{quantile="0.99"}',
        description="99th percentile read latency in seconds",
    ),
    "data_freshness": SLI(
        name="data_freshness",
        metric_query="max by (dataset) (time() - heber_dataset_latest_ts_available_timestamp_seconds)",
        description="Seconds since latest data became available",
    ),
    "catalog_availability": SLI(
        name="catalog_availability",
        metric_query='up{job="heber-catalog"}',
        description="Catalog service availability",
    ),
    "catalog_latency_p99": SLI(
        name="catalog_latency_p99",
        metric_query='heber_catalog_request_duration_seconds{quantile="0.99"}',
        description="99th percentile catalog request latency",
    ),
}

DEFAULT_SLOS: list[SLO] = [
    SLO(
        name="Ingestion Availability",
        sli=DEFAULT_SLIS["ingestion_availability"],
        target=0.999,  # 99.9%
        window=SLOWindow.WINDOW_30D,
    ),
    SLO(
        name="Write Success Rate",
        sli=DEFAULT_SLIS["write_success_rate"],
        target=0.9995,  # 99.95%
        window=SLOWindow.WINDOW_30D,
    ),
    SLO(
        name="Catalog Availability",
        sli=DEFAULT_SLIS["catalog_availability"],
        target=0.999,  # 99.9%
        window=SLOWindow.WINDOW_30D,
    ),
]

# Default burn rate alerts from PRD §37.4
DEFAULT_BURN_RATE_ALERTS: list[BurnRateAlert] = [
    BurnRateAlert(
        burn_rate=14,
        window=SLOWindow.WINDOW_1H,
        severity=AlertSeverity.CRITICAL,
        action="Page on-call",
    ),
    BurnRateAlert(
        burn_rate=6,
        window=SLOWindow.WINDOW_6H,
        severity=AlertSeverity.WARNING,
        action="Notify in Slack",
    ),
    BurnRateAlert(
        burn_rate=3,
        window=SLOWindow.WINDOW_1D,
        severity=AlertSeverity.INFO,
        action="Review in standup",
    ),
    BurnRateAlert(
        burn_rate=1,
        window=SLOWindow.WINDOW_3D,
        severity=AlertSeverity.INFO,
        action="Track in weekly review",
    ),
]


class SLOManager:
    """Manage SLOs and calculate status."""

    def __init__(
        self,
        slos: list[SLO] | None = None,
        burn_rate_alerts: list[BurnRateAlert] | None = None,
    ):
        self.slos = {slo.name: slo for slo in (slos or DEFAULT_SLOS)}
        self.burn_rate_alerts = burn_rate_alerts or DEFAULT_BURN_RATE_ALERTS

    def get_slo(self, name: str) -> SLO | None:
        """Get SLO by name."""
        return self.slos.get(name)

    def list_slos(self) -> list[dict[str, Any]]:
        """List all SLOs."""
        return [slo.to_dict() for slo in self.slos.values()]

    def calculate_status(
        self,
        slo_name: str,
        current_value: float,
        error_count: int,
        total_count: int,
        _window_hours: float = 720,  # 30 days - reserved for future granular calculations
    ) -> SLOStatus:
        """Calculate SLO status from current metrics.

        Args:
            slo_name: Name of the SLO
            current_value: Current SLI value (0-1)
            error_count: Number of errors in window
            total_count: Total requests in window
            window_hours: Window size in hours

        Returns:
            SLOStatus with calculated metrics
        """
        slo = self.slos.get(slo_name)
        if not slo:
            raise ValueError(f"SLO not found: {slo_name}")

        # Calculate error budget
        allowed_errors = total_count * slo.error_budget_ratio
        budget_consumed = error_count / allowed_errors if allowed_errors > 0 else 1.0
        budget_remaining = max(0, 1 - budget_consumed)

        # Calculate burn rate (errors per hour vs budget per hour)
        actual_error_rate = error_count / total_count if total_count > 0 else 0
        burn_rate = actual_error_rate / slo.error_budget_ratio if slo.error_budget_ratio > 0 else 0

        # Determine health
        is_healthy = budget_remaining > 0.25 and current_value >= slo.target

        return SLOStatus(
            slo=slo,
            current_value=current_value,
            budget_remaining=budget_remaining,
            burn_rate=burn_rate,
            is_healthy=is_healthy,
        )

    def generate_prometheus_rules(self) -> list[dict[str, Any]]:
        """Generate Prometheus alerting rules for all SLOs."""
        rules = []
        for slo in self.slos.values():
            for alert in self.burn_rate_alerts:
                rules.append(alert.to_prometheus_rule(slo))
        return rules
