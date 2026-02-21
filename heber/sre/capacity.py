"""Capacity Planning (PRD §42).

Resource metrics, scaling triggers, and capacity forecasting.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class ResourceType(str, Enum):
    """Resource types for capacity planning."""

    CPU = "cpu"
    MEMORY = "memory"
    STORAGE = "storage"
    CONNECTIONS = "connections"
    IOPS = "iops"


class ScalingAction(str, Enum):
    """Scaling actions."""

    SCALE_UP = "scale_up"
    SCALE_OUT = "scale_out"
    NO_ACTION = "no_action"
    REPARTITION = "repartition"


@dataclass
class BaselineMetric:
    """Baseline capacity metric (PRD §42.1)."""

    name: str
    value: float
    unit: str
    source: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "unit": self.unit,
            "source": self.source,
        }


@dataclass
class ScalingTrigger:
    """Scaling trigger threshold (PRD §42.2)."""

    metric: str
    threshold: float
    unit: str
    sustained_minutes: int
    action: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "threshold": self.threshold,
            "unit": self.unit,
            "sustained_minutes": self.sustained_minutes,
            "action": self.action,
        }

    def is_triggered(self, current_value: float) -> bool:
        """Check if current value exceeds threshold."""
        return current_value > self.threshold


@dataclass
class CapacityForecast:
    """Capacity forecast entry (PRD §42.3)."""

    quarter: str
    events_per_day: int
    storage_tb: float
    compute_nodes: int
    growth_percent: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "quarter": self.quarter,
            "events_per_day": self.events_per_day,
            "events_per_day_fmt": f"{self.events_per_day / 1_000_000:.0f}M",
            "storage_tb": self.storage_tb,
            "compute_nodes": self.compute_nodes,
            "growth_percent": f"+{self.growth_percent:.0f}%" if self.growth_percent > 0 else "baseline",
        }


@dataclass
class BottleneckAnalysis:
    """Component bottleneck analysis (PRD §42.4)."""

    component: str
    cpu_bound: str  # "Low", "Medium", "High", "Very High"
    memory_bound: str
    io_bound: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component,
            "cpu_bound": self.cpu_bound,
            "memory_bound": self.memory_bound,
            "io_bound": self.io_bound,
            "notes": self.notes,
        }


@dataclass
class CostProjection:
    """Cost projection for capacity change (PRD §42.5)."""

    scenario: str
    base_cost_monthly: float
    projected_cost_monthly: float
    delta: float
    delta_percent: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "base_cost": f"${self.base_cost_monthly:,.0f}/month",
            "projected_cost": f"${self.projected_cost_monthly:,.0f}/month",
            "delta": f"+${self.delta:,.0f}",
            "delta_percent": f"+{self.delta_percent:.0f}%",
        }


# Default baselines from PRD §42.1
DEFAULT_BASELINES: list[BaselineMetric] = [
    BaselineMetric("events_per_day", 50_000_000, "events", "bars + quotes + trades"),
    BaselineMetric("peak_events_per_sec", 10_000, "events/sec", "Market open"),
    BaselineMetric("silver_storage_per_day", 5, "GB", "Parquet, compressed"),
    BaselineMetric("silver_storage_per_year", 1.8, "TB", ""),
]

# Default scaling triggers from PRD §42.2
DEFAULT_TRIGGERS: list[ScalingTrigger] = [
    ScalingTrigger("consumer_cpu", 70, "%", 15, "Add consumer replicas"),
    ScalingTrigger("consumer_lag", 60, "seconds", 10, "Add consumer replicas"),
    ScalingTrigger("writer_memory", 80, "%", 5, "Increase memory limit"),
    ScalingTrigger("compactor_duration", 30, "minutes/partition", 1, "Increase CPU/memory"),
    ScalingTrigger("rds_connections", 80, "% of max", 5, "Increase max_connections or scale"),
    ScalingTrigger("s3_request_rate", 3500, "requests/sec/prefix", 1, "Re-partition prefixes"),
]

# Default forecasts from PRD §42.3
DEFAULT_FORECASTS: list[CapacityForecast] = [
    CapacityForecast("Q1 2026", 50_000_000, 1.8, 6, 0),
    CapacityForecast("Q2 2026", 75_000_000, 2.7, 8, 50),
    CapacityForecast("Q3 2026", 100_000_000, 3.6, 10, 33),
    CapacityForecast("Q4 2026", 150_000_000, 5.4, 12, 50),
]

# Default bottleneck analysis from PRD §42.4
DEFAULT_BOTTLENECKS: list[BottleneckAnalysis] = [
    BottleneckAnalysis("Consumer", "Medium", "High", "Low", "bloom filter memory"),
    BottleneckAnalysis("Writer", "Low", "High", "High", "batch buffer, S3 writes"),
    BottleneckAnalysis("Compactor", "High", "Very High", "High", "S3 reads/writes"),
    BottleneckAnalysis("Catalog", "Low", "Low", "Medium", "Postgres queries"),
]


class CapacityPlanner:
    """Capacity planning and forecasting."""

    def __init__(
        self,
        baselines: list[BaselineMetric] | None = None,
        triggers: list[ScalingTrigger] | None = None,
        forecasts: list[CapacityForecast] | None = None,
        bottlenecks: list[BottleneckAnalysis] | None = None,
    ):
        self.baselines = {b.name: b for b in (baselines or DEFAULT_BASELINES)}
        self.triggers = {t.metric: t for t in (triggers or DEFAULT_TRIGGERS)}
        self.forecasts = forecasts or DEFAULT_FORECASTS
        self.bottlenecks = {b.component: b for b in (bottlenecks or DEFAULT_BOTTLENECKS)}

    def get_baseline(self, name: str) -> BaselineMetric | None:
        """Get baseline metric by name."""
        return self.baselines.get(name)

    def check_triggers(
        self,
        current_metrics: dict[str, float],
    ) -> list[tuple[str, ScalingTrigger]]:
        """Check which scaling triggers are activated.

        Args:
            current_metrics: Dict of metric_name -> current_value

        Returns:
            List of (metric_name, trigger) for triggered thresholds
        """
        triggered = []
        for metric_name, value in current_metrics.items():
            trigger = self.triggers.get(metric_name)
            if trigger and trigger.is_triggered(value):
                triggered.append((metric_name, trigger))
        return triggered

    def get_scaling_recommendations(
        self,
        current_metrics: dict[str, float],
    ) -> list[dict[str, Any]]:
        """Get scaling recommendations based on current metrics."""
        triggered = self.check_triggers(current_metrics)
        return [
            {
                "metric": metric_name,
                "current_value": current_metrics[metric_name],
                "threshold": trigger.threshold,
                "unit": trigger.unit,
                "action": trigger.action,
                "urgency": "immediate" if trigger.sustained_minutes <= 5 else "soon",
            }
            for metric_name, trigger in triggered
        ]

    def project_cost(
        self,
        volume_multiplier: float,
        base_cost: float = 1575.0,
    ) -> CostProjection:
        """Project cost for volume increase.

        Based on PRD §42.5 cost scaling model.
        """
        # Cost components at 3x volume:
        # EKS: 540 -> 1080 (+540)
        # S3: 40 -> 120 (+80)
        # RDS: 200 -> 400 (+200)
        # ClickHouse: 330 -> 660 (+330)
        # Total +$1150 at 3x

        # Linear approximation: ~$383/x increase above 1x
        cost_per_multiplier = 383
        additional_cost = (volume_multiplier - 1) * cost_per_multiplier
        projected = base_cost + additional_cost

        return CostProjection(
            scenario=f"{volume_multiplier}x volume",
            base_cost_monthly=base_cost,
            projected_cost_monthly=projected,
            delta=additional_cost,
            delta_percent=(additional_cost / base_cost) * 100,
        )

    def get_bottleneck(self, component: str) -> BottleneckAnalysis | None:
        """Get bottleneck analysis for a component."""
        return self.bottlenecks.get(component)

    def generate_report(self) -> dict[str, Any]:
        """Generate comprehensive capacity report."""
        return {
            "baselines": [b.to_dict() for b in self.baselines.values()],
            "triggers": [t.to_dict() for t in self.triggers.values()],
            "forecasts": [f.to_dict() for f in self.forecasts],
            "bottlenecks": [b.to_dict() for b in self.bottlenecks.values()],
            "cost_projections": [
                self.project_cost(1.5).to_dict(),
                self.project_cost(2.0).to_dict(),
                self.project_cost(3.0).to_dict(),
            ],
            "generated_at": datetime.now(UTC).isoformat(),
        }
