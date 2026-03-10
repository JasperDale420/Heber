"""Performance Testing (PRD §51).

Performance benchmarks and regression detection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class PerformanceSLO:
    """Performance SLO (PRD §51.1)."""

    name: str
    metric: str
    target: float
    unit: str
    test_scenario: str

    def is_met(self, measured: float) -> bool:
        """Check if the SLO is met."""
        # For latency, lower is better
        if "latency" in self.metric.lower() or "time" in self.metric.lower():
            return measured <= self.target
        # For throughput, higher is better
        return measured >= self.target

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "metric": self.metric,
            "target": f"{self.target} {self.unit}",
            "test_scenario": self.test_scenario,
        }


# Default performance SLOs from PRD §51.1
DEFAULT_PERFORMANCE_SLOS: list[PerformanceSLO] = [
    PerformanceSLO(
        name="Ingestion Throughput",
        metric="events_per_second",
        target=10000,
        unit="events/sec",
        test_scenario="Sustained load test",
    ),
    PerformanceSLO(
        name="Write Latency (p99)",
        metric="write_latency_p99",
        target=5.0,
        unit="seconds",
        test_scenario="Batch write benchmark",
    ),
    PerformanceSLO(
        name="Read Latency (p99)",
        metric="read_latency_p99",
        target=0.5,
        unit="seconds",
        test_scenario="SDK read benchmark",
    ),
    PerformanceSLO(
        name="Compaction Time",
        metric="compaction_time",
        target=30.0,
        unit="minutes/partition",
        test_scenario="Compaction benchmark",
    ),
]


@dataclass
class LoadTestScenario:
    """Load test scenario definition (PRD §51.2)."""

    name: str
    events_per_second: int
    duration_minutes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "events_per_second": self.events_per_second,
            "duration_minutes": self.duration_minutes,
        }


# Default load test scenarios from PRD §51.2
DEFAULT_LOAD_SCENARIOS: list[LoadTestScenario] = [
    LoadTestScenario("Baseline", 1000, 10),
    LoadTestScenario("Normal Load", 5000, 30),
    LoadTestScenario("Peak Load", 10000, 15),
    LoadTestScenario("Burst", 20000, 5),
    LoadTestScenario("Sustained", 5000, 240),
]


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""

    benchmark_name: str
    metric: str
    value: float
    unit: str
    run_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark_name": self.benchmark_name,
            "metric": self.metric,
            "value": self.value,
            "unit": self.unit,
            "run_at": self.run_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class RegressionDetection:
    """Regression detection result."""

    metric: str
    current_value: float
    baseline_value: float
    threshold_percent: float
    is_regression: bool
    change_percent: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "current_value": self.current_value,
            "baseline_value": self.baseline_value,
            "threshold_percent": f"{self.threshold_percent}%",
            "is_regression": self.is_regression,
            "change_percent": f"{self.change_percent:+.1f}%",
        }


class PerformanceTester:
    """Performance testing and regression detection."""

    def __init__(
        self,
        slos: list[PerformanceSLO] | None = None,
        load_scenarios: list[LoadTestScenario] | None = None,
    ):
        self.slos = {s.name: s for s in (slos or DEFAULT_PERFORMANCE_SLOS)}
        self.load_scenarios = {s.name: s for s in (load_scenarios or DEFAULT_LOAD_SCENARIOS)}
        self.baseline: dict[str, float] = {}
        self.results: list[BenchmarkResult] = []

    def set_baseline(self, metric: str, value: float) -> None:
        """Set baseline value for a metric."""
        self.baseline[metric] = value

    def check_slo(self, slo_name: str, measured: float) -> tuple[bool, str]:
        """Check if a performance SLO is met.

        Args:
            slo_name: Name of the SLO
            measured: Measured value

        Returns:
            Tuple of (passed, message)
        """
        slo = self.slos.get(slo_name)
        if not slo:
            return False, f"Unknown SLO: {slo_name}"

        if slo.is_met(measured):
            return True, f"{slo_name}: {measured} {slo.unit} meets target {slo.target} {slo.unit}"

        return (
            False,
            f"{slo_name}: {measured} {slo.unit} does NOT meet target {slo.target} {slo.unit}",
        )

    def detect_regression(
        self,
        metric: str,
        current_value: float,
        threshold_percent: float = 10.0,
    ) -> RegressionDetection:
        """Detect performance regression.

        Args:
            metric: Metric name
            current_value: Current measured value
            threshold_percent: Regression threshold as percentage

        Returns:
            Regression detection result
        """
        baseline_value = self.baseline.get(metric)
        if baseline_value is None:
            return RegressionDetection(
                metric=metric,
                current_value=current_value,
                baseline_value=0,
                threshold_percent=threshold_percent,
                is_regression=False,
                change_percent=0,
            )

        if baseline_value == 0:
            change_percent = 100.0 if current_value > 0 else 0.0
        else:
            change_percent = ((current_value - baseline_value) / baseline_value) * 100

        # For throughput metrics, regression is when value decreases
        # For latency metrics, regression is when value increases
        is_latency = "latency" in metric.lower() or "time" in metric.lower()

        if is_latency:
            is_regression = change_percent > threshold_percent
        else:
            is_regression = change_percent < -threshold_percent

        return RegressionDetection(
            metric=metric,
            current_value=current_value,
            baseline_value=baseline_value,
            threshold_percent=threshold_percent,
            is_regression=is_regression,
            change_percent=change_percent,
        )

    def record_result(self, result: BenchmarkResult) -> None:
        """Record a benchmark result."""
        self.results.append(result)

    def run_synthetic_benchmark(
        self,
        name: str,
        iterations: int = 100,
    ) -> BenchmarkResult:
        """Run a synthetic benchmark (for demonstration).

        In production, this would run actual performance tests.
        """
        import random

        # Simulate benchmark with some variance
        base_time = 0.1  # 100ms base
        variance = random.uniform(0.8, 1.2)
        measured_time = base_time * variance

        result = BenchmarkResult(
            benchmark_name=name,
            metric="latency_seconds",
            value=measured_time,
            unit="seconds",
            metadata={"iterations": iterations},
        )

        self.record_result(result)
        return result

    def generate_report(self) -> dict[str, Any]:
        """Generate performance test report."""
        slo_status = []
        for slo_name, slo in self.slos.items():
            # Find most recent result for this SLO's metric
            matching_results = [r for r in self.results if r.metric == slo.metric]
            if matching_results:
                latest = matching_results[-1]
                passed, msg = self.check_slo(slo_name, latest.value)
                slo_status.append(
                    {
                        "slo": slo_name,
                        "passed": passed,
                        "message": msg,
                    }
                )

        return {
            "slos": [s.to_dict() for s in self.slos.values()],
            "load_scenarios": [s.to_dict() for s in self.load_scenarios.values()],
            "slo_status": slo_status,
            "results": [r.to_dict() for r in self.results[-20:]],  # Last 20
            "generated_at": datetime.now(UTC).isoformat(),
        }
