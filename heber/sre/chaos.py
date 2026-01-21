"""Chaos Engineering (PRD §41).

Fault injection experiments and resilience testing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class ExperimentFrequency(str, Enum):
    """Chaos experiment frequency (PRD §41.3)."""

    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUALLY = "annually"


class ExperimentScope(str, Enum):
    """Scope of chaos experiment."""

    SINGLE_POD = "single_pod"
    MULTI_POD = "multi_pod"
    NETWORK = "network"
    MULTI_COMPONENT = "multi_component"
    DR_DRILL = "dr_drill"


class Environment(str, Enum):
    """Target environment for chaos."""

    STAGING = "staging"
    PRODUCTION = "production"


class ExperimentStatus(str, Enum):
    """Status of a chaos experiment run."""

    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ABORTED = "aborted"


@dataclass
class SuccessCriterion:
    """A success criterion for a chaos experiment."""

    description: str
    passed: bool | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "description": self.description,
            "passed": self.passed,
            "notes": self.notes,
        }


@dataclass
class ChaosExperiment:
    """Chaos experiment definition (PRD §41.2).

    Defines a fault injection experiment with hypothesis and expected outcome.
    """

    name: str
    target: str
    hypothesis: str
    expected_outcome: str
    procedure: list[str]
    success_criteria: list[SuccessCriterion]
    frequency: ExperimentFrequency = ExperimentFrequency.WEEKLY
    scope: ExperimentScope = ExperimentScope.SINGLE_POD
    environment: Environment = Environment.STAGING

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "target": self.target,
            "hypothesis": self.hypothesis,
            "expected_outcome": self.expected_outcome,
            "procedure": self.procedure,
            "success_criteria": [c.to_dict() for c in self.success_criteria],
            "frequency": self.frequency.value,
            "scope": self.scope.value,
            "environment": self.environment.value,
        }

    def to_markdown(self) -> str:
        """Generate markdown runbook for experiment."""
        lines = [
            f"## Experiment: {self.name}",
            "",
            f"**Hypothesis:** {self.hypothesis}",
            f"**Target:** `{self.target}`",
            f"**Expected:** {self.expected_outcome}",
            "",
            "### Procedure",
            "",
        ]

        for i, step in enumerate(self.procedure, 1):
            lines.append(f"{i}. {step}")

        lines.extend(["", "### Success Criteria", ""])
        for criterion in self.success_criteria:
            checkbox = "[ ]"
            if criterion.passed is True:
                checkbox = "[x]"
            elif criterion.passed is False:
                checkbox = "[-]"
            lines.append(f"- {checkbox} {criterion.description}")

        lines.extend(
            [
                "",
                "### Results",
                "",
                "- **Date:** ____",
                "- **Outcome:** PASS / FAIL",
                "- **Notes:** ____",
            ]
        )

        return "\n".join(lines)


@dataclass
class ExperimentRun:
    """Record of a chaos experiment run."""

    experiment_name: str
    started_at: datetime
    ended_at: datetime | None = None
    status: ExperimentStatus = ExperimentStatus.PENDING
    results: dict[str, bool] = field(default_factory=dict)
    notes: str = ""

    @property
    def duration_seconds(self) -> float | None:
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_name": self.experiment_name,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "results": self.results,
            "notes": self.notes,
        }


# Default chaos experiments from PRD §41.2
DEFAULT_EXPERIMENTS: list[ChaosExperiment] = [
    ChaosExperiment(
        name="Kill Consumer Pod",
        target="heber-consumer",
        hypothesis=(
            "When a consumer pod is killed, the consumer group rebalances and resumes processing without message loss"
        ),
        expected_outcome="Rebalance in <30s, no message loss",
        procedure=[
            "Establish baseline metrics (lag, throughput)",
            "kubectl delete pod -l app=heber-consumer --wait=false",
            "Observe for 2 minutes",
            "Verify pod restarts",
            "Verify consumer lag recovers",
        ],
        success_criteria=[
            SuccessCriterion("No data loss"),
            SuccessCriterion("Recovery within 30 seconds"),
            SuccessCriterion("Alerts fired correctly"),
        ],
    ),
    ChaosExperiment(
        name="Kill Writer Pod",
        target="heber-writer",
        hypothesis="When a writer pod is killed, in-flight batch goes to DLQ and new pod starts cleanly",
        expected_outcome="In-flight batch to DLQ, restart clean",
        procedure=[
            "Establish baseline metrics",
            "kubectl delete pod -l app=heber-writer --wait=false",
            "Observe for 2 minutes",
            "Check DLQ for in-flight events",
            "Verify new pod starts successfully",
        ],
        success_criteria=[
            SuccessCriterion("No data loss (events in DLQ or written)"),
            SuccessCriterion("Recovery within 60 seconds"),
            SuccessCriterion("DLQ events can be replayed"),
        ],
    ),
    ChaosExperiment(
        name="Throttle S3",
        target="object_storage",
        hypothesis="When S3 is throttled, writers apply backpressure without crashing",
        expected_outcome="Backpressure, writes queue, no crash",
        procedure=[
            "Establish baseline metrics",
            "Apply network throttle: tc qdisc add dev eth0 root tbf rate 1mbit burst 32kbit latency 400ms",
            "Observe for 5 minutes",
            "Remove throttle",
            "Verify writes complete",
        ],
        success_criteria=[
            SuccessCriterion("No crashes or OOMs"),
            SuccessCriterion("Backpressure metrics visible"),
            SuccessCriterion("All queued writes complete after recovery"),
        ],
        scope=ExperimentScope.NETWORK,
        frequency=ExperimentFrequency.MONTHLY,
    ),
    ChaosExperiment(
        name="Block Catalog",
        target="heber-catalog",
        hypothesis="When Catalog is blocked, system operates in degraded mode using cached metadata",
        expected_outcome="Degraded mode, cache-only, no crash",
        procedure=[
            "Establish baseline metrics",
            "Block Catalog ingress: kubectl scale deployment heber-catalog --replicas=0",
            "Observe for 5 minutes",
            "Verify SDK uses cached metadata",
            "Restore Catalog: kubectl scale deployment heber-catalog --replicas=2",
        ],
        success_criteria=[
            SuccessCriterion("Writers continue (degraded mode)"),
            SuccessCriterion("SDK uses cached metadata"),
            SuccessCriterion("Recovery after restore"),
        ],
    ),
    ChaosExperiment(
        name="Inject Bad Event",
        target="event_bus",
        hypothesis="When a malformed event is injected, it goes to DLQ without affecting other events",
        expected_outcome="Event to DLQ, others unaffected",
        procedure=[
            "Establish baseline metrics",
            "Inject malformed JSON to event bus",
            "Observe for 1 minute",
            "Verify bad event in DLQ",
            "Verify other events processed normally",
        ],
        success_criteria=[
            SuccessCriterion("Bad event in DLQ"),
            SuccessCriterion("Good events unaffected"),
            SuccessCriterion("No consumer restarts"),
        ],
    ),
    ChaosExperiment(
        name="Network Partition ClickHouse",
        target="clickhouse",
        hypothesis="When ClickHouse is unreachable, SDK falls back to Silver layer",
        expected_outcome="Hot Store fails, Silver fallback works",
        procedure=[
            "Establish baseline metrics",
            "Block ClickHouse network: kubectl exec -it clickhouse-0 -- iptables -A INPUT -j DROP",
            "Query via SDK",
            "Verify fallback to Silver",
            "Restore network",
        ],
        success_criteria=[
            SuccessCriterion("SDK queries succeed via Silver fallback"),
            SuccessCriterion("Circuit breaker trips"),
            SuccessCriterion("Recovery after restore"),
        ],
        scope=ExperimentScope.NETWORK,
        frequency=ExperimentFrequency.MONTHLY,
    ),
    ChaosExperiment(
        name="High CPU Stress",
        target="any_service",
        hypothesis="Under CPU stress, services slow down gracefully without OOM",
        expected_outcome="Graceful slowdown, no OOM",
        procedure=[
            "Establish baseline metrics",
            "Inject CPU stress: kubectl exec -it <pod> -- stress --cpu 4",
            "Observe for 3 minutes",
            "Remove stress",
            "Verify recovery",
        ],
        success_criteria=[
            SuccessCriterion("No OOM kills"),
            SuccessCriterion("Latency increases gracefully"),
            SuccessCriterion("Recovery within 30 seconds"),
        ],
        frequency=ExperimentFrequency.MONTHLY,
    ),
]


class ChaosRegistry:
    """Registry and scheduler for chaos experiments."""

    def __init__(self, experiments: list[ChaosExperiment] | None = None):
        self.experiments = {e.name: e for e in (experiments or DEFAULT_EXPERIMENTS)}
        self.runs: list[ExperimentRun] = []

    def get(self, name: str) -> ChaosExperiment | None:
        """Get experiment by name."""
        return self.experiments.get(name)

    def list_by_frequency(self, frequency: ExperimentFrequency) -> list[ChaosExperiment]:
        """List experiments by frequency."""
        return [e for e in self.experiments.values() if e.frequency == frequency]

    def list_all(self) -> list[dict[str, Any]]:
        """List all experiments."""
        return [e.to_dict() for e in self.experiments.values()]

    def get_schedule(self) -> dict[str, list[str]]:
        """Get experiment schedule by frequency."""
        schedule = {f.value: [] for f in ExperimentFrequency}
        for exp in self.experiments.values():
            schedule[exp.frequency.value].append(exp.name)
        return schedule

    def start_run(self, experiment_name: str) -> ExperimentRun | None:
        """Start a new experiment run."""
        experiment = self.get(experiment_name)
        if not experiment:
            return None

        run = ExperimentRun(
            experiment_name=experiment_name,
            started_at=datetime.now(UTC),
            status=ExperimentStatus.RUNNING,
        )
        self.runs.append(run)

        logger.info(
            "Chaos experiment started",
            experiment=experiment_name,
            target=experiment.target,
        )

        return run

    def complete_run(
        self,
        run: ExperimentRun,
        results: dict[str, bool],
        notes: str = "",
    ) -> None:
        """Complete an experiment run."""
        run.ended_at = datetime.now(UTC)
        run.results = results
        run.notes = notes

        # Determine pass/fail
        all_passed = all(results.values()) if results else False
        run.status = ExperimentStatus.PASSED if all_passed else ExperimentStatus.FAILED

        logger.info(
            "Chaos experiment completed",
            experiment=run.experiment_name,
            status=run.status.value,
            duration_seconds=run.duration_seconds,
        )

    def export_all_markdown(self) -> str:
        """Export all experiment runbooks as markdown."""
        parts = ["# Chaos Engineering Runbooks", ""]
        for exp in self.experiments.values():
            parts.append(exp.to_markdown())
            parts.append("\n---\n")
        return "\n".join(parts)
