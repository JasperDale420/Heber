"""CI Gates and Test Policy (PRD §53).

CI/CD gate configurations and test policy enforcement.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class GateType(str, Enum):
    """Types of CI gates."""

    PR_MERGE = "pr_merge"
    MAIN_MERGE = "main_merge"
    STAGING_DEPLOY = "staging_deploy"
    PROD_DEPLOY = "prod_deploy"


class TestCategory(str, Enum):
    """Test categories (PRD §45.4)."""

    UNIT = "unit"
    INTEGRATION = "integration"
    E2E = "e2e"
    LEAKAGE = "leakage"
    PERFORMANCE = "performance"
    CHAOS = "chaos"


@dataclass
class CoverageRequirement:
    """Coverage requirement for a component (PRD §45.3)."""

    component: str
    min_line_coverage: float
    min_branch_coverage: float

    def is_met(self, line_coverage: float, branch_coverage: float) -> bool:
        """Check if coverage requirement is met."""
        return line_coverage >= self.min_line_coverage and branch_coverage >= self.min_branch_coverage

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component,
            "min_line_coverage": f"{self.min_line_coverage}%",
            "min_branch_coverage": f"{self.min_branch_coverage}%",
        }


# Default coverage requirements from PRD §45.3
DEFAULT_COVERAGE_REQUIREMENTS: list[CoverageRequirement] = [
    CoverageRequirement("heber-sdk", 90, 85),
    CoverageRequirement("heber-consumer", 80, 75),
    CoverageRequirement("heber-writer", 80, 75),
    CoverageRequirement("heber-compactor", 75, 70),
    CoverageRequirement("heber-catalog", 85, 80),
    CoverageRequirement("heber-hotloader", 75, 70),
]


@dataclass
class CIGate:
    """CI gate definition (PRD §53)."""

    gate_type: GateType
    name: str
    required_tests: list[TestCategory]
    required_checks: list[str]
    blocking: bool = True
    timeout_minutes: int = 30

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_type": self.gate_type.value,
            "name": self.name,
            "required_tests": [t.value for t in self.required_tests],
            "required_checks": self.required_checks,
            "blocking": self.blocking,
            "timeout_minutes": self.timeout_minutes,
        }


# Default CI gates from PRD §53
DEFAULT_CI_GATES: list[CIGate] = [
    CIGate(
        gate_type=GateType.PR_MERGE,
        name="PR Merge Gate",
        required_tests=[
            TestCategory.UNIT,
            TestCategory.INTEGRATION,
            TestCategory.LEAKAGE,
        ],
        required_checks=["lint", "typecheck", "coverage"],
        timeout_minutes=20,
    ),
    CIGate(
        gate_type=GateType.MAIN_MERGE,
        name="Main Branch Gate",
        required_tests=[
            TestCategory.UNIT,
            TestCategory.INTEGRATION,
            TestCategory.LEAKAGE,
            TestCategory.E2E,
        ],
        required_checks=["lint", "typecheck", "coverage", "security"],
        timeout_minutes=45,
    ),
    CIGate(
        gate_type=GateType.STAGING_DEPLOY,
        name="Staging Deploy Gate",
        required_tests=[
            TestCategory.E2E,
            TestCategory.PERFORMANCE,
        ],
        required_checks=["security", "images"],
        timeout_minutes=60,
    ),
    CIGate(
        gate_type=GateType.PROD_DEPLOY,
        name="Production Deploy Gate",
        required_tests=[
            TestCategory.E2E,
            TestCategory.LEAKAGE,
        ],
        required_checks=["security", "images", "approval"],
        timeout_minutes=90,
    ),
]


@dataclass
class FlakyTestPolicy:
    """Flaky test policy (PRD §53)."""

    quarantine_threshold_percent: float = 5.0
    lookback_runs: int = 20
    auto_quarantine: bool = True

    def is_flaky(self, failures: int, total_runs: int) -> bool:
        """Check if a test is considered flaky."""
        if total_runs < 5:
            return False
        failure_rate = (failures / total_runs) * 100
        return 0 < failure_rate <= self.quarantine_threshold_percent

    def to_dict(self) -> dict[str, Any]:
        return {
            "quarantine_threshold": f">{self.quarantine_threshold_percent}%",
            "lookback_runs": self.lookback_runs,
            "auto_quarantine": self.auto_quarantine,
        }


@dataclass
class TestRun:
    """Record of a test run."""

    test_name: str
    category: TestCategory
    passed: bool
    duration_seconds: float
    run_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    error_message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_name": self.test_name,
            "category": self.category.value,
            "passed": self.passed,
            "duration_seconds": self.duration_seconds,
            "run_at": self.run_at.isoformat(),
            "error_message": self.error_message,
        }


class CIGateEnforcer:
    """Enforce CI gate policies."""

    def __init__(
        self,
        gates: list[CIGate] | None = None,
        coverage_requirements: list[CoverageRequirement] | None = None,
        flaky_policy: FlakyTestPolicy | None = None,
    ):
        self.gates = {g.gate_type: g for g in (gates or DEFAULT_CI_GATES)}
        self.coverage_requirements = {c.component: c for c in (coverage_requirements or DEFAULT_COVERAGE_REQUIREMENTS)}
        self.flaky_policy = flaky_policy or FlakyTestPolicy()
        self.test_history: dict[str, list[TestRun]] = {}

    def get_gate(self, gate_type: GateType) -> CIGate:
        """Get gate configuration."""
        return self.gates[gate_type]

    def check_gate(
        self,
        gate_type: GateType,
        test_results: dict[TestCategory, bool],
        check_results: dict[str, bool],
    ) -> tuple[bool, list[str]]:
        """Check if a gate passes.

        Args:
            gate_type: Type of gate to check
            test_results: Dict of test category -> passed
            check_results: Dict of check name -> passed

        Returns:
            Tuple of (passed, list of failures)
        """
        gate = self.gates[gate_type]
        failures = []

        # Check required tests
        for test_category in gate.required_tests:
            if not test_results.get(test_category, False):
                failures.append(f"Test failed: {test_category.value}")

        # Check required checks
        for check in gate.required_checks:
            if not check_results.get(check, False):
                failures.append(f"Check failed: {check}")

        passed = len(failures) == 0
        return passed, failures

    def check_coverage(
        self,
        component: str,
        line_coverage: float,
        branch_coverage: float,
    ) -> tuple[bool, str]:
        """Check if coverage requirements are met."""
        requirement = self.coverage_requirements.get(component)
        if not requirement:
            return True, "No requirement defined"

        if requirement.is_met(line_coverage, branch_coverage):
            return True, "Coverage requirements met"

        return False, (
            f"Coverage too low: line={line_coverage}% (min {requirement.min_line_coverage}%), "
            f"branch={branch_coverage}% (min {requirement.min_branch_coverage}%)"
        )

    def record_test_run(self, test_run: TestRun) -> None:
        """Record a test run for flaky detection."""
        if test_run.test_name not in self.test_history:
            self.test_history[test_run.test_name] = []
        self.test_history[test_run.test_name].append(test_run)

        # Trim to lookback window
        max_runs = self.flaky_policy.lookback_runs
        self.test_history[test_run.test_name] = self.test_history[test_run.test_name][-max_runs:]

    def get_flaky_tests(self) -> list[str]:
        """Get list of flaky tests."""
        flaky = []
        for test_name, runs in self.test_history.items():
            if len(runs) < 5:
                continue
            failures = sum(1 for r in runs if not r.passed)
            if self.flaky_policy.is_flaky(failures, len(runs)):
                flaky.append(test_name)
        return flaky

    def generate_report(self) -> dict[str, Any]:
        """Generate CI gate report."""
        return {
            "gates": [g.to_dict() for g in self.gates.values()],
            "coverage_requirements": [c.to_dict() for c in self.coverage_requirements.values()],
            "flaky_policy": self.flaky_policy.to_dict(),
            "flaky_tests": self.get_flaky_tests(),
            "generated_at": datetime.now(UTC).isoformat(),
        }
