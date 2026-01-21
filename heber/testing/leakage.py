"""Leakage Validation Suite (PRD §49).

Tests to validate the zero-leakage invariant.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class LeakageTestResult(str, Enum):
    """Leakage test result status."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class LeakageTestCase:
    """A single leakage test case (PRD §49.2).

    Each test validates a specific aspect of zero-leakage.
    """

    test_id: str
    name: str
    description: str
    scenario: str
    expected_result: str
    severity: str = "critical"  # All leakage tests are critical

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_id": self.test_id,
            "name": self.name,
            "description": self.description,
            "scenario": self.scenario,
            "expected_result": self.expected_result,
            "severity": self.severity,
        }


@dataclass
class LeakageTestRun:
    """Result of a leakage test run."""

    test_id: str
    result: LeakageTestResult
    actual_value: Any = None
    expected_value: Any = None
    error_message: str = ""
    run_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_id": self.test_id,
            "result": self.result.value,
            "actual_value": str(self.actual_value) if self.actual_value else None,
            "expected_value": str(self.expected_value) if self.expected_value else None,
            "error_message": self.error_message,
            "run_at": self.run_at.isoformat(),
        }


# Default leakage test cases from PRD §49.2
DEFAULT_LEAKAGE_TESTS: list[LeakageTestCase] = [
    LeakageTestCase(
        test_id="LK-001",
        name="No Future Data Returned",
        description="read_asof must not return rows where ts_available > asof_time",
        scenario="Query with asof_time=T, data exists with ts_available > T",
        expected_result="No future rows returned",
    ),
    LeakageTestCase(
        test_id="LK-002",
        name="AsOf Join Correctness",
        description="asof_join uses the correct earlier ts_available",
        scenario="Join left table at T with right table having mixed ts_available",
        expected_result="Uses earlier ts_available, not future data",
    ),
    LeakageTestCase(
        test_id="LK-003",
        name="Backfill ts_available",
        description="Backfill data must have ts_available = ts_commit, not future",
        scenario="Load historical data with incorrect ts_available",
        expected_result="Rejected or corrected to ts_commit",
    ),
    LeakageTestCase(
        test_id="LK-004",
        name="Gold Build Validation",
        description="Gold writes with future-looking features fail lineage validation",
        scenario="Build Gold feature using data unavailable at train time",
        expected_result="Lineage validation fails",
    ),
    LeakageTestCase(
        test_id="LK-005",
        name="Direct S3 Access",
        description="Direct S3 reads bypassing SDK are audited/blocked",
        scenario="Attempt to read Parquet directly from S3",
        expected_result="Audit log created or access blocked",
    ),
    LeakageTestCase(
        test_id="LK-006",
        name="Clock Skew Simulation",
        description="Clock skew does not cause leakage",
        scenario="Writer clock ahead of consumer clock",
        expected_result="ts_available still correct based on source time",
    ),
    LeakageTestCase(
        test_id="LK-007",
        name="SDK Version Mismatch",
        description="Incompatible SDK versions enforce compatibility check",
        scenario="Old SDK reads data from newer schema version",
        expected_result="Compatibility check enforced",
    ),
]


class LeakageValidator:
    """Validate zero-leakage invariant."""

    def __init__(self, test_cases: list[LeakageTestCase] | None = None):
        self.test_cases = {tc.test_id: tc for tc in (test_cases or DEFAULT_LEAKAGE_TESTS)}
        self.results: list[LeakageTestRun] = []

    def get_test_case(self, test_id: str) -> LeakageTestCase | None:
        """Get test case by ID."""
        return self.test_cases.get(test_id)

    def list_all(self) -> list[dict[str, Any]]:
        """List all test cases."""
        return [tc.to_dict() for tc in self.test_cases.values()]

    def validate_no_future_data(
        self,
        query_result: list[dict[str, Any]],
        asof_time: datetime,
        ts_available_field: str = "ts_available",
    ) -> LeakageTestRun:
        """LK-001: Validate no future data is returned.

        Args:
            query_result: Query results to validate
            asof_time: The asof_time used in the query
            ts_available_field: Field name for ts_available

        Returns:
            Test run result
        """
        violations = []
        for row in query_result:
            ts_available = row.get(ts_available_field)
            if ts_available:
                # Parse if string
                if isinstance(ts_available, str):
                    ts_available = datetime.fromisoformat(ts_available.replace("Z", "+00:00"))

                if ts_available > asof_time:
                    violations.append(
                        {
                            "row": row,
                            "ts_available": ts_available.isoformat(),
                            "asof_time": asof_time.isoformat(),
                        }
                    )

        if violations:
            result = LeakageTestRun(
                test_id="LK-001",
                result=LeakageTestResult.FAILED,
                actual_value=len(violations),
                expected_value=0,
                error_message=f"Found {len(violations)} rows with ts_available > asof_time",
            )
        else:
            result = LeakageTestRun(
                test_id="LK-001",
                result=LeakageTestResult.PASSED,
                actual_value=0,
                expected_value=0,
            )

        self.results.append(result)

        if result.result == LeakageTestResult.FAILED:
            logger.error(
                "LEAKAGE VIOLATION: Future data returned",
                test_id="LK-001",
                violations=len(violations),
            )

        return result

    def validate_backfill_ts_available(
        self,
        ts_available: datetime,
        ts_commit: datetime,
        tolerance_seconds: int = 5,
    ) -> LeakageTestRun:
        """LK-003: Validate backfill ts_available equals ts_commit.

        Args:
            ts_available: The ts_available of backfill data
            ts_commit: The ts_commit (write time)
            tolerance_seconds: Allowed difference in seconds

        Returns:
            Test run result
        """
        diff = abs((ts_available - ts_commit).total_seconds())

        if diff > tolerance_seconds:
            result = LeakageTestRun(
                test_id="LK-003",
                result=LeakageTestResult.FAILED,
                actual_value=diff,
                expected_value=f"≤{tolerance_seconds}s",
                error_message=f"ts_available differs from ts_commit by {diff}s",
            )
        else:
            result = LeakageTestRun(
                test_id="LK-003",
                result=LeakageTestResult.PASSED,
                actual_value=diff,
                expected_value=f"≤{tolerance_seconds}s",
            )

        self.results.append(result)
        return result

    def validate_gold_lineage(
        self,
        feature_ts_event: datetime,
        input_ts_available: datetime,
        label_ts_event: datetime,
    ) -> LeakageTestRun:
        """LK-004: Validate Gold feature lineage.

        Feature data must have been available before label time.

        Args:
            feature_ts_event: When the feature was computed
            input_ts_available: When input data became available
            label_ts_event: Event time of the label

        Returns:
            Test run result
        """
        # Feature inputs must be available BEFORE label time
        if input_ts_available >= label_ts_event:
            result = LeakageTestRun(
                test_id="LK-004",
                result=LeakageTestResult.FAILED,
                actual_value=f"input_ts_available={input_ts_available.isoformat()}",
                expected_value=f"< label_ts_event={label_ts_event.isoformat()}",
                error_message="Feature uses data unavailable at label time (look-ahead bias)",
            )
        else:
            result = LeakageTestRun(
                test_id="LK-004",
                result=LeakageTestResult.PASSED,
            )

        self.results.append(result)
        return result

    def generate_report(self) -> dict[str, Any]:
        """Generate leakage validation report."""
        passed = sum(1 for r in self.results if r.result == LeakageTestResult.PASSED)
        failed = sum(1 for r in self.results if r.result == LeakageTestResult.FAILED)
        skipped = sum(1 for r in self.results if r.result == LeakageTestResult.SKIPPED)

        return {
            "summary": {
                "total": len(self.results),
                "passed": passed,
                "failed": failed,
                "skipped": skipped,
                "pass_rate": f"{passed / len(self.results) * 100:.1f}%" if self.results else "N/A",
            },
            "verdict": "PASSED" if failed == 0 else "FAILED - LEAKAGE DETECTED",
            "results": [r.to_dict() for r in self.results],
            "generated_at": datetime.now(UTC).isoformat(),
        }

    def clear_results(self) -> None:
        """Clear previous results."""
        self.results = []
