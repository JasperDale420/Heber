"""Unit Test Framework (PRD §46).

Unit test utilities and mocking strategies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class UnitTestSpec:
    """Unit test specification (PRD §46.1)."""

    module: str
    test_areas: list[str]
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "module": self.module,
            "test_areas": self.test_areas,
            "description": self.description,
        }


# Default unit test specs from PRD §46.1
DEFAULT_UNIT_TEST_SPECS: list[UnitTestSpec] = [
    UnitTestSpec(
        module="Event parsing",
        test_areas=["Schema validation", "Field extraction", "Error handling"],
    ),
    UnitTestSpec(
        module="Timestamp logic",
        test_areas=["ts_event", "ts_available", "ts_commit calculations"],
    ),
    UnitTestSpec(
        module="Bloom filter",
        test_areas=["Insert", "Lookup", "False positive rate"],
    ),
    UnitTestSpec(
        module="Batch accumulator",
        test_areas=["Size limits", "Flush triggers", "Ordering"],
    ),
    UnitTestSpec(
        module="Manifest operations",
        test_areas=["Read", "Write", "Merge", "Rollback"],
    ),
    UnitTestSpec(
        module="SDK query builder",
        test_areas=["asof_time filtering", "Partition pruning"],
    ),
    UnitTestSpec(
        module="Schema evolution",
        test_areas=["Backward compatibility", "Forward compatibility"],
    ),
]


@dataclass
class MockStrategy:
    """Mock strategy for a dependency (PRD §46.2)."""

    dependency: str
    mock_library: str
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "dependency": self.dependency,
            "mock_library": self.mock_library,
            "notes": self.notes,
        }


# Default mock strategies from PRD §46.2
DEFAULT_MOCK_STRATEGIES: list[MockStrategy] = [
    MockStrategy("S3", "moto", "S3 mock or localstack"),
    MockStrategy("Redis", "fakeredis", "or testcontainers"),
    MockStrategy("Postgres", "testcontainers", "ephemeral DB"),
]


class UnitTestFramework:
    """Unit test framework utilities."""

    def __init__(
        self,
        specs: list[UnitTestSpec] | None = None,
        mock_strategies: list[MockStrategy] | None = None,
    ):
        self.specs = specs or DEFAULT_UNIT_TEST_SPECS
        self.mock_strategies = {m.dependency: m for m in (mock_strategies or DEFAULT_MOCK_STRATEGIES)}

    def get_mock_strategy(self, dependency: str) -> MockStrategy | None:
        """Get mock strategy for a dependency."""
        return self.mock_strategies.get(dependency)

    def list_all_specs(self) -> list[dict[str, Any]]:
        """List all unit test specs."""
        return [s.to_dict() for s in self.specs]


@dataclass
class IntegrationTestSpec:
    """Integration test specification (PRD §47.1)."""

    suite_name: str
    components: list[str]
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "components": self.components,
            "description": self.description,
        }


# Default integration test specs from PRD §47.1
DEFAULT_INTEGRATION_TEST_SPECS: list[IntegrationTestSpec] = [
    IntegrationTestSpec(
        suite_name="Consumer Integration",
        components=["Event Bus", "Consumer", "Internal queue"],
    ),
    IntegrationTestSpec(
        suite_name="Writer Integration",
        components=["Consumer", "Writer", "S3 (mocked)"],
    ),
    IntegrationTestSpec(
        suite_name="Compactor Integration",
        components=["S3", "Compactor", "S3 (manifest updates)"],
    ),
    IntegrationTestSpec(
        suite_name="Catalog Integration",
        components=["Catalog API", "Postgres"],
    ),
    IntegrationTestSpec(
        suite_name="SDK Integration",
        components=["SDK", "Catalog", "S3"],
    ),
]


class IntegrationTestHarness:
    """Integration test harness."""

    def __init__(
        self,
        specs: list[IntegrationTestSpec] | None = None,
    ):
        self.specs = {s.suite_name: s for s in (specs or DEFAULT_INTEGRATION_TEST_SPECS)}

    def get_spec(self, suite_name: str) -> IntegrationTestSpec | None:
        """Get integration test spec by suite name."""
        return self.specs.get(suite_name)

    def list_all_specs(self) -> list[dict[str, Any]]:
        """List all integration test specs."""
        return [s.to_dict() for s in self.specs.values()]


@dataclass
class E2ETestCase:
    """E2E test case definition (PRD §48.1)."""

    flow: str
    scenario: str
    success_criteria: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow": self.flow,
            "scenario": self.scenario,
            "success_criteria": self.success_criteria,
        }


# Default E2E test cases from PRD §48.1
DEFAULT_E2E_TEST_CASES: list[E2ETestCase] = [
    E2ETestCase(
        flow="Happy path",
        scenario="Event → Bronze → Silver → SDK read",
        success_criteria="Data available within SLO",
    ),
    E2ETestCase(
        flow="Malformed event",
        scenario="Bad JSON → DLQ",
        success_criteria="Event in DLQ, others unaffected",
    ),
    E2ETestCase(
        flow="Duplicate event",
        scenario="Same event_id twice",
        success_criteria="Single row in Silver",
    ),
    E2ETestCase(
        flow="Schema evolution",
        scenario="Add new field → Verify backward read",
        success_criteria="Old SDK can read new data",
    ),
    E2ETestCase(
        flow="Backfill",
        scenario="Load historical → Verify ts_available",
        success_criteria="ts_available = ts_commit",
    ),
    E2ETestCase(
        flow="Compaction",
        scenario="Many small files → Compacted",
        success_criteria="File count reduced, data intact",
    ),
]


class E2ETestSuite:
    """E2E test suite."""

    def __init__(
        self,
        test_cases: list[E2ETestCase] | None = None,
    ):
        self.test_cases = test_cases or DEFAULT_E2E_TEST_CASES

    def list_all(self) -> list[dict[str, Any]]:
        """List all E2E test cases."""
        return [tc.to_dict() for tc in self.test_cases]

    def get_schedule(self) -> dict[str, list[str]]:
        """Get E2E test schedule from PRD §48.3."""
        return {
            "on_merge_to_main": ["Happy path", "Malformed event", "Duplicate event"],
            "nightly": [tc.flow for tc in self.test_cases],
            "pre_release": [tc.flow for tc in self.test_cases] + ["Performance"],
        }
