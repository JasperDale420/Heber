"""Testing Module (PRD §45-53).

Test utilities, generators, validation suites, and CI infrastructure.
"""

from heber.testing.ci_gates import (
    DEFAULT_CI_GATES,
    DEFAULT_COVERAGE_REQUIREMENTS,
    CIGate,
    CIGateEnforcer,
    CoverageRequirement,
    FlakyTestPolicy,
    GateType,
    TestCategory,
    TestRun,
)
from heber.testing.framework import (
    DEFAULT_E2E_TEST_CASES,
    DEFAULT_INTEGRATION_TEST_SPECS,
    DEFAULT_MOCK_STRATEGIES,
    DEFAULT_UNIT_TEST_SPECS,
    E2ETestCase,
    E2ETestSuite,
    IntegrationTestHarness,
    IntegrationTestSpec,
    MockStrategy,
    UnitTestFramework,
    UnitTestSpec,
)
from heber.testing.generators import (
    DEFAULT_FIXTURES,
    LEAKAGE_TEST_FIXTURE,
    SIMPLE_BARS_FIXTURE,
    FixtureRegistry,
    SyntheticDataGenerator,
    TestDataConfig,
    TestFixture,
)
from heber.testing.leakage import (
    DEFAULT_LEAKAGE_TESTS,
    LeakageTestCase,
    LeakageTestResult,
    LeakageTestRun,
    LeakageValidator,
)
from heber.testing.performance import (
    DEFAULT_LOAD_SCENARIOS,
    DEFAULT_PERFORMANCE_SLOS,
    BenchmarkResult,
    LoadTestScenario,
    PerformanceSLO,
    PerformanceTester,
    RegressionDetection,
)

__all__ = [
    # Generators
    "TestDataConfig",
    "SyntheticDataGenerator",
    "TestFixture",
    "FixtureRegistry",
    "SIMPLE_BARS_FIXTURE",
    "LEAKAGE_TEST_FIXTURE",
    "DEFAULT_FIXTURES",
    # Leakage Validation
    "LeakageTestResult",
    "LeakageTestCase",
    "LeakageTestRun",
    "LeakageValidator",
    "DEFAULT_LEAKAGE_TESTS",
    # CI Gates
    "GateType",
    "TestCategory",
    "CoverageRequirement",
    "CIGate",
    "FlakyTestPolicy",
    "TestRun",
    "CIGateEnforcer",
    "DEFAULT_COVERAGE_REQUIREMENTS",
    "DEFAULT_CI_GATES",
    # Performance
    "PerformanceSLO",
    "LoadTestScenario",
    "BenchmarkResult",
    "RegressionDetection",
    "PerformanceTester",
    "DEFAULT_PERFORMANCE_SLOS",
    "DEFAULT_LOAD_SCENARIOS",
    # Framework
    "UnitTestSpec",
    "MockStrategy",
    "UnitTestFramework",
    "IntegrationTestSpec",
    "IntegrationTestHarness",
    "E2ETestCase",
    "E2ETestSuite",
    "DEFAULT_UNIT_TEST_SPECS",
    "DEFAULT_MOCK_STRATEGIES",
    "DEFAULT_INTEGRATION_TEST_SPECS",
    "DEFAULT_E2E_TEST_CASES",
]
