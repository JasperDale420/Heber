"""Testing Module (PRD §45-53).

Test utilities, generators, validation suites, and CI infrastructure.
"""

from heber.testing.generators import (
    TestDataConfig,
    SyntheticDataGenerator,
    TestFixture,
    FixtureRegistry,
    SIMPLE_BARS_FIXTURE,
    LEAKAGE_TEST_FIXTURE,
    DEFAULT_FIXTURES,
)
from heber.testing.leakage import (
    LeakageTestResult,
    LeakageTestCase,
    LeakageTestRun,
    LeakageValidator,
    DEFAULT_LEAKAGE_TESTS,
)
from heber.testing.ci_gates import (
    GateType,
    TestCategory,
    CoverageRequirement,
    CIGate,
    FlakyTestPolicy,
    TestRun,
    CIGateEnforcer,
    DEFAULT_COVERAGE_REQUIREMENTS,
    DEFAULT_CI_GATES,
)
from heber.testing.performance import (
    PerformanceSLO,
    LoadTestScenario,
    BenchmarkResult,
    RegressionDetection,
    PerformanceTester,
    DEFAULT_PERFORMANCE_SLOS,
    DEFAULT_LOAD_SCENARIOS,
)
from heber.testing.framework import (
    UnitTestSpec,
    MockStrategy,
    UnitTestFramework,
    IntegrationTestSpec,
    IntegrationTestHarness,
    E2ETestCase,
    E2ETestSuite,
    DEFAULT_UNIT_TEST_SPECS,
    DEFAULT_MOCK_STRATEGIES,
    DEFAULT_INTEGRATION_TEST_SPECS,
    DEFAULT_E2E_TEST_CASES,
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
