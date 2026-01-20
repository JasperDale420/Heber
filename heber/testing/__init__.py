"""Testing Module (PRD §45-50).

Test utilities, generators, and validation suites.
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
]
