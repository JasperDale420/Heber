"""Tests for CI Gates, Performance, and Framework (PRD §46-53)."""

from heber.testing.ci_gates import (
    CIGateEnforcer,
    CoverageRequirement,
    GateType,
    TestCategory,
    TestRun,
)
from heber.testing.framework import (
    E2ETestSuite,
    IntegrationTestHarness,
    UnitTestFramework,
)
from heber.testing.performance import (
    PerformanceTester,
)


class TestCoverageRequirement:
    """Test CoverageRequirement."""

    def test_is_met_true(self):
        req = CoverageRequirement("sdk", 90, 85)

        assert req.is_met(95, 90)

    def test_is_met_false(self):
        req = CoverageRequirement("sdk", 90, 85)

        assert not req.is_met(80, 70)


class TestCIGateEnforcer:
    """Test CIGateEnforcer."""

    def test_check_gate_pass(self):
        enforcer = CIGateEnforcer()

        test_results = {
            TestCategory.UNIT: True,
            TestCategory.INTEGRATION: True,
            TestCategory.LEAKAGE: True,
        }
        check_results = {"lint": True, "typecheck": True, "coverage": True}

        passed, failures = enforcer.check_gate(GateType.PR_MERGE, test_results, check_results)

        assert passed
        assert len(failures) == 0

    def test_check_gate_fail(self):
        enforcer = CIGateEnforcer()

        test_results = {
            TestCategory.UNIT: True,
            TestCategory.INTEGRATION: False,  # Failed
            TestCategory.LEAKAGE: True,
        }
        check_results = {"lint": True, "typecheck": True, "coverage": True}

        passed, failures = enforcer.check_gate(GateType.PR_MERGE, test_results, check_results)

        assert not passed
        assert len(failures) == 1

    def test_check_coverage(self):
        enforcer = CIGateEnforcer()

        passed, msg = enforcer.check_coverage("heber-sdk", 92, 88)

        assert passed

    def test_flaky_detection(self):
        enforcer = CIGateEnforcer()

        # Record mostly passing tests with occasional failures
        for i in range(20):
            run = TestRun(
                test_name="test_example",
                category=TestCategory.UNIT,
                passed=(i % 10 != 0),  # 10% failure rate
                duration_seconds=0.1,
            )
            enforcer.record_test_run(run)

        flaky = enforcer.get_flaky_tests()

        # 10% failure rate > 5% threshold, should not be flaky (it's worse)
        assert "test_example" not in flaky

    def test_generate_report(self):
        enforcer = CIGateEnforcer()

        report = enforcer.generate_report()

        assert "gates" in report
        assert "coverage_requirements" in report
        assert len(report["gates"]) == 4


class TestPerformanceTester:
    """Test PerformanceTester."""

    def test_check_slo_pass(self):
        tester = PerformanceTester()

        passed, msg = tester.check_slo("Ingestion Throughput", 12000)

        assert passed

    def test_check_slo_fail(self):
        tester = PerformanceTester()

        passed, msg = tester.check_slo("Ingestion Throughput", 5000)

        assert not passed

    def test_detect_regression_latency(self):
        tester = PerformanceTester()
        tester.set_baseline("read_latency_p99", 0.4)

        # 50% increase in latency is a regression
        result = tester.detect_regression("read_latency_p99", 0.6, threshold_percent=10)

        assert result.is_regression
        assert result.change_percent > 0

    def test_detect_no_regression(self):
        tester = PerformanceTester()
        tester.set_baseline("read_latency_p99", 0.4)

        # Similar latency is not a regression
        result = tester.detect_regression("read_latency_p99", 0.42, threshold_percent=10)

        assert not result.is_regression

    def test_generate_report(self):
        tester = PerformanceTester()

        report = tester.generate_report()

        assert "slos" in report
        assert "load_scenarios" in report
        assert len(report["slos"]) == 5


class TestUnitTestFramework:
    """Test UnitTestFramework."""

    def test_list_all_specs(self):
        framework = UnitTestFramework()

        specs = framework.list_all_specs()

        assert len(specs) == 7
        assert any(s["module"] == "Bloom filter" for s in specs)

    def test_get_mock_strategy(self):
        framework = UnitTestFramework()

        strategy = framework.get_mock_strategy("S3")

        assert strategy is not None
        assert strategy.mock_library == "moto"


class TestIntegrationTestHarness:
    """Test IntegrationTestHarness."""

    def test_list_all_specs(self):
        harness = IntegrationTestHarness()

        specs = harness.list_all_specs()

        assert len(specs) >= 6

    def test_get_spec(self):
        harness = IntegrationTestHarness()

        spec = harness.get_spec("SDK Integration")

        assert spec is not None
        assert "SDK" in spec.components


class TestE2ETestSuite:
    """Test E2ETestSuite."""

    def test_list_all(self):
        suite = E2ETestSuite()

        tests = suite.list_all()

        assert len(tests) == 7

    def test_get_schedule(self):
        suite = E2ETestSuite()

        schedule = suite.get_schedule()

        assert "on_merge_to_main" in schedule
        assert "nightly" in schedule
        assert len(schedule["on_merge_to_main"]) == 3


def run_all_framework_tests() -> dict[str, bool]:
    """Run all framework tests."""
    results = {}

    test_classes = [
        TestCoverageRequirement,
        TestCIGateEnforcer,
        TestPerformanceTester,
        TestUnitTestFramework,
        TestIntegrationTestHarness,
        TestE2ETestSuite,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nFramework Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_framework_tests()
