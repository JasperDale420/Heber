"""Tests for SRE Module (PRD §37-38)."""

from datetime import UTC, datetime, timedelta

import pytest

from heber.sre.error_budget import (
    BudgetState,
    DeployRisk,
    ErrorBudget,
    ErrorBudgetManager,
)
from heber.sre.slo import (
    SLI,
    SLO,
    AlertSeverity,
    BurnRateAlert,
    SLOManager,
    SLOWindow,
)


class TestSLOWindow:
    """Test SLO window conversions."""

    def test_to_timedelta(self):
        assert SLOWindow.WINDOW_1H.to_timedelta() == timedelta(hours=1)
        assert SLOWindow.WINDOW_30D.to_timedelta() == timedelta(days=30)


class TestSLI:
    """Test SLI dataclass."""

    def test_to_dict(self):
        sli = SLI(
            name="test_sli",
            metric_query="test_metric",
            description="Test description",
        )

        d = sli.to_dict()

        assert d["name"] == "test_sli"
        assert d["metric_query"] == "test_metric"


class TestSLO:
    """Test SLO calculations."""

    def test_target_percentage(self):
        slo = SLO(
            name="Test SLO",
            sli=SLI("test", "query"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        assert slo.target_percentage == 99.9

    def test_error_budget_ratio(self):
        slo = SLO(
            name="Test SLO",
            sli=SLI("test", "query"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        assert slo.error_budget_ratio == pytest.approx(0.001)


class TestBurnRateAlert:
    """Test burn rate alert generation."""

    def test_to_prometheus_rule(self):
        slo = SLO(
            name="Ingestion Availability",
            sli=SLI("ingestion", "error_rate_query"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        alert = BurnRateAlert(
            burn_rate=14,
            window=SLOWindow.WINDOW_1H,
            severity=AlertSeverity.CRITICAL,
            action="Page on-call",
        )

        rule = alert.to_prometheus_rule(slo)

        assert "HeberIngestionAvailabilityBurnRateCritical" in rule["alert"]
        assert rule["labels"]["severity"] == "critical"


class TestSLOManager:
    """Test SLO management."""

    def test_list_slos(self):
        manager = SLOManager()

        slos = manager.list_slos()

        assert len(slos) >= 3

    def test_calculate_status_healthy(self):
        manager = SLOManager()

        status = manager.calculate_status(
            slo_name="Ingestion Availability",
            current_value=0.9995,  # Above 99.9% target
            error_count=50,
            total_count=100000,  # 0.05% error rate
        )

        assert status.is_healthy
        assert status.budget_remaining > 0.5  # More than 50% remaining

    def test_calculate_status_unhealthy(self):
        manager = SLOManager()

        status = manager.calculate_status(
            slo_name="Ingestion Availability",
            current_value=0.995,  # Below 99.9% target
            error_count=500,
            total_count=100000,  # 0.5% error rate, way over budget
        )

        assert not status.is_healthy

    def test_generate_prometheus_rules(self):
        manager = SLOManager()

        rules = manager.generate_prometheus_rules()

        assert len(rules) > 0
        assert all("alert" in rule for rule in rules)


class TestBudgetState:
    """Test budget state classification."""

    def test_from_remaining_healthy(self):
        assert BudgetState.from_remaining(0.75) == BudgetState.HEALTHY

    def test_from_remaining_warning(self):
        assert BudgetState.from_remaining(0.35) == BudgetState.WARNING

    def test_from_remaining_critical(self):
        assert BudgetState.from_remaining(0.15) == BudgetState.CRITICAL

    def test_from_remaining_exhausted(self):
        assert BudgetState.from_remaining(0) == BudgetState.EXHAUSTED


class TestErrorBudget:
    """Test error budget calculations."""

    def test_allowed_errors(self):
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=0,
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        # 0.1% of 1M = 1000 allowed errors
        assert budget.allowed_errors == pytest.approx(1000)

    def test_remaining_ratio_healthy(self):
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=200,  # 200 of 1000 allowed
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        assert budget.remaining_ratio == pytest.approx(0.8)
        assert budget.state == BudgetState.HEALTHY

    def test_remaining_ratio_exhausted(self):
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=2000,  # 2000 of 1000 allowed, over budget
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        assert budget.remaining_ratio == 0
        assert budget.state == BudgetState.EXHAUSTED


class TestErrorBudgetManager:
    """Test error budget management."""

    def test_can_deploy_healthy(self):
        manager = ErrorBudgetManager()
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=100,  # 10% of budget consumed
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        allowed, reason = manager.can_deploy(budget, DeployRisk.HIGH_RISK)

        assert allowed

    def test_can_deploy_warning_blocks_risky(self):
        manager = ErrorBudgetManager()
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=700,  # 70% of budget consumed
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        assert budget.state == BudgetState.WARNING

        allowed, reason = manager.can_deploy(budget, DeployRisk.HIGH_RISK)

        assert not allowed
        assert "risky" in reason.lower()

    def test_can_deploy_exhausted_blocks_all(self):
        manager = ErrorBudgetManager()
        budget = ErrorBudget(
            slo_name="Test",
            target=0.999,
            total_requests=1_000_000,
            error_count=2000,  # Over budget
            period_start=datetime.now(UTC) - timedelta(days=30),
            period_end=datetime.now(UTC),
        )

        allowed, reason = manager.can_deploy(budget, DeployRisk.STANDARD)

        assert not allowed

    def test_generate_report(self):
        manager = ErrorBudgetManager()

        budgets = [
            ErrorBudget(
                "SLO1",
                0.999,
                1_000_000,
                100,
                datetime.now(UTC) - timedelta(days=30),
                datetime.now(UTC),
            ),
            ErrorBudget(
                "SLO2",
                0.999,
                1_000_000,
                900,
                datetime.now(UTC) - timedelta(days=30),
                datetime.now(UTC),
            ),
        ]

        report = manager.generate_report(budgets)

        assert "overall_state" in report
        assert "summary" in report
        assert len(report["budgets"]) == 2


def run_all_sre_tests() -> dict[str, bool]:
    """Run all SRE tests."""
    results = {}

    test_classes = [
        TestSLOWindow,
        TestSLI,
        TestSLO,
        TestBurnRateAlert,
        TestSLOManager,
        TestBudgetState,
        TestErrorBudget,
        TestErrorBudgetManager,
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
    print(f"\nSRE Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_sre_tests()
