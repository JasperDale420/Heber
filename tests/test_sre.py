"""Tests for SLOs and Error Budgets (PRD §37, §38)."""

from datetime import UTC, datetime, timedelta

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
    """Test SLOWindow enum."""

    def test_window_durations(self):
        assert SLOWindow.WINDOW_1H.value == "1h"
        assert SLOWindow.WINDOW_1D.value == "1d"
        assert SLOWindow.WINDOW_7D.value == "7d"
        assert SLOWindow.WINDOW_30D.value == "30d"


class TestSLI:
    """Test SLI dataclass."""

    def test_sli_creation(self):
        sli = SLI(
            name="availability",
            metric_query="up{service='heber'}",
            description="Service availability",
        )

        assert sli.name == "availability"

    def test_sli_to_dict(self):
        sli = SLI(
            name="availability",
            metric_query="up{service='heber'}",
            description="Service availability",
        )

        d = sli.to_dict()

        assert d["name"] == "availability"
        assert "metric_query" in d


class TestSLO:
    """Test SLO dataclass."""

    def test_slo_creation(self):
        slo = SLO(
            name="heber-availability",
            sli=SLI("availability", "up{service='heber'}"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        assert slo.target == 0.999
        assert slo.error_budget_ratio == (1 - 0.999)

    def test_slo_target_percentage(self):
        slo = SLO(
            name="heber-availability",
            sli=SLI("availability", "up{service='heber'}"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        assert slo.target_percentage == 99.9


class TestBurnRateAlert:
    """Test BurnRateAlert."""

    def test_create_alert(self):
        alert = BurnRateAlert(
            burn_rate=14.4,
            window=SLOWindow.WINDOW_1H,
            severity=AlertSeverity.CRITICAL,
            action="Page on-call immediately",
        )

        assert alert.burn_rate == 14.4

    def test_to_prometheus_rule(self):
        alert = BurnRateAlert(
            burn_rate=14.4,
            window=SLOWindow.WINDOW_1H,
            severity=AlertSeverity.CRITICAL,
            action="Page on-call immediately",
        )
        slo = SLO(
            name="Availability",
            sli=SLI("availability", "up{service='heber'}"),
            target=0.999,
            window=SLOWindow.WINDOW_30D,
        )

        rule = alert.to_prometheus_rule(slo)

        assert "burn_rate" in rule["alert"].lower() or "BurnRate" in rule["alert"]


class TestSLOManager:
    """Test SLOManager."""

    def test_get_slo(self):
        manager = SLOManager()

        slo = manager.get_slo("Ingestion Availability")

        assert slo is not None
        assert slo.target == 0.999

    def test_list_slos(self):
        manager = SLOManager()

        slos = manager.list_slos()

        assert len(slos) >= 3

    def test_default_slos_loaded(self):
        manager = SLOManager()

        assert manager.get_slo("Ingestion Availability") is not None
        assert manager.get_slo("Write Success Rate") is not None
        assert manager.get_slo("Catalog Availability") is not None


class TestBudgetState:
    """Test BudgetState enum."""

    def test_states(self):
        assert BudgetState.HEALTHY.value == "healthy"
        assert BudgetState.WARNING.value == "warning"
        assert BudgetState.CRITICAL.value == "critical"
        assert BudgetState.EXHAUSTED.value == "exhausted"


class TestErrorBudget:
    """Test ErrorBudget calculation."""

    def test_healthy_budget(self):
        now = datetime.now(UTC)
        budget = ErrorBudget(
            slo_name="test",
            target=0.999,
            total_requests=1_000_000,
            error_count=100,
            period_start=now - timedelta(days=30),
            period_end=now,
        )

        assert budget.state == BudgetState.HEALTHY
        assert budget.remaining_ratio > 0.5

    def test_exhausted_budget(self):
        now = datetime.now(UTC)
        budget = ErrorBudget(
            slo_name="test",
            target=0.999,
            total_requests=1_000_000,
            error_count=5000,
            period_start=now - timedelta(days=30),
            period_end=now,
        )

        assert budget.state == BudgetState.EXHAUSTED


class TestErrorBudgetManager:
    """Test ErrorBudgetManager."""

    def test_calculate_budget(self):
        manager = ErrorBudgetManager()

        budget = manager.calculate_budget(
            slo_name="heber-availability",
            target=0.999,
            total_requests=1_000_000,
            error_count=100,
        )

        assert isinstance(budget, ErrorBudget)

    def test_can_deploy(self):
        manager = ErrorBudgetManager()
        now = datetime.now(UTC)

        budget = ErrorBudget(
            slo_name="test",
            target=0.999,
            total_requests=1_000_000,
            error_count=100,
            period_start=now - timedelta(days=30),
            period_end=now,
        )

        allowed, reason = manager.can_deploy(budget, DeployRisk.STANDARD)

        assert allowed

    def test_generate_report(self):
        manager = ErrorBudgetManager()
        now = datetime.now(UTC)

        budgets = [
            ErrorBudget(
                slo_name="test",
                target=0.999,
                total_requests=1_000_000,
                error_count=100,
                period_start=now - timedelta(days=30),
                period_end=now,
            )
        ]

        report = manager.generate_report(budgets)

        assert "budgets" in report
