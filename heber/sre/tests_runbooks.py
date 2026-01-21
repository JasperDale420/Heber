"""Tests for Runbooks and On-Call (PRD §39-40)."""

from datetime import UTC, datetime, timedelta

import pytest

from heber.sre.oncall import (
    CommunicationChannel,
    Incident,
    OnCallManager,
    OnCallRole,
    OnCallSchedule,
)
from heber.sre.runbooks import (
    IncidentSeverity,
    ResolutionAction,
    Runbook,
    RunbookRegistry,
    TriageStep,
)


class TestRunbook:
    """Test Runbook dataclass."""

    def test_to_dict(self):
        runbook = Runbook(
            title="Test Runbook",
            alert_name="TestAlert",
            severity=IncidentSeverity.P2_HIGH,
            symptoms=["High latency", "Errors increasing"],
            triage_steps=[
                TriageStep(1, "kubectl get pods", "Check pods"),
            ],
            common_causes=["Pod OOM"],
            resolutions=[
                ResolutionAction("Pod OOM", "Restart pod"),
            ],
        )

        d = runbook.to_dict()

        assert d["title"] == "Test Runbook"
        assert d["severity"] == "P2"
        assert len(d["triage_steps"]) == 1

    def test_to_markdown(self):
        runbook = Runbook(
            title="Test Runbook",
            alert_name="TestAlert",
            severity=IncidentSeverity.P2_HIGH,
            symptoms=["High latency"],
            triage_steps=[
                TriageStep(1, "kubectl get pods", "Check pods"),
            ],
            common_causes=["Pod OOM"],
            resolutions=[
                ResolutionAction("Pod OOM", "Restart pod"),
            ],
        )

        md = runbook.to_markdown()

        assert "# Test Runbook" in md
        assert "TestAlert" in md
        assert "kubectl get pods" in md


class TestRunbookRegistry:
    """Test RunbookRegistry."""

    def test_default_runbooks_loaded(self):
        registry = RunbookRegistry()

        assert len(registry.runbooks) >= 6
        assert "consumer_lag" in registry.runbooks
        assert "leakage_violation" in registry.runbooks

    def test_get_by_key(self):
        registry = RunbookRegistry()

        runbook = registry.get("consumer_lag")

        assert runbook is not None
        assert runbook.alert_name == "HeberConsumerLagHigh"

    def test_get_by_alert(self):
        registry = RunbookRegistry()

        runbook = registry.get_by_alert("HeberDLQGrowing")

        assert runbook is not None
        assert runbook.title == "DLQ Growing"

    def test_export_all_markdown(self):
        registry = RunbookRegistry()

        md = registry.export_all_markdown()

        assert "Consumer Lag Spike" in md
        assert "DLQ Growing" in md


class TestOnCallSchedule:
    """Test OnCallSchedule."""

    def test_is_active_current(self):
        now = datetime.now(UTC)
        schedule = OnCallSchedule(
            role=OnCallRole.PRIMARY,
            user="alice",
            start_time=now - timedelta(hours=1),
            end_time=now + timedelta(hours=23),
        )

        assert schedule.is_active(now)

    def test_is_active_past(self):
        now = datetime.now(UTC)
        schedule = OnCallSchedule(
            role=OnCallRole.PRIMARY,
            user="alice",
            start_time=now - timedelta(days=7),
            end_time=now - timedelta(days=1),
        )

        assert not schedule.is_active(now)


class TestEscalationPolicy:
    """Test EscalationPolicy."""

    def test_p1_policy(self):
        manager = OnCallManager()

        policy = manager.get_escalation_policy(IncidentSeverity.P1_CRITICAL)

        assert policy.initial_response_minutes == 5
        assert policy.escalation_trigger_minutes == 15

    def test_p4_policy(self):
        manager = OnCallManager()

        policy = manager.get_escalation_policy(IncidentSeverity.P4_LOW)

        assert policy.initial_response_minutes == 1440  # Next business day


class TestOnCallManager:
    """Test OnCallManager."""

    def test_get_channels_for_severity(self):
        manager = OnCallManager()

        channels = manager.get_channels_for_severity(IncidentSeverity.P1_CRITICAL)

        assert CommunicationChannel.PAGERDUTY in channels
        assert CommunicationChannel.SLACK_INCIDENTS in channels

    def test_create_incident(self):
        manager = OnCallManager()

        incident = manager.create_incident(
            incident_id="INC-001",
            title="Test Incident",
            severity=IncidentSeverity.P2_HIGH,
            alert_name="TestAlert",
        )

        assert incident.id == "INC-001"
        assert incident.is_open
        assert incident.acknowledged_at is None

    def test_acknowledge_incident(self):
        manager = OnCallManager()
        incident = manager.create_incident(
            incident_id="INC-002",
            title="Test",
            severity=IncidentSeverity.P2_HIGH,
            alert_name="TestAlert",
        )

        result = manager.acknowledge_incident("INC-002", "bob")

        assert result
        assert incident.acknowledged_at is not None
        assert incident.assigned_to == "bob"

    def test_resolve_incident(self):
        manager = OnCallManager()
        manager.create_incident(
            incident_id="INC-003",
            title="Test",
            severity=IncidentSeverity.P2_HIGH,
            alert_name="TestAlert",
        )

        result = manager.resolve_incident("INC-003")

        assert result
        assert not manager.incidents["INC-003"].is_open

    def test_should_escalate(self):
        manager = OnCallManager()
        incident = Incident(
            id="INC-004",
            title="Old Incident",
            severity=IncidentSeverity.P1_CRITICAL,
            alert_name="TestAlert",
            created_at=datetime.now(UTC) - timedelta(minutes=20),  # 20 min ago
        )
        manager.incidents["INC-004"] = incident

        assert manager.should_escalate(incident)  # P1 escalates after 15 min

    def test_get_open_incidents(self):
        manager = OnCallManager()
        manager.create_incident("INC-005", "Open", IncidentSeverity.P2_HIGH, "Alert1")
        manager.create_incident("INC-006", "Will Close", IncidentSeverity.P3_MEDIUM, "Alert2")
        manager.resolve_incident("INC-006")

        open_incidents = manager.get_open_incidents()

        assert len(open_incidents) == 1
        assert open_incidents[0].id == "INC-005"


class TestIncident:
    """Test Incident dataclass."""

    def test_time_to_acknowledge(self):
        now = datetime.now(UTC)
        incident = Incident(
            id="INC-007",
            title="Test",
            severity=IncidentSeverity.P2_HIGH,
            alert_name="TestAlert",
            created_at=now - timedelta(minutes=5),
            acknowledged_at=now,
        )

        assert incident.time_to_acknowledge is not None
        assert incident.time_to_acknowledge.total_seconds() == pytest.approx(300, rel=1)

    def test_time_to_resolve(self):
        now = datetime.now(UTC)
        incident = Incident(
            id="INC-008",
            title="Test",
            severity=IncidentSeverity.P2_HIGH,
            alert_name="TestAlert",
            created_at=now - timedelta(hours=1),
            resolved_at=now,
        )

        assert incident.time_to_resolve is not None
        assert incident.time_to_resolve.total_seconds() == pytest.approx(3600, rel=1)


def run_all_runbook_tests() -> dict[str, bool]:
    """Run all runbook and on-call tests."""
    results = {}

    test_classes = [
        TestRunbook,
        TestRunbookRegistry,
        TestOnCallSchedule,
        TestEscalationPolicy,
        TestOnCallManager,
        TestIncident,
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
    print(f"\nRunbook & On-Call Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_runbook_tests()
