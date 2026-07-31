"""JetStream alert rules use Prometheus durations and notifier recovery state."""

from heber.ops.alerting import get_alerting_rules


def test_jetstream_and_availability_rules_have_sustained_thresholds() -> None:
    rules = {rule.name: rule for rule in get_alerting_rules()}

    assert rules["HeberJetStreamConsumerUnbound"].for_duration == "1m"
    assert rules["HeberJetStreamAckBacklog"].for_duration == "5m"
    assert rules["HeberAvailabilityLagSpike"].for_duration == "5m"
    assert "> 60" in rules["HeberAvailabilityLagSpike"].expr
    assert rules["HeberAvailabilityLagCritical"].for_duration == "1m"
    assert "300" in rules["HeberAvailabilityLagCritical"].expr
    assert rules["HeberDurableBackfillLedgerCapacityHigh"].for_duration == "5m"
    assert "0.8" in rules["HeberDurableBackfillLedgerCapacityHigh"].expr
    assert rules["HeberDurableBackfillLedgerCapacityCritical"].for_duration == "1m"
    assert "0.95" in rules["HeberDurableBackfillLedgerCapacityCritical"].expr
