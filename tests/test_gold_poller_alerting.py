"""Gold poller error outcomes must page rather than silently leaving stale Gold."""

from heber.ops.alerting import Severity, get_alerting_rules


def test_gold_pipeline_error_rule_is_critical_and_covers_expected_input_misses() -> None:
    rules = {rule.name: rule for rule in get_alerting_rules()}

    rule = rules["HeberGoldPipelineError"]
    assert rule.severity == Severity.CRITICAL
    assert rule.for_duration == "1m"
    assert 'status="error"' in rule.expr
    assert "25h" in rule.expr
