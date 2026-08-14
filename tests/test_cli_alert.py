"""Tests for `heber alert-test` and `heber alert-calibrate` CLI commands."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from heber.cli import _cmd_alert_check, _cmd_alert_test


@pytest.mark.unit
def test_alert_test_sends_message() -> None:
    fake_notifier = MagicMock()
    fake_notifier.send_test = MagicMock(return_value=True)
    with patch("heber.cli.DiscordNotifier", return_value=fake_notifier):
        rc = _cmd_alert_test(SimpleNamespace(message=None))
    assert rc == 0
    fake_notifier.send_test.assert_called_once()


@pytest.mark.unit
def test_alert_test_reports_failure() -> None:
    fake_notifier = MagicMock()
    fake_notifier.send_test = MagicMock(return_value=False)
    with patch("heber.cli.DiscordNotifier", return_value=fake_notifier):
        rc = _cmd_alert_test(SimpleNamespace(message="hi"))
    assert rc == 1


@pytest.mark.unit
def test_alert_check_runs_one_cycle_and_dispatches() -> None:
    """alert-check runs a single liveness cycle and dispatches the results once."""
    from datetime import UTC, datetime

    from heber.health_monitor.models import CheckResult, Severity, Status

    fail = CheckResult(
        check_name="feed_liveness",
        feed="darkpool",
        severity=Severity.P0_CRITICAL,
        status=Status.FAIL,
        message="darkpool dark",
        details={},
        ts_checked=datetime.now(UTC),
    )
    fake_notifier = MagicMock()
    with (
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(return_value=[fail])),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[]),
        patch("heber.cli.DiscordNotifier", return_value=fake_notifier),
    ):
        rc = _cmd_alert_check(SimpleNamespace())
    assert rc == 0
    fake_notifier.dispatch.assert_called_once_with([fail])


@pytest.mark.unit
def test_alert_check_dispatches_stack_and_liveness_together() -> None:
    """One dispatch, one state file — two notifiers would clobber each other's throttling."""
    stack = _stack_result()
    fake_notifier = MagicMock()
    with (
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(return_value=[])),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[stack]),
        patch("heber.cli.DiscordNotifier", return_value=fake_notifier),
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 0

    fake_notifier.dispatch.assert_called_once_with([stack])


@pytest.mark.unit
def test_a_broken_lakehouse_cannot_mute_the_stack_alarm() -> None:
    """The stack alarm exists for outages; a failing HeberReader must not swallow it."""
    stack = _stack_result()
    fake_notifier = MagicMock()
    with (
        patch("heber.cli.DiscordNotifier", return_value=fake_notifier),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[stack]),
        patch("heber.reader.HeberReader", side_effect=OSError("volume gone")),
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 0

    fake_notifier.dispatch.assert_called_once_with([stack])


@pytest.mark.unit
def test_stack_warnings_are_not_gated_out_by_a_critical_only_floor() -> None:
    """`watchdog acted` is a P1 warning; the deployed floor is `critical`."""
    captured = {}

    def _capture(settings, **kwargs):
        captured["min_severity"] = settings.alert_min_severity
        return MagicMock()

    with (
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(return_value=[])),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[]),
        patch("heber.cli.DiscordNotifier", side_effect=_capture),
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 0

    assert captured["min_severity"] == "warning"


def _stack_result():
    from datetime import UTC, datetime

    from heber.health_monitor.models import CheckResult, Severity, Status

    return CheckResult(
        check_name="stack_container",
        feed="data-gateway-redis",
        severity=Severity.P0_CRITICAL,
        status=Status.FAIL,
        message="Container data-gateway-redis is exited (expected running)",
        details={},
        ts_checked=datetime.now(UTC),
    )
