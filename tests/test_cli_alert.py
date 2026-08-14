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


# --- dead-man heartbeat ------------------------------------------------------


@pytest.mark.unit
def test_a_completed_run_pings_the_heartbeat() -> None:
    """No ping means the external service alerts — so a healthy run must ping."""
    with (
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(return_value=[])),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[]),
        patch("heber.cli.DiscordNotifier", return_value=MagicMock(**{"dispatch.return_value": True})),
        patch("heber.ops.heartbeat.ping") as ping,
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 0

    assert ping.call_args.kwargs["ok"] is True


@pytest.mark.unit
def test_a_crashed_run_reports_failure_instead_of_pinging_healthy() -> None:
    """The alarm dying is exactly what the dead-man exists to catch."""
    with (
        patch("heber.ops.stack_check.run_stack_checks", side_effect=RuntimeError("docker exploded")),
        patch("heber.cli.DiscordNotifier", return_value=MagicMock()),
        patch("heber.ops.heartbeat.ping") as ping,
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 1

    assert ping.call_args.kwargs["ok"] is False


@pytest.mark.unit
def test_a_failed_discord_send_reports_failure() -> None:
    """A revoked webhook makes every alert vanish silently; the heartbeat is the backstop."""
    with (
        patch("heber.health_monitor.checks.liveness.run_liveness_checks", new=AsyncMock(return_value=[])),
        patch("heber.ops.stack_check.run_stack_checks", return_value=[_stack_result()]),
        patch("heber.cli.DiscordNotifier", return_value=MagicMock(**{"dispatch.return_value": False})),
        patch("heber.ops.heartbeat.ping") as ping,
    ):
        assert _cmd_alert_check(SimpleNamespace()) == 0

    assert ping.call_args.kwargs["ok"] is False


@pytest.mark.unit
def test_the_alert_heartbeat_is_a_separate_check_from_dataflow_healths() -> None:
    """One check for both jobs would let a live dataflow-health mask a dead alert-check."""
    from heber.config import Settings

    s = Settings(_env_file=None, data_root="/tmp", heartbeat_url="https://hc/dataflow")
    assert s.alert_heartbeat_url != s.heartbeat_url
