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
        patch("heber.cli.DiscordNotifier", return_value=fake_notifier),
    ):
        rc = _cmd_alert_check(SimpleNamespace())
    assert rc == 0
    fake_notifier.dispatch.assert_called_once_with([fail])
