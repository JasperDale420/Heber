"""Tests for `heber alert-test` and `heber alert-calibrate` CLI commands."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from heber.cli import _cmd_alert_test


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
