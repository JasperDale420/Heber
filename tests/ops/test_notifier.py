"""Tests for the Discord critical-alert notifier."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops.notifier import DiscordNotifier

T0 = datetime(2026, 3, 25, 15, 0, tzinfo=UTC)


def _settings(tmp_path: Path, **overrides) -> Settings:
    base = {
        "data_root": str(tmp_path),
        "alert_discord_enabled": True,
        "alert_discord_webhook_url": "https://discord.com/api/webhooks/1/abc",
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)


def _fail(feed: str = "flow_alerts") -> CheckResult:
    return CheckResult(
        check_name="feed_liveness",
        feed=feed,
        severity=Severity.P0_CRITICAL,
        status=Status.FAIL,
        message=f"{feed} dark",
        details={},
        ts_checked=T0,
    )


def _passing(feed: str = "flow_alerts") -> CheckResult:
    return CheckResult(
        check_name="feed_liveness",
        feed=feed,
        severity=Severity.P2_INFO,
        status=Status.PASS,
        message=f"{feed} ok",
        details={},
        ts_checked=T0,
    )


def _client() -> MagicMock:
    client = MagicMock()
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    client.post = MagicMock(return_value=resp)
    return client


@pytest.mark.unit
def test_critical_sends_once(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)
    assert client.post.call_count == 1


@pytest.mark.unit
def test_repeat_within_cooldown_suppressed(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_cooldown_seconds=3600), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_fail()], now=T0 + timedelta(minutes=10))
    assert client.post.call_count == 1


@pytest.mark.unit
def test_repeat_after_cooldown_resends(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_cooldown_seconds=3600), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_fail()], now=T0 + timedelta(hours=2))
    assert client.post.call_count == 2


@pytest.mark.unit
def test_recovery_sent_after_fail(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)
    n.dispatch([_passing()], now=T0 + timedelta(minutes=10))
    assert client.post.call_count == 2
    recovery_call = client.post.call_args_list[1]
    assert "recover" in recovery_call.kwargs["json"]["content"].lower()


@pytest.mark.unit
def test_pass_without_prior_fail_is_silent(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_passing()], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_warning_below_min_severity_not_sent(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path), client=client)
    warn = CheckResult(
        check_name="x",
        feed="f",
        severity=Severity.P1_WARNING,
        status=Status.WARN,
        message="m",
        details={},
        ts_checked=T0,
    )
    n.dispatch([warn], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_disabled_notifier_sends_nothing(tmp_path: Path) -> None:
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_discord_enabled=False), client=client)
    n.dispatch([_fail()], now=T0)
    assert client.post.call_count == 0


@pytest.mark.unit
def test_post_failure_is_swallowed(tmp_path: Path) -> None:
    client = MagicMock()
    client.post = MagicMock(side_effect=OSError("network down"))
    n = DiscordNotifier(_settings(tmp_path), client=client)
    n.dispatch([_fail()], now=T0)  # must not raise


@pytest.mark.unit
def test_state_persists_across_instances(tmp_path: Path) -> None:
    settings = _settings(tmp_path, alert_cooldown_seconds=3600)
    n1 = DiscordNotifier(settings, client=_client())
    n1.dispatch([_fail()], now=T0)
    client2 = _client()
    n2 = DiscordNotifier(settings, client=client2)
    n2.dispatch([_fail()], now=T0 + timedelta(minutes=10))  # still within cooldown
    assert client2.post.call_count == 0
