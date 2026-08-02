"""Tests for the Discord critical-alert notifier."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

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
        # Default to immediate alerting so the cooldown/recovery tests below read
        # cleanly; the debounce behaviour has its own dedicated tests.
        "alert_debounce_cycles": 1,
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


@pytest.mark.unit
def test_failed_state_replace_preserves_previous_state(tmp_path: Path) -> None:
    state_path = tmp_path / "ops" / "alerts" / "state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text("{}")
    notifier = DiscordNotifier(_settings(tmp_path), client=_client(), state_path=state_path)

    with patch.object(Path, "replace", side_effect=OSError("interrupted replace")):
        notifier.dispatch([_fail()], now=T0)

    assert json.loads(state_path.read_text()) == {}
    assert list(state_path.parent.glob(".state.json.*.tmp")) == []


@pytest.mark.unit
def test_service_specific_state_files_do_not_share_cooldown(tmp_path: Path) -> None:
    settings = _settings(tmp_path, alert_cooldown_seconds=3600)
    first_client = _client()
    second_client = _client()
    first_path = tmp_path / "ops" / "alerts" / "dataflow-health-state.json"
    second_path = tmp_path / "ops" / "alerts" / "health-monitor-state.json"

    DiscordNotifier(settings, client=first_client, state_path=first_path).dispatch([_fail()], now=T0)
    DiscordNotifier(settings, client=second_client, state_path=second_path).dispatch(
        [_fail()],
        now=T0 + timedelta(minutes=10),
    )

    assert first_client.post.call_count == 1
    assert second_client.post.call_count == 1
    assert first_path.read_text() != ""
    assert second_path.read_text() != ""


@pytest.mark.unit
def test_post_without_injected_client_uses_shared_http_factory(tmp_path: Path) -> None:
    # When no client is injected, the notifier must build one via the repo's
    # shared http_client factory (logging hooks, standard defaults) and close it.
    client = _client()
    with patch("heber.ops.notifier.create_http_client", return_value=client) as factory:
        n = DiscordNotifier(_settings(tmp_path))
        n.dispatch([_fail()], now=T0)
    factory.assert_called_once_with(timeout=10.0)
    assert client.post.call_count == 1
    client.close.assert_called_once()


@pytest.mark.unit
def test_debounce_suppresses_single_cycle_flap(tmp_path: Path) -> None:
    # One failing cycle then recovery (debounce=2) must be completely silent —
    # this is the transient read-error / inter-batch-gap blip that caused the spam.
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_debounce_cycles=2), client=client)
    n.dispatch([_fail("darkpool")], now=T0)
    n.dispatch([_passing("darkpool")], now=T0 + timedelta(minutes=5))
    assert client.post.call_count == 0


@pytest.mark.unit
def test_debounce_alerts_after_consecutive_fails(tmp_path: Path) -> None:
    # Two consecutive failing cycles clears the debounce -> exactly one CRITICAL.
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_debounce_cycles=2), client=client)
    n.dispatch([_fail("darkpool")], now=T0)
    assert client.post.call_count == 0  # first cycle accrues, does not alert
    n.dispatch([_fail("darkpool")], now=T0 + timedelta(minutes=5))
    assert client.post.call_count == 1
    assert "critical" in client.post.call_args.kwargs["json"]["content"].lower()


@pytest.mark.unit
def test_debounce_recovery_note_only_after_real_alert(tmp_path: Path) -> None:
    # After a debounced CRITICAL fires, a recovery DOES send a note...
    client = _client()
    n = DiscordNotifier(_settings(tmp_path, alert_debounce_cycles=2), client=client)
    n.dispatch([_fail("darkpool")], now=T0)
    n.dispatch([_fail("darkpool")], now=T0 + timedelta(minutes=5))  # CRITICAL
    n.dispatch([_passing("darkpool")], now=T0 + timedelta(minutes=10))  # RECOVERED
    assert client.post.call_count == 2
    assert "recover" in client.post.call_args.kwargs["json"]["content"].lower()
