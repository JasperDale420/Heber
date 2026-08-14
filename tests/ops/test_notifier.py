"""Tests for the Discord critical-alert notifier."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from heber.config import Settings
from heber.health_monitor.models import CheckResult, Severity, Status
from heber.ops import notifier as notifier_module
from heber.ops.notifier import DiscordNotifier, default_state_path

T0 = datetime(2026, 3, 25, 15, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    """Keep throttling state per-test — the default path is now a real shared file."""
    monkeypatch.setattr(notifier_module, "default_state_path", lambda: tmp_path / "alerts-state.json")


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


# --- severity-aware message prefix -------------------------------------------


def _warning(feed: str = "watchdog") -> CheckResult:
    return CheckResult(
        check_name="stack_watchdog",
        feed=feed,
        severity=Severity.P1_WARNING,
        status=Status.FAIL,
        message="Docker watchdog intervened: unpausing service(s): heber-watch",
        details={},
        ts_checked=T0,
    )


def test_warning_severity_is_not_labelled_critical(tmp_path):
    """A routine self-heal must not cry wolf as a 🚨 CRITICAL."""
    client = MagicMock()
    notifier = DiscordNotifier(
        _settings(tmp_path, alert_min_severity="warning"), client=client, state_path=tmp_path / "s.json"
    )

    notifier.dispatch([_warning()], now=T0)

    content = client.post.call_args.kwargs["json"]["content"]
    assert "CRITICAL" not in content
    assert "WARNING" in content


def test_critical_severity_still_labelled_critical(tmp_path):
    client = MagicMock()
    notifier = DiscordNotifier(
        _settings(tmp_path, alert_min_severity="warning"), client=client, state_path=tmp_path / "s.json"
    )

    notifier.dispatch([_fail()], now=T0)

    assert "CRITICAL" in client.post.call_args.kwargs["json"]["content"]


def test_warning_is_gated_out_when_min_severity_is_critical(tmp_path):
    client = MagicMock()
    notifier = DiscordNotifier(
        _settings(tmp_path, alert_min_severity="critical"), client=client, state_path=tmp_path / "s.json"
    )

    notifier.dispatch([_warning()], now=T0)

    client.post.assert_not_called()


def test_default_state_path_is_off_the_lakehouse_volume():
    """Cooldown state on the data volume dies exactly when the volume does."""
    path = default_state_path()
    assert path.is_absolute()
    assert not str(path).startswith("/Volumes/")
