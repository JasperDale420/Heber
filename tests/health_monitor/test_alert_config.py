"""Tests for HEBER_ALERT_* settings."""

from __future__ import annotations

import pytest

from heber.config import Settings


@pytest.mark.unit
def test_alert_defaults() -> None:
    s = Settings(_env_file=None)
    assert s.alert_discord_enabled is False
    assert s.alert_discord_webhook_url == ""
    assert s.alert_min_severity == "critical"
    assert s.alert_cooldown_seconds == 3600
    assert s.alert_send_recovery is True
    assert s.alert_liveness_check_interval_seconds == 300
    assert s.alert_floor_overrides == {}


@pytest.mark.unit
def test_alert_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HEBER_ALERT_DISCORD_ENABLED", "true")
    monkeypatch.setenv("HEBER_ALERT_DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/abc")
    monkeypatch.setenv("HEBER_ALERT_FLOOR_OVERRIDES", '{"darkpool": 8, "flow_alerts": 25}')
    s = Settings(_env_file=None)
    assert s.alert_discord_enabled is True
    assert s.alert_discord_webhook_url.endswith("/abc")
    assert s.alert_floor_overrides == {"darkpool": 8, "flow_alerts": 25}
