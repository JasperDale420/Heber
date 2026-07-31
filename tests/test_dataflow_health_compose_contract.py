"""Static contract tests for scheduled dataflow-health docker wiring."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from heber.config import Settings

ROOT = Path(__file__).resolve().parents[1]
ALERT_ENV = {
    "HEBER_ALERT_DISCORD_ENABLED": "${HEBER_ALERT_DISCORD_ENABLED:-false}",
    "HEBER_ALERT_DISCORD_WEBHOOK_URL": "${HEBER_ALERT_DISCORD_WEBHOOK_URL:-}",
    "HEBER_ALERT_MIN_SEVERITY": "${HEBER_ALERT_MIN_SEVERITY:-critical}",
    "HEBER_ALERT_COOLDOWN_SECONDS": "${HEBER_ALERT_COOLDOWN_SECONDS:-3600}",
    "HEBER_ALERT_SEND_RECOVERY": "${HEBER_ALERT_SEND_RECOVERY:-true}",
    "HEBER_ALERT_DEBOUNCE_CYCLES": "${HEBER_ALERT_DEBOUNCE_CYCLES:-2}",
    "HEBER_ALERT_LIVENESS_CHECK_INTERVAL_SECONDS": "${HEBER_ALERT_LIVENESS_CHECK_INTERVAL_SECONDS:-300}",
    "HEBER_ALERT_FLOOR_OVERRIDES": "${HEBER_ALERT_FLOOR_OVERRIDES:-{}}",
}


def _load_compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def _env_to_dict(raw_env: list[str] | dict[str, str] | None) -> dict[str, str]:
    if raw_env is None:
        return {}
    if isinstance(raw_env, dict):
        return {str(k): str(v) for k, v in raw_env.items()}
    parsed: dict[str, str] = {}
    for entry in raw_env:
        key, _, value = entry.partition("=")
        parsed[key] = value
    return parsed


def test_dataflow_health_service_exists_with_expected_command_and_env() -> None:
    compose = _load_compose()
    services = compose["services"]

    assert "heber-dataflow-health" in services
    service = services["heber-dataflow-health"]

    command = service.get("command", [])
    assert command[:3] == ["python", "-m", "heber.ops.dataflow_health"]
    assert "--loop" in command
    assert "--mode" in command
    assert "scheduled" in command

    env = _env_to_dict(service.get("environment"))
    assert env["HEBER_HEALTH_CONSUMER_METRICS_URL"] == "http://heber-consumer:9090/metrics"
    assert env["HEBER_HEALTH_WATCH_METRICS_URL"] == "http://heber-watch:9090/metrics"
    assert env["HEBER_HEALTH_REPORT_DIR"] == "/data/ops/dataflow-health"
    assert env["HEBER_HEARTBEAT_URL"] == "${HEBER_HEARTBEAT_URL:-}"
    assert {key: env[key] for key in ALERT_ENV} == ALERT_ENV


def test_health_monitor_receives_discord_configuration() -> None:
    compose = _load_compose()
    env = _env_to_dict(compose["services"]["heber-health-monitor"].get("environment"))

    assert {key: env[key] for key in ALERT_ENV} == ALERT_ENV
    assert env["HEBER_INGEST_TRANSPORT"] == "${HEBER_LIVE_INGEST_TRANSPORT:-redis}"


def test_sample_environment_documents_alert_and_deadman_settings() -> None:
    keys = {
        line.partition("=")[0]
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }

    assert {"HEBER_HEARTBEAT_URL", *ALERT_ENV} <= keys


def test_rendered_monitor_alert_environment_parses_as_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker Compose is required to validate the rendered container environment")

    for key in {"HEBER_HEARTBEAT_URL", *ALERT_ENV}:
        monkeypatch.setenv(key, "")
    rendered = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=ROOT,
        env=os.environ.copy(),
        check=True,
        capture_output=True,
        text=True,
    )
    services = json.loads(rendered.stdout)["services"]

    for service_name in ("heber-dataflow-health", "heber-health-monitor"):
        environment = services[service_name]["environment"]
        for key, value in environment.items():
            if key.startswith("HEBER_ALERT_"):
                monkeypatch.setenv(key, value)
        settings = Settings(_env_file=None)
        assert settings.alert_floor_overrides == {}
        assert settings.alert_discord_enabled is False
        assert settings.alert_send_recovery is True


def test_consumer_and_watch_metrics_ports_exposed_to_host() -> None:
    compose = _load_compose()
    services = compose["services"]

    consumer_ports = services["heber-consumer"].get("ports", [])
    watch_ports = services["heber-watch"].get("ports", [])

    # Published loopback-only: dataflow-health scrapes from the same host.
    assert "127.0.0.1:9090:9090" in consumer_ports
    assert "127.0.0.1:9091:9090" in watch_ports


def test_watch_service_has_gateway_api_key_env_wiring() -> None:
    compose = _load_compose()
    services = compose["services"]
    env = _env_to_dict(services["heber-watch"].get("environment"))

    assert "HEBER_WATCH_GATEWAY_API_KEY" in env
