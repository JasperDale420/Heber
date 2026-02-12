"""Static contract tests for scheduled dataflow-health docker wiring."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


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


def test_consumer_and_watch_metrics_ports_exposed_to_host() -> None:
    compose = _load_compose()
    services = compose["services"]

    consumer_ports = services["heber-consumer"].get("ports", [])
    watch_ports = services["heber-watch"].get("ports", [])

    assert "9090:9090" in consumer_ports
    assert "9091:9090" in watch_ports
