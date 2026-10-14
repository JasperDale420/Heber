"""The consumer services default to Redis, and their JetStream settings are valid when switched on."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]
from pydantic import SecretStr

from heber.config import Settings

ROOT = Path(__file__).resolve().parents[1]
_DEFAULTED = re.compile(r"^\$\{[A-Z_]+:-(?P<default>[^}]*)\}$")


def _environment(service: str) -> dict[str, str]:
    compose: dict[str, Any] = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    env: dict[str, str] = {}
    for item in compose["services"][service]["environment"]:
        key, _, value = str(item).partition("=")
        match = _DEFAULTED.match(value)
        env[key] = match.group("default") if match else value
    return env


@pytest.mark.parametrize(("service", "lane"), [("heber-consumer", "live"), ("heber-backfill-consumer", "backfill")])
def test_consumer_service_defaults_to_redis_on_its_own_lane(service: str, lane: str) -> None:
    env = _environment(service)
    assert env["HEBER_INGEST_TRANSPORT"] == "redis"
    assert env["HEBER_INGEST_LANE"] == lane


@pytest.mark.parametrize("service", ["heber-consumer", "heber-backfill-consumer"])
def test_consumer_service_jetstream_settings_pass_validation(service: str, monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _environment(service).items():
        if key.startswith(("HEBER_JETSTREAM_", "HEBER_INGEST_LANE")):
            monkeypatch.setenv(key, value)
    configured = Settings(
        ingest_transport="jetstream",
        nats_username="heber",
        nats_password=SecretStr("compose-contract"),  # pragma: allowlist secret — validation only
    )
    assert configured.jetstream_ack_wait_seconds >= 2 * configured.jetstream_ack_barrier_seconds
