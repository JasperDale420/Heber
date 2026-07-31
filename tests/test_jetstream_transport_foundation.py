"""Contracts for the opt-in JetStream writer transport."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from heber.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def _environment(service: dict[str, object]) -> dict[str, str]:
    values = service["environment"]
    assert isinstance(values, list)
    return dict(item.split("=", 1) for item in values)


def test_jetstream_dependency_and_consumer_api_are_pinned() -> None:
    assert version("nats-py") == "2.15.0"

    from nats.js.api import ConsumerConfig

    config = ConsumerConfig(ack_wait=300, max_ack_pending=1000)
    assert config.ack_wait == 300
    assert config.max_ack_pending == 1000


def test_redis_remains_the_default_transport() -> None:
    settings = Settings(_env_file=None)

    assert settings.ingest_transport == "redis"
    assert settings.ingest_lane == "live"
    assert settings.jetstream_live_stream_name == "HEBER_LIVE"
    assert settings.jetstream_backfill_stream_name == "HEBER_BACKFILL"
    assert settings.jetstream_live_durable_name == "heber-live-writers"
    assert settings.jetstream_backfill_durable_name == "heber-backfill-writers"
    assert settings.jetstream_stream_name == "HEBER_LIVE"
    assert settings.jetstream_durable_name == "heber-live-writers"
    assert settings.jetstream_ack_wait_seconds == 300
    assert settings.jetstream_max_ack_pending == settings.redis_read_batch_size * 2


def test_backfill_lane_selects_backfill_stream_and_consumer() -> None:
    settings = Settings(_env_file=None, ingest_lane="backfill")

    assert settings.jetstream_stream_name == "HEBER_BACKFILL"
    assert settings.jetstream_durable_name == "heber-backfill-writers"


def test_max_ack_pending_tracks_the_configured_batch_size() -> None:
    settings = Settings(_env_file=None, redis_read_batch_size=2400)

    assert settings.jetstream_max_ack_pending == 4800


def test_explicit_max_ack_pending_is_respected() -> None:
    settings = Settings(_env_file=None, redis_read_batch_size=2400, jetstream_max_ack_pending=9000)

    assert settings.jetstream_max_ack_pending == 9000


def test_backfill_lane_pending_window_covers_largest_allowed_chunk() -> None:
    settings = Settings(
        _env_file=None,
        ingest_transport="jetstream",
        ingest_lane="backfill",
        nats_username="heber",
        nats_password="secret",  # pragma: allowlist secret
    )

    assert settings.jetstream_max_ack_pending >= settings.backfill_proof_max_expected_records


def test_backfill_lane_rejects_pending_window_smaller_than_allowed_chunk() -> None:
    with pytest.raises(ValueError, match="max_ack_pending"):
        Settings(
            _env_file=None,
            ingest_transport="jetstream",
            ingest_lane="backfill",
            nats_username="heber",
            nats_password="secret",  # pragma: allowlist secret
            jetstream_max_ack_pending=1000,
            backfill_proof_max_expected_records=5000,
        )


def test_jetstream_transport_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="NATS_USERNAME.*NATS_PASSWORD"):
        Settings(_env_file=None, ingest_transport="jetstream")


def test_jetstream_password_is_secret_and_available_to_the_client() -> None:
    settings = Settings(
        _env_file=None,
        ingest_transport="jetstream",
        nats_url="nats://nats:4222",
        nats_username="heber",
        nats_password="test-only-password",  # pragma: allowlist secret
    )

    assert settings.nats_password is not None
    assert str(settings.nats_password) == "**********"
    assert settings.nats_password.get_secret_value() == "test-only-password"  # pragma: allowlist secret
    assert "test-only-password" not in repr(settings)


def test_unknown_transport_or_lane_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, ingest_transport="kafka")
    with pytest.raises(ValidationError):
        Settings(_env_file=None, ingest_lane="archive")


def test_writer_services_are_redis_by_default_and_ready_for_jetstream() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    live = _environment(services["heber-consumer"])
    backfill = _environment(services["heber-backfill-consumer"])

    assert live["HEBER_INGEST_TRANSPORT"] == "${HEBER_LIVE_INGEST_TRANSPORT:-redis}"
    assert backfill["HEBER_INGEST_TRANSPORT"] == "${HEBER_BACKFILL_INGEST_TRANSPORT:-redis}"
    assert live["HEBER_INGEST_LANE"] == "live"
    assert backfill["HEBER_INGEST_LANE"] == "backfill"

    for environment in (live, backfill):
        assert environment["HEBER_NATS_URL"] == "${HEBER_NATS_URL:-nats://host.docker.internal:4222}"
        assert environment["HEBER_NATS_USERNAME"] == "${HEBER_NATS_USERNAME:-}"
        assert environment["HEBER_NATS_PASSWORD"] == "${HEBER_NATS_PASSWORD:-}"

    watch = _environment(services["heber-watch"])
    health = _environment(services["heber-dataflow-health"])
    assert watch["HEBER_WATCH_INGEST_TRANSPORT"] == "${HEBER_WATCH_INGEST_TRANSPORT:-redis}"
    assert watch["HEBER_NATS_URL"] == "${HEBER_NATS_URL:-nats://host.docker.internal:4222}"
    assert watch["HEBER_NATS_USERNAME"] == "${HEBER_NATS_USERNAME:-}"
    assert watch["HEBER_NATS_PASSWORD"] == "${HEBER_NATS_PASSWORD:-}"
    assert health["HEBER_INGEST_TRANSPORT"] == "${HEBER_LIVE_INGEST_TRANSPORT:-redis}"
    assert health["HEBER_WATCH_INGEST_TRANSPORT"] == "${HEBER_WATCH_INGEST_TRANSPORT:-redis}"
