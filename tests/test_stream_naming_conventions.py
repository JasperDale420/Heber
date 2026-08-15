"""Regression tests for stream naming consistency."""

from __future__ import annotations

from heber.bus import StreamName
from heber.bus.streams import StreamConfig, StreamPriority
from heber.config import settings
from heber.watch.consumer import AlertWatchConsumer


class _NoopManager:
    async def has_alert_claim_async(self, alert_id: str) -> bool:
        return False

    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        return None


def test_event_bus_stream_names_share_namespace() -> None:
    expected_prefix = "heber:events:"
    assert all(stream.value.startswith(expected_prefix) for stream in StreamName)


def test_stream_registry_uses_events_namespace() -> None:
    config = StreamConfig("test", "test_dataset", StreamPriority.NORMAL, "test-consumer")
    assert config.stream_key == "heber:events:test"


def test_watch_consumer_defaults_to_configured_stream_name() -> None:
    consumer = AlertWatchConsumer(redis_client=object(), watch_manager=_NoopManager())
    assert consumer.stream_name == settings.redis_stream_name
