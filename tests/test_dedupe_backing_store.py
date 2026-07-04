"""Tests for the Redis-backed exact dedupe store behind the Bloom prefilter.

A Bloom filter alone turns false positives into silently dropped events
(the consumer ACKs them as "duplicate"). The backing store makes a Bloom
hit an exact check: Bloom-negative is provably new (no Redis call),
Bloom-positive consults Redis. Redis failure fails OPEN — a duplicate
write is recoverable, a dropped event is not.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

from heber.config import settings
from heber.ops.reliability import (
    DEDUPE_KEY_PREFIX,
    EventDeduplicator,
    RedisDedupeStore,
)
from heber.writer.consumer import EventConsumer

NOW = datetime(2026, 6, 10, 15, 0, tzinfo=UTC)


class FakeStore:
    """In-memory stand-in for RedisDedupeStore."""

    def __init__(self) -> None:
        self.seen: set[str] = set()

    def contains(self, event_id: str) -> bool:
        return event_id in self.seen

    def add(self, event_id: str) -> None:
        self.seen.add(event_id)


def _bars_payload(event_id: str) -> dict:
    return {
        "event_id": event_id,
        "provider": "alpaca",
        "feed": "bars",
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW.isoformat(),
        "ts_ingest": NOW.isoformat(),
        "payload": {
            "t": NOW.isoformat(),
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100.5",
            "v": "1000",
        },
    }


class TestBloomFalsePositiveBehavior:
    def test_without_backing_store_a_bloom_fp_drops_the_event(self, monkeypatch) -> None:
        """Documents the hazard: bloom-only mode treats FPs as duplicates."""
        dedup = EventDeduplicator()
        monkeypatch.setattr(dedup.bloom, "contains", lambda _eid: True)

        result = dedup.check("never-seen-event")

        assert result.is_duplicate is True
        assert result.reason == "bloom_filter_match"

    def test_backing_store_recovers_bloom_false_positive(self, monkeypatch) -> None:
        dedup = EventDeduplicator(backing_store=FakeStore())
        monkeypatch.setattr(dedup.bloom, "contains", lambda _eid: True)

        result = dedup.check("never-seen-event")

        assert result.is_duplicate is False

    def test_backing_store_confirms_true_duplicate(self) -> None:
        store = FakeStore()
        dedup = EventDeduplicator(backing_store=store)
        dedup.register("evt-1")

        result = dedup.check("evt-1")

        assert result.is_duplicate is True
        assert result.reason == "backing_store_match"

    def test_register_writes_to_backing_store(self) -> None:
        store = FakeStore()
        dedup = EventDeduplicator(backing_store=store)

        dedup.register("evt-2")

        assert "evt-2" in store.seen

    def test_bloom_negative_skips_backing_store(self) -> None:
        store = MagicMock()
        dedup = EventDeduplicator(backing_store=store)

        result = dedup.check("brand-new-event")

        assert result.is_duplicate is False
        store.contains.assert_not_called()


class TestRedisDedupeStore:
    def test_contains_checks_prefixed_key(self) -> None:
        client = MagicMock()
        client.exists.return_value = 1
        store = RedisDedupeStore(redis_url="redis://unused:0", ttl_seconds=100, client=client)

        assert store.contains("evt-a") is True
        client.exists.assert_called_once_with(f"{DEDUPE_KEY_PREFIX}evt-a")

    def test_add_sets_with_nx_and_ttl(self) -> None:
        client = MagicMock()
        store = RedisDedupeStore(redis_url="redis://unused:0", ttl_seconds=123, client=client)

        store.add("evt-b")

        client.set.assert_called_once_with(f"{DEDUPE_KEY_PREFIX}evt-b", 1, ex=123, nx=True)

    def test_contains_fails_open_on_redis_error(self) -> None:
        client = MagicMock()
        client.exists.side_effect = ConnectionError("redis down")
        store = RedisDedupeStore(redis_url="redis://unused:0", ttl_seconds=100, client=client)

        assert store.contains("evt-c") is False
        assert store.error_count == 1

    def test_add_swallows_redis_error(self) -> None:
        client = MagicMock()
        client.set.side_effect = ConnectionError("redis down")
        store = RedisDedupeStore(redis_url="redis://unused:0", ttl_seconds=100, client=client)

        store.add("evt-d")  # must not raise

        assert store.error_count == 1

    def test_client_is_lazy(self) -> None:
        """Constructing the store must not connect — consumers build it at import-ish time."""
        store = RedisDedupeStore(redis_url="redis://localhost:1", ttl_seconds=100)
        assert store._client is None


class TestConsumerWiring:
    def test_consumer_gets_redis_backed_store_when_enabled(self, monkeypatch) -> None:
        monkeypatch.setattr(settings, "dedupe_redis_enabled", True)

        consumer = EventConsumer()

        assert isinstance(consumer.event_deduplicator.backing_store, RedisDedupeStore)
        assert consumer.event_deduplicator.backing_store.ttl_seconds == settings.dedupe_redis_ttl_seconds

    def test_consumer_has_no_store_when_disabled(self, monkeypatch) -> None:
        monkeypatch.setattr(settings, "dedupe_redis_enabled", False)

        consumer = EventConsumer()

        assert consumer.event_deduplicator.backing_store is None

    def test_bloom_fp_does_not_drop_event_end_to_end(self, monkeypatch) -> None:
        """The audit F-6 scenario: bloom FP on a never-seen event must still write."""
        consumer = EventConsumer(
            event_deduplicator=EventDeduplicator(backing_store=FakeStore()),
        )
        consumer.bronze_writer.write = MagicMock()
        consumer.silver_writer.write_row = MagicMock()
        monkeypatch.setattr(consumer.event_deduplicator.bloom, "contains", lambda _eid: True)

        success, error, retryable = consumer._process_event_once({"data": json.dumps(_bars_payload("evt-bloom-fp"))})

        assert success is True
        assert error is None
        consumer.bronze_writer.write.assert_called_once()
        consumer.silver_writer.write_row.assert_called_once()
