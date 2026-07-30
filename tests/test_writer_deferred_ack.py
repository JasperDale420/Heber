"""The consumer must hold its ACKs until the events are written.

``_consume_iteration`` acknowledged a batch whenever ``_flush_layers()`` did not
raise, but the flush only writes partitions past a size or elapsed-time
threshold. On a quiet iteration nothing reached disk and the batch was
acknowledged anyway — and an acknowledged Redis message is never redelivered, so
a container kill lost those events for good.

Deferring the ACK is not sufficient on its own. Dedup registered each
``event_id`` during processing, before the flush, so a redelivered event was
dropped as a duplicate even though it had never been written. Both the ACK and
the registration have to wait for the same durable commit.

The deferral is bounded in two directions. A forced full flush fires once the
held set gets too large or too old, so the pending list cannot grow toward the
stream's retention window and become unrecoverable. And a hard cap stops the
consumer reading at all, because an unbounded pending set would be a worse
failure than the bug being fixed.
"""

import json
from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.consumer import EventConsumer

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)
STREAM = b"heber:events"


def _make_envelope(event_id: str = "evt-1", **overrides) -> EventEnvelope:
    defaults = {
        "event_id": event_id,
        "provider": "alpaca",
        "feed": "bars",
        "source": "websocket",
        "instrument_type": "equity",
        "instrument_key": "equity:AAPL",
        "symbol": "AAPL",
        "ts_event": NOW,
        "ts_ingest": NOW,
        "ts_available": NOW,
        "payload": {"t": NOW.isoformat(), "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000},
    }
    defaults.update(overrides)
    return EventEnvelope(**defaults)


def _entry(msg_id: str, envelope: EventEnvelope) -> tuple:
    return (msg_id.encode(), {b"data": json.dumps(envelope.model_dump(mode="json")).encode()})


class _ConsumeStubRedis:
    """Serves queued batches to xreadgroup and snapshots the lake at each ACK."""

    def __init__(self, data_root=None):
        self.batches: list[list[tuple]] = []
        self.acked: list[list[str]] = []
        self.ack_snapshots: list[list[str]] = []
        self.added: list[tuple] = []
        self.read_calls = 0
        self._data_root = data_root

    async def xreadgroup(self, **_kwargs):
        self.read_calls += 1
        if not self.batches:
            return []
        return [(STREAM, self.batches.pop(0))]

    async def xack(self, _stream, _group, *ids):
        self.acked.append([i if isinstance(i, str) else i.decode() for i in ids])
        if self._data_root is not None:
            self.ack_snapshots.append(
                sorted(p.name for p in self._data_root.rglob("*") if p.suffix in {".gz", ".parquet", ".tmp"})
            )
        return len(ids)

    async def xadd(self, stream: str, payload: dict, **_kwargs):
        self.added.append((stream, payload))
        return "9-0"

    @property
    def all_acked(self) -> set[str]:
        return {i for batch in self.acked for i in batch}


def _never_flush(monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 99_999)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9_999)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 9_999)
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 9_999)


def _wide_bounds(monkeypatch) -> None:
    """Bounds high enough that no forced flush fires during the test."""
    from heber.config import settings

    monkeypatch.setattr(settings, "writer_max_unacked_messages", 1_000_000)
    monkeypatch.setattr(settings, "writer_max_unacked_seconds", 9_999.0)


def _consumer(monkeypatch, tmp_path, redis_stub) -> EventConsumer:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = EventConsumer()
    consumer.redis = redis_stub
    return consumer


async def test_consume_iteration_does_not_ack_when_nothing_was_flushed(tmp_path, monkeypatch) -> None:
    """The headline case: no ACK while the events are only in memory."""
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    await consumer._consume_iteration()

    assert stub.acked == []
    assert consumer.bronze_writer.has_buffered() is True
    assert "1-0" in consumer._pending_ack_ids


async def test_pending_acks_accumulate_then_commit_together(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis(data_root=tmp_path)
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [
        [_entry("1-0", _make_envelope("evt-1"))],
        [_entry("2-0", _make_envelope("evt-2"))],
    ]

    await consumer._consume_iteration()
    await consumer._consume_iteration()
    assert stub.acked == []

    # Let the real thresholds fire, then settle.
    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)
    await consumer._settle_and_commit()

    assert stub.all_acked == {"1-0", "2-0"}
    assert consumer._pending_ack_ids == set()


async def test_ack_happens_only_after_the_files_exist(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis(data_root=tmp_path)
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    await consumer._consume_iteration()

    assert stub.all_acked == {"1-0"}
    # The lake as it stood at the moment of the ACK: a real published file, and
    # no half-written .tmp — the atomic rename had completed.
    snapshot = stub.ack_snapshots[0]
    assert any(name.endswith(".jsonl.gz") for name in snapshot)
    assert not any(name.endswith(".tmp") for name in snapshot)


async def test_register_is_deferred_until_the_flush_commits(tmp_path, monkeypatch) -> None:
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    await consumer._consume_iteration()

    # Not yet in the dedupe store: the event is not durable, so a redelivery
    # must be allowed to write it again.
    assert consumer.event_deduplicator.check("evt-1").is_duplicate is False
    assert "evt-1" in consumer._pending_register_ids


async def test_redelivery_to_the_holding_process_is_not_written_twice(tmp_path, monkeypatch) -> None:
    """A redelivery while the first copy is still buffered must not double-write.

    Recovery can hand a message back to the same process that is still holding
    it. The event is already buffered and will be written, so writing it again
    would duplicate — but it must also stay unacknowledged until that write
    happens, which is what keeps it recoverable.
    """
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    envelope = _make_envelope("evt-1")
    stub.batches = [[_entry("1-0", envelope)]]
    await consumer._consume_iteration()

    stub.batches = [[_entry("1-1", envelope)]]
    await consumer._consume_iteration()

    assert sum(len(v) for v in consumer.bronze_writer.buffers.values()) == 1
    assert stub.acked == [], "acknowledged an event that is still only in memory"


async def test_redelivery_after_a_crash_is_reprocessed_not_deduped(tmp_path, monkeypatch) -> None:
    """The loss scenario: a surviving dedupe store must not swallow unwritten data.

    The first process buffers the event and dies before any flush. Its message
    was never acknowledged, so Redis redelivers it. If the event had been
    registered as seen at processing time, the replacement process would drop it
    as a duplicate of something that was never written — silent loss. The shared
    deduplicator here stands in for the Redis-backed store, which outlives a
    restart in a way the in-process Bloom filter does not.
    """
    from heber.ops.reliability import EventDeduplicator

    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    envelope = _make_envelope("evt-1")
    shared_dedupe = EventDeduplicator()

    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    dying = EventConsumer(event_deduplicator=shared_dedupe)
    dying.redis = _ConsumeStubRedis()
    dying.redis.batches = [[_entry("1-0", envelope)]]
    await dying._consume_iteration()
    assert dying.redis.acked == []

    # Process replaced: fresh in-memory state, same durable dedupe store.
    restarted = EventConsumer(event_deduplicator=shared_dedupe)
    restarted.redis = _ConsumeStubRedis()
    restarted.redis.batches = [[_entry("1-0", envelope)]]
    await restarted._consume_iteration()

    buffered = sum(len(v) for v in restarted.bronze_writer.buffers.values())
    assert buffered == 1, "redelivery of never-written data was dropped as a duplicate"


async def test_register_happens_once_the_commit_succeeds(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    await consumer._consume_iteration()

    assert consumer.event_deduplicator.check("evt-1").is_duplicate is True
    assert consumer._pending_register_ids == set()


async def test_duplicate_within_the_same_batch_is_still_dropped(tmp_path, monkeypatch) -> None:
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    envelope = _make_envelope("evt-dup")
    stub.batches = [[_entry("1-0", envelope), _entry("2-0", envelope)]]

    await consumer._consume_iteration()

    buffered = sum(len(v) for v in consumer.bronze_writer.buffers.values())
    assert buffered == 1, "the same event_id was buffered twice within one batch"


async def test_forced_flush_when_held_count_exceeds_bound(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    _never_flush(monkeypatch)
    monkeypatch.setattr(settings, "writer_max_unacked_messages", 2)
    monkeypatch.setattr(settings, "writer_max_unacked_seconds", 9_999.0)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [
        [_entry("1-0", _make_envelope("evt-1")), _entry("2-0", _make_envelope("evt-2"))],
    ]

    await consumer._consume_iteration()

    # No threshold was met, so only the count bound can have produced this.
    assert stub.all_acked == {"1-0", "2-0"}
    assert consumer.bronze_writer.has_buffered() is False


async def test_forced_flush_when_held_age_exceeds_bound(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    _never_flush(monkeypatch)
    monkeypatch.setattr(settings, "writer_max_unacked_messages", 1_000_000)
    monkeypatch.setattr(settings, "writer_max_unacked_seconds", 30.0)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    await consumer._consume_iteration()
    assert stub.acked == []

    # Age the hold past the bound.
    consumer._pending_since = consumer._pending_since - 60.0
    await consumer._settle_and_commit()

    assert stub.all_acked == {"1-0"}


async def test_no_forced_flush_while_within_both_bounds(tmp_path, monkeypatch) -> None:
    """Protects throughput: the backstop must stay rare in steady state."""
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)
    stub.batches = [[_entry("1-0", _make_envelope("evt-1"))]]

    forced: list[str] = []
    original = consumer._force_flush_layers
    monkeypatch.setattr(
        consumer,
        "_force_flush_layers",
        lambda reason: (forced.append(reason), original(reason))[1],
    )

    await consumer._consume_iteration()

    assert forced == []


async def test_ack_is_chunked(tmp_path, monkeypatch) -> None:
    from heber.config import settings
    from heber.writer import consumer as consumer_module

    monkeypatch.setattr(consumer_module, "_ACK_CHUNK_SIZE", 100)
    monkeypatch.setattr(settings, "data_root", tmp_path)
    _wide_bounds(monkeypatch)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    consumer._pending_ack_ids = {f"{i}-0" for i in range(250)}
    await consumer._settle_and_commit()

    assert len(stub.acked) == 3
    assert stub.all_acked == {f"{i}-0" for i in range(250)}
    assert consumer._pending_ack_ids == set()


async def test_backpressure_stops_reading_when_hard_cap_exceeded(tmp_path, monkeypatch) -> None:
    """An unbounded pending set is worse than the bug — stop consuming instead."""
    from heber.config import settings

    _never_flush(monkeypatch)
    monkeypatch.setattr(settings, "writer_max_unacked_messages", 10)
    monkeypatch.setattr(settings, "writer_max_unacked_seconds", 9_999.0)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    # Held far beyond the hard cap, and unflushable.
    consumer._pending_ack_ids = {f"{i}-0" for i in range(500)}
    consumer.bronze_writer.buffers["provider=t/feed=bars/dt=2026-01-01/hour=00"] = [{"event_id": "x"}]
    monkeypatch.setattr(consumer, "_force_flush_layers", lambda reason: False)
    stub.batches = [[_entry("9-0", _make_envelope("evt-9"))]]

    await consumer._consume_iteration()

    assert stub.read_calls == 0, "kept reading while the pending set was over the hard cap"
