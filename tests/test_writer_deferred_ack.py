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
import time
from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer import consumer as consumer_module
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


class _RecoveryStubRedis(_ConsumeStubRedis):
    """Adds the pending/claim surface the recovery drain uses."""

    def __init__(self, data_root=None):
        super().__init__(data_root=data_root)
        self.pending: list[dict] = []
        self.claim_payloads: dict = {}
        self.claim_requests: list[list[str]] = []

    async def xpending_range(self, _stream, _group, start, _max, count, idle=0):
        entries = self.pending
        if isinstance(start, str) and start.startswith("("):
            after = start[1:]
            entries = [e for e in entries if e["message_id"] > after]
        return entries[:count]

    async def xclaim(self, _stream, _group, _consumer, _idle, message_ids):
        self.claim_requests.append(list(message_ids))
        claimed = [(mid, self.claim_payloads.pop(mid)) for mid in message_ids if mid in self.claim_payloads]
        ids = set(message_ids)
        self.pending = [p for p in self.pending if p["message_id"] not in ids]
        return claimed


async def test_recovery_does_not_claim_messages_this_consumer_holds(tmp_path, monkeypatch) -> None:
    """Reclaiming our own held message would write the same event twice."""
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _RecoveryStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    consumer._pending_ack_ids = {"1-0"}
    stub.pending = [{"message_id": "1-0"}, {"message_id": "2-0"}]
    stub.claim_payloads = {"2-0": {b"data": json.dumps(_make_envelope("evt-2").model_dump(mode="json")).encode()}}

    await consumer._recover_pending_batch()

    assert stub.claim_requests == [["2-0"]], "tried to reclaim a message it was already holding"


async def test_recovery_commits_through_the_same_path(tmp_path, monkeypatch) -> None:
    """Recovered events must register on commit, not leak as pending forever."""
    from heber.config import settings

    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)
    _wide_bounds(monkeypatch)
    stub = _RecoveryStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    stub.pending = [{"message_id": "5-0"}]
    stub.claim_payloads = {"5-0": {b"data": json.dumps(_make_envelope("evt-5").model_dump(mode="json")).encode()}}

    await consumer._recover_pending_batch()

    assert stub.all_acked == {"5-0"}
    assert consumer._pending_register_ids == set(), "recovered event never reached the dedupe store"
    assert consumer.event_deduplicator.check("evt-5").is_duplicate is True


async def test_recovery_does_not_ack_when_the_flush_is_incomplete(tmp_path, monkeypatch) -> None:
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _RecoveryStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    stub.pending = [{"message_id": "6-0"}]
    stub.claim_payloads = {"6-0": {b"data": json.dumps(_make_envelope("evt-6").model_dump(mode="json")).encode()}}

    recovered = await consumer._recover_pending_batch()

    assert stub.acked == []
    assert recovered == 0, "reported progress on messages it did not acknowledge"


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


async def test_recovery_pages_past_ids_this_consumer_holds(tmp_path, monkeypatch) -> None:
    """Held ids must not crowd a claim page and stall the whole drain.

    XPENDING returns oldest-first and has no cursor, so if every id on the first
    page belongs to this consumer the naive filter yields an empty candidate
    list, the batch reports zero, and the drain loop treats that as "nothing left
    to recover" — never reaching the reclaimable entries behind them.
    """
    from heber.config import settings

    monkeypatch.setattr(settings, "redis_claim_batch_size", 2)
    _never_flush(monkeypatch)
    _wide_bounds(monkeypatch)
    stub = _RecoveryStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    consumer._pending_ack_ids = {"1-0", "2-0"}
    stub.pending = [{"message_id": "1-0"}, {"message_id": "2-0"}, {"message_id": "3-0"}]
    stub.claim_payloads = {"3-0": {b"data": json.dumps(_make_envelope("evt-3").model_dump(mode="json")).encode()}}

    await consumer._recover_pending_batch()

    claimed_ids = [i for req in stub.claim_requests for i in req]
    assert "3-0" in claimed_ids, "drain stalled behind ids this consumer was holding"
    assert "1-0" not in claimed_ids and "2-0" not in claimed_ids


async def test_backpressure_branch_does_not_hot_loop(tmp_path, monkeypatch) -> None:
    """Over the cap with a failing flush, back off instead of spinning."""
    from heber.config import settings

    _never_flush(monkeypatch)
    monkeypatch.setattr(settings, "writer_max_unacked_messages", 10)
    monkeypatch.setattr(settings, "writer_max_unacked_seconds", 9_999.0)
    stub = _ConsumeStubRedis()
    consumer = _consumer(monkeypatch, tmp_path, stub)

    consumer._pending_ack_ids = {f"{i}-0" for i in range(500)}
    # Genuinely unflushable: a buffered row plus a forced flush that cannot
    # drain it, so the held set stays over the cap after settling.
    consumer.bronze_writer.buffers["provider=t/feed=bars/dt=2026-01-01/hour=00"] = [{"event_id": "stuck"}]
    monkeypatch.setattr(consumer, "_force_flush_layers", lambda reason: False)

    monkeypatch.setattr(consumer_module, "_BACKPRESSURE_SLEEP_SECONDS", 0.05)

    started = time.monotonic()
    await consumer._consume_iteration()
    elapsed = time.monotonic() - started

    assert elapsed >= 0.04, "spun on a stuck flush without backing off"
    assert stub.read_calls == 0


def test_direct_flush_callers_can_commit_registrations(tmp_path, monkeypatch) -> None:
    """process_event + a direct flush must still reach the dedupe store.

    The DLQ reprocessor flushes the writers itself instead of going through the
    consumer's commit. Without an explicit registration step its events land on
    disk but are never recorded as seen, so a later replay writes them again —
    and the held set grows for the life of the run.
    """
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = EventConsumer()

    envelope = _make_envelope("evt-dlq")
    assert consumer.process_event({"data": json.dumps(envelope.model_dump(mode="json"))}) is True
    assert consumer.event_deduplicator.check("evt-dlq").is_duplicate is False

    consumer.bronze_writer.flush()
    consumer.silver_writer.flush()
    assert consumer.commit_pending_registrations() == 1

    assert consumer.event_deduplicator.check("evt-dlq").is_duplicate is True
    assert consumer._pending_register_ids == set()
