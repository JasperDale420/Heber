"""Every path out of the consumer loop must flush before the process goes away.

``_final_flush()`` sat after the ``while`` loop rather than in a ``finally``, so
two exits skipped it entirely: the ``CancelledError`` re-raise, and the re-raise
of a non-NOGROUP ``ResponseError``. Both leave whatever is buffered to die with
the process — and with acknowledgements now deferred until a flush succeeds, the
shutdown flush is also what releases the held messages.

``stop()`` additionally closed the Redis connection immediately, so a final
acknowledgement could never have been sent even when the flush did run.
"""

import asyncio
from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.consumer import EventConsumer

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)


def _make_envelope(event_id: str = "evt-1") -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        provider="alpaca",
        feed="bars",
        source="websocket",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=NOW,
        ts_ingest=NOW,
        ts_available=NOW,
        payload={"t": NOW.isoformat(), "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.5, "v": 1000},
    )


class _ShutdownStubRedis:
    def __init__(self):
        self.calls: list[str] = []
        self.acked: list[str] = []
        self.closed = False

    async def xack(self, _stream, _group, *ids):
        self.calls.append("xack")
        self.acked.extend(i if isinstance(i, str) else i.decode() for i in ids)
        return len(ids)

    async def aclose(self):
        self.calls.append("aclose")
        self.closed = True

    async def close(self):
        await self.aclose()

    async def xgroup_create(self, **_kwargs):
        return True


def _noop_connect(consumer: EventConsumer):
    async def _connect() -> None:
        return None

    return _connect


@pytest.mark.parametrize("exit_kind", ["cancelled", "fatal_response_error"])
async def test_buffered_events_are_flushed_on_every_exit_path(tmp_path, monkeypatch, exit_kind) -> None:
    """Both loop exits that re-raise used to skip the flush entirely."""
    import redis as redis_module

    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "bronze_max_batch_size", 99_999)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9_999)

    consumer = EventConsumer()
    consumer.redis = _ShutdownStubRedis()
    monkeypatch.setattr(consumer, "connect", _noop_connect(consumer))

    # An event buffered but not yet written, exactly as an interrupted iteration
    # would leave it.
    consumer.bronze_writer.write(_make_envelope())

    raised: BaseException = (
        asyncio.CancelledError()
        if exit_kind == "cancelled"
        else redis_module.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
    )

    async def failing_iteration() -> None:
        raise raised

    monkeypatch.setattr(consumer, "_consume_iteration", failing_iteration)

    # Caught by hand rather than with pytest.raises: letting a CancelledError
    # escape the test coroutine wedges the event loop during teardown.
    escaped: BaseException | None = None
    try:
        await consumer.run()
    except BaseException as exc:  # noqa: BLE001 — asserting on what escaped
        escaped = exc

    assert type(escaped) is type(raised), f"expected {type(raised).__name__}, got {escaped!r}"
    assert list((tmp_path / "bronze").rglob("*.jsonl.gz")), "buffered event died with the process"


async def test_shutdown_commits_held_acknowledgements(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "bronze_max_batch_size", 99_999)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9_999)

    consumer = EventConsumer()
    stub = _ShutdownStubRedis()
    consumer.redis = stub
    monkeypatch.setattr(consumer, "connect", _noop_connect(consumer))

    consumer.bronze_writer.write(_make_envelope())
    consumer._hold_for_commit(["1-0"])

    async def stop_after_one() -> None:
        consumer.running = False

    monkeypatch.setattr(consumer, "_consume_iteration", stop_after_one)

    await consumer.run()

    assert stub.acked == ["1-0"], "held message was never acknowledged at shutdown"
    assert stub.calls.index("xack") < stub.calls.index("aclose"), "closed Redis before the final ACK"


async def test_stop_does_not_close_redis(tmp_path, monkeypatch) -> None:
    """``stop()`` only signals; closing is the loop's job once it has committed."""
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    consumer = EventConsumer()
    stub = _ShutdownStubRedis()
    consumer.redis = stub
    consumer.running = True

    await consumer.stop()

    assert consumer.running is False
    assert stub.closed is False


async def test_final_flush_failure_does_not_mask_the_original_error(tmp_path, monkeypatch) -> None:
    """A failing shutdown flush must not replace the exception that caused the exit."""
    import redis as redis_module

    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    consumer = EventConsumer()
    consumer.redis = _ShutdownStubRedis()
    monkeypatch.setattr(consumer, "connect", _noop_connect(consumer))

    def broken_flush() -> None:
        raise OSError("volume unmounted")

    monkeypatch.setattr(consumer.bronze_writer, "flush", broken_flush)

    async def fatal_iteration() -> None:
        raise redis_module.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")

    monkeypatch.setattr(consumer, "_consume_iteration", fatal_iteration)

    # The ResponseError is what escaped the loop; the OSError from the shutdown
    # flush must be logged and swallowed, not surface in its place.
    with pytest.raises(redis_module.ResponseError, match="WRONGTYPE"):
        await consumer.run()


async def test_silver_still_flushes_when_bronze_flush_fails(tmp_path, monkeypatch) -> None:
    """Per-layer isolation at shutdown: one broken layer must not skip the other."""
    import redis as redis_module

    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)

    consumer = EventConsumer()
    consumer.redis = _ShutdownStubRedis()
    monkeypatch.setattr(consumer, "connect", _noop_connect(consumer))

    def broken_flush() -> None:
        raise OSError("volume unmounted")

    monkeypatch.setattr(consumer.bronze_writer, "flush", broken_flush)
    silver_flushed: list[bool] = []
    monkeypatch.setattr(consumer.silver_writer, "flush", lambda: silver_flushed.append(True))

    async def fatal_iteration() -> None:
        raise redis_module.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")

    monkeypatch.setattr(consumer, "_consume_iteration", fatal_iteration)

    with pytest.raises(redis_module.ResponseError):
        await consumer.run()

    assert silver_flushed == [True]
