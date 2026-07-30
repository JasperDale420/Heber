"""JetStream must preserve the writer's existing durable-ack contract."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nats.js.api import AckPolicy

from heber.writer.consumer import EventConsumer, build_ingest_consumer
from heber.writer.jetstream_consumer import JetStreamEventConsumer

pytestmark = pytest.mark.unit

NOW = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)


class FakeMessage:
    def __init__(self, data: bytes, sequence: int = 1) -> None:
        self.data = data
        self.metadata = SimpleNamespace(sequence=SimpleNamespace(stream=sequence), num_delivered=1)
        self.ack = AsyncMock()
        self.nak = AsyncMock()
        self.in_progress = AsyncMock()


def _event_bytes() -> bytes:
    return json.dumps(
        {
            "event_id": "evt-jetstream-001",
            "provider": "alpaca",
            "feed": "bars",
            "source": "websocket",
            "instrument_type": "equity",
            "instrument_key": "equity:AAPL",
            "symbol": "AAPL",
            "ts_event": NOW.isoformat(),
            "ts_ingest": NOW.isoformat(),
            "payload": {
                "t": NOW.isoformat(),
                "o": 100.0,
                "h": 101.0,
                "l": 99.0,
                "c": 100.5,
                "v": 1000,
            },
        }
    ).encode()


async def test_success_is_acked_only_after_bronze_silver_and_commit_marker(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "bronze_max_batch_size", 1)
    monkeypatch.setattr(settings, "silver_min_rows_per_flush", 1)

    message = FakeMessage(_event_bytes())

    async def assert_storage_is_durable() -> None:
        assert list((tmp_path / "bronze").rglob("*.jsonl.gz"))
        assert list((tmp_path / "silver").rglob("*.parquet"))
        assert list((tmp_path / "_ingest_commits").rglob("commits.jsonl"))

    message.ack.side_effect = assert_storage_is_durable
    consumer = JetStreamEventConsumer()

    await consumer._process_batch([message])

    assert message.ack.await_count == 1
    assert message.nak.await_count == 0


async def test_poison_message_is_acked_only_after_filesystem_dlq_capture(
    tmp_path,
    monkeypatch,
) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "dlq_fallback_dir", tmp_path / "dlq")
    settings.dlq_fallback_path.mkdir()
    message = FakeMessage(b"{not-json")
    consumer = JetStreamEventConsumer()

    await consumer._process_batch([message])

    assert message.ack.await_count == 1
    assert message.nak.await_count == 0
    assert list((tmp_path / "dlq").rglob("*.json"))


async def test_dlq_write_failure_naks_instead_of_acking(monkeypatch) -> None:
    from heber.config import settings

    message = FakeMessage(b"{not-json")
    consumer = JetStreamEventConsumer()
    monkeypatch.setattr(consumer, "_write_jetstream_dlq", AsyncMock(return_value=False))

    await consumer._process_batch([message])

    assert message.ack.await_count == 0
    message.nak.assert_awaited_once_with(delay=settings.redis_retry_backoff_seconds)


async def test_redelivery_of_held_sequence_extends_ack_without_reprocessing(monkeypatch) -> None:
    first = FakeMessage(_event_bytes(), sequence=7)
    redelivery = FakeMessage(_event_bytes(), sequence=7)
    consumer = JetStreamEventConsumer()
    consumer._pending_messages["7"] = first
    consumer._hold_for_commit(["7"])
    process = AsyncMock()
    monkeypatch.setattr(consumer, "_process_with_retry", process)
    monkeypatch.setattr(consumer, "_settle_and_commit", AsyncMock())

    await consumer._process_batch([redelivery])

    process.assert_not_awaited()
    redelivery.in_progress.assert_awaited_once()
    assert consumer._pending_messages["7"] is redelivery


async def test_backfill_messages_wait_for_whole_chunk_proof(monkeypatch) -> None:
    first = FakeMessage(_event_bytes(), sequence=7)
    second = FakeMessage(_event_bytes(), sequence=8)
    consumer = JetStreamEventConsumer()
    consumer._pending_messages = {"7": first, "8": second}
    consumer._pending_message_chunks = {
        "7": ("job", "chunk"),
        "8": ("job", "chunk"),
    }
    consumer._hold_for_commit(["7", "8"])
    prepare = AsyncMock(side_effect=[set(), {("job", "chunk")}])
    monkeypatch.setattr(consumer, "_prepare_durable_commit", prepare)

    await consumer._settle_and_commit()

    first.ack.assert_not_awaited()
    second.ack.assert_not_awaited()
    first.in_progress.assert_awaited_once()
    second.in_progress.assert_awaited_once()

    await consumer._settle_and_commit()

    first.ack.assert_awaited_once()
    second.ack.assert_awaited_once()


async def test_finalized_chunk_retries_after_transient_jetstream_ack_failure(monkeypatch) -> None:
    message = FakeMessage(_event_bytes(), sequence=7)
    message.ack.side_effect = [ConnectionError("broker down"), None]
    consumer = JetStreamEventConsumer()
    consumer._pending_messages = {"7": message}
    consumer._pending_message_chunks = {"7": ("job", "chunk")}
    consumer._hold_for_commit(["7"])
    prepare = AsyncMock(side_effect=[{("job", "chunk")}, set()])
    monkeypatch.setattr(consumer, "_prepare_durable_commit", prepare)

    await consumer._settle_and_commit()

    assert message.ack.await_count == 1
    assert consumer._pending_ack_ids == {"7"}
    assert consumer._transport_ack_eligible_chunks == {("job", "chunk")}

    await consumer._settle_and_commit()

    assert message.ack.await_count == 2
    assert consumer._pending_ack_ids == set()
    assert consumer._pending_messages == {}
    assert consumer._pending_message_chunks == {}
    assert consumer._transport_ack_eligible_chunks == set()


async def test_connect_uses_explicit_pull_consumer_contract(monkeypatch) -> None:
    from heber.config import settings

    subscription = object()
    jetstream = SimpleNamespace(pull_subscribe=AsyncMock(return_value=subscription))
    connection = SimpleNamespace(jetstream=lambda: jetstream)
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr("heber.writer.jetstream_consumer.nats.connect", connect)
    monkeypatch.setattr(settings, "nats_username", "heber")
    monkeypatch.setattr(
        settings,
        "nats_password",
        SimpleNamespace(get_secret_value=lambda: "secret"),  # pragma: allowlist secret
    )
    monkeypatch.setattr(settings, "ingest_lane", "live")

    consumer = JetStreamEventConsumer()
    await consumer.connect()

    assert consumer.subscription is subscription
    assert consumer._backfill_binding == (
        "jetstream",
        "live",
        settings.jetstream_live_stream_name,
        settings.jetstream_live_durable_name,
    )
    args = jetstream.pull_subscribe.await_args
    assert args.args == ("heber.live.>",)
    assert args.kwargs["stream"] == settings.jetstream_live_stream_name
    assert args.kwargs["durable"] == settings.jetstream_live_durable_name
    assert args.kwargs["config"].ack_policy is AckPolicy.EXPLICIT
    assert args.kwargs["config"].ack_wait == settings.jetstream_ack_wait_seconds
    assert args.kwargs["config"].max_ack_pending == settings.jetstream_max_ack_pending


async def test_initial_connect_downgrades_prior_healthy_readiness_first(monkeypatch) -> None:
    from heber.config import settings

    consumer = JetStreamEventConsumer()
    consumer.redis = AsyncMock()
    consumer._backfill_binding = (
        "jetstream",
        "backfill",
        "HEBER_BACKFILL",
        "heber-backfill-writers",
    )
    monkeypatch.setattr(settings, "ingest_lane", "backfill")

    async def fail_after_downgrade(**_kwargs):
        consumer.redis.hset.assert_awaited_once()
        assert consumer._backfill_binding is None
        raise ConnectionError("broker unavailable")

    monkeypatch.setattr("heber.writer.jetstream_consumer.nats.connect", fail_after_downgrade)

    with pytest.raises(ConnectionError, match="broker unavailable"):
        await consumer.connect()

    assert consumer.redis.hset.await_args.kwargs["mapping"]["consumer_healthy"] == "false"


async def test_nats_disconnect_callback_downgrades_readiness(monkeypatch) -> None:
    from heber.config import settings

    subscription = object()
    jetstream = SimpleNamespace(pull_subscribe=AsyncMock(return_value=subscription))
    connection = SimpleNamespace(jetstream=lambda: jetstream, is_connected=True)
    connect = AsyncMock(return_value=connection)
    monkeypatch.setattr("heber.writer.jetstream_consumer.nats.connect", connect)
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    consumer = JetStreamEventConsumer()
    consumer.redis = AsyncMock()

    await consumer.connect()
    consumer.redis.hset.reset_mock()
    disconnected_cb = connect.await_args.kwargs["disconnected_cb"]

    await disconnected_cb()
    await asyncio.gather(*consumer._readiness_downgrade_tasks)

    assert consumer._backfill_binding is None
    assert consumer.redis.hset.await_args.kwargs["mapping"]["consumer_healthy"] == "false"
    assert connect.await_args.kwargs["closed_cb"] is not None


async def test_graceful_shutdown_downgrades_before_closing_nats(monkeypatch) -> None:
    from heber.config import settings

    calls: list[str] = []
    consumer = JetStreamEventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.hset.side_effect = lambda *_args, **_kwargs: calls.append("readiness")
    consumer.connection = SimpleNamespace(close=AsyncMock(side_effect=lambda: calls.append("nats_close")))
    consumer._backfill_binding = (
        "jetstream",
        "backfill",
        "HEBER_BACKFILL",
        "heber-backfill-writers",
    )
    monkeypatch.setattr(settings, "ingest_lane", "backfill")
    monkeypatch.setattr(
        "heber.writer.consumer.EventConsumer._shutdown",
        AsyncMock(side_effect=lambda: calls.append("writer_shutdown")),
    )

    await consumer._shutdown()

    assert calls == ["readiness", "writer_shutdown", "nats_close"]
    assert consumer._backfill_binding is None


async def test_fetch_failure_closes_stale_connection_and_rebinds(monkeypatch) -> None:
    from heber.config import settings

    stale_subscription = SimpleNamespace(fetch=AsyncMock(side_effect=RuntimeError("closed")))
    stale_connection = SimpleNamespace(close=AsyncMock(), is_connected=True)
    consumer = JetStreamEventConsumer()
    consumer.subscription = stale_subscription
    consumer.connection = stale_connection
    consumer.redis = AsyncMock()
    reconnect = AsyncMock()
    monkeypatch.setattr(consumer, "connect", reconnect)
    monkeypatch.setattr("heber.writer.jetstream_consumer.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(settings, "ingest_lane", "backfill")

    await consumer._consume_iteration()

    stale_connection.close.assert_awaited_once()
    assert consumer.redis.hset.await_args.kwargs["mapping"]["consumer_healthy"] == "false"
    reconnect.assert_awaited_once()


async def test_backfill_readiness_is_false_until_subscription_is_bound(monkeypatch) -> None:
    consumer = JetStreamEventConsumer()
    consumer.redis = AsyncMock()
    consumer.redis.eval.return_value = b"PONG"
    monkeypatch.setattr(consumer, "_check_backfill_writer_durability", AsyncMock())

    with pytest.raises(RuntimeError, match="not bound"):
        await consumer._write_backfill_readiness()

    assert consumer.redis.hset.await_args.kwargs["mapping"]["consumer_healthy"] == "false"


def test_backfill_readiness_requires_connected_subscription() -> None:
    consumer = JetStreamEventConsumer()
    consumer.subscription = object()
    consumer.connection = SimpleNamespace(is_connected=False)

    assert consumer._backfill_consumer_bound() is False

    consumer.connection.is_connected = True
    consumer._backfill_binding = (
        "jetstream",
        "backfill",
        "HEBER_BACKFILL",
        "heber-backfill-writers",
    )
    assert consumer._backfill_consumer_bound() is True


def test_build_ingest_consumer_keeps_redis_default(monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "ingest_transport", "redis")
    assert type(build_ingest_consumer()) is EventConsumer

    monkeypatch.setattr(settings, "ingest_transport", "jetstream")
    assert isinstance(build_ingest_consumer(), JetStreamEventConsumer)
