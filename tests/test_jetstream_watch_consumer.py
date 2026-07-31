"""JetStream watch delivery preserves the Redis watch handler ACK contract."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from heber.watch.jetstream_consumer import JetStreamAlertWatchConsumer

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def isolate_watch_receipts(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)


class FakeMessage:
    def __init__(self, sequence: int = 1) -> None:
        self.data = b'{"feed":"flow_alerts"}'
        self.metadata = SimpleNamespace(sequence=SimpleNamespace(stream=sequence))
        self.ack = AsyncMock()
        self.ack_sync = self.ack
        self.nak = AsyncMock()


async def test_watch_acks_only_after_existing_handler_reports_durable_success(monkeypatch) -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage()
    monkeypatch.setattr(consumer.handler, "_handle_message", AsyncMock(return_value=True))

    await consumer._dispatch([message])

    message.ack.assert_awaited_once()
    message.nak.assert_not_awaited()


async def test_watch_leaves_message_unacked_when_handler_cannot_create_watch_or_dlq(monkeypatch) -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage()
    monkeypatch.setattr(consumer.handler, "_handle_message", AsyncMock(return_value=False))

    await consumer._dispatch([message])

    message.ack.assert_not_awaited()
    message.nak.assert_not_awaited()


async def test_watch_non_flow_receipt_survives_ambiguous_ack_until_confirmed_retry(monkeypatch) -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage(sequence=17)
    message.data = b'{"event_id":"invalid-1","feed":"darkpool"}'

    async def capture_after_receipt(*_args, **_kwargs) -> bool:
        assert consumer.durable_watch_receipts.load_pending_watch_messages() == {"invalid-1": message.data}
        return True

    dead_letter = AsyncMock(side_effect=capture_after_receipt)
    monkeypatch.setattr(consumer.handler, "_dead_letter_message", dead_letter)
    message.ack_sync.side_effect = ConnectionError("ack timeout")

    with pytest.raises(ConnectionError, match="ack timeout"):
        await consumer._dispatch([message])

    dead_letter.assert_awaited_once()
    assert consumer.durable_watch_receipts.load_pending_watch_messages() == {"invalid-1": message.data}

    retry = FakeMessage(sequence=17)
    retry.data = message.data
    await consumer._dispatch([retry])

    retry.ack_sync.assert_awaited_once()
    assert consumer.durable_watch_receipts.load_pending_watch_messages() == {}


async def test_watch_malformed_subject_payload_requires_durable_dlq_before_ack(monkeypatch) -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage()
    message.data = b"not-json"
    dead_letter = AsyncMock(return_value=True)
    monkeypatch.setattr(consumer.handler, "_dead_letter_message", dead_letter)

    await consumer._dispatch([message])

    dead_letter.assert_awaited_once()
    message.ack.assert_awaited_once()


async def test_retryable_watch_state_failure_stays_unacked_without_dlq(monkeypatch) -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    consumer.handler.max_process_retries = 1
    message = FakeMessage()
    monkeypatch.setattr(consumer.handler, "_process_alert", AsyncMock(return_value=(False, True, "redis_down")))
    dead_letter = AsyncMock(return_value=True)
    monkeypatch.setattr(consumer.handler, "_dead_letter_message", dead_letter)

    await consumer._dispatch([message])

    message.ack.assert_not_awaited()
    dead_letter.assert_not_awaited()


async def test_watch_ack_sync_ambiguity_retains_durable_receipt(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage()
    message.data = b'{"event_id":"watch-1","feed":"flow_alerts"}'
    message.ack_sync.side_effect = ConnectionError("ack timeout")
    monkeypatch.setattr(consumer.handler, "_handle_message", AsyncMock(return_value=True))

    with pytest.raises(ConnectionError):
        await consumer._dispatch([message])

    assert "watch-1" in consumer.durable_watch_receipts.load_pending_watch_messages()


async def test_watch_changed_payload_conflicts_before_handler_or_ack(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    first = FakeMessage()
    first.data = b'{"event_id":"watch-1","feed":"flow_alerts","payload":{"price":100}}'
    first.ack_sync.side_effect = ConnectionError("ack timeout")
    handle = AsyncMock(return_value=True)
    monkeypatch.setattr(consumer.handler, "_handle_message", handle)

    with pytest.raises(ConnectionError):
        await consumer._dispatch([first])

    changed = FakeMessage()
    changed.data = b'{"event_id":"watch-1","feed":"flow_alerts","payload":{"price":101}}'
    handle.reset_mock()
    with pytest.raises(RuntimeError, match="watch event payload conflict"):
        await consumer._dispatch([changed])

    handle.assert_not_awaited()
    changed.ack_sync.assert_not_awaited()


async def test_watch_changed_payload_cannot_bypass_conflict_as_non_flow(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    first = FakeMessage()
    first.data = b'{"event_id":"watch-1","feed":"flow_alerts","payload":{"price":100}}'
    first.ack_sync.side_effect = ConnectionError("ack timeout")
    monkeypatch.setattr(consumer.handler, "_handle_message", AsyncMock(return_value=True))

    with pytest.raises(ConnectionError):
        await consumer._dispatch([first])

    changed = FakeMessage()
    changed.data = b'{"event_id":"watch-1","feed":"darkpool","payload":{"price":101}}'
    dead_letter = AsyncMock(return_value=True)
    monkeypatch.setattr(consumer.handler, "_dead_letter_message", dead_letter)

    with pytest.raises(RuntimeError, match="watch event payload conflict"):
        await consumer._dispatch([changed])

    dead_letter.assert_not_awaited()
    changed.ack_sync.assert_not_awaited()
    assert consumer.durable_watch_receipts.load_pending_watch_messages()["watch-1"] == first.data


async def test_watch_receipt_capacity_fails_before_ack(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "durable_watch_receipt_max_rows", 0)
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    message = FakeMessage()
    monkeypatch.setattr(consumer.handler, "_handle_message", AsyncMock(return_value=True))

    with pytest.raises(RuntimeError, match="capacity exhausted"):
        await consumer._dispatch([message])

    message.ack_sync.assert_not_awaited()


async def test_watch_binds_the_gateway_watch_stream_and_subject(monkeypatch) -> None:
    from heber.config import settings

    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    subscription = object()
    info = SimpleNamespace(
        config=SimpleNamespace(
            filter_subject="heber.watch.flow_alerts",
            ack_policy="explicit",
            ack_wait=settings.jetstream_ack_wait_seconds,
            max_ack_pending=settings.jetstream_max_ack_pending,
        ),
        num_pending=0,
        num_ack_pending=0,
        num_redelivered=0,
    )
    jetstream = SimpleNamespace(
        pull_subscribe=AsyncMock(return_value=subscription), consumer_info=AsyncMock(return_value=info)
    )
    connection = SimpleNamespace(jetstream=lambda: jetstream)
    monkeypatch.setattr("heber.watch.jetstream_consumer.nats.connect", AsyncMock(return_value=connection))
    monkeypatch.setattr(settings, "nats_username", "heber")
    monkeypatch.setattr(settings, "nats_password", SimpleNamespace(get_secret_value=lambda: "secret"))

    await consumer.connect()

    assert settings.watch_jetstream_stream_name == "HEBER_WATCH"
    assert jetstream.pull_subscribe.await_args.args[0] == "heber.watch.flow_alerts"
    assert jetstream.pull_subscribe.await_args.kwargs["stream"] == "HEBER_WATCH"


async def test_watch_metrics_refresh_reclaims_one_ack_floor_batch_at_a_time(tmp_path, monkeypatch) -> None:
    from heber.config import settings

    monkeypatch.setattr(settings, "data_root", tmp_path)
    monkeypatch.setattr(settings, "redis_read_batch_size", 2)
    monkeypatch.setattr(settings, "jetstream_metrics_refresh_seconds", 0)
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    for sequence in range(1, 5):
        consumer.durable_watch_receipts.store_pending_watch_message(
            f"watch-{sequence}",
            b"{}",
            sequence,
        )
    info = SimpleNamespace(
        ack_floor=SimpleNamespace(stream_seq=3),
        num_pending=0,
        num_ack_pending=0,
        num_redelivered=0,
    )
    jetstream = SimpleNamespace(consumer_info=AsyncMock(return_value=info))
    consumer.connection = SimpleNamespace(jetstream=lambda: jetstream)
    consumer._transport_connected = True

    await consumer._refresh_jetstream_state_if_due()
    assert set(consumer.durable_watch_receipts.load_pending_watch_messages()) == {"watch-3", "watch-4"}

    await consumer._refresh_jetstream_state_if_due()
    assert set(consumer.durable_watch_receipts.load_pending_watch_messages()) == {"watch-4"}

    await consumer._refresh_jetstream_state_if_due()
    assert set(consumer.durable_watch_receipts.load_pending_watch_messages()) == {"watch-4"}
    assert jetstream.consumer_info.await_count == 3


async def test_watch_close_drains_then_closes_nats_connection() -> None:
    consumer = JetStreamAlertWatchConsumer(redis_client=object(), manager=object())
    connection = SimpleNamespace(drain=AsyncMock(), close=AsyncMock())
    consumer.connection = connection

    await consumer.close()

    connection.drain.assert_awaited_once()
    connection.close.assert_awaited_once()
    assert consumer.connection is None
