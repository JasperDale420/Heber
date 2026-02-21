"""Reliability regression tests for the Redis stream writer consumer."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import heber.writer.consumer as consumer_module
from heber.models.envelope import EventEnvelope
from heber.writer.consumer import EventConsumer


class _StubRedis:
    def __init__(self):
        self.pending = []
        self.claimed = []
        self.acked: list[tuple] = []
        self.added: list[tuple] = []
        self.fail_xadd = False

    async def xpending_range(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self.pending

    async def xclaim(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self.claimed

    async def xack(self, *args):  # noqa: ANN002
        self.acked.append(args)
        return len(args) - 2

    async def xadd(self, stream: str, payload: dict):
        if self.fail_xadd:
            raise RuntimeError("dlq unavailable")
        self.added.append((stream, payload))
        return "9-0"


@pytest.mark.asyncio
async def test_recover_pending_messages_claims_and_acks() -> None:
    consumer = EventConsumer()
    redis = _StubRedis()
    redis.pending = [{"message_id": "1-0"}]
    redis.claimed = [("1-0", {"data": "{}"})]
    consumer.redis = redis

    consumer._process_stream_messages = AsyncMock(return_value=(["1-0"], []))
    consumer.bronze_writer.flush_if_needed = MagicMock()
    consumer.silver_writer.flush_if_needed = MagicMock()

    recovered = await consumer._recover_pending_messages()

    assert recovered == 1
    assert len(redis.acked) == 1
    assert redis.acked[0][0] == "heber:events"
    assert redis.acked[0][1] == "heber-writers"
    assert redis.acked[0][2] == "1-0"


@pytest.mark.asyncio
async def test_process_stream_messages_moves_failures_to_dlq() -> None:
    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis

    consumer._process_with_retry = AsyncMock(
        side_effect=[
            (True, "", 1),
            (False, "validation_error", 3),
        ]
    )

    ack_ids, failed_ids = await consumer._process_stream_messages(
        [
            ("1-0", {"data": "{}"}),
            ("2-0", {"data": "{}"}),
        ]
    )

    assert ack_ids == ["1-0", "2-0"]
    assert failed_ids == []
    assert len(redis.added) == 1
    assert redis.added[0][0] == "heber:events:dlq"
    assert redis.added[0][1]["source_message_id"] == "2-0"


@pytest.mark.asyncio
async def test_process_stream_messages_keeps_pending_when_dlq_fails() -> None:
    consumer = EventConsumer()
    redis = _StubRedis()
    redis.fail_xadd = True
    consumer.redis = redis

    consumer._process_with_retry = AsyncMock(return_value=(False, "boom", 3))

    ack_ids, failed_ids = await consumer._process_stream_messages(
        [
            ("2-0", {"data": "{}"}),
        ]
    )

    assert ack_ids == []
    assert failed_ids == ["2-0"]


def test_process_event_rejects_invalid_instrument_key() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    now = datetime(2026, 2, 7, 16, 0, tzinfo=UTC)
    envelope = {
        "event_id": "evt-invalid-key",
        "provider": "alpaca",
        "feed": "bars",
        "source": "websocket",
        "instrument_type": "equity",
        "instrument_key": "!!!INVALID",
        "symbol": "!!!",
        "ts_event": now.isoformat(),
        "ts_ingest": now.isoformat(),
        "payload": {},
    }

    success, error, retryable = consumer._process_event_once({"data": json.dumps(envelope)})

    assert success is False
    assert error is not None
    assert "Invalid instrument_key format" in error
    assert retryable is False
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()


@pytest.mark.asyncio
async def test_run_transient_redis_errors_backoff_without_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    consumer.connect = AsyncMock()
    consumer._consume_iteration = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RuntimeError("Error 101 connecting to host.docker.internal:6379. Network is unreachable."),
            asyncio.CancelledError(),
        ]
    )
    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    warning_mock = MagicMock()
    error_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "warning", warning_mock)
    monkeypatch.setattr(consumer_module.logger, "error", error_mock)
    monkeypatch.setattr(consumer_module.asyncio, "sleep", _capture_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()

    assert len(sleep_calls) == 1
    assert 0.0 < sleep_calls[0] < 1.0
    warning_mock.assert_called_once()
    error_mock.assert_not_called()


@pytest.mark.asyncio
async def test_run_unknown_errors_keep_traceback_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    consumer.connect = AsyncMock()
    consumer._consume_iteration = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RuntimeError("unexpected failure"),
            asyncio.CancelledError(),
        ]
    )

    async def _capture_sleep(_delay: float) -> None:
        return None

    warning_mock = MagicMock()
    error_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "warning", warning_mock)
    monkeypatch.setattr(consumer_module.logger, "error", error_mock)
    monkeypatch.setattr(consumer_module.asyncio, "sleep", _capture_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()

    warning_mock.assert_not_called()
    assert error_mock.call_count == 1
    assert error_mock.call_args.kwargs["exc_info"] is True


def test_market_tide_schema_allows_call_put_ratio_without_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    warning_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "warning", warning_mock)
    now = datetime(2026, 2, 12, 21, 0, tzinfo=UTC)
    envelope = EventEnvelope(
        event_id="evt-market-tide-1",
        provider="unusual_whales",
        feed="market_tide",
        source="rest",
        instrument_type="equity",
        instrument_key="equity:MARKET",
        symbol="MARKET",
        ts_event=now,
        ts_ingest=now,
        ts_available=now,
        payload={
            "timestamp": now.isoformat(),
            "date": "2026-02-12",
            "net_call_premium": 1200000,
            "net_put_premium": 800000,
            "net_volume": 5000,
            "sentiment": "bullish",
            "call_put_ratio": 1.5,
        },
    )

    consumer._validate_payload_schema(envelope)

    warning_mock.assert_not_called()


def test_flow_alert_schema_allows_id_without_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    warning_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "warning", warning_mock)
    now = datetime(2026, 2, 12, 22, 0, tzinfo=UTC)
    envelope = EventEnvelope(
        event_id="evt-flow-1",
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
        instrument_type="option",
        instrument_key="option:SPY260320C00700000",
        symbol="SPY",
        ts_event=now,
        ts_ingest=now,
        ts_available=now,
        payload={
            "id": "flow-1",
            "timestamp": now.isoformat(),
            "symbol": "SPY",
            "strike": 700,
            "expiry": "2026-03-20",
            "put_call": "call",
            "premium": 250000,
            "volume": 120,
        },
    )

    consumer._validate_payload_schema(envelope)

    warning_mock.assert_not_called()
