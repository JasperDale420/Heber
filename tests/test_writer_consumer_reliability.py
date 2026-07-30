"""Reliability regression tests for the Redis stream writer consumer."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import heber.writer.consumer as consumer_module
import heber.writer.dlq_fallback as dlq_fallback_module
from heber.models.envelope import EventEnvelope
from heber.writer.consumer import EventConsumer


class _StubRedis:
    def __init__(self):
        self.pending: list[dict] = []  # idle-pending entries: [{"message_id": id}, ...]
        self.claim_payloads: dict = {}  # id -> payload returned by xclaim
        self.acked: list[tuple] = []
        self.added: list[tuple] = []
        self.fail_xadd = False

    @staticmethod
    def _mid(entry: dict):  # noqa: ANN205
        return entry.get("message_id") or entry.get(b"message_id")

    async def xpending_range(self, _stream, _group, _min, _max, count, idle=0):  # noqa: ANN001
        return self.pending[:count]

    async def xclaim(self, _stream, _group, _consumer, _idle, message_ids):  # noqa: ANN001
        ids = set(message_ids)
        claimed = [(mid, self.claim_payloads.pop(mid)) for mid in message_ids if mid in self.claim_payloads]
        # Claimed messages leave the idle-pending window — so a drain loop terminates.
        self.pending = [p for p in self.pending if self._mid(p) not in ids]
        return claimed

    async def xack(self, *args):  # noqa: ANN002
        self.acked.append(args)
        return len(args) - 2

    async def xadd(self, stream: str, payload: dict, **_kwargs):  # noqa: ANN003
        if self.fail_xadd:
            raise RuntimeError("dlq unavailable")
        self.added.append((stream, payload))
        return "9-0"


def test_extract_payload_value_prefers_data_over_payload() -> None:
    consumer = EventConsumer()
    message_data = {
        b"data": b"primary",
        "payload": "secondary",
    }

    assert consumer._extract_payload_value(message_data) == b"primary"


@pytest.mark.asyncio
async def test_recover_pending_messages_claims_and_acks(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(consumer_module.settings, "data_root", tmp_path)
    consumer = EventConsumer()
    redis = _StubRedis()
    redis.pending = [{"message_id": "1-0"}]
    redis.claim_payloads = {"1-0": {"data": "{}"}}
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
async def test_recover_pending_messages_drains_all_batches(tmp_path, monkeypatch) -> None:
    # A consumer that died holding more than one claim-batch of pending messages
    # must have ALL of them reclaimed in a single recovery cycle, not just the
    # first batch (the bug that stranded ~1,800 of 1,900 messages forever).
    monkeypatch.setattr(consumer_module.settings, "data_root", tmp_path)
    consumer = EventConsumer()
    redis = _StubRedis()
    ids = [f"{i}-0" for i in range(250)]  # > default claim batch of 100
    redis.pending = [{"message_id": i} for i in ids]
    redis.claim_payloads = {i: {"data": "{}"} for i in ids}
    consumer.redis = redis

    async def _proc(messages):  # noqa: ANN001 — acks exactly the ids it was handed
        return [mid for mid, _ in messages], []

    consumer._process_stream_messages = _proc  # type: ignore[assignment]
    consumer.bronze_writer.flush_if_needed = MagicMock()
    consumer.silver_writer.flush_if_needed = MagicMock()

    recovered = await consumer._recover_pending_messages()

    assert recovered == 250
    assert sum(len(a) - 2 for a in redis.acked) == 250  # every id acked across batches
    assert redis.pending == []


@pytest.mark.asyncio
async def test_recover_pending_messages_completes_chunk_beyond_five_pages(monkeypatch) -> None:
    monkeypatch.setattr(consumer_module.settings, "redis_claim_batch_size", 100)
    monkeypatch.setattr(consumer_module.settings, "backfill_proof_max_expected_records", 5_000)
    consumer = EventConsumer()
    redis = _StubRedis()
    ids = [f"{i}-0" for i in range(5_000)]
    redis.pending = [{"message_id": message_id} for message_id in ids]
    redis.claim_payloads = {message_id: {"data": "{}"} for message_id in ids}
    consumer.redis = redis

    async def _process(messages):  # noqa: ANN001
        processed = [mid for mid, _ in messages]
        for message_id in processed:
            consumer._pending_message_chunks[message_id] = ("bf-job", "chunk-1")
        return processed, []

    async def _prepare(**_kwargs):  # noqa: ANN003
        if len(consumer._pending_ack_ids) == len(ids):
            return {("bf-job", "chunk-1")}
        return set()

    consumer._process_stream_messages = _process  # type: ignore[assignment]
    monkeypatch.setattr(consumer, "_prepare_durable_commit", _prepare)

    recovered = await consumer._recover_pending_messages()

    assert recovered == 5_000
    assert sum(len(ack) - 2 for ack in redis.acked) == 5_000
    assert consumer._pending_ack_ids == set()
    assert redis.pending == []


@pytest.mark.asyncio
async def test_recovered_backfill_page_stays_pending_until_whole_chunk_finalizes(monkeypatch) -> None:
    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis
    redis.pending = [{"message_id": "1-0"}]
    redis.claim_payloads = {"1-0": {"data": "{}"}}

    async def _process(messages):  # noqa: ANN001
        ids = [mid for mid, _ in messages]
        for message_id in ids:
            consumer._pending_message_chunks[message_id] = ("bf-job", "chunk-1")
        return ids, []

    consumer._process_stream_messages = _process  # type: ignore[assignment]
    prepare = AsyncMock(side_effect=[set(), {("bf-job", "chunk-1")}])
    monkeypatch.setattr(consumer, "_prepare_durable_commit", prepare)

    assert await consumer._recover_pending_batch() == 0
    assert redis.acked == []
    assert consumer._pending_ack_ids == {"1-0"}

    redis.pending = [{"message_id": "2-0"}]
    redis.claim_payloads = {"2-0": {"data": "{}"}}

    assert await consumer._recover_pending_batch() == 2
    assert {message_id for ack in redis.acked for message_id in ack[2:]} == {
        "1-0",
        "2-0",
    }
    assert consumer._pending_ack_ids == set()


@pytest.mark.asyncio
async def test_maybe_recover_pending_respects_interval() -> None:
    # Periodic recovery must fire only after the interval elapses (default 300s),
    # and advance its clock when it does — so the run loop reclaims messages
    # stranded mid-session without waiting for a restart.
    consumer = EventConsumer()
    calls: list[int] = []

    async def _rec() -> int:
        calls.append(1)
        return 0

    consumer._recover_pending_messages = _rec  # type: ignore[assignment]
    consumer._last_recovery_monotonic = 1000.0

    assert await consumer._maybe_recover_pending(1000.0 + 299) == 0  # within interval
    assert calls == []

    await consumer._maybe_recover_pending(1000.0 + 301)  # interval elapsed
    assert len(calls) == 1
    assert consumer._last_recovery_monotonic == 1000.0 + 301


@pytest.mark.asyncio
async def test_process_stream_messages_moves_failures_to_dlq(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)
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
async def test_process_stream_messages_falls_back_to_disk_when_dlq_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    consumer = EventConsumer()
    redis = _StubRedis()
    redis.fail_xadd = True
    consumer.redis = redis

    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _no_sleep)

    consumer._process_with_retry = AsyncMock(return_value=(False, "boom", 3))

    ack_ids, failed_ids = await consumer._process_stream_messages(
        [
            ("2-0", {"data": "{}"}),
        ]
    )

    assert ack_ids == ["2-0"]
    assert failed_ids == []
    fallback_files = list(tmp_path.rglob("*.json"))
    assert len(fallback_files) == 1


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


def test_process_event_rate_limits_repeated_insider_identifier_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    warning_mock = MagicMock()
    error_mock = MagicMock()
    monkeypatch.setattr(consumer_module.logger, "warning", warning_mock)
    monkeypatch.setattr(consumer_module.logger, "error", error_mock)

    now = datetime(2026, 2, 7, 16, 0, tzinfo=UTC)
    envelope = {
        "event_id": "evt-empty-insider",
        "provider": "unusual_whales",
        "feed": "insider_trades",
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:",
        "symbol": "",
        "ts_event": now.isoformat(),
        "ts_ingest": now.isoformat(),
        "payload": {
            "ticker": "",
            "owner_name": "John Exec",
            "transaction_date": "2026-02-01",
        },
    }

    for _ in range(2):
        success, error, retryable = consumer._process_event_once({"data": json.dumps(envelope)})
        assert success is False
        # With empty ticker, normalization falls back to UNKNOWN sentinel.
        # The record then fails at required-field validation (missing trade_type).
        assert "missing_required_fields:insider_trades:" in error
        assert retryable is False

    consumer.bronze_writer.write.assert_called()
    consumer.silver_writer.write.assert_not_called()
    error_mock.assert_not_called()

    silver_validation_logs = [
        call for call in warning_mock.call_args_list if call.args and call.args[0] == "silver_validation_failed"
    ]
    assert len(silver_validation_logs) == 1
    assert silver_validation_logs[0].kwargs["occurrence_count"] == 1


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
