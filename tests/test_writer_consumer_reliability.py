"""Reliability regression tests for the Redis stream writer consumer."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

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
    consumer.bronze_writer.flush_if_needed = AsyncMock()
    consumer.silver_writer.flush_if_needed = AsyncMock()

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
