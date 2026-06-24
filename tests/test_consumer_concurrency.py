"""Tests for concurrent message processing in the EventConsumer.

Validates that the refactored _process_stream_messages correctly uses
asyncio.gather with a semaphore for concurrent processing while maintaining
correct ACK/DLQ routing behavior.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import heber.writer.consumer as consumer_module
from heber.writer.consumer import EventConsumer


class _StubRedis:
    def __init__(self):
        self.acked: list[tuple] = []
        self.added: list[tuple] = []
        self.fail_xadd = False

    async def xack(self, *args):  # noqa: ANN002
        self.acked.append(args)
        return len(args) - 2

    async def xadd(self, stream: str, payload: dict, **_kwargs):  # noqa: ANN003
        if self.fail_xadd:
            raise RuntimeError("dlq unavailable")
        self.added.append((stream, payload))
        return "9-0"


@pytest.mark.asyncio
async def test_concurrent_processing_faster_than_serial() -> None:
    """Messages process concurrently, not serially."""
    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis

    call_count = 0
    concurrency_high_water = 0
    active = 0

    async def _slow_process(data):  # noqa: ANN001
        nonlocal call_count, concurrency_high_water, active
        call_count += 1
        active += 1
        concurrency_high_water = max(concurrency_high_water, active)
        await asyncio.sleep(0.05)
        active -= 1
        return True, "", 1

    consumer._process_with_retry = AsyncMock(side_effect=_slow_process)

    messages = [(f"{i}-0", {"data": "{}"}) for i in range(10)]

    ack_ids, failed_ids = await consumer._process_stream_messages(messages)

    assert len(ack_ids) == 10
    assert len(failed_ids) == 0
    assert call_count == 10
    # With concurrency=10 (default), all 10 messages should run simultaneously
    assert concurrency_high_water > 1, f"Expected concurrent execution, got max {concurrency_high_water}"


@pytest.mark.asyncio
async def test_error_isolation_between_concurrent_messages(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """One failing message does not block or corrupt others."""
    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)
    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis

    async def _mixed_process(data):  # noqa: ANN001
        return True, "", 1

    consumer._process_with_retry = AsyncMock(
        side_effect=[
            (True, "", 1),
            (False, "validation_error", 3),
            (True, "", 1),
        ]
    )

    messages = [
        ("1-0", {"data": "{}"}),
        ("2-0", {"data": "{}"}),
        ("3-0", {"data": "{}"}),
    ]

    ack_ids, failed_ids = await consumer._process_stream_messages(messages)

    # Messages 1 and 3 succeed, message 2 goes to DLQ then gets acked
    assert "1-0" in ack_ids
    assert "3-0" in ack_ids
    assert "2-0" in ack_ids  # DLQ write succeeded
    assert len(failed_ids) == 0
    assert len(redis.added) == 1  # One DLQ entry
    assert redis.added[0][0] == "heber:events:dlq"


@pytest.mark.asyncio
async def test_dlq_failure_under_concurrency_leaves_message_pending(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Total DLQ failure (Redis and durable file) leaves the message in pending."""
    monkeypatch.setattr(consumer_module.settings, "dlq_fallback_dir", tmp_path)

    def _fail_fallback(*args, **kwargs):  # noqa: ANN002, ANN003
        raise OSError("disk full")

    monkeypatch.setattr(consumer_module, "write_dlq_fallback_file", _fail_fallback)

    consumer = EventConsumer()
    redis = _StubRedis()
    redis.fail_xadd = True
    consumer.redis = redis

    consumer._process_with_retry = AsyncMock(return_value=(False, "boom", 3))

    messages = [("2-0", {"data": "{}"})]

    ack_ids, failed_ids = await consumer._process_stream_messages(messages)

    assert ack_ids == []
    assert failed_ids == ["2-0"]


@pytest.mark.asyncio
async def test_concurrency_limit_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Semaphore limits concurrent processing to configured value."""
    import heber.config as config_module

    monkeypatch.setattr(config_module.settings, "redis_process_concurrency", 2)

    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis

    active = 0
    max_active = 0

    async def _tracked_process(data):  # noqa: ANN001
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.05)
        active -= 1
        return True, "", 1

    consumer._process_with_retry = AsyncMock(side_effect=_tracked_process)

    messages = [(f"{i}-0", {"data": "{}"}) for i in range(8)]

    ack_ids, failed_ids = await consumer._process_stream_messages(messages)

    assert len(ack_ids) == 8
    assert len(failed_ids) == 0
    assert max_active <= 2, f"Concurrency limit violated: max_active={max_active}"


@pytest.mark.asyncio
async def test_unexpected_exception_in_gather_handled() -> None:
    """Unexpected exceptions from gather are caught and message is left pending."""
    consumer = EventConsumer()
    redis = _StubRedis()
    consumer.redis = redis

    async def _exploding_process(data):  # noqa: ANN001
        raise RuntimeError("unexpected crash")

    consumer._process_with_retry = AsyncMock(side_effect=_exploding_process)

    messages = [("1-0", {"data": "{}"})]

    ack_ids, failed_ids = await consumer._process_stream_messages(messages)

    assert ack_ids == []
    assert failed_ids == ["1-0"]
