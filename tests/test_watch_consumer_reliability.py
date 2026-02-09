"""Regression tests for watch consumer retry and DLQ behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from heber.watch.consumer import AlertWatchConsumer


class _RedisWithDlq:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict]] = []

    def xgroup_create(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    def xreadgroup(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []

    def xack(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1

    def xadd(self, stream: str, payload: dict):  # noqa: ANN001
        self.added.append((stream, payload))
        return "1-0"


class _RedisDlqFailure(_RedisWithDlq):
    def xadd(self, stream: str, payload: dict):  # noqa: ANN001
        raise RuntimeError("dlq unavailable")


class _NoopManager:
    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        return None


@pytest.mark.asyncio
async def test_process_flow_alert_retries_then_dead_letters() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=3,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    consumer._process_alert = AsyncMock(return_value=False)  # type: ignore[method-assign]

    ackable = await consumer._process_flow_alert_with_retries("1-0", {b"data": b"{}"})

    assert ackable is True
    assert consumer._process_alert.await_count == 3
    assert len(redis_client.added) == 1
    assert redis_client.added[0][0] == "heber:watch:dlq"


def test_retry_backoff_preserves_explicit_zero_value() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        retry_backoff_seconds=0.0,
    )
    assert consumer.retry_backoff_seconds == 0.0


@pytest.mark.asyncio
async def test_process_flow_alert_zero_retries_clamped_to_single_attempt() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=0,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    consumer._process_alert = AsyncMock(return_value=False)  # type: ignore[method-assign]

    ackable = await consumer._process_flow_alert_with_retries("11-0", {b"data": b"{}"})

    assert ackable is True
    assert consumer.max_process_retries == 1
    assert consumer._process_alert.await_count == 1
    assert len(redis_client.added) == 1


@pytest.mark.asyncio
async def test_process_flow_alert_negative_backoff_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=2,
        retry_backoff_seconds=-1.0,
    )
    consumer._process_alert = AsyncMock(return_value=False)  # type: ignore[method-assign]
    observed_delays: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        observed_delays.append(delay)

    monkeypatch.setattr("heber.watch.consumer.asyncio.sleep", _capture_sleep)

    ackable = await consumer._process_flow_alert_with_retries("9-0", {b"data": b"{}"})

    assert ackable is True
    assert observed_delays == [0.0]


@pytest.mark.asyncio
async def test_handle_message_keeps_pending_when_dlq_write_fails() -> None:
    redis_client = _RedisDlqFailure()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=2,
        retry_backoff_seconds=0.0,
    )
    consumer._process_alert = AsyncMock(return_value=False)  # type: ignore[method-assign]

    # _is_flow_alert is sync; override with lambda to keep deterministic.
    consumer._is_flow_alert = lambda data: True  # type: ignore[method-assign]

    should_ack = await consumer._handle_message("2-0", {b"data": b"{}"})

    assert should_ack is False


@pytest.mark.asyncio
async def test_handle_message_skips_non_flow_alerts_with_ack() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    consumer._is_flow_alert = lambda data: False  # type: ignore[method-assign]

    should_ack = await consumer._handle_message("3-0", {b"data": b"{}"})

    assert should_ack is True


def test_is_flow_alert_supports_string_data_key() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    is_flow = consumer._is_flow_alert({"data": '{"feed":"flow_alerts","payload":{}}'})

    assert is_flow is True


def test_map_alert_fields_preserves_zero_price_values() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    mapped = consumer._map_alert_fields(
        {
            "spot_px": 0.0,
            "underlying_price": 125.5,
            "contract_px": 0.0,
            "price": 3.25,
        }
    )

    assert mapped["spot_px"] == 0.0
    assert mapped["contract_px"] == 0.0


def test_map_alert_fields_coerces_invalid_or_non_finite_numbers_to_zero() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    mapped = consumer._map_alert_fields(
        {
            "strike": "nan",
            "spot_px": "inf",
            "contract_px": "bad",
        }
    )

    assert mapped["strike"] == 0.0
    assert mapped["spot_px"] == 0.0
    assert mapped["contract_px"] == 0.0


def test_parse_timestamp_normalizes_naive_iso_to_utc() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    ts = consumer._parse_timestamp({"ts_event": "2026-02-09T09:30:00"})

    assert ts.tzinfo is UTC
    assert ts.hour == 9
    assert ts.minute == 30


def test_parse_timestamp_invalid_string_falls_back_to_now_utc() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    before = datetime.now(UTC)

    ts = consumer._parse_timestamp({"ts_event": "not-a-date"})

    after = datetime.now(UTC)
    assert ts.tzinfo is UTC
    assert before <= ts <= after


def test_parse_timestamp_non_finite_numeric_falls_back_to_now_utc() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    before = datetime.now(UTC)

    ts = consumer._parse_timestamp({"timestamp": float("nan")})

    after = datetime.now(UTC)
    assert ts.tzinfo is UTC
    assert before <= ts <= after


def test_parse_timestamp_interprets_epoch_milliseconds() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    ts_from_int = consumer._parse_timestamp({"timestamp": 1_704_067_200_000})
    ts_from_str = consumer._parse_timestamp({"timestamp": "1704067200000"})

    assert ts_from_int == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert ts_from_str == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def test_decode_stream_data_handles_invalid_utf8_bytes() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    parsed = consumer._decode_stream_data({b"\xffdata": b"\xfe\xff", b"payload": b'{"x":1}'})

    assert all(isinstance(key, str) for key in parsed)
    assert all(not isinstance(value, bytes) for value in parsed.values())
