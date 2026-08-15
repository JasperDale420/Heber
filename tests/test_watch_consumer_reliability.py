"""Regression tests for watch consumer retry and DLQ behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import heber.watch.consumer as watch_consumer_module
from heber.watch.consumer import AlertWatchConsumer


@pytest.fixture(autouse=True)
def _isolated_dlq_fallback_dir(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Keep durable DLQ fallback files out of the real data root."""
    monkeypatch.setattr(watch_consumer_module.settings, "dlq_fallback_dir", tmp_path)


class _RedisWithDlq:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict]] = []

    def xgroup_create(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    def xreadgroup(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []

    def xack(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1

    def xadd(self, stream: str, payload: dict, **_kwargs):  # noqa: ANN001, ANN003
        self.added.append((stream, payload))
        return "1-0"


class _RedisDlqFailure(_RedisWithDlq):
    def xadd(self, stream: str, payload: dict, **_kwargs):  # noqa: ANN001, ANN003
        raise RuntimeError("dlq unavailable")


class _NoopManager:
    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        return None


class _CaptureManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.calendar = SimpleNamespace(add_trading_hours=lambda alert_time, hours: alert_time + timedelta(hours=hours))

    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        self.calls.append(kwargs)
        return SimpleNamespace(watch_id="watch-1")


class _SyncFeatureRedis:
    def __init__(self) -> None:
        self.set_calls: list[tuple[str, str, int | None]] = []

    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.set_calls.append((key, value, ex))
        return True


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


@pytest.mark.asyncio
async def test_process_flow_alert_non_retriable_failure_skips_retries_but_dead_letters() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=5,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    consumer._process_alert = AsyncMock(return_value=(False, False, "alert_parse_failed"))  # type: ignore[method-assign]

    ackable = await consumer._process_flow_alert_with_retries("1-9", {b"data": b"{}"})

    assert ackable is True
    assert consumer._process_alert.await_count == 1
    # Non-retriable failures are still dead-lettered for observability
    assert len(redis_client.added) == 1
    assert redis_client.added[0][0] == "heber:watch:dlq"
    assert "non_retriable_failure:alert_parse_failed" in redis_client.added[0][1]["error"]


@pytest.mark.asyncio
async def test_process_flow_alert_dead_letter_error_includes_last_retry_reason() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(
        redis_client,
        _NoopManager(),
        max_process_retries=2,
        retry_backoff_seconds=0.0,
        dlq_stream_name="heber:watch:dlq",
    )
    consumer._process_alert = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            (False, True, "gateway_timeout"),
            (False, True, "gateway_payload_invalid"),
        ]
    )

    ackable = await consumer._process_flow_alert_with_retries("5-0", {b"data": b"{}"})

    assert ackable is True
    assert consumer._process_alert.await_count == 2
    assert len(redis_client.added) == 1
    assert redis_client.added[0][1]["error"] == "processing_failed_after_retries:gateway_payload_invalid"


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
async def test_handle_message_falls_back_to_disk_when_dlq_write_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    import heber.writer.dlq_fallback as dlq_fallback_module

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

    monkeypatch.setattr(watch_consumer_module.settings, "dlq_fallback_dir", tmp_path)

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(dlq_fallback_module.asyncio, "sleep", _no_sleep)

    should_ack = await consumer._handle_message("2-0", {b"data": b"{}"})

    assert should_ack is True
    fallback_files = list(tmp_path.rglob("*.json"))
    assert len(fallback_files) == 1


@pytest.mark.asyncio
async def test_handle_message_skips_non_flow_alerts_with_ack() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    consumer._is_flow_alert = lambda data: False  # type: ignore[method-assign]

    should_ack = await consumer._handle_message("3-0", {b"data": b"{}"})

    assert should_ack is True


@pytest.mark.asyncio
async def test_run_transient_redis_errors_backoff_without_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager(), retry_backoff_seconds=0.2)
    consumer._setup_consumer_group_async = AsyncMock()  # type: ignore[method-assign]
    consumer._read_messages = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RuntimeError("Connection closed by server."),
            asyncio.CancelledError(),
        ]
    )

    sleep_calls: list[float] = []

    async def _capture_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    warning_mock = MagicMock()
    error_mock = MagicMock()
    monkeypatch.setattr(watch_consumer_module.logger, "warning", warning_mock)
    monkeypatch.setattr(watch_consumer_module.logger, "error", error_mock)
    monkeypatch.setattr(watch_consumer_module.asyncio, "sleep", _capture_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()

    assert len(sleep_calls) == 1
    assert 0.0 < sleep_calls[0] < 1.0
    warning_mock.assert_called_once()
    error_mock.assert_not_called()


@pytest.mark.asyncio
async def test_run_unknown_errors_keep_error_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager(), retry_backoff_seconds=0.2)
    consumer._setup_consumer_group_async = AsyncMock()  # type: ignore[method-assign]
    consumer._read_messages = AsyncMock(  # type: ignore[method-assign]
        side_effect=[
            RuntimeError("unexpected"),
            asyncio.CancelledError(),
        ]
    )

    async def _capture_sleep(_delay: float) -> None:
        return None

    warning_mock = MagicMock()
    error_mock = MagicMock()
    monkeypatch.setattr(watch_consumer_module.logger, "warning", warning_mock)
    monkeypatch.setattr(watch_consumer_module.logger, "error", error_mock)
    monkeypatch.setattr(watch_consumer_module.asyncio, "sleep", _capture_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()

    warning_mock.assert_not_called()
    assert error_mock.call_count == 1


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


def test_map_alert_fields_preserves_flow_metadata_for_feature_extraction() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    mapped = consumer._map_alert_fields(
        {
            "id": "evt-1",
            "option_chain": "AAPL260220C00100000",
            "symbol": "AAPL",
            "put_call": "C",
            "expiry": "2026-02-20",
            "strike": 100.0,
            "premium": 250000.0,
            "volume": 150.0,
            "open_interest": 500.0,
            "alert_type": "SWEEP",
            "side": "ask",
            "aggressor": "BULLISH",
            "tags": ["bullish", "unusual"],
        }
    )

    assert mapped["premium"] == 250000.0
    assert mapped["volume"] == 150.0
    assert mapped["open_interest"] == 500.0
    assert mapped["alert_type"] == "SWEEP"
    assert mapped["side"] == "ask"
    assert mapped["aggressor"] == "BULLISH"
    assert mapped["tags"] == ["bullish", "unusual"]


def test_map_alert_fields_supports_common_numeric_aliases() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    mapped = consumer._map_alert_fields(
        {
            "event_id": "evt-2",
            "option_chain": "AAPL260220P00100000",
            "symbol": "AAPL",
            "put_call": "P",
            "expiry": "2026-02-20",
            "strike": 100.0,
            "total_premium": 12345.0,
            "size": 42.0,
            "oi": 777.0,
            "price": 1.25,
            "underlying_price": 199.5,
        }
    )

    assert mapped["premium"] == 12345.0
    assert mapped["volume"] == 42.0
    assert mapped["open_interest"] == 777.0
    assert mapped["contract_px"] == 1.25
    assert mapped["spot_px"] == 199.5


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


def test_parse_timestamp_boolean_value_falls_back_to_now_utc() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    before = datetime.now(UTC)

    ts = consumer._parse_timestamp({"timestamp": True})

    after = datetime.now(UTC)
    assert ts.tzinfo is UTC
    assert before <= ts <= after


def test_decode_stream_data_handles_invalid_utf8_bytes() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    parsed = consumer._decode_stream_data({b"\xffdata": b"\xfe\xff", b"payload": b'{"x":1}'})

    assert all(isinstance(key, str) for key in parsed)
    assert all(not isinstance(value, bytes) for value in parsed.values())


def test_decode_stream_data_parses_json_with_leading_whitespace() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    parsed = consumer._decode_stream_data(
        {
            b"data": b' { "id": "a1", "feed": "flow_alerts" }',
            b"payload": b' { "option_chain": "AAPL260220C00100000" }',
        }
    )

    assert parsed["id"] == "a1"
    assert parsed["option_chain"] == "AAPL260220C00100000"


def test_map_alert_fields_normalizes_non_string_put_call() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    mapped = consumer._map_alert_fields(
        {
            "put_call": 1,
            "type": None,
        }
    )

    assert mapped["put_call"] is None

    mapped = consumer._map_alert_fields(
        {
            "put_call": "put",
        }
    )
    assert mapped["put_call"] == "P"


def test_parse_alert_missing_required_fields_returns_none() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())

    parsed = consumer._parse_alert(
        {
            b"data": b'{"payload":{"option_chain":"AAPL260220C00100000","put_call":"C"}}',
        }
    )

    assert parsed is None


@pytest.mark.asyncio
async def test_process_alert_parse_failure_is_non_retriable() -> None:
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    consumer._parse_alert = lambda _data: None  # type: ignore[method-assign]

    result = await consumer._process_alert("2-1", {b"data": b"{}"})

    assert result == (False, False, "alert_parse_failed")


@pytest.mark.asyncio
async def test_process_alert_uses_contract_price_fallback_when_gateway_price_missing() -> None:
    redis_client = _RedisWithDlq()
    manager = _CaptureManager()
    consumer = AlertWatchConsumer(redis_client, manager)
    consumer._parse_alert = lambda _data: {  # type: ignore[method-assign]
        "id": "alert-1",
        "occ_symbol": "AAPL260220C00100000",
        "underlying": "AAPL",
        "put_call": "C",
        "expiry": "2026-02-20",
        "strike": 100.0,
        "spot_px": 200.0,
        "contract_px": 1.2,
        "ts_event": datetime.now(UTC),
        "dte": 5,
    }
    consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await consumer._process_alert("2-2", {b"data": b"{}"})

    assert result == (True, False, "watch_created")
    assert manager.calls
    assert manager.calls[0]["entry_price"] == 1.2


@pytest.mark.asyncio
async def test_process_alert_defaults_entry_price_when_fallback_not_positive() -> None:
    redis_client = _RedisWithDlq()
    manager = _CaptureManager()
    consumer = AlertWatchConsumer(redis_client, manager)
    consumer._parse_alert = lambda _data: {  # type: ignore[method-assign]
        "id": "alert-1",
        "occ_symbol": "AAPL260220C00100000",
        "underlying": "AAPL",
        "put_call": "C",
        "expiry": "2026-02-20",
        "strike": 100.0,
        "spot_px": 200.0,
        "contract_px": 0.0,
        "ts_event": datetime.now(UTC),
        "dte": 5,
    }
    consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await consumer._process_alert("2-3", {b"data": b"{}"})

    assert result == (True, False, "watch_created")
    assert manager.calls
    assert manager.calls[0]["entry_price"] == 1.0


@pytest.mark.asyncio
async def test_process_alert_prefers_contract_price_without_gateway_lookup() -> None:
    redis_client = _RedisWithDlq()
    manager = _CaptureManager()
    consumer = AlertWatchConsumer(redis_client, manager)
    consumer._parse_alert = lambda _data: {  # type: ignore[method-assign]
        "id": "alert-1",
        "occ_symbol": "AAPL260220C00100000",
        "underlying": "AAPL",
        "put_call": "C",
        "expiry": "2026-02-20",
        "strike": 100.0,
        "spot_px": 200.0,
        "contract_px": 2.25,
        "ts_event": datetime.now(UTC),
        "dte": 5,
    }
    consumer._get_entry_price = AsyncMock(return_value=9.99)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await consumer._process_alert("2-4", {b"data": b"{}"})

    assert result == (True, False, "watch_created")
    assert manager.calls
    assert manager.calls[0]["entry_price"] == 2.25
    consumer._get_entry_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_alert_skips_stale_window_without_creating_watch() -> None:
    redis_client = _RedisWithDlq()
    manager = _CaptureManager()
    consumer = AlertWatchConsumer(redis_client, manager)
    consumer._parse_alert = lambda _data: {  # type: ignore[method-assign]
        "id": "alert-stale-1",
        "occ_symbol": "AAPL250101C00100000",
        "underlying": "AAPL",
        "put_call": "C",
        "expiry": "2025-01-01",
        "strike": 100.0,
        "spot_px": 200.0,
        "contract_px": 1.25,
        "ts_event": datetime.now(UTC) - timedelta(days=30),
        "dte": 0,
    }
    consumer._get_entry_price = AsyncMock(return_value=None)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    result = await consumer._process_alert("2-5", {b"data": b"{}"})

    assert result == (True, False, "stale_alert_window")
    assert manager.calls == []
    consumer._extract_and_store_features.assert_not_awaited()


@pytest.mark.asyncio
async def test_extract_and_store_features_uses_sync_redis_fallback_when_async_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client = _SyncFeatureRedis()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    features = SimpleNamespace(
        alert_id="alert-1",
        to_dict=lambda: {"alert_id": "alert-1"},
        numeric_feature_names=lambda: ["f1", "f2"],
    )
    consumer.feature_extractor.extract = AsyncMock(return_value=features)  # type: ignore[method-assign]
    monkeypatch.setattr(watch_consumer_module, "persist_features_to_gold", lambda _features: None)

    await consumer._extract_and_store_features(
        {
            "id": "alert-1",
            "underlying": "AAPL",
            "occ_symbol": "AAPL260220C00100000",
            "put_call": "C",
            "expiry": "2026-02-20",
            "strike": 100.0,
            "premium": 1000.0,
            "volume": 50.0,
            "ts_event": datetime.now(UTC),
        },
        "watch-1",
    )

    assert len(redis_client.set_calls) == 1
    key, _value, ttl_seconds = redis_client.set_calls[0]
    assert key == "heber:watch:features:alert-1"
    assert ttl_seconds == 86400 * 7


@pytest.mark.asyncio
async def test_extract_and_store_features_preserves_flow_payload_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client = _SyncFeatureRedis()
    consumer = AlertWatchConsumer(redis_client, _NoopManager())
    captured: dict[str, object] = {}

    features = SimpleNamespace(
        alert_id="alert-2",
        to_dict=lambda: {"alert_id": "alert-2"},
        numeric_feature_names=lambda: ["f1", "f2"],
    )

    async def _extract(record):  # noqa: ANN001
        captured["record"] = record
        return features

    consumer.feature_extractor.extract = _extract  # type: ignore[method-assign]
    monkeypatch.setattr(watch_consumer_module, "persist_features_to_gold", lambda _features: None)

    await consumer._extract_and_store_features(
        {
            "id": "alert-2",
            "underlying": "TSLA",
            "occ_symbol": "TSLA260220C00700000",
            "put_call": "C",
            "expiry": "2026-02-20",
            "strike": 700.0,
            "premium": 345000.0,
            "volume": 210.0,
            "open_interest": 1500.0,
            "spot_px": 690.5,
            "contract_px": 12.25,
            "alert_type": "SWEEP",
            "side": "ask",
            "aggressor": "BULLISH",
            "tags": ["bullish", "unusual"],
            "ts_event": datetime(2026, 2, 12, 15, 30, tzinfo=UTC),
        },
        "watch-2",
    )

    record = captured["record"]
    assert record.premium == 345000.0
    assert record.volume == 210.0
    assert record.open_interest == 1500.0
    assert record.alert_type == "SWEEP"
    assert record.side == "ask"
    assert record.aggressor == "BULLISH"
    assert record.tags == ["bullish", "unusual"]


@pytest.mark.asyncio
async def test_run_retries_consumer_group_setup_when_redis_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup must ride out an upstream Redis restart instead of exiting.

    `run()` calls `_setup_consumer_group_async()` before entering the loop that
    handles transient errors, so on 2026-08-08 the watch service died here —
    `ConnectionError: Error 101 ... Network is unreachable` inside xgroup_create —
    and crash-looped for 1h38m while the watchdog restarted it every 120s against
    the same dead Redis. data-gateway-redis takes ~77s to load its AOF.
    """
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager(), retry_backoff_seconds=0.01)

    attempts = 0

    async def _flaky_setup() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("Error 101 connecting to host.docker.internal:6379. Network is unreachable.")

    consumer._setup_consumer_group_async = _flaky_setup  # type: ignore[method-assign]
    consumer._read_messages = AsyncMock(side_effect=[asyncio.CancelledError()])  # type: ignore[method-assign]

    async def _instant_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)

    with pytest.raises(asyncio.CancelledError):
        await consumer.run()

    assert attempts == 3, "startup consumer-group setup must retry transient Redis errors"


@pytest.mark.asyncio
async def test_run_still_raises_non_transient_setup_errors() -> None:
    """A real misconfiguration stays loud — only transient errors are retried."""
    redis_client = _RedisWithDlq()
    consumer = AlertWatchConsumer(redis_client, _NoopManager(), retry_backoff_seconds=0.01)

    async def _fatal() -> None:
        raise ValueError("invalid stream name")

    consumer._setup_consumer_group_async = _fatal  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="invalid stream name"):
        await consumer.run()


@pytest.mark.asyncio
async def test_read_messages_advances_the_upstream_progress_gauge() -> None:
    """Watch needs its own stall signal now that it waits Redis out instead of dying.

    Its run loop keeps spinning (and the process keeps looking alive) through a
    Redis outage, so without this the crash-loop it used to announce itself with
    is simply replaced by silence. An empty read still counts — Redis answered.
    """
    import time

    from heber.ops.metrics import watch_last_xread_success_unixtime

    consumer = AlertWatchConsumer(_RedisWithDlq(), _NoopManager())
    watch_last_xread_success_unixtime.set(1000.0)  # long-stale

    await consumer._read_messages()

    assert watch_last_xread_success_unixtime._value.get() > time.time() - 5


@pytest.mark.asyncio
async def test_failed_read_leaves_the_progress_gauge_stale() -> None:
    """A failed read must not look like progress, or the gauge is worthless."""
    from heber.ops.metrics import watch_last_xread_success_unixtime

    class _DeadRedis(_RedisWithDlq):
        def xreadgroup(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise ConnectionError("Error 101 ... Network is unreachable")

    consumer = AlertWatchConsumer(_DeadRedis(), _NoopManager())
    watch_last_xread_success_unixtime.set(1000.0)

    with pytest.raises(ConnectionError):
        await consumer._read_messages()

    assert watch_last_xread_success_unixtime._value.get() == 1000.0
