from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from heber.writer.consumer import EventConsumer

NOW = datetime(2026, 2, 11, 15, 0, tzinfo=UTC)


def _event_payload(feed: str, payload: dict, **overrides) -> dict:
    base = {
        "event_id": f"evt-{feed}",
        "provider": overrides.get("provider", "unusual_whales"),
        "feed": feed,
        "source": "rest",
        "instrument_type": overrides.get("instrument_type", "equity"),
        "instrument_key": overrides.get("instrument_key", "equity:AAPL"),
        "symbol": overrides.get("symbol", "AAPL"),
        "ts_event": NOW.isoformat(),
        "ts_ingest": NOW.isoformat(),
        "payload": payload,
    }
    return base


def test_bronze_is_written_before_silver_on_success_path() -> None:
    consumer = EventConsumer()
    call_order: list[str] = []

    consumer.bronze_writer.write = MagicMock(side_effect=lambda _envelope: call_order.append("bronze"))
    consumer.silver_writer.write_row = MagicMock(side_effect=lambda _pk, _row: call_order.append("silver"))

    event = _event_payload(
        "bars",
        {
            "t": NOW.isoformat(),
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100.5",
            "v": "1000",
        },
        provider="alpaca",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    assert call_order == ["bronze", "silver"]


def test_bronze_write_persists_even_when_silver_normalization_fails() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    event = _event_payload(
        "flow_alerts",
        {
            "timestamp": NOW.isoformat(),
            "premium": "100000",
            "volume": "15",
        },
        instrument_type="option",
        instrument_key="option:AAPL",
        symbol="",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is False
    assert error is not None
    assert retryable is False
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()


def test_rest_bars_aggregate_payload_is_expanded_into_multiple_silver_writes() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write_row = MagicMock()

    event = _event_payload(
        "bars",
        {
            "symbol": "AAPL",
            "timeframe": "1Min",
            "bars": [
                {
                    "symbol": "AAPL",
                    "timestamp": "2026-02-11T14:30:00Z",
                    "open": "100.0",
                    "high": "101.0",
                    "low": "99.5",
                    "close": "100.8",
                    "volume": 1000,
                    "trade_count": 25,
                    "vwap": "100.4",
                },
                {
                    "symbol": "AAPL",
                    "timestamp": "2026-02-11T14:31:00Z",
                    "open": "100.8",
                    "high": "101.5",
                    "low": "100.6",
                    "close": "101.2",
                    "volume": 850,
                    "trade_count": 18,
                    "vwap": "101.0",
                },
            ],
        },
        provider="alpaca",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    consumer.bronze_writer.write.assert_called_once()
    assert consumer.silver_writer.write_row.call_count == 2

    # Verify first row has expected open value, second has expected close value
    first_row = consumer.silver_writer.write_row.call_args_list[0].args[1]
    second_row = consumer.silver_writer.write_row.call_args_list[1].args[1]
    assert first_row["open"] == 100.0
    assert second_row["close"] == 101.2
    assert first_row["timeframe"] == "1Min"


def test_rest_trades_aggregate_empty_list_skips_silver_write() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    event = _event_payload(
        "trades",
        {
            "symbol": "SPY",
            "trades": [],
        },
        provider="alpaca",
        symbol="SPY",
        instrument_key="equity:SPY",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()


def test_rest_bars_aggregate_mixed_valid_invalid_only_writes_valid_items() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()

    event = _event_payload(
        "bars",
        {
            "symbol": "AAPL",
            "timeframe": "1Min",
            "bars": [
                {
                    "timestamp": "2026-02-11T14:30:00Z",
                    "open": "100.0",
                    "high": "101.0",
                    "low": "99.5",
                    "close": "100.8",
                    "volume": 1000,
                },
                {
                    "timestamp": "2026-02-11T14:31:00Z",
                    "high": "101.5",
                    "low": "100.6",
                    "close": "101.2",
                    "volume": 850,
                },
            ],
        },
        provider="alpaca",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    consumer.bronze_writer.write.assert_called_once()
    assert sum(len(rows) for rows in consumer.silver_writer.buffers.values()) == 1


def test_non_aggregate_bars_missing_required_fields_fails_after_bronze_write() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()

    event = _event_payload(
        "bars",
        {
            "t": NOW.isoformat(),
            "h": "101",
            "l": "99",
            "c": "100.5",
            "v": "1000",
        },
        provider="alpaca",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is False
    assert error is not None
    assert "missing_required_fields" in error
    assert retryable is False
    consumer.bronze_writer.write.assert_called_once()
    assert sum(len(rows) for rows in consumer.silver_writer.buffers.values()) == 0


def test_bronze_only_news_feed_skips_silver_by_policy() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    event = _event_payload(
        "news",
        {
            "article_id": "news-1",
            "headline": "AAPL posts strong guidance",
            "published_at": NOW.isoformat(),
            "symbols": ["AAPL"],
            "source": "Reuters",
        },
        provider="alpaca",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()
    assert sum(len(rows) for rows in consumer.silver_writer.buffers.values()) == 0


def test_bronze_only_alias_feed_skips_silver_by_policy() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    event = _event_payload(
        "institutions",
        {
            "symbol": "AAPL",
            "institution_name": "Example Capital",
            "institution_id": "0000123456",
            "market_value": "24500000",
            "report_date": "2025-12-31",
            "shares": "120000",
            "percent_portfolio": "2.4",
        },
        provider="unusual_whales",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    success, error, retryable = consumer._process_event_once({"data": json.dumps(event)})

    assert success is True
    assert error is None
    assert retryable is True
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()
    assert sum(len(rows) for rows in consumer.silver_writer.buffers.values()) == 0


@pytest.mark.asyncio
async def test_unknown_feed_is_non_retriable_and_still_bronze_first() -> None:
    consumer = EventConsumer()
    consumer.bronze_writer.write = MagicMock()
    consumer.silver_writer.write = MagicMock()

    event = _event_payload(
        "future_feed",
        {"foo": "bar"},
    )

    success, error, attempts = await consumer._process_with_retry({"data": json.dumps(event)})

    assert success is False
    assert error == "uncontracted_feed"
    assert attempts == 1
    consumer.bronze_writer.write.assert_called_once()
    consumer.silver_writer.write.assert_not_called()
