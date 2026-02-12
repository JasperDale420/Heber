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
    consumer.silver_writer.write = MagicMock(side_effect=lambda _envelope: call_order.append("silver"))

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
