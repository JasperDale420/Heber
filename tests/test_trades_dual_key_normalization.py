"""Tests for trades field mapping with both short Alpaca WS keys and full-name REST keys.

The normalizer must handle both key formats:
- Short keys from Alpaca WebSocket streams: p, s, x, i, z
- Full-name keys from Alpaca REST / aggregate payloads: price, size, exchange, trade_id, tape
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope
from heber.writer.normalizer import envelope_to_silver_row

NOW = datetime(2026, 2, 12, 15, 30, tzinfo=UTC)


def _build_trade_envelope(payload: dict, *, source: str = "ws") -> EventEnvelope:
    return EventEnvelope(
        event_id="test-trade-1",
        provider="alpaca",
        feed="trades",
        source=source,
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=NOW,
        ts_ingest=NOW,
        payload=payload,
    )


def test_short_alpaca_keys_map_to_silver_columns() -> None:
    """Short Alpaca WS keys (p, s, x, i, z) map correctly to Silver trade columns."""
    envelope = _build_trade_envelope(
        {"p": "155.50", "s": "100", "x": "XNAS", "i": "trade-abc", "z": "A"},
        source="ws",
    )
    row = envelope_to_silver_row(envelope)
    assert math.isclose(row["price"], 155.50)
    assert row["size"] == 100
    assert row["exchange"] == "XNAS"
    assert row["trade_id"] == "trade-abc"
    assert row["tape"] == "A"


def test_full_name_keys_map_to_silver_columns() -> None:
    """Full-name REST keys (price, size, exchange) map correctly to Silver trade columns."""
    envelope = _build_trade_envelope(
        {"price": 155.50, "size": 100, "exchange": "XNAS", "trade_id": "trade-xyz", "tape": "A"},
        source="rest",
    )
    row = envelope_to_silver_row(envelope)
    assert math.isclose(row["price"], 155.50)
    assert row["size"] == 100
    assert row["exchange"] == "XNAS"
    assert row["trade_id"] == "trade-xyz"
    assert row["tape"] == "A"


def test_short_keys_take_precedence_over_full_name_keys() -> None:
    """When both short and full-name keys are present, short keys take precedence."""
    envelope = _build_trade_envelope(
        {
            "p": "200.00",
            "price": 999.99,
            "s": "50",
            "size": 9999,
            "x": "NYSE",
            "exchange": "WRONG",
        },
        source="ws",
    )
    row = envelope_to_silver_row(envelope)
    assert math.isclose(row["price"], 200.00)
    assert row["size"] == 50
    assert row["exchange"] == "NYSE"


def test_empty_payload_produces_null_trade_fields() -> None:
    """An empty payload produces null values for all trade-specific fields."""
    envelope = _build_trade_envelope({})
    row = envelope_to_silver_row(envelope)
    assert row["price"] is None
    assert row["size"] is None
    assert row["exchange"] is None
    assert row["trade_id"] is None
    assert row["tape"] is None
