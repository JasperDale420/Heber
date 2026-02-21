from __future__ import annotations

from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import envelope_to_silver_row


def test_bars_payload_timestamp_maps_to_bar_start_ts() -> None:
    now = datetime(2026, 2, 12, 14, 35, tzinfo=UTC)
    envelope = EventEnvelope(
        event_id="evt-bars-ts-map",
        provider="alpaca",
        feed="bars",
        source="rest",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=now,
        ts_ingest=now,
        ts_available=now,
        payload={
            "symbol": "AAPL",
            "timestamp": "2026-02-12T14:30:00Z",
            "open": "100.0",
            "high": "101.0",
            "low": "99.5",
            "close": "100.8",
            "volume": 1000,
            "trade_count": 25,
            "vwap": "100.4",
        },
    )

    row = envelope_to_silver_row(normalize_envelope_for_silver(envelope))

    assert row["bar_start_ts"] == datetime(2026, 2, 12, 14, 30, tzinfo=UTC)
