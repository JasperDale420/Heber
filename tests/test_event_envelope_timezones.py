from __future__ import annotations

from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope


def _build_event(ts_event: str, ts_ingest: str, ts_available: str | None = None) -> EventEnvelope:
    payload = {
        "event_id": "evt-1",
        "provider": "unusual_whales",
        "feed": "greek_exposure",
        "source": "rest",
        "instrument_type": "equity",
        "instrument_key": "equity:HOOD",
        "symbol": "HOOD",
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "payload": {},
    }
    if ts_available is not None:
        payload["ts_available"] = ts_available
    return EventEnvelope.model_validate(payload)


def test_naive_timestamps_are_normalized_to_utc() -> None:
    envelope = _build_event(
        ts_event="2026-02-12T00:00:00",
        ts_ingest="2026-02-12T00:00:01",
        ts_available="2026-02-12T00:00:02",
    )

    assert envelope.ts_event.tzinfo == UTC
    assert envelope.ts_ingest.tzinfo == UTC
    assert envelope.ts_available is not None
    assert envelope.ts_available.tzinfo == UTC


def test_offset_aware_timestamps_are_converted_to_utc() -> None:
    envelope = _build_event(
        ts_event="2026-02-12T01:30:00-05:00",
        ts_ingest="2026-02-12T01:30:01-05:00",
    )

    assert envelope.ts_event == datetime(2026, 2, 12, 6, 30, 0, tzinfo=UTC)
    assert envelope.ts_ingest == datetime(2026, 2, 12, 6, 30, 1, tzinfo=UTC)
