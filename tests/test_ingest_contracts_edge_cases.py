"""Targeted tests for untested edge cases in heber.writer.ingest_contracts and
heber.writer.key_normalization.

Covers two real gaps found while auditing test coverage:

1. The ``_backfill_empty_date`` helper (and its ``ts_event``-driven callers
   ``_normalize_short_data_payload``, ``_normalize_oi_change_payload``,
   ``_normalize_hov_payload``) — UnusualWhales per-symbol aggregate endpoints
   often omit ``date`` entirely, and this module derives it from the event
   timestamp so the Silver required-field gate (``oi_date``, ``hov_date``,
   ``short_date``) doesn't reject otherwise-valid rows. No existing test
   exercised the ts_event-driven derivation path.
2. ``_build_occ_from_payload`` in ``key_normalization`` — the fallback OCC
   symbol synthesis used when a flow_alerts payload has no ``option_chain``/
   ``contract_symbol`` field but does carry ``symbol``/``strike``/``expiry``/
   ``put_call``. Every existing flow_alerts fixture already includes
   ``option_chain``, so this fallback path was never exercised.
"""

from __future__ import annotations

from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope
from heber.writer.ingest_contracts import _backfill_empty_date, normalize_payload_for_feed
from heber.writer.key_normalization import normalize_envelope_for_silver

NOW = datetime(2026, 4, 1, 16, 30, tzinfo=UTC)


# ---------------------------------------------------------------------------
# _backfill_empty_date (direct)
# ---------------------------------------------------------------------------


def test_backfill_empty_date_sets_date_from_ts_event_when_missing() -> None:
    payload: dict = {}
    _backfill_empty_date(payload, ts_event=NOW)
    assert payload["date"] == "2026-04-01"


def test_backfill_empty_date_sets_date_from_ts_event_when_blank_string() -> None:
    payload = {"date": ""}
    _backfill_empty_date(payload, ts_event=NOW)
    assert payload["date"] == "2026-04-01"


def test_backfill_empty_date_preserves_existing_non_blank_date() -> None:
    payload = {"date": "2026-01-15"}
    _backfill_empty_date(payload, ts_event=NOW)
    assert payload["date"] == "2026-01-15"


def test_backfill_empty_date_leaves_missing_date_when_ts_event_absent() -> None:
    payload: dict = {}
    _backfill_empty_date(payload, ts_event=None)
    assert "date" not in payload


# ---------------------------------------------------------------------------
# normalize_payload_for_feed: short_data / oi_change / historic_option_volume
# date derivation from ts_event (dispatcher-level, mirrors real consumer usage)
# ---------------------------------------------------------------------------


def test_normalize_short_data_derives_date_from_ts_event_when_blank() -> None:
    payload = {"symbol": "AAPL", "date": "", "short_volume": 12345}
    result = normalize_payload_for_feed("short_data", payload, ts_event=NOW)
    assert result["date"] == "2026-04-01"


def test_normalize_oi_change_derives_date_from_ts_event_when_missing() -> None:
    payload = {"symbol": "AAPL", "call_oi": 1000, "put_oi": 800}
    result = normalize_payload_for_feed("oi_change", payload, ts_event=NOW)
    assert result["date"] == "2026-04-01"


def test_normalize_oi_change_preserves_real_date_over_ts_event() -> None:
    payload = {"symbol": "AAPL", "date": "2026-03-20", "call_oi": 1000, "put_oi": 800}
    result = normalize_payload_for_feed("oi_change", payload, ts_event=NOW)
    assert result["date"] == "2026-03-20"


def test_normalize_historic_option_volume_derives_date_from_ts_event() -> None:
    payload = {"symbol": "AAPL", "volume": 5000}
    result = normalize_payload_for_feed("historic_option_volume", payload, ts_event=NOW)
    assert result["date"] == "2026-04-01"


def test_normalize_historic_option_volume_without_ts_event_leaves_date_missing() -> None:
    payload = {"symbol": "AAPL", "volume": 5000}
    result = normalize_payload_for_feed("historic_option_volume", payload)
    assert "date" not in result


# ---------------------------------------------------------------------------
# key_normalization: flow_alerts OCC synthesis fallback
# ---------------------------------------------------------------------------


def _flow_alert_envelope_without_option_chain() -> EventEnvelope:
    return EventEnvelope(
        event_id="evt-occ-synth",
        provider="unusual_whales",
        feed="flow_alerts",
        source="rest",
        instrument_type="option",
        instrument_key="option:AAPL",
        symbol="AAPL",
        ts_event=NOW,
        ts_ingest=NOW,
        ts_available=NOW,
        payload={
            "symbol": "AAPL",
            # No option_chain / contract_symbol / occ_symbol field — the
            # writer must synthesize the OCC symbol from strike/expiry/put_call.
            "strike": "200",
            "expiry": "2026-03-20",
            "put_call": "call",
            "premium": "100000",
            "volume": "12",
        },
    )


def test_flow_alert_synthesizes_occ_symbol_when_no_option_chain_field() -> None:
    normalized = normalize_envelope_for_silver(_flow_alert_envelope_without_option_chain())

    assert normalized.instrument_key == "option:OCC:AAPL260320C00200000"
    assert normalized.payload["option_chain"] == "AAPL260320C00200000"
    assert normalized.symbol == "AAPL"
    assert normalized.is_valid_instrument_key()


def test_flow_alert_occ_synthesis_uses_put_side_and_pads_strike() -> None:
    envelope = _flow_alert_envelope_without_option_chain()
    envelope.payload["put_call"] = "put"
    envelope.payload["strike"] = "5"

    normalized = normalize_envelope_for_silver(envelope)

    # strike 5 -> 5 * 1000 = 5000 -> zero-padded to 8 digits.
    assert normalized.instrument_key == "option:OCC:AAPL260320P00005000"
