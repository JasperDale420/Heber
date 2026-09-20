"""Additional untested edge cases in heber.writer.key_normalization.

1. ``_coerce_strike`` rejects a negative strike (returns ``None``), which
   makes OCC synthesis for a flow_alerts payload without ``option_chain``
   fail — the writer must fall back to an equity key rather than crash or
   emit a garbage OCC symbol. No existing test drove a negative strike
   through this path.
2. A crypto symbol that cannot be normalized (unknown quote currency, no
   separator) must not be silently accepted as a valid instrument key: Silver
   never stores rows with malformed instrument keys, so this must raise
   ``InvalidInstrumentKeyError`` rather than writing e.g. ``crypto:XRPZZZ``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.key_normalization import (
    InvalidInstrumentKeyError,
    normalize_envelope_for_silver,
)

NOW = datetime(2026, 4, 1, 16, 30, tzinfo=UTC)


def test_flow_alert_negative_strike_falls_back_to_equity_key_instead_of_garbage_occ() -> None:
    """A negative strike can't be encoded into an OCC symbol (_coerce_strike
    rejects it), so synthesis must fail cleanly and fall back to the equity
    key rather than producing an invalid or nonsensical OCC string."""
    envelope = EventEnvelope(
        event_id="evt-negative-strike",
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
            # No option_chain/contract_symbol -> must synthesize from these,
            # but a negative strike can't round-trip into an OCC symbol.
            "strike": "-5",
            "expiry": "2026-03-20",
            "put_call": "call",
            "premium": "100000",
            "volume": "12",
        },
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.instrument_type == "equity"
    assert normalized.instrument_key == "equity:AAPL"
    assert normalized.is_valid_instrument_key()


def test_crypto_symbol_that_cannot_be_normalized_raises_instead_of_writing_bad_key() -> None:
    """An unsplittable, unmapped crypto symbol must dead-letter rather than
    silently land in Silver with an invalid instrument key."""
    envelope = EventEnvelope(
        event_id="evt-bad-crypto",
        provider="alpaca",
        feed="crypto_bars",
        source="rest",
        instrument_type="crypto",
        instrument_key="crypto:XRPZZZ",
        symbol="XRPZZZ",  # no separator, "ZZZ"/"PZZZ" are not known quote currencies
        ts_event=NOW,
        ts_ingest=NOW,
        ts_available=NOW,
        payload={
            "t": "2026-04-01T16:30:00Z",
            "o": "1.0",
            "h": "1.1",
            "l": "0.9",
            "c": "1.05",
            "v": "10",
        },
    )

    with pytest.raises(InvalidInstrumentKeyError):
        normalize_envelope_for_silver(envelope)
