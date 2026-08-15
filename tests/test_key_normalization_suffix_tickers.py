"""Trailing-suffix tickers from UW darkpool must not silently dead-letter.

`=` is an accepted units/warrants marker in this repo (stripped, raw ticker kept
in Bronze). `+` is undocumented, so it stays quarantined rather than guessed at.
"""

from datetime import UTC, datetime

import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.key_normalization import (
    InvalidInstrumentKeyError,
    normalize_envelope_for_silver,
)


def _darkpool_envelope(symbol: str) -> EventEnvelope:
    now = datetime(2026, 8, 7, 14, 30, tzinfo=UTC)
    return EventEnvelope(
        event_id=f"evt-{symbol}",
        provider="unusual_whales",
        feed="darkpool",
        source="rest",
        instrument_type="equity",
        instrument_key=f"equity:{symbol}",
        symbol=symbol,
        ts_event=now,
        ts_ingest=now,
        payload={"ticker": symbol, "price": 10.0, "size": 100},
    )


@pytest.mark.parametrize("dirty,clean", [("AAC=", "AAC"), ("PNAQ=", "PNAQ"), ("SAMO=", "SAMO")])
def test_equals_suffix_tickers_are_normalized_not_dead_lettered(dirty: str, clean: str) -> None:
    """The '=' cleanup is currently unreachable: _normalize_symbol rejects the
    dirty symbol first, so the lenient _normalize_equity_symbol never runs."""
    result = normalize_envelope_for_silver(_darkpool_envelope(dirty))

    assert result.symbol == clean
    assert result.instrument_key == f"equity:{clean}"
    assert result.is_valid_instrument_key()


@pytest.mark.parametrize("dirty", ["VAL+", "FOO+"])
def test_plus_suffix_tickers_remain_quarantined(dirty: str) -> None:
    """'+' has no documented meaning here — quarantine beats guessing.

    Dead-lettering is the intended outcome, so the writer must still raise.
    """
    with pytest.raises(InvalidInstrumentKeyError) as exc:
        normalize_envelope_for_silver(_darkpool_envelope(dirty))

    assert dirty in str(exc.value)
