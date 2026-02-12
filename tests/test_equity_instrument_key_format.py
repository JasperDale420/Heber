from __future__ import annotations

from heber.models.envelope import validate_instrument_key


def test_equity_instrument_key_accepts_extended_tickers() -> None:
    assert validate_instrument_key("equity:AAPL", "equity")
    assert validate_instrument_key("equity:BRK.B", "equity")
    assert validate_instrument_key("equity:JRI.RT", "equity")
    assert validate_instrument_key("equity:VAL.WS", "equity")


def test_equity_instrument_key_rejects_malformed_values() -> None:
    assert not validate_instrument_key("equity:.AAPL", "equity")
    assert not validate_instrument_key("equity:AAPL.", "equity")
    assert not validate_instrument_key("equity:AAPL..WS", "equity")
    assert not validate_instrument_key("equity:AAPL/WS", "equity")
    assert not validate_instrument_key("equity:aapl", "equity")
