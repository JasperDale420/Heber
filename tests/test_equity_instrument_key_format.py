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


def test_macro_instrument_key_accepts_treasury_yields() -> None:
    """Data-Gateway emits treasury_yields as macro:treasury_yield:{maturity}."""
    assert validate_instrument_key("macro:treasury_yield:2year", "macro")
    assert validate_instrument_key("macro:treasury_yield:10year", "macro")
    assert validate_instrument_key("macro:treasury_yield:30year", "macro")


def test_macro_instrument_key_accepts_other_macro_indicators() -> None:
    """Pattern should generalize to other macro indicators."""
    assert validate_instrument_key("macro:cpi", "macro")
    assert validate_instrument_key("macro:gdp:annual", "macro")
    assert validate_instrument_key("macro:fed_funds_rate", "macro")
    assert validate_instrument_key("macro:vix", "macro")


def test_macro_instrument_key_rejects_malformed_values() -> None:
    assert not validate_instrument_key("macro:", "macro")
    assert not validate_instrument_key("macro:Treasury_Yield", "macro")  # uppercase
    assert not validate_instrument_key("macro:treasury yield", "macro")  # whitespace
    assert not validate_instrument_key("treasury_yield:2year", "macro")  # missing prefix
