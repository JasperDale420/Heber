from __future__ import annotations

from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope
from heber.writer.key_normalization import normalize_envelope_for_silver

NOW = datetime(2026, 2, 11, 16, 0, tzinfo=UTC)


def _build_envelope(feed: str, payload: dict, **overrides) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"evt-{feed}",
        provider=overrides.get("provider", "unusual_whales"),
        feed=feed,
        source="rest",
        instrument_type=overrides.get("instrument_type", "equity"),
        instrument_key=overrides.get("instrument_key", "equity:AAPL"),
        symbol=overrides.get("symbol", "AAPL"),
        ts_event=NOW,
        ts_ingest=NOW,
        ts_available=NOW,
        payload=payload,
    )


def test_flow_alerts_occ_symbol_synthesizes_canonical_option_key() -> None:
    envelope = _build_envelope(
        "flow_alerts",
        {
            "symbol": "AAPL",
            "option_chain": "AAPL260320C00200000",
            "strike": "200",
            "expiry": "2026-03-20",
            "put_call": "call",
            "premium": "100000",
            "volume": "12",
        },
        instrument_type="option",
        instrument_key="option:AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "AAPL"
    assert normalized.instrument_type == "option"
    assert normalized.instrument_key == "option:OCC:AAPL260320C00200000"
    assert normalized.is_valid_instrument_key()


def test_market_and_sector_tide_use_etf_proxy_keys() -> None:
    market = _build_envelope(
        "market_tide",
        {
            "date": "2026-02-11",
            "net_call_premium": "1200000",
            "net_put_premium": "800000",
            "sentiment": "bullish",
        },
        symbol="MARKET",
        instrument_key="equity:MARKET",
    )
    sector = _build_envelope(
        "sector_tide",
        {
            "sector": "Technology",
            "net_call_premium": "100000",
            "net_put_premium": "50000",
            "sentiment": "bullish",
        },
        symbol="TECHNOLOGY",
        instrument_key="equity:TECHNOLOGY",
    )
    unknown_sector = _build_envelope(
        "sector_tide",
        {
            "sector": "Unknown Sector",
            "net_call_premium": "100000",
            "net_put_premium": "50000",
            "sentiment": "bullish",
        },
        symbol="UNKNOWN",
        instrument_key="equity:UNKNOWN",
    )

    market_normalized = normalize_envelope_for_silver(market)
    sector_normalized = normalize_envelope_for_silver(sector)
    unknown_sector_normalized = normalize_envelope_for_silver(unknown_sector)

    assert market_normalized.symbol == "SPY"
    assert market_normalized.instrument_key == "equity:SPY"
    assert market_normalized.is_valid_instrument_key()

    assert sector_normalized.symbol == "XLK"
    assert sector_normalized.instrument_key == "equity:XLK"
    assert sector_normalized.is_valid_instrument_key()

    assert unknown_sector_normalized.symbol == "SPY"
    assert unknown_sector_normalized.instrument_key == "equity:SPY"
    assert unknown_sector_normalized.is_valid_instrument_key()


def test_congress_insider_and_news_fill_symbol_from_payload_fields() -> None:
    congress = _build_envelope(
        "congress_trades",
        {
            "ticker": "AAPL",
            "name": "Jane Doe",
            "txn_type": "buy",
            "transaction_date": "2026-02-01",
        },
        symbol="",
        instrument_key="equity:",
    )
    insider = _build_envelope(
        "insider_trades",
        {
            "ticker": "MSFT",
            "owner_name": "John Exec",
            "transaction_code": "P",
            "amount": "100",
            "transaction_date": "2026-02-01",
        },
        symbol="",
        instrument_key="equity:",
    )
    news = _build_envelope(
        "news",
        {
            "article_id": "n-1",
            "headline": "TSLA article",
            "symbols": ["TSLA", "AAPL"],
            "published_at": "2026-02-11T14:00:00Z",
        },
        provider="alpaca",
        symbol="",
        instrument_key="equity:",
    )

    congress_normalized = normalize_envelope_for_silver(congress)
    insider_normalized = normalize_envelope_for_silver(insider)
    news_normalized = normalize_envelope_for_silver(news)

    assert congress_normalized.symbol == "AAPL"
    assert congress_normalized.instrument_key == "equity:AAPL"
    assert congress_normalized.is_valid_instrument_key()

    assert insider_normalized.symbol == "MSFT"
    assert insider_normalized.instrument_key == "equity:MSFT"
    assert insider_normalized.is_valid_instrument_key()

    assert news_normalized.symbol == "TSLA"
    assert news_normalized.instrument_key == "equity:TSLA"
    assert news_normalized.is_valid_instrument_key()


# ---------------------------------------------------------------------------
# Market Data Feed Normalization (bars, quotes, trades)
# ---------------------------------------------------------------------------


def test_crypto_bars_normalize_symbol_and_key_slash_format() -> None:
    """Crypto bars with BTC/USD format should normalize to BTC-USD."""
    envelope = _build_envelope(
        "crypto_bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "96000.0",
            "h": "96250.0",
            "l": "95800.0",
            "c": "96100.0",
            "v": "225",
        },
        provider="alpaca",
        instrument_type="crypto",
        instrument_key="crypto:BTC-USD",
        symbol="BTC/USD",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "BTC-USD"
    assert normalized.instrument_type == "crypto"
    assert normalized.instrument_key == "crypto:BTC-USD"
    assert normalized.is_valid_instrument_key()


def test_crypto_bars_normalize_symbol_dash_format() -> None:
    """Crypto bars with BTC-USD format should pass through correctly."""
    envelope = _build_envelope(
        "crypto_bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "3400.0",
            "h": "3420.0",
            "l": "3380.0",
            "c": "3410.0",
            "v": "100",
        },
        provider="alpaca",
        instrument_type="crypto",
        instrument_key="crypto:ETH-USD",
        symbol="ETH-USD",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "ETH-USD"
    assert normalized.instrument_type == "crypto"
    assert normalized.instrument_key == "crypto:ETH-USD"
    assert normalized.is_valid_instrument_key()


def test_crypto_bars_normalize_concatenated_symbol() -> None:
    """Crypto bars with BTCUSD concatenated format should normalize to BTC-USD."""
    envelope = _build_envelope(
        "crypto_bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "96000.0",
            "h": "96250.0",
            "l": "95800.0",
            "c": "96100.0",
            "v": "225",
        },
        provider="alpaca",
        instrument_type="crypto",
        instrument_key="crypto:BTC-USD",
        symbol="BTCUSD",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "BTC-USD"
    assert normalized.instrument_key == "crypto:BTC-USD"
    assert normalized.is_valid_instrument_key()


def test_crypto_trades_preserve_instrument_type() -> None:
    """Crypto trades should normalize to crypto instrument type even through alias."""
    envelope = _build_envelope(
        "crypto_trades",
        {
            "p": "96110.0",
            "s": "2",
            "x": "CBSE",
            "i": "crypto-trade-1",
        },
        provider="alpaca",
        instrument_type="crypto",
        instrument_key="crypto:BTC-USD",
        symbol="BTC-USD",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.feed == "trades"
    assert normalized.instrument_type == "crypto"
    assert normalized.instrument_key == "crypto:BTC-USD"
    assert normalized.symbol == "BTC-USD"
    assert normalized.is_valid_instrument_key()


def test_crypto_bars_ethusdt_four_char_quote() -> None:
    """Crypto symbol with 4-char quote currency like ETHUSDT should normalize."""
    envelope = _build_envelope(
        "crypto_bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "3400.0",
            "h": "3420.0",
            "l": "3380.0",
            "c": "3410.0",
            "v": "100",
        },
        provider="alpaca",
        instrument_type="crypto",
        instrument_key="crypto:ETH-USDT",
        symbol="ETHUSDT",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "ETH-USDT"
    assert normalized.instrument_key == "crypto:ETH-USDT"
    assert normalized.is_valid_instrument_key()


def test_equity_bars_normalize_symbol() -> None:
    """Equity bars should normalize symbol and produce valid instrument key."""
    envelope = _build_envelope(
        "bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "100.0",
            "h": "101.0",
            "l": "99.0",
            "c": "100.5",
            "v": "1200",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "AAPL"
    assert normalized.instrument_type == "equity"
    assert normalized.instrument_key == "equity:AAPL"
    assert normalized.is_valid_instrument_key()


def test_equity_bars_extended_ticker_brk_b() -> None:
    """Equity bars with extended tickers like BRK.B should normalize correctly."""
    envelope = _build_envelope(
        "bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "400.0",
            "h": "405.0",
            "l": "398.0",
            "c": "403.0",
            "v": "500",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:BRK.B",
        symbol="BRK.B",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.symbol == "BRK.B"
    assert normalized.instrument_type == "equity"
    assert normalized.instrument_key == "equity:BRK.B"
    assert normalized.is_valid_instrument_key()


def test_option_trades_preserve_occ_key() -> None:
    """Option trades should preserve the OCC instrument key from Data-Gateway."""
    envelope = _build_envelope(
        "option_trades",
        {
            "p": "4.2",
            "s": "10",
            "x": "OPRA",
            "i": "opt-trade-1",
        },
        provider="alpaca",
        instrument_type="option",
        instrument_key="option:OCC:AAPL260320C00200000",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert normalized.feed == "trades"
    assert normalized.instrument_type == "option"
    assert normalized.instrument_key == "option:OCC:AAPL260320C00200000"
    assert normalized.symbol == "AAPL"
    assert normalized.is_valid_instrument_key()


# ---------------------------------------------------------------------------
# Data Quality Validation
# ---------------------------------------------------------------------------


def test_market_data_validation_flags_suspect_ohlcv() -> None:
    """Bars with high < low should get quality flag."""
    envelope = _build_envelope(
        "bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "100.0",
            "h": "98.0",  # lower than low
            "l": "99.0",
            "c": "100.5",
            "v": "1200",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert "high_below_low" in normalized.quality_flags


def test_market_data_validation_flags_negative_price() -> None:
    """Bars with negative OHLCV values should get quality flag."""
    envelope = _build_envelope(
        "bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "-100.0",
            "h": "101.0",
            "l": "99.0",
            "c": "100.5",
            "v": "1200",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert "negative_price" in normalized.quality_flags


def test_market_data_validation_flags_inverted_spread() -> None:
    """Quotes with bid > ask should get inverted_spread quality flag."""
    envelope = _build_envelope(
        "quotes",
        {
            "bp": "101.0",
            "ap": "100.0",
            "bs": "10",
            "as": "12",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert "inverted_spread" in normalized.quality_flags


def test_market_data_validation_clean_bars_no_flags() -> None:
    """Clean bars should not get any suspect quality flags."""
    envelope = _build_envelope(
        "bars",
        {
            "t": "2026-02-11T14:30:00Z",
            "o": "100.0",
            "h": "101.0",
            "l": "99.0",
            "c": "100.5",
            "v": "1200",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    suspect_flags = {"negative_price", "negative_volume", "high_below_low"}
    assert not suspect_flags.intersection(set(normalized.quality_flags))


def test_trade_validation_flags_non_positive_price() -> None:
    """Trades with zero price should get non_positive_price quality flag."""
    envelope = _build_envelope(
        "trades",
        {
            "p": "0.0",
            "s": "50",
            "x": "XNAS",
        },
        provider="alpaca",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
    )

    normalized = normalize_envelope_for_silver(envelope)

    assert "non_positive_price" in normalized.quality_flags
