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
