from __future__ import annotations

from datetime import UTC, datetime

from heber.models.envelope import EventEnvelope
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import (
    REQUIRED_NON_NULL_FIELDS,
    is_bronze_only_feed,
    is_contracted_feed,
    resolve_feed_alias,
)
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import envelope_to_silver_row
from heber.writer.transformer import BronzeToSilverTransformer

# Reference / metadata feed names that were previously emitting
# ``silver_feed_uncontracted`` warnings and being routed to DLQ.  Sourced from
# 13 days of heber-catalog error logs (3,350 warnings across 67 unique feeds).
# Each name must now satisfy ``is_contracted_feed`` (no uncontracted warning)
# AND ``is_bronze_only_feed`` (no Silver typing attempted).
PREVIOUSLY_UNCONTRACTED_BRONZE_ONLY_FEEDS: tuple[str, ...] = (
    "13f",
    "account",
    "alerts",
    "assets",
    "balance-sheet",
    "calendar",
    "cash-flow",
    "clock",
    "company",
    "concept",
    "congress",
    "congress-trading",
    "contract",
    "corporate-actions",
    "crypto",
    "economic",
    "estimates",
    "etf_metadata",
    "executives",
    "fda-calendar",
    "filings",
    "fixed-income",
    "forex",
    "frames",
    "fund-ownership",
    "income-statement",
    "index",
    "indicator",
    "insider",
    "insider-sentiment",
    "insider-transactions",
    "institution_holdings",
    "listing-status",
    "lobbying",
    "logos",
    "market",
    "meta",
    "metrics",
    "mutual-fund",
    "option-contract",
    "option_contract",
    "orders",
    "orders:by_client_order_id",
    "ownership",
    "patterns",
    "peers",
    "politician_trades",
    "portfolio",
    "positions",
    "price-target",
    "recommendations",
    "screener_result",
    "search",
    "seasonality",
    "sectors",
    "sentiment",
    "social-sentiment",
    "stock",
    "stock_fundamentals",
    "stocks",
    "support-resistance",
    "ticker",
    "upgrade-downgrade",
    "usa-spending",
    "watchlists",
    "{symbol}",
)

NOW = datetime(2026, 2, 11, 14, 30, tzinfo=UTC)


def _envelope(
    *,
    feed: str,
    payload: dict,
    provider: str,
    instrument_type: str = "equity",
    instrument_key: str = "equity:AAPL",
    symbol: str = "AAPL",
) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"evt-{feed}",
        provider=provider,
        feed=feed,
        source="rest",
        instrument_type=instrument_type,
        instrument_key=instrument_key,
        symbol=symbol,
        ts_event=NOW,
        ts_ingest=NOW,
        ts_available=NOW,
        payload=payload,
    )


def _cases() -> list[tuple[str, str, EventEnvelope]]:
    return [
        (
            "bars",
            "bars",
            _envelope(
                feed="bars",
                provider="alpaca",
                payload={
                    "t": "2026-02-11T14:30:00Z",
                    "o": "100.0",
                    "h": "101.0",
                    "l": "99.0",
                    "c": "100.5",
                    "v": "1200",
                    "n": "10",
                    "vw": "100.2",
                },
            ),
        ),
        (
            "quotes",
            "quotes",
            _envelope(
                feed="quotes",
                provider="alpaca",
                payload={
                    "bp": "100.1",
                    "ap": "100.3",
                    "bs": "10",
                    "as": "12",
                    "bx": "XNAS",
                    "ax": "ARCX",
                },
            ),
        ),
        (
            "trades",
            "trades",
            _envelope(
                feed="trades",
                provider="alpaca",
                payload={
                    "p": "100.2",
                    "s": "50",
                    "x": "XNAS",
                    "i": "trade-1",
                    "z": "A",
                },
            ),
        ),
        (
            "option_trades",
            "trades",
            _envelope(
                feed="option_trades",
                provider="alpaca",
                instrument_type="option",
                instrument_key="option:OCC:AAPL260320C00200000",
                payload={
                    "p": "4.2",
                    "s": "10",
                    "x": "OPRA",
                    "i": "opt-trade-1",
                    "z": "C",
                },
            ),
        ),
        (
            "option_chain_snapshot",
            "option_chain_snapshot",
            _envelope(
                feed="option_chain_snapshot",
                provider="alpaca",
                instrument_type="equity",
                instrument_key="equity:SPY",
                symbol="SPY",
                payload={
                    "timestamp": "2026-02-11T14:30:00Z",
                    "underlying": "SPY",
                    "underlying_price": 600.5,
                    "expiry": "2026-02-11",
                    "chain_json": {
                        "data": {
                            "contracts": [
                                {
                                    "contract_symbol": "SPY260211C00600000",
                                    "underlying": "SPY",
                                    "expiration": "2026-02-11",
                                    "strike": 600.0,
                                    "option_type": "call",
                                    "bid": 2.0,
                                    "ask": 2.1,
                                    "last": 2.05,
                                    "volume": 1000,
                                    "open_interest": 2500,
                                    "delta": 0.51,
                                    "gamma": 0.09,
                                    "theta": -0.02,
                                    "vega": 0.04,
                                    "iv": 0.24,
                                    "timestamp": "2026-02-11T14:30:00Z",
                                }
                            ]
                        }
                    },
                    "total_call_volume": 1000.0,
                    "total_put_volume": 500.0,
                    "total_call_oi": 2500.0,
                    "total_put_oi": 1750.0,
                    "atm_iv": 0.24,
                },
            ),
        ),
        (
            "crypto_bars",
            "bars",
            _envelope(
                feed="crypto_bars",
                provider="alpaca",
                instrument_type="crypto",
                instrument_key="crypto:BTC-USD",
                symbol="BTC-USD",
                payload={
                    "t": "2026-02-11T14:30:00Z",
                    "o": "96000.0",
                    "h": "96250.0",
                    "l": "95800.0",
                    "c": "96100.0",
                    "v": "225",
                    "n": "30",
                    "vw": "96075.0",
                },
            ),
        ),
        (
            "crypto_trades",
            "trades",
            _envelope(
                feed="crypto_trades",
                provider="alpaca",
                instrument_type="crypto",
                instrument_key="crypto:BTC-USD",
                symbol="BTC-USD",
                payload={
                    "p": "96110.0",
                    "s": "2",
                    "x": "CBSE",
                    "i": "crypto-trade-1",
                    "z": "C",
                },
            ),
        ),
        (
            "news",
            "news",
            _envelope(
                feed="news",
                provider="alpaca",
                symbol="",
                instrument_key="equity:",
                payload={
                    "article_id": "news-1",
                    "headline": "AAPL headline",
                    "published_at": "2026-02-11T14:00:00Z",
                    "symbols": ["AAPL"],
                    "source": "Reuters",
                },
            ),
        ),
        (
            "flow_alerts",
            "flow_alerts",
            _envelope(
                feed="flow_alerts",
                provider="unusual_whales",
                instrument_type="option",
                instrument_key="option:AAPL",
                payload={
                    "timestamp": "2026-02-11T14:30:00Z",
                    "symbol": "AAPL",
                    "option_chain": "AAPL260320C00200000",
                    "strike": "200",
                    "expiry": "2026-03-20",
                    "put_call": "call",
                    "premium": "100000",
                    "volume": "25",
                    "price": "3.5",
                    "underlying_price": "199.1",
                    "alert_rule": "SWEEP",
                },
            ),
        ),
        (
            "ticker_flow",
            "flow_alerts",
            _envelope(
                feed="ticker_flow",
                provider="unusual_whales",
                instrument_type="option",
                instrument_key="option:AAPL",
                payload={
                    "timestamp": "2026-02-11T14:31:00Z",
                    "symbol": "AAPL",
                    "option_chain": "AAPL260320C00200000",
                    "strike": "200",
                    "expiry": "2026-03-20",
                    "put_call": "call",
                    "premium": "90000",
                    "volume": "18",
                    "price": "3.2",
                    "underlying_price": "198.8",
                },
            ),
        ),
        (
            "darkpool",
            "darkpool",
            _envelope(
                feed="darkpool",
                provider="unusual_whales",
                payload={
                    "timestamp": "2026-02-11T14:30:00Z",
                    "symbol": "AAPL",
                    "price": "50.1",
                    "size": "1000",
                    "notional": "50100",
                    "exchange": "XNYS",
                    "tracking_id": "dp-1",
                    "bid": "50.0",
                    "ask": "50.2",
                },
            ),
        ),
        (
            "darkpool_ticker",
            "darkpool",
            _envelope(
                feed="darkpool_ticker",
                provider="unusual_whales",
                payload={
                    "timestamp": "2026-02-11T14:32:00Z",
                    "symbol": "AAPL",
                    "ticker": "AAPL",
                    "price": "50.3",
                    "size": "700",
                    "notional": "35210",
                    "market_center": "XNYS",
                    "tracking_id": "dp-ticker-1",
                    "bid": "50.2",
                    "ask": "50.4",
                },
            ),
        ),
        (
            "market_tide",
            "market_tide",
            _envelope(
                feed="market_tide",
                provider="unusual_whales",
                symbol="MARKET",
                instrument_key="equity:MARKET",
                payload={
                    "timestamp": "2026-02-11T14:30:00Z",
                    "date": "2026-02-11",
                    "net_call_premium": "1200000",
                    "net_put_premium": "800000",
                    "net_volume": "5000",
                    "sentiment": "bullish",
                },
            ),
        ),
        (
            "sector_tide",
            "sector_tide",
            _envelope(
                feed="sector_tide",
                provider="unusual_whales",
                symbol="TECHNOLOGY",
                instrument_key="equity:TECHNOLOGY",
                payload={
                    "timestamp": "2026-02-11T14:30:00Z",
                    "sector": "Technology",
                    "net_call_premium": "100000",
                    "net_put_premium": "50000",
                    "net_volume": "1200",
                    "sentiment": "bullish",
                },
            ),
        ),
        (
            "greek_exposure",
            "greek_exposure",
            _envelope(
                feed="greek_exposure",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "timestamp": "2026-02-11T14:30:00Z",
                    "call_gamma": "1200000",
                    "put_gamma": "800000",
                    "call_delta": "500000",
                    "put_delta": "300000",
                    "call_vanna": "100000",
                    "put_vanna": "50000",
                    "call_charm": "25000",
                    "put_charm": "12000",
                    "strike": "200",
                    "expiry": "2026-03-20",
                    "dte": "30",
                },
            ),
        ),
        (
            "iv_rank",
            "iv_rank",
            _envelope(
                feed="iv_rank",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "iv_rank": "0.42",
                },
            ),
        ),
        (
            "oi_change",
            "oi_change",
            _envelope(
                feed="oi_change",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "date": "2026-02-11",
                    "call_oi": "20000",
                    "put_oi": "18000",
                    "call_oi_change": "1000",
                    "put_oi_change": "900",
                },
            ),
        ),
        (
            "historic_option_volume",
            "historic_option_volume",
            _envelope(
                feed="historic_option_volume",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "date": "2026-02-11",
                    "expiry": "2026-03-20",
                    "volume": "10000",
                    "open_interest": "20000",
                    "call_volume": "6000",
                    "put_volume": "4000",
                    "premium": "1250000.5",
                },
            ),
        ),
        (
            "short_interest",
            "short_data",
            _envelope(
                feed="short_interest",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "date": "2026-02-10",
                    "short_interest": "1000000",
                    "days_to_cover": "2.1",
                    "short_percent_float": "1.2",
                    "short_percent_outstanding": "0.8",
                },
            ),
        ),
        (
            "short_volume",
            "short_data",
            _envelope(
                feed="short_volume",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "date": "2026-02-10",
                    "short_volume": "350000",
                    "short_ratio": "0.4",
                },
            ),
        ),
        (
            "ftds",
            "ftd",
            _envelope(
                feed="ftds",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "date": "2026-02-10",
                    "quantity": "5000",
                    "price": "180.2",
                    "value": "901000",
                },
            ),
        ),
        (
            "congress_trades",
            "congress_trades",
            _envelope(
                feed="congress_trades",
                provider="unusual_whales",
                symbol="",
                instrument_key="equity:",
                payload={
                    "ticker": "AAPL",
                    "name": "Jane Doe",
                    "txn_type": "buy",
                    "amounts": "$15,001 - $50,000",
                    "transaction_date": "2026-01-15",
                    "filed_at_date": "2026-01-20",
                    "member_type": "house",
                },
            ),
        ),
        (
            "insider_trades",
            "insider_trades",
            _envelope(
                feed="insider_trades",
                provider="unusual_whales",
                symbol="",
                instrument_key="equity:",
                payload={
                    "ticker": "AAPL",
                    "owner_name": "John Exec",
                    "officer_title": "CEO",
                    "transaction_code": "P",
                    "amount": "500",
                    "price": "200.5",
                    "transaction_date": "2026-02-01T00:00:00Z",
                    "filing_date": "2026-02-02",
                    "id": "ins-1",
                    "is_10b5_1": True,
                    "is_officer": True,
                },
            ),
        ),
        (
            "institutions",
            "institution_holdings",
            _envelope(
                feed="institutions",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "institution_name": "Example Capital",
                    "institution_id": "0000123456",
                    "market_value": "24500000",
                    "report_date": "2025-12-31",
                    "shares": "120000",
                    "percent_portfolio": "2.4",
                },
            ),
        ),
        (
            "earnings",
            "earnings",
            _envelope(
                feed="earnings",
                provider="unusual_whales",
                payload={
                    "symbol": "AAPL",
                    "report_date": "2026-02-12",
                    "time": "amc",
                    "eps_estimate": "2.10",
                    "eps_actual": "2.20",
                    "revenue_estimate": "115000000000",
                    "revenue_actual": "116000000000",
                },
            ),
        ),
        (
            "treasury_yields",
            "treasury_yields",
            _envelope(
                feed="treasury_yields",
                provider="alphavantage",
                instrument_type="macro",
                instrument_key="macro:treasury_yield:2year",
                symbol="TREASURY_2YEAR",
                payload={
                    "date": "2026-04-27",
                    "maturity": "2year",
                    "yield_pct": 3.78,
                },
            ),
        ),
    ]


def test_data_gateway_feed_matrix_routes_to_known_silver_schemas() -> None:
    for source_feed, expected_feed, _ in _cases():
        assert resolve_feed_alias(source_feed) == expected_feed
        assert expected_feed in SILVER_SCHEMAS
        assert expected_feed in REQUIRED_NON_NULL_FIELDS


def test_all_data_gateway_feeds_normalize_with_non_null_contract_fields() -> None:
    for source_feed, expected_feed, envelope in _cases():
        normalized = normalize_envelope_for_silver(envelope)
        row = envelope_to_silver_row(normalized)

        assert normalized.feed == expected_feed
        assert normalized.is_valid_instrument_key(), source_feed
        assert row["feed"] == expected_feed

        for required_field in REQUIRED_NON_NULL_FIELDS[expected_feed]:
            assert row[required_field] is not None, f"{source_feed} missing {required_field}"


def test_live_and_backfill_paths_produce_equivalent_rows_for_shared_feeds() -> None:
    transformer = BronzeToSilverTransformer()
    shared_cases = [case for case in _cases() if case[0] in {"bars", "flow_alerts", "short_volume", "ticker_flow"}]

    for source_feed, _, envelope in shared_cases:
        live_row = envelope_to_silver_row(normalize_envelope_for_silver(envelope))
        backfill_row = transformer._envelope_to_silver_row(envelope, source_feed)  # noqa: SLF001

        assert backfill_row is not None
        assert backfill_row["feed"] == live_row["feed"]
        assert backfill_row["instrument_key"] == live_row["instrument_key"]

        for required_field in REQUIRED_NON_NULL_FIELDS[live_row["feed"]]:
            assert backfill_row[required_field] == live_row[required_field]


def test_previously_uncontracted_feeds_now_route_to_bronze_only() -> None:
    """Regression: 67 reference/metadata feeds that emitted ``silver_feed_uncontracted``
    in 13 days of production logs (3,350 warnings) must now pass the contracted gate
    AND be flagged Bronze-only.  Adding to ``CONTRACTED_RAW_FEEDS`` silences the DLQ;
    adding the canonical name to ``BRONZE_ONLY_SILVER_DATASETS`` skips Silver typing.
    """
    not_contracted: list[str] = []
    not_bronze_only: list[str] = []
    for feed in PREVIOUSLY_UNCONTRACTED_BRONZE_ONLY_FEEDS:
        if not is_contracted_feed(feed):
            not_contracted.append(feed)
        if not is_bronze_only_feed(feed):
            not_bronze_only.append(feed)
    assert not not_contracted, f"Still uncontracted (would emit silver_feed_uncontracted): {not_contracted}"
    assert not not_bronze_only, f"Not flagged as Bronze-only (would attempt Silver write): {not_bronze_only}"


def test_short_data_feeds_route_to_silver_not_bronze_only() -> None:
    """Regression: short_interest / short_volume / short_data carry a typed Silver
    schema (short_date, short_interest, days_to_cover, short_percent_float) and must
    NOT be Bronze-only. The 2026-05-20 67-feed reconciliation swept ``short_data``
    into ``BRONZE_ONLY_SILVER_DATASETS`` despite the existing schema, silently
    halting Silver ``short_data`` production (last partition 2026-05-20) while Bronze
    ``short_interest`` kept arriving through 06-09.
    """
    assert "short_data" in SILVER_SCHEMAS
    for feed in ("short_data", "short_interest", "short_volume"):
        assert resolve_feed_alias(feed) == "short_data"
        assert not is_bronze_only_feed(feed), f"{feed} must route to Silver, not Bronze-only"


def test_option_chain_snapshot_preserves_underlying_price() -> None:
    _, _, envelope = next(case for case in _cases() if case[0] == "option_chain_snapshot")

    normalized = normalize_envelope_for_silver(envelope)
    row = envelope_to_silver_row(normalized)

    assert row["underlying_price"] == 600.5
