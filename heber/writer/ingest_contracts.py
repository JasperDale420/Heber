"""Canonical Bronze->Silver ingest contracts.

This module is the single source of truth for:
- feed aliasing from Data Gateway feed names to Silver dataset names
- payload field mappings from provider payload keys to Silver column names
- feed-specific payload normalization rules
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from heber.schemas.silver import SILVER_SCHEMAS

# Data-Gateway feed aliases to canonical Silver dataset names.
FEED_ALIASES: dict[str, str] = {
    "ftds": "ftd",
    "short_interest": "short_data",
    "short_volume": "short_data",
    "historic_option_volume": "historic_option_volume",
    "flow": "flow_alerts",
    "ticker_flow": "flow_alerts",
    "darkpool_ticker": "darkpool",
    "greeks": "greek_exposure",
    "gex": "greek_exposure",
    "option_trades": "trades",
    "option_bars": "bars",
    "option_quotes": "quotes",
    "crypto_bars": "bars",
    "crypto_trades": "trades",
    "crypto_quotes": "quotes",
    "daily_bars": "bars",
    "updated_bars": "bars",
    "institutions": "institution_holdings",
    "filings": "news",
}
FEED_ALIAS_MAP = FEED_ALIASES

CONTRACTED_RAW_FEEDS: tuple[str, ...] = (
    "bars",
    "quotes",
    "trades",
    "news",
    "flow_alerts",
    "darkpool",
    "market_tide",
    "sector_tide",
    "greek_exposure",
    "iv_rank",
    "oi_change",
    "historic_option_volume",
    "short_interest",
    "short_volume",
    "ftds",
    "congress_trades",
    "insider_trades",
    "option_trades",
    "option_bars",
    "option_quotes",
    "option_chain_snapshot",
    "crypto_bars",
    "crypto_trades",
    "crypto_quotes",
    "ticker_flow",
    "darkpool_ticker",
    "institutions",
    "earnings",
    "treasury_yields",
    "daily_bars",
    "updated_bars",
    "lulds",
    "statuses",
    "corrections",
    # Phase 3: Cerberus advanced strategies
    "iv_term_structure",
    "etf_tide",
    "auctions",
)

# Backwards-compatible name used by tests and docs for gateway feed inventory.
DATA_GATEWAY_FEEDS: tuple[str, ...] = CONTRACTED_RAW_FEEDS

# Feeds to persist in Bronze but skip Silver by default until an active Gold use-case exists.
# Currently empty — all contracted feeds get Silver normalization. Add canonical Silver
# dataset names here to gate them out of Silver writes.
BRONZE_ONLY_SILVER_DATASETS: frozenset[str] = frozenset({"news", "institution_holdings"})

# Required and allowed payload keys for schema validation.
# Required keys trigger a warning when missing; unexpected keys (outside
# the allowed set) also trigger a warning.
PAYLOAD_REQUIRED_FIELDS: dict[str, set[str]] = {
    "flow_alerts": {
        "timestamp",
        "symbol",
        "strike",
        "expiry",
        "put_call",
        "premium",
        "volume",
    },
    "market_tide": {
        "timestamp",
        "date",
        "net_call_premium",
        "net_put_premium",
        "net_volume",
        "sentiment",
    },
}

PAYLOAD_ALLOWED_FIELDS: dict[str, set[str]] = {
    "flow_alerts": {
        "id",
        "alert_id",
        "event_id",
        "timestamp",
        "symbol",
        "strike",
        "expiry",
        "put_call",
        "premium",
        "volume",
        "open_interest",
        "side",
        "is_sweep",
        "is_unusual",
        "option_chain",
        "price",
        "underlying_price",
        "alert_rule",
        "alert_type",
        "aggressor",
        "tags",
        "provider",
        "trade_count",
        "volume_oi_ratio",
        "total_ask_side_prem",
        "total_bid_side_prem",
        "has_floor",
        "has_multileg",
        "has_singleleg",
        "all_opening_trades",
        "total_size",
        "expiry_count",
        "sentiment",
        "gex",
        "vex",
        "max_pain_strike",
        "max_pain_distance_pct",
    },
    "market_tide": {
        "timestamp",
        "date",
        "net_call_premium",
        "net_put_premium",
        "net_volume",
        "sentiment",
        "call_put_ratio",
        "provider",
    },
    "oi_change": {
        "timestamp",
        "symbol",
        "date",
        "call_oi",
        "put_oi",
        "call_oi_change",
        "put_oi_change",
        "avg_price",
        "prev_oi",
        "last_oi",
        "option_symbol",
        "volume",
        "trades",
        "provider",
        # New fields
        "last_ask",
        "last_bid",
        "last_fill",
        "percentage_of_total",
        "prev_ask_volume",
        "prev_bid_volume",
        "prev_multi_leg_volume",
        "prev_total_premium",
        "last_date",
        "prev_mid_volume",
        "prev_neutral_volume",
        "prev_stock_multi_leg_volume",
    },
    "iv_rank": {
        "timestamp",
        "symbol",
        "iv_rank",
        "iv_percentile",
        "current_iv",
        "one_year_high",
        "one_year_low",
        "provider",
        # New fields
        "close",
        "date",
    },
    "insider_trades": {
        "timestamp",
        "symbol",
        "id",
        "transaction_id",
        "filing_id",
        "owner_name",
        "insider_name",
        "officer_title",
        "insider_title",
        "insider_relationship",
        "transaction_code",
        "transaction_type",
        "trade_type",
        "transaction_date",
        "trade_date",
        "amount",
        "shares",
        "price",
        "value",
        "shares_owned",
        "shares_owned_after",
        "filing_date",
        "is_10b5_1",
        "is_director",
        "is_officer",
        "is_ten_percent_owner",
        "provider",
        # New fields
        "stock_price",
    },
    "earnings": {
        "timestamp",
        "symbol",
        "earnings_date",
        "report_date",
        "date",
        "time",
        "eps_estimate",
        "eps_actual",
        "revenue_estimate",
        "revenue_actual",
        "expected_move",
        "expected_move_perc",
        "pre_earnings_close",
        "prior_close",
        "has_options",
        "market_cap",
        "sector",
        "provider",
        # New fields
        "is_s_p_500",
        "post_earnings_close",
        "reaction",
    },
    "option_chain_snapshot": {
        "timestamp",
        "symbol",
        "snapshot_ts",
        "underlying",
        "underlying_price",
        "expiry",
        "expiration",
        "chain_json",
        "total_call_volume",
        "total_put_volume",
        "total_call_oi",
        "total_put_oi",
        "atm_iv",
        "occ_symbol",
        "contract_symbol",
        "strike",
        "put_call",
        "option_type",
        "bid",
        "ask",
        "last",
        "volume",
        "open_interest",
        "delta",
        "gamma",
        "theta",
        "vega",
        "rho",
        "iv",
        "provider",
        # New fields
        "bid_size",
        "ask_size",
        "last_trade_size",
    },
    "most_active": {
        "timestamp",
        "symbol",
        "volume",
        "trade_count",
        "provider",
        # New fields
        "vwap",
        "price",
        "change",
        "change_percent",
    },
    "mover": {
        "timestamp",
        "symbol",
        "price",
        "change",
        "percent_change",
        "direction",
        "provider",
        # New fields
        "volume",
    },
    "lulds": {
        "timestamp",
        "symbol",
        "upper_limit",
        "lower_limit",
        "indicator",
        "provider",
    },
    "statuses": {
        "timestamp",
        "symbol",
        "status_code",
        "status_message",
        "reason_code",
        "reason_message",
        "tape",
        "provider",
    },
    "corrections": {
        "timestamp",
        "symbol",
        "exchange",
        "original_trade_id",
        "original_price",
        "original_size",
        "original_conditions",
        "corrected_trade_id",
        "corrected_price",
        "corrected_size",
        "corrected_conditions",
        "tape",
        "provider",
    },
}

DLQ_REASON_UNCONTRACTED = "uncontracted_feed"

LEGACY_MAPPABLE_FEEDS: tuple[str, ...] = (
    "borrow_cost",
    "flow",
    "greeks",
    "gex",
    "stock_fundamentals",
    "option_contract",
    "option_trades",
    "screener_result",
    "institution_holdings",
    "politician_trades",
    "etf_holding",
    "etf_flow",
    "etf_metadata",
    "short_data",
    "ftd",
    "volatility_stats",
    "seasonality",
    "max_pain",
    "forex",
    "insider_flow",
    "institution_activity",
    "etf_sector_weights",
    "option_chain_snapshot",
    "option_history",
    "volume_profile",
    "group_flow",
    "net_premium_tick",
    "hottest_chain",
    "iv_term_structure",
    "mover",
    "most_active",
    "corporate_action",
    "orderbook",
    "lulds",
    "statuses",
    "corrections",
)

# Field mappings: payload field -> Silver schema field
FIELD_MAPPINGS: dict[str, dict[str, str]] = {
    # Core Market Data
    "bars": {
        "t": "bar_start_ts",
        "timestamp": "bar_start_ts",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "n": "trade_count",
        "vw": "vwap",
    },
    "quotes": {
        "bp": "bid_px",
        "bid_price": "bid_px",
        "bs": "bid_sz",
        "bid_size": "bid_sz",
        "ap": "ask_px",
        "ask_price": "ask_px",
        "as": "ask_sz",
        "ask_size": "ask_sz",
        "bx": "bid_exchange",
        "ax": "ask_exchange",
        "c": "conditions",
        "conditions": "conditions",
        "z": "tape",
        "tape": "tape",
    },
    "trades": {
        # Short Alpaca WebSocket keys
        "p": "price",
        "s": "size",
        "x": "exchange",
        "i": "trade_id",
        "z": "tape",
        "c": "conditions",
        "tks": "taker_side",
        # Full-name keys from Alpaca REST / aggregate payloads
        "price": "price",
        "size": "size",
        "exchange": "exchange",
        "trade_id": "trade_id",
        "tape": "tape",
        "conditions": "conditions",
        "taker_side": "taker_side",
        # Code-table disambiguation (SIP vs OPRA)
        "exchange_type": "exchange_type",
        "condition_type": "condition_type",
    },
    # Options Flow
    "flow_alerts": {
        "price": "contract_px",
        "underlying_price": "spot_px",
        "option_chain": "occ_symbol",
        "symbol": "underlying",
        "ticker": "underlying",
        "alert_rule": "alert_type",
    },
    "darkpool": {
        "symbol": "underlying",
        "ticker": "underlying",
        "exchange": "venue",
        "market_center": "venue",
        "tracking_id": "print_id",
        "ask": "nbbo_ask",
        "bid": "nbbo_bid",
        "nbbo_bid_quantity": "nbbo_bid_size",
        "nbbo_ask_quantity": "nbbo_ask_size",
        "ext_hour_sold_codes": "ext_hours",
    },
    # Sentiment
    "sector_tide": {},
    "market_tide": {
        "net_call_premium": "total_call_premium",
        "net_put_premium": "total_put_premium",
        "date": "tide_date",
    },
    # Core Analytics
    "greek_exposure": {},
    "max_pain": {},
    "net_premium_tick": {},
    "hottest_chain": {
        "option_type": "put_call",
    },
    # Reference Data
    "earnings": {
        "report_date": "earnings_date",
        "date": "earnings_date",
        "expected_move_perc": "expected_move_pct",
        "pre_earnings_close": "prior_close",
    },
    "corporate_action": {},
    # Screeners
    "most_active": {},
    "mover": {},
    "screener_result": {},
    # Advanced Analytics
    "iv_rank": {},
    "iv_term_structure": {
        "days_to_expiry": "dte",
    },
    "volatility_stats": {},
    "oi_change": {
        "date": "oi_date",
        "last_oi": "prev_oi",
    },
    # ETF Feeds
    "etf_holding": {},
    "etf_flow": {
        "date": "flow_date",
    },
    # Short / FTD
    "short_data": {
        "date": "short_date",
        "short_volume": "short_interest",
        "short_ratio": "short_percent_float",
    },
    "ftd": {
        "date": "ftd_date",
    },
    "historic_option_volume": {
        "date": "hov_date",
    },
    # Seasonality
    "seasonality": {},
    # Reference Data (SCD)
    "option_contract": {
        "contract_symbol": "occ_symbol",
        "expiration": "expiry",
        "option_type": "put_call",
    },
    "news": {
        "article_id": "news_id",
        "id": "news_id",
        "published_at": "ts_published",
        "created_at": "ts_published",
        "updated_at": "ts_updated",
        "source": "source_name",
        "content": "body",
    },
    "orderbook": {
        "bids": "bids_json",
        "asks": "asks_json",
    },
    # Alternative Data
    "congress_trades": {
        "id": "trade_id",
        "transaction_id": "trade_id",
        "name": "politician_name",
        "reporter": "politician_name",
        "party": "politician_party",
        "state": "politician_state",
        "member_type": "politician_chamber",
        "chamber": "politician_chamber",
        "txn_type": "trade_type",
        "transaction_type": "trade_type",
        "transaction_date": "trade_date",
        "date": "trade_date",
        "filed_at_date": "disclosure_date",
        "description": "asset_description",
    },
    "insider_trades": {
        "id": "filing_id",
        "transaction_id": "filing_id",
        "owner_name": "insider_name",
        "officer_title": "insider_title",
        "transaction_code": "trade_type",
        "transaction_type": "trade_type",
        "transaction_date": "trade_date",
        "amount": "shares",
        "is_10b5_1": "insider_relationship",
        "shares_owned": "shares_owned_after",
    },
    "insider_flow": {},
    "institution_holdings": {
        "institution_name": "institution_name",
        "institution_id": "institution_cik",
        "market_value": "value",
        "percent_portfolio": "portfolio_pct",
        "report_date": "quarter_end",
    },
    "institution_activity": {
        "institution": "institution_name",
        "cik": "institution_cik",
    },
    "politician_trades": {
        "transaction_type": "trade_type",
        "transaction_date": "trade_date",
        "amount_range": "amount_min",
        "description": "asset_description",
        "transaction_id": "trade_id",
    },
    # Market Analytics
    "analyst_ratings": {
        "firm": "analyst_firm",
        "analyst": "analyst_name",
        "rating_current": "rating",
        "date": "rating_date",
        "id": "rating_id",
    },
    "stock_fundamentals": {
        "name": "company_name",
        "week_52_high": "high_52w",
        "week_52_low": "low_52w",
        "date": "snapshot_date",
    },
    "economic_events": {
        "name": "event_name",
        "type": "event_type",
        "date": "event_date",
        "time": "event_time",
    },
    "market_indicators": {
        "name": "indicator_name",
        "date": "indicator_date",
        "time": "indicator_time",
    },
    "treasury_yields": {
        "date": "date",
        "maturity": "maturity",
        "yield_pct": "yield_pct",
    },
    # Options Deep Data
    "option_history": {
        "contract_symbol": "occ_symbol",
        "date": "history_date",
    },
    "option_chain_snapshot": {
        "timestamp": "snapshot_ts",
        "contract_symbol": "occ_symbol",
        "expiration": "expiry",
        "option_type": "put_call",
    },
    "volume_profile": {
        "contract_symbol": "occ_symbol",
        "date": "profile_date",
    },
    "group_flow": {
        "flow_group": "group_name",
        "type": "group_type",
        "date": "flow_date",
    },
    # ETF Deep Data
    "etf_metadata": {
        "name": "fund_name",
        "date": "snapshot_date",
    },
    "etf_sector_weights": {
        "date": "weight_date",
        "type": "weight_type",
        "name": "weight_name",
        "weight": "weight_pct",
    },
    # Forex
    "forex": {
        "currency_pair": "pair",
    },
    # Borrow Cost
    "borrow_cost": {
        "available_shares": "short_shares_available",
    },
    # Option Trades (dedicated schema)
    "option_trades": {
        "option_chain_id": "option_symbol",
        "flow_alert_id": "trade_id",
    },
    # Real-time market status feeds
    "lulds": {},
    "statuses": {},
    "corrections": {},
}

# ML-facing required fields for emitted Data-Gateway feeds.
REQUIRED_FIELDS_BY_FEED: dict[str, set[str]] = {
    "bars": {"open", "high", "low", "close", "volume"},
    "quotes": {"bid_px", "ask_px"},
    "trades": {"price", "size"},
    "news": {"news_id", "headline", "ts_published"},
    "flow_alerts": {"occ_symbol", "strike", "put_call", "premium", "volume"},
    "darkpool": {"underlying", "price", "size"},
    "market_tide": {"total_call_premium", "total_put_premium"},
    "sector_tide": {"sector", "net_call_premium", "net_put_premium"},
    "greek_exposure": {"call_gamma"},
    "iv_rank": {"iv_rank"},
    "oi_change": {"oi_date", "call_oi", "put_oi"},
    "historic_option_volume": {"hov_date", "volume"},
    "option_chain_snapshot": {"snapshot_ts", "underlying", "chain_json"},
    "short_data": {"short_date", "short_interest"},
    "ftd": {"ftd_date", "quantity"},
    "congress_trades": {"politician_name", "trade_type", "trade_date"},
    "insider_trades": {"insider_name", "trade_type", "trade_date"},
    "earnings": {"earnings_date"},
    "institution_holdings": {"institution_name", "value", "quarter_end"},
    "treasury_yields": {"date", "maturity", "yield_pct"},
    "lulds": {"upper_limit", "lower_limit"},
    "statuses": {"status_code"},
    "corrections": {"original_trade_id", "original_price", "original_size"},
}
REQUIRED_NON_NULL_FIELDS = REQUIRED_FIELDS_BY_FEED


class UnmappedFeedError(ValueError):
    """Raised when a feed cannot be routed to a known Silver schema."""


_NUM_PATTERN = r"[\d,]+(?:\.\d+)?"
_AMOUNT_RANGE_RE = re.compile(rf"\$?\s*({_NUM_PATTERN})\s*(?:-\s*\$?\s*({_NUM_PATTERN}))?")


def resolve_silver_feed(feed: str) -> str | None:
    """Resolve incoming feed to canonical Silver dataset name."""
    canonical = FEED_ALIASES.get(feed, feed)
    if canonical in SILVER_SCHEMAS:
        return canonical
    return None


def resolve_feed_alias(feed: str) -> str:
    """Resolve a feed alias without checking schema coverage."""
    return FEED_ALIASES.get(feed, feed)


def is_contracted_feed(feed: str) -> bool:
    """Return True when raw feed is explicitly allowed for Silver routing."""
    return feed in CONTRACTED_RAW_FEEDS


def is_bronze_only_feed(feed: str) -> bool:
    """Return True when feed should be stored in Bronze but skipped for Silver writes."""
    canonical = resolve_feed_alias(feed)
    return canonical in BRONZE_ONLY_SILVER_DATASETS


def normalize_payload_for_feed(
    feed: str, payload: dict[str, Any], *, ts_event: datetime | None = None
) -> dict[str, Any]:
    """Apply feed-specific payload normalization before column mapping.

    Parameters
    ----------
    feed:
        Canonical Silver feed name.
    payload:
        Raw provider payload dict.
    ts_event:
        Event timestamp from the envelope.  Used to derive missing date
        fields when the upstream provider sends empty strings (common for
        UnusualWhales per-symbol aggregate endpoints).
    """
    normalized = dict(payload)

    if feed == "news":
        _normalize_news_payload(normalized)
    elif feed == "congress_trades":
        _normalize_congress_payload(normalized)
    elif feed == "insider_trades":
        _normalize_insider_payload(normalized)
    elif feed == "market_tide" or feed == "sector_tide":
        _normalize_tide_payload(normalized, call_key="net_call_premium", put_key="net_put_premium")
    elif feed == "flow_alerts":
        _normalize_flow_payload(normalized)
    elif feed == "short_data":
        _normalize_short_data_payload(normalized, ts_event=ts_event)
    elif feed == "oi_change":
        _normalize_oi_change_payload(normalized, ts_event=ts_event)
    elif feed == "historic_option_volume":
        _normalize_hov_payload(normalized, ts_event=ts_event)

    return normalized


def required_fields_for_feed(feed: str) -> set[str]:
    """Return required normalized fields for a feed."""
    return REQUIRED_FIELDS_BY_FEED.get(feed, set())


def _normalize_news_payload(payload: dict[str, Any]) -> None:
    if "article_id" not in payload and payload.get("id") is not None:
        payload["article_id"] = payload.get("id")
    if "published_at" not in payload:
        published = payload.get("created_at") or payload.get("updated_at")
        if published is not None:
            payload["published_at"] = published
    if "body" not in payload and payload.get("content") is not None:
        payload["body"] = payload.get("content")
    source = payload.get("source")
    if isinstance(source, dict):
        payload["source"] = source.get("name") or source.get("id")


def _normalize_congress_payload(payload: dict[str, Any]) -> None:
    if payload.get("politician_name") is None:
        payload["politician_name"] = payload.get("name") or payload.get("reporter") or payload.get("politician")
    if payload.get("trade_type") is None:
        payload["trade_type"] = payload.get("txn_type") or payload.get("transaction_type")
    if payload.get("trade_date") is None:
        payload["trade_date"] = payload.get("transaction_date") or payload.get("date")
    if payload.get("disclosure_date") is None:
        payload["disclosure_date"] = payload.get("filed_at_date") or payload.get("filing_date")
    if payload.get("trade_id") is None:
        payload["trade_id"] = payload.get("transaction_id") or payload.get("id")
    amount_range = payload.get("amounts") or payload.get("amount_range") or payload.get("amount")
    min_max = _parse_amount_range(amount_range)
    if min_max is not None:
        payload["amount_min"], payload["amount_max"] = min_max


def _normalize_insider_payload(payload: dict[str, Any]) -> None:
    if payload.get("insider_name") is None:
        payload["insider_name"] = payload.get("owner_name") or payload.get("insider")
    if payload.get("insider_title") is None:
        payload["insider_title"] = payload.get("officer_title")
    if payload.get("trade_type") is None:
        payload["trade_type"] = payload.get("transaction_code") or payload.get("transaction_type")
    if payload.get("trade_date") is None:
        payload["trade_date"] = payload.get("transaction_date")
    if payload.get("filing_id") is None:
        payload["filing_id"] = payload.get("id") or payload.get("transaction_id")
    if payload.get("shares") is None:
        payload["shares"] = payload.get("amount") or payload.get("size")

    relationship = _build_insider_relationships(payload)
    if relationship and payload.get("insider_relationship") is None:
        payload["insider_relationship"] = relationship


def _build_insider_relationships(payload: dict[str, Any]) -> str | None:
    """Aggregate boolean insider role flags into a pipe-delimited relationship string."""
    flag_map = {
        "is_director": "director",
        "is_officer": "officer",
        "is_ten_percent_owner": "ten_percent_owner",
        "is_10b5_1": "10b5-1",
    }
    parts = [label for flag, label in flag_map.items() if payload.get(flag)]
    return "|".join(parts) if parts else None


def _normalize_tide_payload(payload: dict[str, Any], call_key: str, put_key: str) -> None:
    call_premium = _to_decimal_or_none(payload.get(call_key))
    put_premium = _to_decimal_or_none(payload.get(put_key))
    if payload.get("call_put_ratio") is None and call_premium is not None and put_premium not in (None, Decimal("0")):
        payload["call_put_ratio"] = float(call_premium / put_premium)  # type: ignore[operator]


def _normalize_flow_payload(payload: dict[str, Any]) -> None:
    put_call = payload.get("put_call")
    if isinstance(put_call, str):
        lowered = put_call.lower()
        if lowered.startswith("c"):
            payload["put_call"] = "C"
        elif lowered.startswith("p"):
            payload["put_call"] = "P"


def _normalize_short_data_payload(payload: dict[str, Any], *, ts_event: datetime | None = None) -> None:
    """Normalize short_data / short_interest payload.

    UW sends ``short_volume`` and ``short_ratio`` as the provider field names;
    remap to the canonical Silver names.  When the ``date`` field is empty
    (common for UW per-symbol endpoints), derive it from ``ts_event``.
    """
    if "short_interest" not in payload and "short_volume" in payload:
        payload["short_interest"] = payload.get("short_volume")
    if "short_percent_float" not in payload and "short_ratio" in payload:
        payload["short_percent_float"] = payload.get("short_ratio")
    _backfill_empty_date(payload, ts_event=ts_event)


def _normalize_oi_change_payload(payload: dict[str, Any], *, ts_event: datetime | None = None) -> None:
    """Normalize oi_change payload.

    When the ``date`` field is empty (common for UW aggregate endpoints),
    derive it from ``ts_event`` so that the ``oi_date`` Silver field is populated.
    """
    _backfill_empty_date(payload, ts_event=ts_event)


def _normalize_hov_payload(payload: dict[str, Any], *, ts_event: datetime | None = None) -> None:
    """Normalize historic_option_volume payload.

    When the ``date`` field is empty, derive it from ``ts_event`` so the
    ``hov_date`` Silver field is populated.
    """
    _backfill_empty_date(payload, ts_event=ts_event)


def _backfill_empty_date(payload: dict[str, Any], *, ts_event: datetime | None = None) -> None:
    """Set ``payload["date"]`` from *ts_event* when the field is missing or blank.

    Many UnusualWhales per-symbol aggregate endpoints omit the ``date`` field
    (sending ``""``).  Deriving the date from the event timestamp keeps the
    Silver pipeline from rejecting otherwise valid rows.
    """
    date_val = payload.get("date")
    if date_val is not None and str(date_val).strip():
        return  # Already has a real date — nothing to do
    if ts_event is not None:
        payload["date"] = ts_event.strftime("%Y-%m-%d")


def _parse_amount_range(raw: Any) -> tuple[float, float] | None:
    if raw is None:
        return None
    if isinstance(raw, int | float):
        value = float(raw)
        return value, value
    match = _AMOUNT_RANGE_RE.search(str(raw))
    if not match:
        return None
    low_raw = match.group(1)
    high_raw = match.group(2)
    low = float(low_raw.replace(",", ""))
    high = float(high_raw.replace(",", "")) if high_raw else low
    return low, high


def _to_decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return None


__all__ = [
    "BRONZE_ONLY_SILVER_DATASETS",
    "CONTRACTED_RAW_FEEDS",
    "DATA_GATEWAY_FEEDS",
    "DLQ_REASON_UNCONTRACTED",
    "FEED_ALIAS_MAP",
    "FEED_ALIASES",
    "FIELD_MAPPINGS",
    "LEGACY_MAPPABLE_FEEDS",
    "REQUIRED_FIELDS_BY_FEED",
    "REQUIRED_NON_NULL_FIELDS",
    "is_bronze_only_feed",
    "is_contracted_feed",
    "UnmappedFeedError",
    "normalize_payload_for_feed",
    "required_fields_for_feed",
    "resolve_feed_alias",
    "resolve_silver_feed",
]
