"""Canonical Bronze->Silver ingest contracts.

This module is the single source of truth for:
- feed aliasing from Data Gateway feed names to Silver dataset names
- payload field mappings from provider payload keys to Silver column names
- feed-specific payload normalization rules
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from heber.schemas.silver import SILVER_SCHEMAS

# Data-Gateway feed aliases to canonical Silver dataset names.
FEED_ALIASES: dict[str, str] = {
    "ftds": "ftd",
    "short_interest": "short_data",
    "short_volume": "short_data",
    "historic_option_volume": "historic_option_volume",
}
FEED_ALIAS_MAP = FEED_ALIASES

DATA_GATEWAY_FEEDS: tuple[str, ...] = (
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
)

# Field mappings: payload field -> Silver schema field
FIELD_MAPPINGS: dict[str, dict[str, str]] = {
    # Core Market Data
    "bars": {
        "t": "bar_start_ts",
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
        "bs": "bid_sz",
        "ap": "ask_px",
        "as": "ask_sz",
        "bx": "bid_exchange",
        "ax": "ask_exchange",
    },
    "trades": {
        "p": "price",
        "s": "size",
        "x": "exchange",
        "i": "trade_id",
        "z": "tape",
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
        "ext_hour_sold_codes": "ext_hours",
    },
    # Sentiment
    "sector_tide": {},
    "market_tide": {
        "net_call_premium": "total_call_premium",
        "net_put_premium": "total_put_premium",
    },
    # Core Analytics
    "greek_exposure": {},
    "max_pain": {},
    "net_premium_tick": {},
    "hottest_chain": {},
    # Reference Data
    "earnings": {
        "report_date": "earnings_date",
        "date": "earnings_date",
    },
    "corporate_action": {},
    # Screeners
    "most_active": {},
    "mover": {},
    "screener_result": {},
    # Advanced Analytics
    "iv_rank": {},
    "iv_term_structure": {},
    "volatility_stats": {},
    "oi_change": {
        "date": "oi_date",
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
        "source": "source_name",
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
    # Options Deep Data
    "option_history": {
        "contract_symbol": "occ_symbol",
        "date": "history_date",
    },
    "option_chain_snapshot": {
        "timestamp": "snapshot_ts",
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
    "greek_exposure": {"gamma_exposure"},
    "iv_rank": {"iv_rank"},
    "oi_change": {"oi_date", "call_oi", "put_oi"},
    "historic_option_volume": {"hov_date", "expiry", "volume"},
    "short_data": {"short_date", "short_interest"},
    "ftd": {"ftd_date", "quantity"},
    "congress_trades": {"politician_name", "trade_type", "trade_date"},
    "insider_trades": {"insider_name", "trade_type", "trade_date"},
}
REQUIRED_NON_NULL_FIELDS = REQUIRED_FIELDS_BY_FEED


class UnmappedFeedError(ValueError):
    """Raised when a feed cannot be routed to a known Silver schema."""


_AMOUNT_RANGE_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)\s*(?:-\s*\$?\s*([\d,]+(?:\.\d+)?))?")


def resolve_silver_feed(feed: str) -> str | None:
    """Resolve incoming feed to canonical Silver dataset name."""
    canonical = FEED_ALIASES.get(feed, feed)
    if canonical in SILVER_SCHEMAS:
        return canonical
    return None


def resolve_feed_alias(feed: str) -> str:
    """Resolve a feed alias without checking schema coverage."""
    return FEED_ALIASES.get(feed, feed)


def normalize_payload_for_feed(feed: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Apply feed-specific payload normalization before column mapping."""
    normalized = dict(payload)

    if feed == "news":
        _normalize_news_payload(normalized)
    elif feed == "congress_trades":
        _normalize_congress_payload(normalized)
    elif feed == "insider_trades":
        _normalize_insider_payload(normalized)
    elif feed == "market_tide":
        _normalize_tide_payload(normalized, call_key="net_call_premium", put_key="net_put_premium")
    elif feed == "sector_tide":
        _normalize_tide_payload(normalized, call_key="net_call_premium", put_key="net_put_premium")
    elif feed == "flow_alerts":
        _normalize_flow_payload(normalized)
    elif feed == "short_data":
        if "short_interest" not in normalized and "short_volume" in normalized:
            normalized["short_interest"] = normalized.get("short_volume")
        if "short_percent_float" not in normalized and "short_ratio" in normalized:
            normalized["short_percent_float"] = normalized.get("short_ratio")

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

    relationships: list[str] = []
    if payload.get("is_director"):
        relationships.append("director")
    if payload.get("is_officer"):
        relationships.append("officer")
    if payload.get("is_ten_percent_owner"):
        relationships.append("ten_percent_owner")
    if payload.get("is_10b5_1"):
        relationships.append("10b5-1")
    if relationships and payload.get("insider_relationship") is None:
        payload["insider_relationship"] = "|".join(relationships)


def _normalize_tide_payload(payload: dict[str, Any], call_key: str, put_key: str) -> None:
    call_premium = _to_decimal_or_none(payload.get(call_key))
    put_premium = _to_decimal_or_none(payload.get(put_key))
    if payload.get("call_put_ratio") is None and call_premium is not None and put_premium not in (None, Decimal("0")):
        payload["call_put_ratio"] = float(call_premium / put_premium)


def _normalize_flow_payload(payload: dict[str, Any]) -> None:
    put_call = payload.get("put_call")
    if isinstance(put_call, str):
        lowered = put_call.lower()
        if lowered.startswith("c"):
            payload["put_call"] = "C"
        elif lowered.startswith("p"):
            payload["put_call"] = "P"


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
    except Exception:
        return None


__all__ = [
    "DATA_GATEWAY_FEEDS",
    "FEED_ALIAS_MAP",
    "FEED_ALIASES",
    "FIELD_MAPPINGS",
    "REQUIRED_FIELDS_BY_FEED",
    "REQUIRED_NON_NULL_FIELDS",
    "UnmappedFeedError",
    "normalize_payload_for_feed",
    "required_fields_for_feed",
    "resolve_feed_alias",
    "resolve_silver_feed",
]
