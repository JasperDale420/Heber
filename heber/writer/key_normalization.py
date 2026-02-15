"""Instrument-key synthesis and feed-level envelope normalization.

Handles symbol normalization and instrument key synthesis for all
instrument types (equity, crypto, option, forex) across market data
feeds (bars, quotes, trades) and alternative data feeds.
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import structlog

from heber.models.envelope import EventEnvelope
from heber.writer.ingest_contracts import normalize_payload_for_feed, resolve_feed_alias, resolve_silver_feed

logger = structlog.get_logger(__name__)

OCC_PATTERN = re.compile(r"([A-Z]{1,6}\d{6}[CP]\d{8})")
SYMBOL_PATTERN = re.compile(r"^[A-Z]{1,5}$")
EQUITY_EXTENDED_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+(?:[.-][A-Z0-9]+)*$")
UNDERLYING_PATTERN = re.compile(r"^[A-Z]{1,6}$")
CRYPTO_SYMBOL_PATTERN = re.compile(r"^[A-Z]{2,10}-[A-Z]{2,10}$")

# Feeds that represent core market data and require instrument-type-aware normalization.
MARKET_DATA_FEEDS: frozenset[str] = frozenset({"bars", "quotes", "trades"})

SECTOR_ETF_MAP: dict[str, str] = {
    "TECHNOLOGY": "XLK",
    "FINANCIALS": "XLF",
    "CONSUMER DISCRETIONARY": "XLY",
    "COMMUNICATION SERVICES": "XLC",
    "HEALTH CARE": "XLV",
    "INDUSTRIALS": "XLI",
    "CONSUMER STAPLES": "XLP",
    "ENERGY": "XLE",
    "MATERIALS": "XLB",
    "REAL ESTATE": "XLRE",
    "UTILITIES": "XLU",
}


def normalize_envelope_for_silver(envelope: EventEnvelope) -> EventEnvelope:
    """Normalize feed alias, payload, symbol, and instrument key for Silver writes."""
    canonical_feed = resolve_feed_alias(envelope.feed)
    payload = normalize_payload_for_feed(canonical_feed, envelope.payload)

    symbol = _normalize_symbol(envelope.symbol)
    instrument_type = envelope.instrument_type.lower().strip()
    instrument_key = envelope.instrument_key

    if canonical_feed == "flow_alerts":
        occ_symbol = _extract_occ_symbol(payload) or _extract_occ_from_key(envelope.instrument_key)
        if occ_symbol is None:
            occ_symbol = _build_occ_from_payload(payload)
        if occ_symbol is not None:
            payload["option_chain"] = occ_symbol
            if symbol is None:
                symbol = _extract_underlying_from_occ(occ_symbol)
            if symbol is not None:
                payload["symbol"] = symbol
            instrument_type = "option"
            instrument_key = f"option:OCC:{occ_symbol}"
        else:
            # OCC could not be synthesized — fall back to equity key using payload symbol
            if symbol is None:
                symbol = _normalize_symbol(payload.get("symbol")) or _normalize_symbol(payload.get("ticker"))
            if symbol is not None:
                instrument_type = "equity"
                instrument_key = f"equity:{symbol}"
    elif canonical_feed == "market_tide":
        symbol = "SPY"
        instrument_type = "equity"
        instrument_key = "equity:SPY"
    elif canonical_feed == "sector_tide":
        symbol = _sector_to_etf(payload.get("sector"))
        instrument_type = "equity"
        instrument_key = f"equity:{symbol}"
    elif canonical_feed in {"congress_trades", "insider_trades"}:
        if symbol is None:
            symbol = _normalize_symbol(payload.get("ticker")) or _normalize_symbol(payload.get("symbol"))
        if symbol is not None:
            instrument_type = "equity"
            instrument_key = f"equity:{symbol}"
    elif canonical_feed == "news":
        if symbol is None:
            symbol = _normalize_symbol(payload.get("symbol"))
        if symbol is None:
            symbols = payload.get("symbols")
            if isinstance(symbols, list) and symbols:
                symbol = _normalize_symbol(symbols[0])
        if symbol is not None:
            payload["symbol"] = symbol
            instrument_type = "equity"
            instrument_key = f"equity:{symbol}"
    elif canonical_feed in MARKET_DATA_FEEDS:
        symbol, instrument_type, instrument_key = _normalize_market_data_envelope(
            symbol=envelope.symbol,
            instrument_type=instrument_type,
            instrument_key=instrument_key,
            payload=payload,
        )
    elif symbol is None:
        symbol = _normalize_symbol(payload.get("symbol")) or _normalize_symbol(payload.get("ticker"))
        if symbol is not None and instrument_type == "equity":
            instrument_key = f"equity:{symbol}"

    # Validate market data payload and add quality flags for suspect data.
    quality_flags = list(envelope.quality_flags)
    if canonical_feed in MARKET_DATA_FEEDS:
        suspect_flags = validate_market_data_payload(canonical_feed, payload)
        if suspect_flags:
            quality_flags.extend(suspect_flags)

    updates: dict[str, Any] = {
        "feed": canonical_feed,
        "payload": payload,
        "instrument_type": instrument_type or envelope.instrument_type,
        "instrument_key": instrument_key,
        "quality_flags": quality_flags,
    }
    if symbol is not None:
        updates["symbol"] = symbol

    normalized = envelope.model_copy(update=updates)

    # For mapped feeds, strict key validation remains mandatory after synthesis.
    if resolve_silver_feed(envelope.feed) is not None and not normalized.is_valid_instrument_key():
        raise ValueError(
            f"Invalid instrument_key format for instrument_type "
            f"{normalized.instrument_type}: {normalized.instrument_key}"
        )

    return normalized


def _normalize_market_data_envelope(
    *,
    symbol: str | None,
    instrument_type: str,
    instrument_key: str,
    payload: dict[str, Any],
) -> tuple[str | None, str, str]:
    """Normalize symbol and instrument key for core market data feeds.

    Routes normalization by instrument type so that equity, crypto, and option
    data all get properly validated instrument keys.

    Returns:
        (symbol, instrument_type, instrument_key) tuple
    """
    if instrument_type == "crypto":
        crypto_sym = _normalize_crypto_symbol(symbol or payload.get("symbol") or payload.get("S"))
        if crypto_sym is not None:
            return crypto_sym, "crypto", f"crypto:{crypto_sym}"
        # Symbol couldn't be normalized — preserve whatever we have
        logger.warning(
            "crypto_symbol_normalization_failed",
            raw_symbol=symbol or payload.get("symbol"),
            instrument_key=instrument_key,
        )
        return symbol, "crypto", instrument_key

    if instrument_type == "option":
        # Option trades should preserve the instrument key from Data-Gateway.
        # Attempt OCC key extraction if not already present.
        if instrument_key and instrument_key.startswith("option:OCC:"):
            return symbol, "option", instrument_key
        occ = _extract_occ_symbol(payload) or _extract_occ_from_key(instrument_key)
        if occ is not None:
            return (symbol or _extract_underlying_from_occ(occ)), "option", f"option:OCC:{occ}"
        return symbol, "option", instrument_key

    # Equity (default): validate and normalize symbol
    norm_sym = _normalize_equity_symbol(symbol or payload.get("symbol") or payload.get("S") or payload.get("ticker"))
    if norm_sym is not None:
        return norm_sym, "equity", f"equity:{norm_sym}"
    # Fallback: preserve whatever Data-Gateway set
    return symbol, instrument_type, instrument_key


def _normalize_crypto_symbol(value: Any) -> str | None:
    """Normalize a crypto symbol to canonical BASE-QUOTE format.

    Handles common crypto symbol formats from providers:
      - BTC/USD  → BTC-USD
      - BTC-USD  → BTC-USD (already canonical)
      - BTCUSD   → BTC-USD (assumes 3-char or 4-char base)
      - BTC_USD  → BTC-USD
      - btc/usd  → BTC-USD

    Returns:
        Normalized symbol like 'BTC-USD', or None if invalid.
    """
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None

    # Replace separators to canonical dash
    normalized = text.replace("/", "-").replace("_", "-")

    # Already in BASE-QUOTE format?
    if CRYPTO_SYMBOL_PATTERN.fullmatch(normalized):
        return normalized

    # Try splitting concatenated symbols like BTCUSD, ETHUSDT.
    # Use known quote currencies to disambiguate the split point.
    if "-" not in normalized and len(normalized) >= 5:
        _KNOWN_QUOTES = frozenset(
            {
                "USD",
                "USDT",
                "USDC",
                "EUR",
                "GBP",
                "BTC",
                "ETH",
                "BUSD",
                "DAI",
                "JPY",
                "AUD",
                "CAD",
                "CHF",
            }
        )
        for quote_len in (4, 3):
            if len(normalized) > quote_len:
                base = normalized[:-quote_len]
                quote = normalized[-quote_len:]
                if quote in _KNOWN_QUOTES:
                    candidate = f"{base}-{quote}"
                    if CRYPTO_SYMBOL_PATTERN.fullmatch(candidate):
                        return candidate

    return None


def _normalize_equity_symbol(value: Any) -> str | None:
    """Normalize an equity symbol, supporting extended tickers like BRK.B."""
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        return None
    if EQUITY_EXTENDED_SYMBOL_PATTERN.fullmatch(text):
        return text
    return None


def validate_market_data_payload(feed: str, payload: dict[str, Any]) -> list[str]:
    """Validate market data payload values for backtesting accuracy.

    Flags suspect data with quality flags rather than rejecting. This preserves
    data completeness for backtesting while marking potentially unreliable records.

    Returns:
        List of quality flag strings (empty if data looks clean).
    """
    flags: list[str] = []

    if feed == "bars":
        flags.extend(_validate_bar_payload(payload))
    elif feed == "quotes":
        flags.extend(_validate_quote_payload(payload))
    elif feed == "trades":
        flags.extend(_validate_trade_payload(payload))

    return flags


def _validate_bar_payload(payload: dict[str, Any]) -> list[str]:
    """Validate OHLCV bar data for statistical integrity."""
    flags: list[str] = []
    o = _safe_float(payload.get("open") or payload.get("o"))
    h = _safe_float(payload.get("high") or payload.get("h"))
    low = _safe_float(payload.get("low") or payload.get("l"))
    c = _safe_float(payload.get("close") or payload.get("c"))
    v = _safe_float(payload.get("volume") or payload.get("v"))

    if any(x is not None and x < 0 for x in (o, h, low, c)):
        flags.append("negative_price")
    if v is not None and v < 0:
        flags.append("negative_volume")
    if h is not None and low is not None and h < low:
        flags.append("high_below_low")
    return flags


def _validate_quote_payload(payload: dict[str, Any]) -> list[str]:
    """Validate quote data for inverted or invalid spreads."""
    flags: list[str] = []
    bid = _safe_float(payload.get("bid_px") or payload.get("bp"))
    ask = _safe_float(payload.get("ask_px") or payload.get("ap"))

    if bid is not None and ask is not None and bid > ask:
        flags.append("inverted_spread")
    if any(x is not None and x < 0 for x in (bid, ask)):
        flags.append("negative_price")
    return flags


def _validate_trade_payload(payload: dict[str, Any]) -> list[str]:
    """Validate trade data for zero/negative prices or sizes."""
    flags: list[str] = []
    price = _safe_float(payload.get("price") or payload.get("p"))
    size = _safe_float(payload.get("size") or payload.get("s"))

    if price is not None and price <= 0:
        flags.append("non_positive_price")
    if size is not None and size < 0:
        flags.append("negative_size")
    return flags


def _safe_float(value: Any) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _extract_occ_symbol(payload: dict[str, Any]) -> str | None:
    for key in ("option_chain", "contract_symbol", "contract", "occ_symbol"):
        raw = payload.get(key)
        if raw is None:
            continue
        text = str(raw).strip().upper().replace(" ", "")
        text = text.removeprefix("OCC:")
        if text.startswith("OPTION:OCC:"):
            text = text.removeprefix("OPTION:OCC:")
        match = OCC_PATTERN.search(text)
        if match:
            return match.group(1)
    return None


def _extract_occ_from_key(instrument_key: str) -> str | None:
    key = instrument_key.strip().upper()
    if key.startswith("OPTION:OCC:"):
        candidate = key.removeprefix("OPTION:OCC:")
        match = OCC_PATTERN.fullmatch(candidate)
        if match:
            return match.group(1)
    return None


def _build_occ_from_payload(payload: dict[str, Any]) -> str | None:
    raw_symbol = payload.get("symbol") or payload.get("ticker") or payload.get("underlying")
    underlying = _normalize_underlying(raw_symbol)
    expiry = _coerce_date(payload.get("expiry") or payload.get("expiration") or payload.get("date"))
    put_call = _coerce_put_call(payload.get("put_call") or payload.get("type") or payload.get("option_type"))
    strike = _coerce_strike(payload.get("strike"))

    if underlying is None or expiry is None or put_call is None or strike is None:
        return None

    return f"{underlying}{expiry.strftime('%y%m%d')}{put_call}{strike:08d}"


def _normalize_symbol(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text if SYMBOL_PATTERN.fullmatch(text) else None


def _normalize_underlying(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text if UNDERLYING_PATTERN.fullmatch(text) else None


def _extract_underlying_from_occ(occ_symbol: str) -> str | None:
    match = re.match(r"^([A-Z]{1,6})\d{6}[CP]\d{8}$", occ_symbol)
    if not match:
        return None
    underlying = match.group(1)
    if SYMBOL_PATTERN.fullmatch(underlying):
        return underlying
    return None


def _coerce_date(value: Any) -> date | None:
    """Coerce a value to a date. Delegates to normalizer._coerce_date."""
    from heber.writer.normalizer import _coerce_date as _normalizer_coerce_date

    return _normalizer_coerce_date(value)


def _coerce_put_call(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text.startswith("C"):
        return "C"
    if text.startswith("P"):
        return "P"
    return None


def _coerce_strike(value: Any) -> int | None:
    if value is None:
        return None
    try:
        strike = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if strike < 0:
        return None
    return int(strike * Decimal("1000"))


def _sector_to_etf(sector: Any) -> str:
    if sector is None:
        return "SPY"
    text = str(sector).strip().upper()
    text = re.sub(r"\s+", " ", text)
    return SECTOR_ETF_MAP.get(text, "SPY")
