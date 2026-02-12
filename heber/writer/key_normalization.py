"""Instrument-key synthesis and feed-level envelope normalization."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from heber.models.envelope import EventEnvelope
from heber.writer.ingest_contracts import normalize_payload_for_feed, resolve_feed_alias, resolve_silver_feed

OCC_PATTERN = re.compile(r"([A-Z]{1,6}\d{6}[CP]\d{8})")
SYMBOL_PATTERN = re.compile(r"^[A-Z]{1,5}$")
UNDERLYING_PATTERN = re.compile(r"^[A-Z]{1,6}$")

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
    elif symbol is None:
        symbol = _normalize_symbol(payload.get("symbol")) or _normalize_symbol(payload.get("ticker"))
        if symbol is not None and instrument_type == "equity":
            instrument_key = f"equity:{symbol}"

    updates: dict[str, Any] = {
        "feed": canonical_feed,
        "payload": payload,
        "instrument_type": instrument_type or envelope.instrument_type,
        "instrument_key": instrument_key,
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
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        return None


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
