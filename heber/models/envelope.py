"""EventEnvelope model - the canonical event contract from Data Gateway.

This module provides a compatible EventEnvelope model that accepts events
from Data-Gateway while adding Heber-specific extensions for zero-leakage.
"""

import re
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field


class Lineage(BaseModel):
    """Lineage metadata for tracing event origin.
    
    Data-Gateway sends lineage as a dict, so we keep it flexible.
    """

    client_id: str | None = None
    project: str | None = None
    request_id: str | None = None
    subscription_id: str | None = None
    sequence: int | None = None
    trace_id: str | None = None
    stream_type: str | None = None


# Instrument key patterns per PRD §6.2
INSTRUMENT_KEY_PATTERNS = {
    "equity": re.compile(r"^equity:[A-Z]{1,5}$"),
    "crypto": re.compile(r"^crypto:[A-Z]{2,10}-[A-Z]{2,10}$"),
    "forex": re.compile(r"^forex:[A-Z]{3}-[A-Z]{3}$"),
    "option": re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$"),
}


def validate_instrument_key(instrument_key: str, instrument_type: str) -> bool:
    """Validate instrument_key format per PRD §6.2.
    
    Args:
        instrument_key: The instrument key to validate
        instrument_type: One of equity, crypto, forex, option
        
    Returns:
        True if valid, False otherwise
        
    Examples:
        >>> validate_instrument_key("equity:AAPL", "equity")
        True
        >>> validate_instrument_key("crypto:BTC-USD", "crypto")
        True
        >>> validate_instrument_key("option:OCC:AAPL260116C00200000", "option")
        True
    """
    pattern = INSTRUMENT_KEY_PATTERNS.get(instrument_type)
    if pattern is None:
        return False
    return bool(pattern.match(instrument_key))


class EventEnvelope(BaseModel):
    """Universal event envelope from Data Gateway.
    
    This model is COMPATIBLE with Data-Gateway's EventEnvelope (gateway/core/envelope.py).
    Fields match Data-Gateway's structure, with Heber-specific extensions marked.
    
    See PRD Section 6.1 for full specification.
    """

    # === Fields from Data-Gateway (required) ===
    event_id: str = Field(..., description="SHA256 idempotency hash (32 chars)")
    provider: str = Field(..., description="Data provider: alpaca, unusual_whales, etc")
    feed: str = Field(..., description="Feed type: bars, quotes, trades, flow, etc")
    source: str = Field(..., description="Delivery method: websocket, rest")
    instrument_type: str = Field(..., description="Asset class: equity, option, crypto, forex")
    instrument_key: str = Field(..., description="Canonical key: equity:AAPL, crypto:BTC-USD")
    symbol: str = Field(..., description="Human-readable symbol")
    ts_event: datetime = Field(..., description="Event time from provider")
    ts_ingest: datetime = Field(..., description="Gateway receive/process time")
    
    # === Fields from Data-Gateway (optional with defaults) ===
    schema_version: str = Field(default="v1", description="Envelope schema version")
    lineage: dict[str, Any] = Field(default_factory=dict, description="Sequence numbers, stream IDs")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, cached")
    payload: dict[str, Any] = Field(..., description="Normalized event data")

    # === Heber Extensions (not in Data-Gateway) ===
    ts_available: datetime | None = Field(
        default=None,
        description="First safe time this record is queryable (anti-leakage gate). Set by Heber on write.",
    )
    raw: dict[str, Any] | None = Field(
        default=None, description="Original provider message (for Bronze fidelity, PRD §6.7)"
    )
    processing_delay_ms: int = Field(
        default=0, description="Processing delay for ts_effective calculation (PRD §6.4)"
    )

    @property
    def ts_effective(self) -> datetime | None:
        """Calculate ts_effective = ts_available + processing_delay_ms (PRD §6.4).
        
        Returns the time at which this record can be realistically used,
        accounting for processing delays.
        """
        if self.ts_available is None:
            return None
        return self.ts_available + timedelta(milliseconds=self.processing_delay_ms)

    def with_ts_available(self, ts: datetime) -> "EventEnvelope":
        """Return a copy with ts_available set."""
        return self.model_copy(update={"ts_available": ts})

    def is_valid_instrument_key(self) -> bool:
        """Check if instrument_key matches expected format per PRD §6.2."""
        return validate_instrument_key(self.instrument_key, self.instrument_type)


