"""Deterministic event_id for backfilled bars (idempotent re-ingest)."""

from datetime import datetime
from hashlib import blake2b


def compute_bar_event_id(provider: str, feed: str, instrument_key: str, ts_event: datetime, timeframe: str) -> str:
    """BLAKE2b(16) hex over the bar's identity fields (mirrors the Data-Gateway envelope id scheme)."""
    parts = f"{provider}|{feed}|{instrument_key}|{ts_event.isoformat()}|{timeframe}"
    return blake2b(parts.encode("utf-8"), digest_size=16).hexdigest()
