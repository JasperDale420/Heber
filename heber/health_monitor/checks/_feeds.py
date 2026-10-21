"""Shared helpers for health-monitor checks."""

from __future__ import annotations

from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import BRONZE_ONLY_SILVER_DATASETS


def _silver_feeds() -> list[str]:
    """Return canonical Silver feed names (those with Arrow schemas, minus bronze-only)."""
    return sorted(name for name in SILVER_SCHEMAS if name not in BRONZE_ONLY_SILVER_DATASETS)
