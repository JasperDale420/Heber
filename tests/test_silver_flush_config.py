"""Regression tests for Silver writer flush timing configuration."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from heber.config import settings
from heber.writer.silver import SilverWriter


@pytest.mark.asyncio
async def test_silver_flush_uses_silver_max_flush_time(monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 5)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9999)

    writer = SilverWriter()
    partition_key = "feed=bars/instrument_type=equity/dt=2026-02-05"
    writer.buffers[partition_key] = [{"event_id": "evt-1"}]
    writer.last_flush = datetime.utcnow() - timedelta(seconds=10)
    writer._flush_partition = AsyncMock()

    await writer.flush_if_needed()

    writer._flush_partition.assert_awaited_once()


@pytest.mark.asyncio
async def test_silver_flush_does_not_use_bronze_interval(monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 600)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 0)

    writer = SilverWriter()
    partition_key = "feed=bars/instrument_type=equity/dt=2026-02-05"
    writer.buffers[partition_key] = [{"event_id": "evt-1"}]
    writer.last_flush = datetime.utcnow() - timedelta(seconds=1)
    writer._flush_partition = AsyncMock()

    await writer.flush_if_needed()

    writer._flush_partition.assert_not_awaited()
