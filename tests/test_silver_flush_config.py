"""Regression tests for Silver writer flush timing configuration."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from heber.config import settings
from heber.writer.silver import SilverWriter


def test_silver_flush_uses_silver_max_flush_time(monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 5)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 9999)

    writer = SilverWriter()
    partition_key = "feed=bars/instrument_type=equity/dt=2026-02-05"
    writer.buffers[partition_key] = [{"event_id": "evt-1"}]
    writer.last_flush = datetime.now(UTC) - timedelta(seconds=10)
    writer._flush_partition = MagicMock()

    writer.flush_if_needed()

    writer._flush_partition.assert_called_once()


def test_silver_flush_does_not_use_bronze_interval(monkeypatch) -> None:
    monkeypatch.setattr(settings, "silver_max_rows_per_file", 999_999)
    monkeypatch.setattr(settings, "silver_max_flush_time_seconds", 600)
    monkeypatch.setattr(settings, "bronze_flush_interval_seconds", 0)

    writer = SilverWriter()
    partition_key = "feed=bars/instrument_type=equity/dt=2026-02-05"
    writer.buffers[partition_key] = [{"event_id": "evt-1"}]
    writer.last_flush = datetime.now(UTC) - timedelta(seconds=1)
    writer._flush_partition = MagicMock()

    writer.flush_if_needed()

    writer._flush_partition.assert_not_called()
