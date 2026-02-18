"""Tests for seed_coverage_from_disk partition scanning."""

from __future__ import annotations

from pathlib import Path

from heber.catalog.seeds import _scan_partition_dates


def test_scan_partition_dates_returns_min_max(tmp_path: Path) -> None:
    """Verify _scan_partition_dates finds dt= partitions and returns correct range."""
    feed_dir = tmp_path / "feed=bars"
    feed_dir.mkdir()

    # Create dt= partitions at different nesting levels
    (feed_dir / "instrument_type=equity" / "dt=2024-01-15").mkdir(parents=True)
    (feed_dir / "instrument_type=equity" / "dt=2024-06-30").mkdir(parents=True)
    (feed_dir / "instrument_type=equity" / "dt=2025-02-01").mkdir(parents=True)
    (feed_dir / "instrument_type=crypto" / "dt=2024-12-25").mkdir(parents=True)

    result = _scan_partition_dates(feed_dir)

    assert result is not None
    dt_min, dt_max, count = result
    assert dt_min == "2024-01-15"
    assert dt_max == "2025-02-01"
    assert count == 4


def test_scan_partition_dates_returns_none_when_empty(tmp_path: Path) -> None:
    """No dt= prtitions should return None."""
    feed_dir = tmp_path / "feed=empty"
    feed_dir.mkdir()
    (feed_dir / "somefile.parquet").touch()

    result = _scan_partition_dates(feed_dir)
    assert result is None


def test_scan_partition_dates_ignores_files(tmp_path: Path) -> None:
    """dt= files (not directories) should be ignored."""
    feed_dir = tmp_path / "feed=bars"
    feed_dir.mkdir()
    (feed_dir / "dt=2024-01-01").touch()  # File, not dir

    result = _scan_partition_dates(feed_dir)
    assert result is None
