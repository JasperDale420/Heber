"""Tests for Tier 2 — Partition Completeness Check."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.config import Settings
from heber.health_monitor.checks.partition import (
    HOURLY_FEEDS,
    _silver_feeds,
    run_partition_checks,
)
from heber.health_monitor.models import CheckContext, Severity, Status

ET = ZoneInfo("America/New_York")
TRADING_DAY = date(2026, 3, 25)  # Wednesday
WEEKEND_DAY = date(2026, 3, 28)  # Saturday
MARKET_OPEN_DT = datetime(2026, 3, 25, 11, 30, tzinfo=ET)


def _write_parquet(path: Path, num_rows: int = 10) -> None:
    """Write a small Parquet file at the given path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


def _write_empty_parquet(path: Path) -> None:
    """Write an empty Parquet file (0 rows)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": pa.array([], type=pa.int64())})
    pq.write_table(table, path)


def _make_ctx(tmp_path: Path, calendar: MagicMock | None = None) -> CheckContext:
    settings = Settings(data_root=str(tmp_path))
    if calendar is None:
        calendar = MagicMock()
        calendar.is_trading_day = MagicMock(return_value=True)
        calendar.elapsed_hours = MagicMock(return_value=[9, 10, 11])
        calendar.adjust_severity = MagicMock(side_effect=lambda sev, _dt: sev)
    return CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=MagicMock(),
        calendar=calendar,
        store=MagicMock(),
    )


def _create_feed_partition(
    silver_root: Path,
    feed: str,
    dt: date,
    *,
    hours: list[int] | None = None,
    empty: bool = False,
) -> None:
    """Create a synthetic Silver partition for a feed."""
    dt_dir = silver_root / f"feed={feed}" / f"dt={dt.isoformat()}"
    if hours is not None:
        for h in hours:
            hour_dir = dt_dir / f"hour={h:02d}"
            if empty:
                _write_empty_parquet(hour_dir / "part-0.parquet")
            else:
                _write_parquet(hour_dir / "part-0.parquet")
    else:
        if empty:
            _write_empty_parquet(dt_dir / "part-0.parquet")
        else:
            _write_parquet(dt_dir / "part-0.parquet")


@pytest.mark.unit
@patch("heber.health_monitor.checks.partition._now_et", return_value=MARKET_OPEN_DT)
async def test_all_partitions_present(mock_now: MagicMock, tmp_path: Path) -> None:
    """All expected partitions exist and have data -- all PASS."""
    silver_root = tmp_path / "silver"
    for feed in _silver_feeds():
        if feed in HOURLY_FEEDS:
            _create_feed_partition(silver_root, feed, TRADING_DAY, hours=[9, 10, 11])
        else:
            _create_feed_partition(silver_root, feed, TRADING_DAY)

    ctx = _make_ctx(tmp_path)
    results = await run_partition_checks(ctx, check_date=TRADING_DAY)

    assert len(results) > 0
    for r in results:
        assert r.status == Status.PASS, f"{r.feed} {r.check_name}: {r.message}"


@pytest.mark.unit
@patch("heber.health_monitor.checks.partition._now_et", return_value=MARKET_OPEN_DT)
async def test_missing_dt_partition_fail(mock_now: MagicMock, tmp_path: Path) -> None:
    """Missing dt= partition for a feed triggers FAIL."""
    silver_root = tmp_path / "silver"
    # Create all feeds except 'bars'
    for feed in _silver_feeds():
        if feed == "bars":
            continue
        if feed in HOURLY_FEEDS:
            _create_feed_partition(silver_root, feed, TRADING_DAY, hours=[9, 10, 11])
        else:
            _create_feed_partition(silver_root, feed, TRADING_DAY)

    ctx = _make_ctx(tmp_path)
    results = await run_partition_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.FAIL
    assert bars_results[0].check_name == "partition_exists"
    assert bars_results[0].severity == Severity.P0_CRITICAL


@pytest.mark.unit
@patch("heber.health_monitor.checks.partition._now_et", return_value=MARKET_OPEN_DT)
async def test_missing_hour_subdirectory_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """Missing hour= subdirectory for hourly feed triggers WARN."""
    silver_root = tmp_path / "silver"
    # Create bars with only hours 9 and 10, but calendar says 9, 10, 11 elapsed
    _create_feed_partition(silver_root, "bars", TRADING_DAY, hours=[9, 10])

    # Create everything else to avoid noise
    for feed in _silver_feeds():
        if feed == "bars":
            continue
        if feed in HOURLY_FEEDS:
            _create_feed_partition(silver_root, feed, TRADING_DAY, hours=[9, 10, 11])
        else:
            _create_feed_partition(silver_root, feed, TRADING_DAY)

    ctx = _make_ctx(tmp_path)
    results = await run_partition_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    r = bars_results[0]
    assert r.check_name == "partition_hourly"
    assert r.status == Status.WARN
    assert 11 in r.details["missing_hours"]


@pytest.mark.unit
@patch("heber.health_monitor.checks.partition._now_et", return_value=MARKET_OPEN_DT)
async def test_empty_partition_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """Empty partition (directory exists but 0-row parquet) triggers WARN."""
    silver_root = tmp_path / "silver"
    # Create an empty partition for a non-hourly feed
    _create_feed_partition(silver_root, "flow_alerts", TRADING_DAY, empty=True)

    # Create everything else valid
    for feed in _silver_feeds():
        if feed == "flow_alerts":
            continue
        if feed in HOURLY_FEEDS:
            _create_feed_partition(silver_root, feed, TRADING_DAY, hours=[9, 10, 11])
        else:
            _create_feed_partition(silver_root, feed, TRADING_DAY)

    ctx = _make_ctx(tmp_path)
    results = await run_partition_checks(ctx, check_date=TRADING_DAY)

    fa_results = [r for r in results if r.feed == "flow_alerts"]
    assert len(fa_results) == 1
    assert fa_results[0].status == Status.WARN
    assert fa_results[0].check_name == "partition_nonempty"


@pytest.mark.unit
@patch("heber.health_monitor.checks.partition._now_et", return_value=MARKET_OPEN_DT)
async def test_weekend_no_checks(mock_now: MagicMock, tmp_path: Path) -> None:
    """On a non-trading day (weekend), return empty results."""
    calendar = MagicMock()
    calendar.is_trading_day = MagicMock(return_value=False)
    ctx = _make_ctx(tmp_path, calendar=calendar)

    results = await run_partition_checks(ctx, check_date=WEEKEND_DAY)
    assert results == []
