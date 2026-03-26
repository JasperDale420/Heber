"""Tests for Tier 2 — Volume Trending Check."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.config import Settings
from heber.health_monitor.checks.volume import run_volume_checks
from heber.health_monitor.models import CheckContext, Severity, Status

ET = ZoneInfo("America/New_York")
TRADING_DAY = date(2026, 3, 25)  # Wednesday
WEEKEND_DAY = date(2026, 3, 28)  # Saturday
MARKET_OPEN_DT = datetime(2026, 3, 25, 11, 30, tzinfo=ET)


def _write_parquet(path: Path, num_rows: int = 100) -> None:
    """Write a Parquet file with a given number of rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"x": list(range(num_rows))})
    pq.write_table(table, path)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
) -> CheckContext:
    settings = Settings(data_root=str(tmp_path))
    if calendar is None:
        calendar = MagicMock()
        calendar.is_trading_day = MagicMock(return_value=True)
        calendar.adjust_severity = MagicMock(side_effect=lambda sev, _dt: sev)
    if store is None:
        store = MagicMock()
        store.read_baselines = MagicMock(return_value=pd.DataFrame())
        store.write_baseline = MagicMock()
    return CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=MagicMock(),
        calendar=calendar,
        store=store,
    )


def _create_feed_partition(silver_root: Path, feed: str, dt: date, num_rows: int = 100) -> None:
    """Create a synthetic Silver partition with a known row count."""
    dt_dir = silver_root / f"feed={feed}" / f"dt={dt.isoformat()}"
    _write_parquet(dt_dir / "part-0.parquet", num_rows=num_rows)


def _baseline_df(feed: str, row_count: int, days: int = 5) -> pd.DataFrame:
    """Create a baseline DataFrame with consistent row counts for N days."""
    rows = []
    for i in range(days):
        rows.append({"feed": feed, "row_count": row_count, "dt": f"2026-03-{20 + i:02d}"})
    return pd.DataFrame(rows)


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_volume_within_baseline_pass(mock_now: MagicMock, tmp_path: Path) -> None:
    """Volume matching or exceeding baseline median results in PASS."""
    silver_root = tmp_path / "silver"
    _create_feed_partition(silver_root, "bars", TRADING_DAY, num_rows=1000)

    baseline = _baseline_df("bars", row_count=1000, days=5)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_volume_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.PASS
    assert bars_results[0].severity == Severity.P2_INFO


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_volume_at_40_percent_warn(mock_now: MagicMock, tmp_path: Path) -> None:
    """Volume at 40% of baseline (below 0.5 warn threshold) results in WARN."""
    silver_root = tmp_path / "silver"
    _create_feed_partition(silver_root, "bars", TRADING_DAY, num_rows=400)

    baseline = _baseline_df("bars", row_count=1000, days=5)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_volume_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.WARN
    assert bars_results[0].severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_volume_at_10_percent_fail(mock_now: MagicMock, tmp_path: Path) -> None:
    """Volume at 10% of baseline (below 0.2 critical threshold) results in FAIL."""
    silver_root = tmp_path / "silver"
    _create_feed_partition(silver_root, "bars", TRADING_DAY, num_rows=100)

    baseline = _baseline_df("bars", row_count=1000, days=5)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_volume_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.FAIL
    assert bars_results[0].severity == Severity.P0_CRITICAL


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_no_baseline_skip_with_info(mock_now: MagicMock, tmp_path: Path) -> None:
    """When no baseline exists for a feed, skip with INFO (no alert on first run)."""
    silver_root = tmp_path / "silver"
    _create_feed_partition(silver_root, "bars", TRADING_DAY, num_rows=1000)

    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_volume_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    assert bars_results[0].status == Status.PASS
    assert bars_results[0].severity == Severity.P2_INFO
    assert "no baseline" in bars_results[0].message.lower()


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_non_trading_day_empty(mock_now: MagicMock, tmp_path: Path) -> None:
    """On a non-trading day, return empty results."""
    calendar = MagicMock()
    calendar.is_trading_day = MagicMock(return_value=False)
    ctx = _make_ctx(tmp_path, calendar=calendar)

    results = await run_volume_checks(ctx, check_date=WEEKEND_DAY)
    assert results == []


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_baseline_written_after_check(mock_now: MagicMock, tmp_path: Path) -> None:
    """Today's row counts are written as new baseline after checks."""
    silver_root = tmp_path / "silver"
    _create_feed_partition(silver_root, "bars", TRADING_DAY, num_rows=500)

    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    await run_volume_checks(ctx, check_date=TRADING_DAY)

    # Verify write_baseline was called
    store.write_baseline.assert_called_once()
    written_df = store.write_baseline.call_args[0][0]
    assert "feed" in written_df.columns
    assert "row_count" in written_df.columns


@pytest.mark.unit
@patch("heber.health_monitor.checks.volume._now_et", return_value=MARKET_OPEN_DT)
async def test_multiple_parquet_files_summed(mock_now: MagicMock, tmp_path: Path) -> None:
    """Multiple Parquet files in a partition have their rows summed."""
    silver_root = tmp_path / "silver"
    dt_dir = silver_root / "feed=bars" / f"dt={TRADING_DAY.isoformat()}"
    _write_parquet(dt_dir / "part-0.parquet", num_rows=300)
    _write_parquet(dt_dir / "part-1.parquet", num_rows=700)

    baseline = _baseline_df("bars", row_count=1000, days=5)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, store=store)
    results = await run_volume_checks(ctx, check_date=TRADING_DAY)

    bars_results = [r for r in results if r.feed == "bars"]
    assert len(bars_results) == 1
    # 300+700=1000 which equals baseline, should PASS
    assert bars_results[0].status == Status.PASS
