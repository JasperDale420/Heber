"""Tests for Tier 3 — Statistical Profiling Check."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from heber.config import Settings
from heber.health_monitor.checks.statistical import run_statistical_checks
from heber.health_monitor.models import CheckContext, Severity, Status

ET = ZoneInfo("America/New_York")
TRADING_DAY = date(2026, 3, 25)  # Wednesday
WEEKEND_DAY = date(2026, 3, 28)  # Saturday
MARKET_OPEN_DT = datetime(2026, 3, 25, 11, 30, tzinfo=ET)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
    reader: MagicMock | None = None,
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
    if reader is None:
        reader = MagicMock()
    return CheckContext(
        settings=settings,
        reader=reader,
        redis=MagicMock(),
        calendar=calendar,
        store=store,
    )


def _silver_df(n: int = 100, null_pct: float = 0.0, mean: float = 50.0, std: float = 10.0) -> pd.DataFrame:
    """Create a synthetic Silver DataFrame with numeric columns."""
    rng = np.random.default_rng(42)
    price = rng.normal(mean, std, n)
    volume = rng.integers(100, 10000, n).astype(np.int64)
    symbol = [f"SYM{i % 10}" for i in range(n)]

    df = pd.DataFrame({"price": price, "volume": volume, "symbol": symbol})

    # Inject nulls into price column
    if null_pct > 0:
        null_count = int(n * null_pct)
        null_indices = rng.choice(n, null_count, replace=False)
        df.loc[null_indices, "price"] = np.nan

    return df


def _baseline_df(feed: str, col: str, mean: float, stddev: float, null_pct: float = 0.01) -> pd.DataFrame:
    """Create a baseline stats DataFrame matching what run_statistical_checks stores."""
    return pd.DataFrame(
        [
            {
                "feed": feed,
                "column": col,
                "count": 100,
                "null_count": int(100 * null_pct),
                "null_pct": null_pct,
                "min": mean - 3 * stddev,
                "max": mean + 3 * stddev,
                "mean": mean,
                "stddev": stddev,
            }
        ]
    )


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_stats_within_baseline_pass(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """Stats within baseline range result in PASS."""
    df = _silver_df(n=100, null_pct=0.01, mean=50.0, std=10.0)

    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=df)

    # Baseline with similar mean/stddev
    baseline = pd.concat(
        [
            _baseline_df("bars", "price", mean=50.0, stddev=10.0),
            _baseline_df("bars", "volume", mean=5000.0, stddev=2800.0),
        ],
        ignore_index=True,
    )
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    # All results should be PASS (no nulls above threshold, no mean shift)
    assert len(results) > 0
    for r in results:
        assert r.status == Status.PASS, f"Expected PASS for {r.check_name}: {r.message}"


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_null_rate_spike_warn(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """Null rate above threshold results in WARN."""
    # 20% nulls in price - well above 5% threshold
    df = _silver_df(n=100, null_pct=0.20, mean=50.0, std=10.0)

    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=df)

    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    warn_results = [r for r in results if r.status == Status.WARN]
    assert len(warn_results) >= 1
    # The price column should trigger a null rate warning
    null_warns = [r for r in warn_results if "null" in r.message.lower() and "price" in r.message.lower()]
    assert len(null_warns) == 1
    assert null_warns[0].severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_mean_shift_warn(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """Mean shifted > 2 stddev from baseline results in WARN."""
    # Today's data has mean ~150, baseline mean=50 with stddev=10 → shift of 10 sigma
    df = _silver_df(n=100, null_pct=0.0, mean=150.0, std=10.0)

    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=df)

    baseline = _baseline_df("bars", "price", mean=50.0, stddev=10.0)
    store = MagicMock()
    store.read_baselines = MagicMock(return_value=baseline)
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    warn_results = [r for r in results if r.status == Status.WARN and "mean" in r.message.lower()]
    assert len(warn_results) >= 1
    assert warn_results[0].severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_first_run_no_baseline_pass(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """First run with no baseline stores baseline and returns PASS."""
    df = _silver_df(n=100, null_pct=0.01, mean=50.0, std=10.0)

    reader = MagicMock()
    reader.read_silver = MagicMock(return_value=df)

    store = MagicMock()
    store.read_baselines = MagicMock(return_value=pd.DataFrame())
    store.write_baseline = MagicMock()

    ctx = _make_ctx(tmp_path, reader=reader, store=store)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    # Should have PASS results (null rates OK, no baseline to compare mean against)
    assert len(results) > 0
    for r in results:
        assert r.status == Status.PASS, f"Unexpected status for {r.check_name}: {r.message}"

    # Baseline should be written
    store.write_baseline.assert_called_once()


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_non_trading_day_empty(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """On a non-trading day, return empty results."""
    calendar = MagicMock()
    calendar.is_trading_day = MagicMock(return_value=False)

    ctx = _make_ctx(tmp_path, calendar=calendar)
    results = await run_statistical_checks(ctx, check_date=WEEKEND_DAY)
    assert results == []
