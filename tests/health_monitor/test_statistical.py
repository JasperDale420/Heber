"""Tests for Tier 3 — Statistical Profiling Check."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.health_monitor.checks.statistical import run_statistical_checks
from heber.health_monitor.models import Severity, Status
from heber.reader import HeberReader
from tests.health_monitor.conftest import (
    MARKET_OPEN_DT,
    TRADING_DAY,
    WEEKEND_DAY,
    make_check_context,
    make_store_mock,
)


def _make_ctx(
    tmp_path: Path,
    calendar: MagicMock | None = None,
    store: MagicMock | None = None,
    reader: MagicMock | HeberReader | None = None,
):
    return make_check_context(tmp_path, calendar=calendar, store=store, reader=reader)


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


def _write_bars_day(root: Path, day: date, *, rows: int, include_end_of_day: bool = False) -> None:
    """Write one Silver ``bars`` day partition of per-minute equity rows.

    ``include_end_of_day`` appends a row on the last representable microsecond
    of the day, so an exclusive upper bound in the read window would drop it.
    """
    ts = list(pd.date_range(f"{day.isoformat()}T13:30:00Z", periods=rows, freq="1min"))
    if include_end_of_day:
        ts.append(pd.Timestamp(f"{day.isoformat()}T23:59:59.999999Z"))
    frame = pd.DataFrame(
        {
            "ts_event": ts,
            "ts_available": ts,
            "instrument_key": ["equity:AAPL"] * len(ts),
            "close": [150.0 + i * 0.01 for i in range(len(ts))],
        }
    )
    part = root / "silver" / "feed=bars" / "instrument_type=equity" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(frame), str(part / "data.parquet"))


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


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_read_failure_reports_error_result(mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path) -> None:
    """A feed whose read raises must surface as an ERROR result, not vanish.

    Silently skipping the feed leaves the audit reporting nothing at all for
    it, which is indistinguishable from "the feed is healthy" to anyone
    reading the health output.

    ``ArrowNotImplementedError`` is the class the real incident raised — an
    unrecoverable schema conflict that ``read_silver`` deliberately lets
    propagate rather than flattening into an empty DataFrame.
    """
    reader = MagicMock()
    reader.read_silver = MagicMock(side_effect=pa.lib.ArrowNotImplementedError("Unsupported cast from string to null"))

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    errors = [r for r in results if r.status == Status.ERROR]
    assert len(errors) == 1
    assert errors[0].feed == "bars"
    assert errors[0].check_name == "statistical_read"
    assert "Unsupported cast from string to null" in errors[0].details["error"]


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars", "flow_alerts"])
async def test_read_failure_does_not_abort_other_feeds(
    mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path
) -> None:
    """One unreadable feed must not stop the remaining feeds being audited."""

    def _read(feed: str, **_kwargs: object) -> pd.DataFrame:
        if feed == "bars":
            raise OSError("volume unavailable")
        return _silver_df(n=100, null_pct=0.01)

    reader = MagicMock()
    reader.read_silver = MagicMock(side_effect=_read)

    ctx = _make_ctx(tmp_path, reader=reader)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    assert any(r.status == Status.ERROR and r.feed == "bars" for r in results)
    assert any(r.status == Status.PASS and r.feed == "flow_alerts" for r in results)


@pytest.mark.unit
@patch("heber.health_monitor.checks.statistical._now_et", return_value=MARKET_OPEN_DT)
@patch("heber.health_monitor.checks.statistical._silver_feeds", return_value=["bars"])
async def test_intraday_rows_produce_results_through_real_reader(
    mock_feeds: MagicMock, mock_now: MagicMock, tmp_path: Path
) -> None:
    """The audit must see a normal intraday trading day, and only that day.

    Bars carry a ``ts_event`` per minute, so a day window that collapses to a
    single instant matches nothing and the feed is skipped as "no data" — the
    audit then reports nothing at all for the primary equity feed.

    A neighbouring day is written too: it must be pruned away, so the profiled
    row count proves the window neither under- nor over-selects. The final row
    sits on the last representable microsecond of the day, pinning the upper
    bound as inclusive.
    """
    _write_bars_day(tmp_path, TRADING_DAY, rows=59, include_end_of_day=True)
    _write_bars_day(tmp_path, date(2026, 3, 26), rows=10)

    store = make_store_mock()
    ctx = _make_ctx(tmp_path, reader=HeberReader(tmp_path), store=store)
    results = await run_statistical_checks(ctx, check_date=TRADING_DAY)

    assert [r for r in results if r.feed == "bars"], "audit produced no result for bars"
    assert any(r.check_name == "statistical_null_rate" and r.feed == "bars" for r in results)

    # The profiled row count proves the window neither under- nor over-selects:
    # 59 intraday rows plus the end-of-day boundary row, and nothing from the
    # neighbouring day.
    profiled = store.write_baseline.call_args[0][0]
    assert set(profiled.loc[profiled["feed"] == "bars", "count"]) == {60}
