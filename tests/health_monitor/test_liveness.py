"""Tests for the per-feed liveness check."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from heber.health_monitor.checks.liveness import run_liveness_checks
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import make_check_context

ET = ZoneInfo("America/New_York")

# A trading weekday at 11:30 ET (inside the 09:30-16:00 continuous windows).
MIDDAY_ET = datetime(2026, 3, 25, 11, 30, tzinfo=ET)
# Same trading day at 18:00 ET (past the 17:30 daily deadline; outside RTH).
EVENING_ET = datetime(2026, 3, 25, 18, 0, tzinfo=ET)


def _reader_returning(counts: dict[str, int]) -> MagicMock:
    """A reader whose read_silver returns a ts_event frame of `counts[feed]` rows."""
    reader = MagicMock()

    def _read(dataset: str, time_range=None, columns=None, **_kw):
        n = counts.get(dataset, 0)
        ts = pd.Timestamp("2026-03-25T15:00:00Z")
        return pd.DataFrame({"ts_event": [ts] * n})

    reader.read_silver = MagicMock(side_effect=_read)
    return reader


def _ctx(tmp_path: Path, reader: MagicMock, overrides: dict | None = None):
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=True)
    settings_overrides = {"alert_floor_overrides": overrides or {}}
    return make_check_context(tmp_path, calendar=cal, reader=reader, settings_overrides=settings_overrides)


@pytest.mark.unit
async def test_continuous_feed_flowing_passes(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.PASS


@pytest.mark.unit
async def test_continuous_feed_dark_fails_critical(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL
    assert flow[0].severity == Severity.P0_CRITICAL
    assert "flow_alerts" in flow[0].message


@pytest.mark.unit
async def test_trickle_below_floor_fails(tmp_path: Path) -> None:
    # darkpool floor raised to 8 via override; only 3 rows in window -> FAIL.
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 3, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader, overrides={"darkpool": 8})
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    dp = [r for r in results if r.feed == "darkpool"]
    assert len(dp) == 1
    assert dp[0].status == Status.FAIL
    assert dp[0].severity == Severity.P0_CRITICAL


@pytest.mark.unit
async def test_missing_partition_treated_as_zero(tmp_path: Path) -> None:
    reader = MagicMock()
    reader.read_silver = MagicMock(side_effect=FileNotFoundError("no partition"))
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL  # NOT skipped


@pytest.mark.unit
async def test_daily_feed_present_before_deadline_no_result(tmp_path: Path) -> None:
    # At midday (before 17:30) daily feeds are not yet in scope -> no result.
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    assert all(r.feed not in {"oi_change", "greek_exposure"} for r in results)


@pytest.mark.unit
async def test_daily_feed_missing_past_deadline_fails(tmp_path: Path) -> None:
    reader = _reader_returning({"oi_change": 0, "greek_exposure": 5})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=EVENING_ET)
    oi = [r for r in results if r.feed == "oi_change"]
    assert len(oi) == 1
    assert oi[0].status == Status.FAIL
    assert oi[0].severity == Severity.P0_CRITICAL
    gx = [r for r in results if r.feed == "greek_exposure"]
    assert gx[0].status == Status.PASS


@pytest.mark.unit
async def test_continuous_feed_outside_window_no_result(tmp_path: Path) -> None:
    # 18:00 ET is outside flow_alerts' 09:30-16:00 window -> no flow_alerts result.
    reader = _reader_returning({"flow_alerts": 0})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=EVENING_ET)
    assert all(r.feed != "flow_alerts" for r in results)


@pytest.mark.unit
async def test_non_trading_day_empty(tmp_path: Path) -> None:
    reader = _reader_returning({"flow_alerts": 0})
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=False)
    ctx = make_check_context(tmp_path, calendar=cal, reader=reader)
    results = await run_liveness_checks(ctx, now=MIDDAY_ET)
    assert results == []


@pytest.mark.unit
async def test_reads_prune_by_dt(tmp_path: Path) -> None:
    """Liveness reads must prune to the day's partition, not scan a feed's full history."""
    reader = _reader_returning({"flow_alerts": 50, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    await run_liveness_checks(ctx, now=MIDDAY_ET)
    assert reader.read_silver.call_args_list, "read_silver was never called"
    for call in reader.read_silver.call_args_list:
        assert call.kwargs.get("prune_by_dt") is True
