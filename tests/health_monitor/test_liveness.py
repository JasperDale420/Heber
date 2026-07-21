"""Tests for the per-feed liveness check."""

from __future__ import annotations

from datetime import datetime, time
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


def _ctx(
    tmp_path: Path,
    reader: MagicMock,
    overrides: dict | None = None,
    close_time: time = time(16, 0),
):
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=True)
    cal.market_hours = MagicMock(return_value=(time(9, 30), close_time))
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
async def test_flow_alerts_first_trading_hour_no_result(tmp_path: Path) -> None:
    # 09:45 ET: the 60m lookback still reaches into pre-market, where flow
    # alerts are legitimately quiet — a low/zero reading is a window-fill
    # artifact, not an outage. This fired false CRITICAL/RECOVERED pairs at
    # nearly every open; the window start must not begin before 10:30.
    early_et = datetime(2026, 3, 25, 9, 45, tzinfo=ET)
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=early_et)
    assert all(r.feed != "flow_alerts" for r in results)


@pytest.mark.unit
async def test_flow_alerts_dark_after_window_fill_fails(tmp_path: Path) -> None:
    # 10:35 ET: a full 60m lookback now fits inside market hours, so a dark
    # feed must FAIL — the delayed start must not leave the feed unmonitored.
    filled_et = datetime(2026, 3, 25, 10, 35, tzinfo=ET)
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=filled_et)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL


@pytest.mark.unit
async def test_darkpool_premarket_no_result(tmp_path: Path) -> None:
    # Darkpool's window starts at the open (09:30); pre-market it has no prints, so
    # at 06:00 ET a dark darkpool feed must NOT fire — this is the pre-market
    # false-positive the window start fixes.
    pre_market_et = datetime(2026, 3, 25, 6, 0, tzinfo=ET)
    reader = _reader_returning({"darkpool": 0})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=pre_market_et)
    assert all(r.feed != "darkpool" for r in results)


@pytest.mark.unit
async def test_darkpool_afterhours_still_monitored(tmp_path: Path) -> None:
    # Darkpool legitimately flows after 16:00 ET (its window runs to 20:00), so a
    # dark feed at 17:00 ET must still FAIL — not be silently unmonitored.
    after_hours_et = datetime(2026, 3, 25, 17, 0, tzinfo=ET)
    reader = _reader_returning({"darkpool": 0})
    ctx = _ctx(tmp_path, reader)
    results = await run_liveness_checks(ctx, now=after_hours_et)
    dp = [r for r in results if r.feed == "darkpool"]
    assert len(dp) == 1
    assert dp[0].status == Status.FAIL
    assert dp[0].severity == Severity.P0_CRITICAL


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


@pytest.mark.unit
async def test_daily_eod_rows_at_utc_midnight_are_counted(tmp_path: Path) -> None:
    """EOD feeds (e.g. greek_exposure) stamp ts_event at UTC-midnight (00:00Z).
    The daily window must start at 00:00 UTC, not ET-midnight (04:00/05:00Z), or
    those rows fall just before the window and are excluded -> false CRITICAL.
    """
    row_ts = pd.Timestamp("2026-03-25T00:00:00Z")  # trading day, UTC midnight

    def _read(dataset, time_range=None, columns=None, **_kw):
        start, end = pd.Timestamp(time_range[0]), pd.Timestamp(time_range[1])
        present = dataset == "greek_exposure" and start <= row_ts <= end
        return pd.DataFrame({"ts_event": [row_ts] * (5 if present else 0)})

    reader = MagicMock()
    reader.read_silver = MagicMock(side_effect=_read)
    cal = MagicMock()
    cal.is_trading_day = MagicMock(return_value=True)
    cal.market_hours = MagicMock(return_value=(time(9, 30), time(16, 0)))
    ctx = make_check_context(tmp_path, calendar=cal, reader=reader, settings_overrides={"alert_floor_overrides": {}})

    results = await run_liveness_checks(ctx, now=EVENING_ET)  # 18:00 ET, past the 17:30 deadline
    gx = [r for r in results if r.feed == "greek_exposure"]
    assert len(gx) == 1
    assert gx[0].status == Status.PASS  # FAILs with an ET-midnight window start


# 2026-11-27 (day after Thanksgiving) is a 13:00 ET early close. 14:30 ET is
# past that close: regular-hours feeds are legitimately quiet.
EARLY_CLOSE_AFTERNOON_ET = datetime(2026, 11, 27, 14, 30, tzinfo=ET)
NORMAL_AFTERNOON_ET = datetime(2026, 11, 30, 14, 30, tzinfo=ET)  # a regular trading day


@pytest.mark.unit
async def test_early_close_no_false_critical_after_close(tmp_path: Path) -> None:
    """After a 13:00 ET early close, a quiet regular-hours feed must NOT alarm."""
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 0, "bars": 0, "trades": 0})
    ctx = _ctx(tmp_path, reader, close_time=time(13, 0))
    results = await run_liveness_checks(ctx, now=EARLY_CLOSE_AFTERNOON_ET)
    # flow_alerts/bars/trades (16:00 window -> shifted to 13:00) are past close: no result.
    for feed in ("flow_alerts", "bars", "trades"):
        assert [r for r in results if r.feed == feed] == [], f"{feed} false-alarmed after early close"
    # darkpool (20:00 = +4h tail -> 13:00+4h = 17:00) is still in window at 14:30.
    darkpool = [r for r in results if r.feed == "darkpool"]
    assert len(darkpool) == 1 and darkpool[0].status == Status.FAIL


@pytest.mark.unit
async def test_normal_day_afternoon_still_alarms(tmp_path: Path) -> None:
    """On a regular 16:00 close day, a dark feed at 14:30 still FAILs (no over-suppression)."""
    reader = _reader_returning({"flow_alerts": 0, "darkpool": 50, "bars": 50, "trades": 50})
    ctx = _ctx(tmp_path, reader, close_time=time(16, 0))
    results = await run_liveness_checks(ctx, now=NORMAL_AFTERNOON_ET)
    flow = [r for r in results if r.feed == "flow_alerts"]
    assert len(flow) == 1
    assert flow[0].status == Status.FAIL
