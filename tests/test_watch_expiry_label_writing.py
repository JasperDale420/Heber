"""Regression tests: expiry must go through BarrierChecker, not bypass it.

Background: WatchManager.cleanup_expired() used to complete expired watches
directly from their mutable current_return/snapshot_count fields, bypassing
BarrierChecker.check_watch entirely. That meant (1) no Gold label was ever
written for watches finalized this way, and (2) the "return" used wasn't
filtered to the watch's own [alert_time, window_end] window. It also raced
BarrierChecker.check_all() (driven by WatchService's 60s check-and-write
loop), which already does this correctly.

Since BarrierChecker.check_all() visits every active watch and already force
-expires (with the correct windowed return path) anything whose window has
passed, the fix removes the redundant, bypassing sweep from
SnapshotPoller/WatchManager entirely rather than teaching it to duplicate
(and race) BarrierChecker's own completion path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from heber.watch import checker as checker_module
from heber.watch import poller as poller_module
from heber.watch.checker import BarrierChecker, outcome_to_label_row
from heber.watch.models import AlertWatch, WatchHorizon, WatchOutcome, WatchSnapshot, WatchStatus
from heber.watch.poller import SnapshotPoller
from heber.watch.writer import LabelWriter


class _CheckerManagerStub:
    """Minimal BarrierChecker collaborator, mirroring test_watch_zero_price_handling.py."""

    def __init__(self, snapshots: list[WatchSnapshot]) -> None:
        self._snapshots = snapshots
        self.completed: tuple[str, WatchStatus, float, int | None, datetime | None] | None = None

    def get_snapshots(self, _watch_id: str) -> list[WatchSnapshot]:
        return self._snapshots

    def complete_watch(
        self,
        watch_id: str,
        status: WatchStatus,
        outcome_return: float,
        bars_to_hit: int | None = None,
        outcome_time: datetime | None = None,
    ) -> None:
        self.completed = (watch_id, status, outcome_return, bars_to_hit, outcome_time)


class _CalendarStub:
    def trading_minutes_until(self, start: datetime, end: datetime) -> int:
        return max(0, int((end - start).total_seconds() // 60))

    def is_market_open(self) -> bool:
        return True


def test_checker_expiry_outcome_writes_windowed_gold_label_row(tmp_path) -> None:  # noqa: ANN001
    """A late, out-of-window snapshot that would cross TP must not win the
    barrier race, and the resulting EXPIRED outcome must actually make it
    to a Gold label row (not just be computed and dropped).
    """
    now = datetime.now(UTC)
    alert_time = now - timedelta(hours=2)
    window_end = now - timedelta(minutes=5)
    watch = AlertWatch(
        watch_id="watch-late-snapshot",
        alert_id="alert-late-snapshot",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=alert_time,
        window_end=window_end,
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=alert_time + timedelta(minutes=10),
            mid_px=1.05,  # inside the window, no barrier hit
            return_pct=None,
        ),
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=window_end + timedelta(minutes=2),  # collected after window_end
            mid_px=1.35,  # crosses TP, but only outside the window
            return_pct=None,
        ),
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED

    writer = LabelWriter(output_path=tmp_path)
    writer.write_outcomes([outcome])

    partition_path = (
        tmp_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1" / f"dt={alert_time.date()}"
    )
    files = list(partition_path.glob("*.parquet"))
    assert len(files) == 1

    df = pd.read_parquet(files[0])
    assert len(df) == 1
    row = df.iloc[0]
    assert row["outcome"] == "expired"
    assert row["outcome_reason"] == "expired"
    assert row["watch_id"] == "watch-late-snapshot"


def test_outcome_to_label_row_stamps_ts_available_at_write_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """ts_available must reflect when the label was actually written, not the
    (possibly long-stale) business outcome_time.

    Regression: with WatchManager.cleanup_expired removed, BarrierChecker is
    now the sole path that finalizes a watch -- including ones recovered
    from a service-restart backlog, whose window_end (and therefore
    outcome_time for an EXPIRED outcome) may be hours or days in the past.
    Stamping ts_available from that stale outcome_time would let a
    point-in-time reader querying between window_end and the actual write
    see a row that did not exist yet -- exactly the look-ahead the
    zero-leakage contract exists to prevent. heber/watch/backfill_scanner.py
    already encodes this same principle ("a value fetched now only becomes
    queryable now") for a different write path; this applies it here too.
    """
    frozen_now = datetime(2026, 2, 10, 12, 0, tzinfo=UTC)

    class _FrozenDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            if tz is None:
                return frozen_now
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(checker_module, "datetime", _FrozenDateTime)

    stale_window_end = frozen_now - timedelta(days=3)  # a backlog watch recovered on restart
    outcome = WatchOutcome(
        watch_id="watch-recovered",
        alert_id="alert-recovered",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        horizon=WatchHorizon.INTRADAY,
        status=WatchStatus.EXPIRED,
        outcome_time=stale_window_end,
        outcome_return=0.0,
        bars_to_hit=0,
        mfe=0.0,
        mae=0.0,
        hit_tp_first=0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=stale_window_end - timedelta(hours=4),
        window_duration_hours=4.0,
    )

    row = outcome_to_label_row(outcome)

    assert row["ts_available"] == frozen_now
    assert row["ts_available"] != outcome.outcome_time


def test_outcome_to_label_row_clamps_ts_available_to_alert_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """ts_available must never be < ts_event (alert_time).

    Regression: stamping ts_available from the wall clock alone (the prior
    fix) is wrong the other direction for a future-dated or clock-skewed
    alert_time -- it would persist ts_available < ts_event, letting an
    as-of read return the row before its own event time. Mirrors the same
    max(now, event) clamp heber/watch/backfill_scanner.py already applies
    for exactly this reason.
    """
    frozen_now = datetime(2026, 2, 10, 12, 0, tzinfo=UTC)

    class _FrozenDateTime:
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            if tz is None:
                return frozen_now
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(checker_module, "datetime", _FrozenDateTime)

    future_alert_time = frozen_now + timedelta(hours=1)
    outcome = WatchOutcome(
        watch_id="watch-future-alert",
        alert_id="alert-future-alert",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        horizon=WatchHorizon.INTRADAY,
        status=WatchStatus.EXPIRED,
        outcome_time=future_alert_time,
        outcome_return=0.0,
        bars_to_hit=0,
        mfe=0.0,
        mae=0.0,
        hit_tp_first=0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=future_alert_time,
        window_duration_hours=4.0,
    )

    row = outcome_to_label_row(outcome)

    assert row["ts_available"] == future_alert_time
    assert row["ts_available"] >= row["ts_event"]


@pytest.mark.asyncio
async def test_poller_run_never_completes_watches_itself(monkeypatch: pytest.MonkeyPatch) -> None:
    """SnapshotPoller must not finalize watches on its own.

    The manager stub below intentionally has no complete_watch or
    cleanup_expired_async method. Before the fix, SnapshotPoller.run()
    called manager.cleanup_expired_async() every cycle; against this stub
    that raises AttributeError, but run()'s own `except Exception` swallows
    it and logs "Poll cycle failed" rather than propagating it -- so the
    regression signal is a captured error log, not an uncaught exception.
    After the fix, the poller only fetches quotes and updates prices, so a
    full run() cycle completes with no error logged at all.
    """
    now = datetime.now(UTC)
    expired_watch = SimpleNamespace(
        watch_id="watch-expired",
        occ_symbol="AAPL260220C00100000",
        entry_price=1.0,
        horizon=WatchHorizon.INTRADAY,
        updated_at=now - timedelta(hours=1),
        alert_time=now - timedelta(hours=3),
    )

    class _NoCompletionManager:
        async def get_active_watches_async(self) -> list[SimpleNamespace]:
            return [expired_watch]

        async def add_snapshot_async(self, _snapshot: WatchSnapshot) -> None:
            return None

        async def update_watch_prices_bulk_async(self, updates: list) -> int:  # noqa: ANN401
            return len(updates)

    manager = _NoCompletionManager()
    poller = SnapshotPoller(manager)
    poller.calendar = _CalendarStub()  # type: ignore[assignment]
    poller._fetch_quotes = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "AAPL260220C00100000": {
                "bp": 1.0,
                "ap": 1.2,
                "last_price": 1.1,
                "underlying_price": 200.0,
            }
        }
    )

    errors: list[str] = []
    monkeypatch.setattr(poller_module.logger, "error", lambda message, **_kwargs: errors.append(message))

    async def _stop_after_first_sleep(_seconds: float) -> None:
        poller._running = False

    monkeypatch.setattr(poller_module.asyncio, "sleep", _stop_after_first_sleep)

    await poller.run()

    assert errors == []
