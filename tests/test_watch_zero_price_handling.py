from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from heber.watch.checker import BarrierChecker
from heber.watch.models import AlertWatch, WatchHorizon, WatchSnapshot, WatchStatus
from heber.watch.poller import SnapshotPoller


class _CheckerManagerStub:
    def __init__(self, snapshots: list[WatchSnapshot]) -> None:
        self._snapshots = snapshots
        self.completed: tuple[str, WatchStatus, float, int | None] | None = None

    def get_snapshots(self, _watch_id: str) -> list[WatchSnapshot]:
        return self._snapshots

    def complete_watch(
        self,
        watch_id: str,
        status: WatchStatus,
        outcome_return: float,
        bars_to_hit: int | None = None,
    ) -> None:
        self.completed = (watch_id, status, outcome_return, bars_to_hit)


class _CalendarStub:
    def trading_minutes_until(self, start: datetime, end: datetime) -> int:
        return max(0, int((end - start).total_seconds() // 60))


def _build_watch() -> AlertWatch:
    now = datetime.now(UTC)
    return AlertWatch(
        watch_id="watch-1",
        alert_id="alert-1",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=now - timedelta(minutes=30),
        window_end=now + timedelta(hours=1),
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )


def test_checker_treats_zero_mid_price_as_valid_return_path() -> None:
    watch = _build_watch()
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=datetime.now(UTC),
            mid_px=0.0,
            return_pct=None,
        )
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.HIT_SL
    assert outcome.outcome_return == -1.0
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.HIT_SL


def test_poller_snapshot_sets_return_pct_when_mid_is_zero() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": 0.0,
            "ap": 0.0,
            "last_price": 0.0,
            "underlying_price": 200.0,
        },
    )

    assert snapshot.mid_px == 0.0
    assert snapshot.return_pct == -1.0
