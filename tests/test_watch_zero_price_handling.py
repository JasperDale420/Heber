from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from heber.watch.checker import BarrierChecker
from heber.watch.models import AlertWatch, WatchHorizon, WatchSnapshot, WatchStatus
from heber.watch.poller import SnapshotPoller


class _CheckerManagerStub:
    def __init__(self, snapshots: list[WatchSnapshot]) -> None:
        self._snapshots = snapshots
        self.completed: tuple[str, WatchStatus, float, int | None, datetime | None] | None = None

    def get_snapshots(self, _watch_id: str) -> list[WatchSnapshot]:
        return self._snapshots

    def complete_watch_and_stage_outcome(
        self,
        watch: AlertWatch,
        status: WatchStatus,
        outcome_return: float,
        bars_to_hit: int | None,
        outcome_time: datetime | None,
        outcome: object,
    ) -> None:
        self.completed = (watch.watch_id, status, outcome_return, bars_to_hit, outcome_time)


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


def test_poller_snapshot_normalizes_numeric_string_quotes() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": "0.0",
            "ap": "0.0",
            "last_price": "0.0",
            "underlying_price": "200.5",
        },
    )

    assert snapshot.bid_px == 0.0
    assert snapshot.ask_px == 0.0
    assert snapshot.mid_px == 0.0
    assert snapshot.last_px == 0.0
    assert snapshot.underlying_price == 200.5
    assert snapshot.return_pct == -1.0


def test_poller_snapshot_ignores_non_numeric_bid_ask_and_uses_last() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": "N/A",
            "ap": "bad",
            "last_price": "1.25",
            "underlying_price": "200.0",
        },
    )

    assert snapshot.bid_px is None
    assert snapshot.ask_px is None
    assert snapshot.mid_px == 1.25
    assert snapshot.last_px == 1.25
    assert snapshot.return_pct == 0.25


def test_poller_snapshot_uses_quote_timestamp_when_present() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": 1.0,
            "ap": 1.2,
            "timestamp": "2026-02-09T14:30:00Z",
        },
    )

    assert snapshot.timestamp == datetime(2026, 2, 9, 14, 30, tzinfo=UTC)


def test_poller_snapshot_treats_non_finite_quote_values_as_missing() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": "NaN",
            "ap": "inf",
            "last_price": "1.25",
            "underlying_price": "-inf",
        },
    )

    assert snapshot.bid_px is None
    assert snapshot.ask_px is None
    assert snapshot.mid_px == 1.25
    assert snapshot.last_px == 1.25
    assert snapshot.underlying_price is None
    assert snapshot.return_pct == 0.25


def test_poller_snapshot_treats_boolean_quote_values_as_missing() -> None:
    poller = SnapshotPoller(SimpleNamespace())
    watch = _build_watch()

    snapshot = poller._create_snapshot(
        watch,
        {
            "bp": True,
            "ap": False,
            "last_price": "1.25",
            "underlying_price": True,
        },
    )

    assert snapshot.bid_px is None
    assert snapshot.ask_px is None
    assert snapshot.mid_px == 1.25
    assert snapshot.last_px == 1.25
    assert snapshot.underlying_price is None
    assert snapshot.return_pct == 0.25


def test_checker_expires_zero_entry_watch_even_without_return_path() -> None:
    now = datetime.now(UTC)
    watch = AlertWatch(
        watch_id="watch-zero-entry",
        alert_id="alert-zero-entry",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=0.0,
        spot_at_alert=200.0,
        alert_time=now - timedelta(hours=2),
        window_end=now - timedelta(minutes=5),
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=now - timedelta(minutes=30),
            mid_px=1.0,
            return_pct=None,
        )
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED
    assert outcome.outcome_return == 0.0
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.EXPIRED


def test_checker_handles_naive_alert_and_window_timestamps() -> None:
    now = datetime.now()
    watch = AlertWatch(
        watch_id="watch-naive-time",
        alert_id="alert-naive-time",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=0.0,
        spot_at_alert=200.0,
        alert_time=now - timedelta(hours=2),
        window_end=now - timedelta(minutes=5),
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=datetime.now(UTC),
            mid_px=1.0,
            return_pct=None,
        )
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.EXPIRED


def test_checker_ignores_non_finite_snapshot_returns() -> None:
    now = datetime.now(UTC)
    watch = AlertWatch(
        watch_id="watch-non-finite",
        alert_id="alert-non-finite",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=0.0,
        spot_at_alert=200.0,
        alert_time=now - timedelta(hours=2),
        window_end=now - timedelta(minutes=5),
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=now - timedelta(minutes=15),
            mid_px=None,
            return_pct=float("nan"),
        )
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED
    assert outcome.outcome_return == 0.0
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.EXPIRED


def test_checker_sorts_snapshots_by_timestamp_before_barrier_detection() -> None:
    now = datetime.now(UTC)
    watch = _build_watch()
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=now + timedelta(minutes=5),
            mid_px=1.4,  # TP hit, but later in time
            return_pct=None,
        ),
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=now - timedelta(minutes=5),
            mid_px=0.8,  # SL hit first in time
            return_pct=None,
        ),
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.HIT_SL
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.HIT_SL


def test_checker_uses_barrier_snapshot_time_for_outcome_metadata() -> None:
    base = datetime(2026, 2, 9, 14, 0, tzinfo=UTC)
    watch = AlertWatch(
        watch_id="watch-hit-time",
        alert_id="alert-hit-time",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=base,
        window_end=base + timedelta(hours=4),
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )
    hit_time = base + timedelta(minutes=20)
    snapshots = [
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=base + timedelta(minutes=5),
            mid_px=1.05,
            return_pct=None,
        ),
        WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=hit_time,
            mid_px=1.30,
            return_pct=None,
        ),
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.HIT_TP
    assert outcome.outcome_time == hit_time
    assert outcome.trading_minutes_to_hit == 20
    assert manager.completed is not None
    assert manager.completed[4] == hit_time


def test_checker_uses_window_end_for_expired_outcome_metadata() -> None:
    now = datetime.now(UTC)
    alert_time = now - timedelta(hours=6)
    window_end = now - timedelta(hours=2)
    watch = AlertWatch(
        watch_id="watch-expired-time",
        alert_id="alert-expired-time",
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
            timestamp=alert_time + timedelta(minutes=30),
            mid_px=1.05,
            return_pct=None,
        )
    ]
    manager = _CheckerManagerStub(snapshots)
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED
    assert outcome.outcome_time == window_end
    assert outcome.trading_minutes_to_hit == 240
    assert manager.completed is not None
    assert manager.completed[4] == window_end


def test_checker_expires_watch_without_snapshots_after_window_end() -> None:
    now = datetime.now(UTC)
    alert_time = now - timedelta(hours=3)
    window_end = now - timedelta(minutes=10)
    watch = AlertWatch(
        watch_id="watch-no-snapshots-expired",
        alert_id="alert-no-snapshots-expired",
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
    manager = _CheckerManagerStub([])
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcome = checker.check_watch(watch)

    assert outcome is not None
    assert outcome.status == WatchStatus.EXPIRED
    assert outcome.outcome_time == window_end
    assert outcome.outcome_return == 0.0
    assert manager.completed is not None
    assert manager.completed[1] == WatchStatus.EXPIRED
    assert manager.completed[4] == window_end
