"""A completed watch's label must survive a failed/interrupted Gold write.

BarrierChecker.check_watch calls WatchManager.complete_watch — removing the
watch from Redis's active set — before the caller ever hands the resulting
WatchOutcome to LabelWriter.write_outcomes. If that write raises (a transient
Parquet/filesystem failure), the outcome only ever lived in a local Python
variable for that one loop iteration: the watch is already gone from the
active set, so no future checker pass revisits it, and the label is silently
dropped forever. See CHANGELOG.md [Unreleased] and the Codex adversarial
review that surfaced this as a pre-existing gap, independent of the
cleanup_expired fix in the same area.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from heber.watch.checker import BarrierChecker
from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon
from heber.watch.writer import LabelWriter, WatchService


class _FakeRedis:
    """Minimal Redis double: real SET/GET/SET-ops semantics, no network."""

    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._sets: dict[str, set[bytes]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _to_bytes(value: str | bytes) -> bytes:
        return value if isinstance(value, bytes) else value.encode("utf-8")

    def set(self, key: str, value: str | bytes, *, nx: bool = False, ex: int | None = None) -> bool | None:
        with self._lock:
            if nx and key in self._kv:
                return None
            self._kv[key] = self._to_bytes(value)
            return True

    def get(self, key: str) -> bytes | None:
        with self._lock:
            return self._kv.get(key)

    def mget(self, keys: list[str]) -> list[bytes | None]:
        with self._lock:
            return [self._kv.get(k) for k in keys]

    def delete(self, key: str) -> None:
        with self._lock:
            self._kv.pop(key, None)
            self._sets.pop(key, None)

    def sadd(self, key: str, *values: str | bytes) -> None:
        with self._lock:
            bucket = self._sets.setdefault(key, set())
            for value in values:
                bucket.add(self._to_bytes(value))

    def srem(self, key: str, value: str | bytes) -> None:
        with self._lock:
            bucket = self._sets.get(key)
            if bucket:
                bucket.discard(self._to_bytes(value))

    def smembers(self, key: str) -> set[bytes]:
        with self._lock:
            return set(self._sets.get(key, set()))

    def lrange(self, key: str, start: int, end: int) -> list[bytes]:
        # No test here exercises add_snapshot; watches are checked with an
        # empty snapshot list (the "expired with no snapshots" path).
        return []

    def eval(self, script: str, numkeys: int, *args: str) -> int:  # noqa: ARG002
        """Apply the complete-and-stage Lua atomically (see manager.COMPLETE_AND_STAGE_LUA)."""
        keys = list(args[:numkeys])
        watch_json, watch_id, outcome_json, outcome_watch_id = list(args[numkeys:])
        watch_key, active_key, by_symbol_key, pending_outcome_key, pending_index_key = keys
        with self._lock:
            self._kv[watch_key] = self._to_bytes(watch_json)
            for bucket_key in (active_key, by_symbol_key):
                bucket = self._sets.get(bucket_key)
                if bucket:
                    bucket.discard(self._to_bytes(watch_id))
            self._kv[pending_outcome_key] = self._to_bytes(outcome_json)
            self._sets.setdefault(pending_index_key, set()).add(self._to_bytes(outcome_watch_id))
        return 1

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)


class _FakePipeline:
    """Queues set/sadd calls and applies them on execute(), like redis-py."""

    def __init__(self, redis: _FakeRedis) -> None:
        self._redis = redis
        self._ops: list[tuple] = []

    def set(self, key: str, value: str | bytes, **kwargs) -> _FakePipeline:  # noqa: ANN003
        self._ops.append(("set", key, value, kwargs))
        return self

    def sadd(self, key: str, *values: str | bytes) -> _FakePipeline:
        self._ops.append(("sadd", key, values))
        return self

    def srem(self, key: str, value: str | bytes) -> _FakePipeline:
        self._ops.append(("srem", key, (value,)))
        return self

    def execute(self) -> None:
        for op, key, *rest in self._ops:
            if op == "set":
                value, kwargs = rest
                self._redis.set(key, value, **kwargs)
            elif op == "sadd":
                (values,) = rest
                self._redis.sadd(key, *values)
            elif op == "srem":
                (values,) = rest
                self._redis.srem(key, *values)
        self._ops = []


def _boom_eval(*_args, **_kwargs) -> int:  # noqa: ANN003
    """Models the EVAL never reaching the server — a connection drop before
    Redis runs the script, so none of its writes take effect.
    """
    raise ConnectionError("redis unreachable")


class _CalendarStub:
    def add_trading_hours(self, ts: datetime, hours: int) -> datetime:
        return ts + timedelta(hours=hours)

    def trading_minutes_until(self, start: datetime, end: datetime) -> int:
        return max(0, int((end - start).total_seconds() // 60))


def _make_manager() -> WatchManager:
    return WatchManager(redis_client=_FakeRedis(), calendar=_CalendarStub())


def _create_already_expired_watch(manager: WatchManager, alert_id: str = "alert-1"):
    """A watch whose window already ended, so check_all() completes it with
    no snapshots needed (the _handle_no_snapshots path in BarrierChecker).
    """
    alert_time = datetime.now(UTC) - timedelta(days=1)
    return manager.create_watch(
        alert_id=alert_id,
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.0,
        spot_at_alert=200.0,
        alert_time=alert_time,
        horizon=WatchHorizon.INTRADAY,
        tp_threshold=0.25,
        sl_threshold=0.10,
    )


def _make_service(manager: WatchManager, checker: BarrierChecker, writer: LabelWriter) -> WatchService:
    service = WatchService.__new__(WatchService)
    service.manager = manager
    service.checker = checker
    service.writer = writer
    service._running = True
    return service


async def _run_one_tick(service: WatchService, monkeypatch: pytest.MonkeyPatch) -> None:
    """Drive WatchService._check_and_write_loop for exactly one iteration."""

    async def _stop_after_sleep(_delay: float) -> None:
        service._running = False

    monkeypatch.setattr(asyncio, "sleep", _stop_after_sleep)
    await service._check_and_write_loop()


@pytest.mark.unit
def test_pending_outcome_stage_get_clear_roundtrip() -> None:
    """WatchManager's durable pending-outcome store is the retry mechanism."""
    manager = _make_manager()
    watch = _create_already_expired_watch(manager)
    assert watch is not None
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    outcomes = checker.check_all()

    assert len(outcomes) == 1
    pending = manager.get_pending_outcomes()
    assert [o.watch_id for o in pending] == [watch.watch_id]

    manager.clear_pending_outcome(watch.watch_id)
    assert manager.get_pending_outcomes() == []


@pytest.mark.unit
async def test_write_failure_does_not_permanently_lose_the_label(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,  # noqa: ANN001
) -> None:
    """A transient Parquet write failure must not silently drop the label.

    Tick 1: the barrier check completes the watch (removing it from the
    active set) and the Gold write fails. The outcome must still be
    retrievable afterward — it must not have existed only in a local
    variable that's now gone.

    Tick 2 (simulating the next loop pass, or a restart that resumes from
    the durable store): with the write no longer failing, the same pending
    outcome is retried and lands in Gold exactly once — not zero (lost) and
    not twice (duplicated by the buffer-then-outbox interaction).
    """
    manager = _make_manager()
    watch = _create_already_expired_watch(manager)
    assert watch is not None
    checker = BarrierChecker(manager, calendar=_CalendarStub())
    writer = LabelWriter(output_path=tmp_path)
    service = _make_service(manager, checker, writer)

    original_to_parquet = pd.DataFrame.to_parquet
    calls = {"n": 0}

    def _flaky_to_parquet(self, path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("simulated write failure")
        return original_to_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _flaky_to_parquet)

    # Tick 1: check_all() completes+removes the watch from Redis's active
    # set, then the Gold write fails.
    await _run_one_tick(service, monkeypatch)

    committed = list(tmp_path.rglob("*.parquet"))
    assert committed == [], "the failed write must not have committed a partial file"
    assert manager.get_active_watches() == [], "the watch is done being watched either way"

    pending_after_failure = manager.get_pending_outcomes()
    assert [o.watch_id for o in pending_after_failure] == [watch.watch_id], (
        "the outcome must still be durably staged after the write failure — "
        "this is what makes it retryable instead of silently gone"
    )

    # Tick 2: retry. check_all() finds nothing new (the watch is no longer
    # active), but the loop must still retry what's pending from tick 1.
    service._running = True
    await _run_one_tick(service, monkeypatch)

    committed = list(tmp_path.rglob("*.parquet"))
    assert len(committed) == 1
    rows = pd.read_parquet(committed[0])
    assert len(rows) == 1, "retry must not duplicate the row that failed last tick"
    assert rows.iloc[0]["watch_id"] == watch.watch_id

    assert manager.get_pending_outcomes() == [], "cleared once durably written"


@pytest.mark.unit
def test_atomic_write_failure_leaves_watch_active_instead_of_done_but_unstaged() -> None:
    """Completing a watch and staging its outcome must be one atomic step.

    Before this, complete_watch and stage_pending_outcome were separate
    writes: a crash between them left the watch off the active set with no
    trace of its outcome anywhere retryable — done, but unfindable. If the
    Redis transaction that does both never lands, the watch must come back
    unchanged (still WATCHING, still active) so the next checker pass
    retries it from scratch, rather than being half-completed.
    """
    manager = _make_manager()
    watch = _create_already_expired_watch(manager)
    assert watch is not None
    checker = BarrierChecker(manager, calendar=_CalendarStub())

    manager.redis.eval = _boom_eval  # type: ignore[method-assign]

    with pytest.raises(ConnectionError):
        checker.check_all()

    active = manager.get_active_watches()
    assert [w.watch_id for w in active] == [watch.watch_id], "must stay retryable, not vanish"
    assert manager.get_pending_outcomes() == []


@pytest.mark.unit
async def test_crash_before_clear_does_not_duplicate_the_row_on_retry(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,  # noqa: ANN001
) -> None:
    """A crash between a successful Gold write and clearing its pending
    entry must not produce a duplicate row when the still-pending outcome
    is retried on the next tick (or after a restart).
    """
    manager = _make_manager()
    watch = _create_already_expired_watch(manager)
    assert watch is not None
    checker = BarrierChecker(manager, calendar=_CalendarStub())
    writer = LabelWriter(output_path=tmp_path)
    service = _make_service(manager, checker, writer)

    original_clear = manager.clear_pending_outcome
    manager.clear_pending_outcome = lambda watch_id: None  # type: ignore[method-assign]

    # Tick 1: writes successfully, then "crashes" before the clear lands.
    await _run_one_tick(service, monkeypatch)

    committed = list(tmp_path.rglob("*.parquet"))
    assert len(committed) == 1
    assert [o.watch_id for o in manager.get_pending_outcomes()] == [watch.watch_id]

    # "Restart": clear works again, and the still-pending outcome is retried.
    manager.clear_pending_outcome = original_clear
    writer2 = LabelWriter(output_path=tmp_path)
    service2 = _make_service(manager, checker, writer2)
    await _run_one_tick(service2, monkeypatch)

    committed = list(tmp_path.rglob("*.parquet"))
    assert len(committed) == 1, "retry must overwrite the same file, not add a second one"
    rows = pd.read_parquet(committed[0])
    assert len(rows) == 1
    assert rows.iloc[0]["watch_id"] == watch.watch_id
    assert manager.get_pending_outcomes() == []
