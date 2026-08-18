"""Completing a watch and staging its outcome must be all-or-nothing.

Redis MULTI/EXEC is *not* a transaction in the rollback sense. Commands are
queued, then EXEC runs every one of them; a command that fails at execution
time — WRONGTYPE, say — returns an error while the commands queued around it
still take effect. So a pipeline that completes the watch and stages its
outcome can half-apply: the watch leaves the active set (no checker pass will
ever revisit it) while its outcome never reaches the pending-outcomes index
(no retry pass will ever find it). That watch's Gold label is then
unrecoverable — the exact loss the durable staging exists to prevent.

The fix routes both halves through a single Lua script, which Redis runs
atomically and which validates every key's type up front, so a bad key aborts
before anything is mutated and the watch stays active and retryable.

`_FakeRedis` below deliberately reproduces the awkward parts of the real
semantics: pipelines half-apply on a runtime error, and `eval` applies the
script's writes only after all its type checks pass.
"""

from __future__ import annotations

import os
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from heber.watch.checker import BarrierChecker
from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon, WatchKeys


class _WrongTypeError(Exception):
    """Stands in for redis.exceptions.ResponseError WRONGTYPE."""


class _MidScriptError(Exception):
    """A Redis command failing partway through the script (OOM, say). Lua gives
    isolation, not rollback, so writes already made by the script persist.
    """


class _FakeRedis:
    """Redis double with real type semantics: a key is a string or a set, and
    using the wrong command family on one raises rather than silently working.
    """

    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._sets: dict[str, set[bytes]] = {}
        self._lock = threading.Lock()
        self.fail_after_writes: int | None = None

    @staticmethod
    def _to_bytes(value: str | bytes) -> bytes:
        return value if isinstance(value, bytes) else value.encode("utf-8")

    def key_type(self, key: str) -> str:
        if key in self._kv:
            return "string"
        if key in self._sets:
            return "set"
        return "none"

    def _require(self, key: str, want: str) -> None:
        actual = self.key_type(key)
        if actual not in ("none", want):
            raise _WrongTypeError(f"WRONGTYPE {key} holds {actual}, expected {want}")

    def set(self, key: str, value: str | bytes, *, nx: bool = False, ex: int | None = None) -> bool | None:  # noqa: ARG002
        with self._lock:
            self._require(key, "string")
            if nx and key in self._kv:
                return None
            self._kv[key] = self._to_bytes(value)
            return True

    def get(self, key: str) -> bytes | None:
        with self._lock:
            self._require(key, "string")
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
            self._require(key, "set")
            bucket = self._sets.setdefault(key, set())
            for value in values:
                bucket.add(self._to_bytes(value))

    def srem(self, key: str, value: str | bytes) -> None:
        with self._lock:
            self._require(key, "set")
            bucket = self._sets.get(key)
            if bucket:
                bucket.discard(self._to_bytes(value))

    def smembers(self, key: str) -> set[bytes]:
        with self._lock:
            self._require(key, "set")
            return set(self._sets.get(key, set()))

    def lrange(self, key: str, start: int, end: int) -> list[bytes]:  # noqa: ARG002
        return []

    def eval(self, script: str, numkeys: int, *args: str) -> int:
        """Apply the complete-and-stage script atomically.

        Mirrors the contract the real Lua relies on: validate every key's type
        first, and only then perform the writes, so a WRONGTYPE aborts with
        nothing mutated.
        """
        keys = list(args[:numkeys])
        argv = list(args[numkeys:])
        watch_key, active_key, by_symbol_key, pending_outcome_key, pending_index_key = keys
        watch_json, watch_id, outcome_json, outcome_watch_id = argv
        with self._lock:
            for key, want in (
                (watch_key, "string"),
                (active_key, "set"),
                (by_symbol_key, "set"),
                (pending_outcome_key, "string"),
                (pending_index_key, "set"),
            ):
                actual = self.key_type(key)
                if actual not in ("none", want):
                    raise _WrongTypeError(f"WRONGTYPE {key} holds {actual}, expected {want}")

            # Same order as COMPLETE_AND_STAGE_LUA: stage, index, then retire.
            # fail_after_writes models a command failing mid-script (OOM, say),
            # which Lua does not roll back.
            writes = [
                lambda: self._kv.__setitem__(pending_outcome_key, self._to_bytes(outcome_json)),
                lambda: self._sets.setdefault(pending_index_key, set()).add(self._to_bytes(outcome_watch_id)),
                lambda: self._kv.__setitem__(watch_key, self._to_bytes(watch_json)),
                lambda: self._sets.get(active_key, set()).discard(self._to_bytes(watch_id)),
                lambda: self._sets.get(by_symbol_key, set()).discard(self._to_bytes(watch_id)),
            ]
            for i, write in enumerate(writes):
                if self.fail_after_writes is not None and i >= self.fail_after_writes:
                    raise _MidScriptError(f"script failed after {i} writes")
                write()
        return 1

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)


class _FakePipeline:
    """Redis MULTI/EXEC semantics: every queued command runs, and a runtime
    failure on one does not undo the others. EXEC re-raises the first error
    after the batch has already taken effect.
    """

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
        first_error: Exception | None = None
        for op, key, *rest in self._ops:
            try:
                if op == "set":
                    value, kwargs = rest
                    self._redis.set(key, value, **kwargs)
                elif op == "sadd":
                    (values,) = rest
                    self._redis.sadd(key, *values)
                elif op == "srem":
                    (values,) = rest
                    self._redis.srem(key, *values)
            except _WrongTypeError as exc:
                if first_error is None:
                    first_error = exc
        self._ops = []
        if first_error is not None:
            raise first_error


class _CalendarStub:
    def add_trading_hours(self, ts: datetime, hours: int) -> datetime:
        return ts + timedelta(hours=hours)

    def trading_minutes_until(self, start: datetime, end: datetime) -> int:
        return max(0, int((end - start).total_seconds() // 60))


def _create_already_expired_watch(manager: WatchManager, alert_id: str = "alert-1"):
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


@pytest.mark.unit
def test_bad_pending_index_type_leaves_the_watch_retryable() -> None:
    """A WRONGTYPE on the pending-outcomes index must not strand the watch.

    With the pending-outcomes index holding a string, staging cannot succeed.
    The watch must therefore stay in the active set so the next checker pass
    retries it. Half-applying — completing the watch but failing to stage —
    would drop that watch's Gold label permanently.
    """
    redis = _FakeRedis()
    manager = WatchManager(redis_client=redis, calendar=_CalendarStub())
    watch = _create_already_expired_watch(manager)
    assert watch is not None

    # Something out of band left a string where the set index belongs.
    redis._kv[WatchKeys.PENDING_OUTCOMES] = b"not-a-set"

    checker = BarrierChecker(manager, calendar=_CalendarStub())
    with pytest.raises(_WrongTypeError):
        checker.check_all()

    still_active = redis._sets.get(WatchKeys.ACTIVE_WATCHES, set())
    assert watch.watch_id.encode() in still_active, (
        "watch was completed even though its outcome could not be staged — its label is now unrecoverable"
    )


@pytest.mark.unit
def test_bad_symbol_index_type_leaves_the_watch_retryable() -> None:
    """The same guarantee for the other set the completion touches."""
    redis = _FakeRedis()
    manager = WatchManager(redis_client=redis, calendar=_CalendarStub())
    watch = _create_already_expired_watch(manager)
    assert watch is not None

    by_symbol = WatchKeys.by_symbol_key(watch.occ_symbol)
    redis._sets.pop(by_symbol, None)
    redis._kv[by_symbol] = b"not-a-set"

    checker = BarrierChecker(manager, calendar=_CalendarStub())
    with pytest.raises(_WrongTypeError):
        checker.check_all()

    assert watch.watch_id.encode() in redis._sets.get(WatchKeys.ACTIVE_WATCHES, set())
    assert watch.watch_id.encode() not in redis._sets.get(WatchKeys.PENDING_OUTCOMES, set())


@pytest.mark.unit
def test_healthy_path_completes_and_stages_together() -> None:
    """The ordinary case still completes the watch and stages its outcome."""
    redis = _FakeRedis()
    manager = WatchManager(redis_client=redis, calendar=_CalendarStub())
    watch = _create_already_expired_watch(manager)
    assert watch is not None

    checker = BarrierChecker(manager, calendar=_CalendarStub())
    outcomes = checker.check_all()

    assert len(outcomes) == 1
    assert watch.watch_id.encode() not in redis._sets.get(WatchKeys.ACTIVE_WATCHES, set())
    assert watch.watch_id.encode() in redis._sets.get(WatchKeys.PENDING_OUTCOMES, set())
    assert len(manager.get_pending_outcomes()) == 1


@pytest.mark.unit
@pytest.mark.parametrize("fail_after", [0, 1, 2, 3, 4])
def test_no_prefix_of_the_script_can_strand_a_watch(fail_after: int) -> None:
    """Lua gives isolation, not rollback — a command can fail partway through
    and leave the earlier writes applied. The writes are ordered so that every
    such prefix is still recoverable: either the watch is still active (so the
    checker retries it) or its outcome is indexed (so the retry pass finds it).
    Never neither.
    """
    redis = _FakeRedis()
    manager = WatchManager(redis_client=redis, calendar=_CalendarStub())
    watch = _create_already_expired_watch(manager)
    assert watch is not None
    redis.fail_after_writes = fail_after

    checker = BarrierChecker(manager, calendar=_CalendarStub())
    with pytest.raises(_MidScriptError):
        checker.check_all()

    wid = watch.watch_id.encode()
    still_active = wid in redis._sets.get(WatchKeys.ACTIVE_WATCHES, set())
    outcome_indexed = wid in redis._sets.get(WatchKeys.PENDING_OUTCOMES, set())
    assert still_active or outcome_indexed, (
        f"failing after {fail_after} writes stranded the watch: "
        "not active, so nothing rechecks it; not indexed, so nothing retries it"
    )


_NAMESPACED_KEYS = (
    "WATCH",
    "ACTIVE_WATCHES",
    "BY_SYMBOL",
    "EXPIRING",
    "SNAPSHOTS",
    "BY_ALERT",
    "PENDING_OUTCOME",
    "PENDING_OUTCOMES",
)


@pytest.fixture
def namespaced_watch_keys(monkeypatch: pytest.MonkeyPatch) -> str:
    """Point every WatchKeys constant at a per-run namespace.

    Without this, a real-Redis test writes and deletes the same global keys the
    live watch service uses (`heber:watches:active`, `heber:watch:pending_outcomes`),
    so a mispointed HEBER_TEST_REDIS_URL would destroy production watch state.
    Namespacing removes the possibility rather than guarding against it — the
    test cannot name a production key even if aimed at production.
    """
    namespace = f"heber-test:{uuid.uuid4().hex}"
    for attr in _NAMESPACED_KEYS:
        original = getattr(WatchKeys, attr)
        monkeypatch.setattr(WatchKeys, attr, original.replace("heber:", f"{namespace}:", 1))
    return namespace


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("HEBER_TEST_REDIS_URL"),
    reason="set HEBER_TEST_REDIS_URL to a scratch Redis to run this",
)
def test_lua_against_real_redis(namespaced_watch_keys: str) -> None:
    """Exercise the actual Lua on a real Redis, both paths.

    The happy path is asserted first and deliberately: a script with a syntax
    error would make *every* call raise, so a test that only checked the
    WRONGTYPE abort would pass on a completely broken script.

    Every key this touches lives under a per-run namespace (see the fixture),
    so it cannot read or delete live watch state even if HEBER_TEST_REDIS_URL
    is pointed somewhere it should not be. Spin up a scratch instance with
    `docker run -d --rm -p 6399:6379 redis:7-alpine`.
    """
    import redis as redis_lib

    client = redis_lib.from_url(os.environ["HEBER_TEST_REDIS_URL"])
    try:
        # Happy path: the script completes the watch and stages its outcome.
        manager = WatchManager(redis_client=client, calendar=_CalendarStub())
        watch = _create_already_expired_watch(manager, alert_id="alert-happy")
        assert watch is not None
        outcomes = BarrierChecker(manager, calendar=_CalendarStub()).check_all()
        assert len(outcomes) == 1
        assert not client.sismember(WatchKeys.ACTIVE_WATCHES, watch.watch_id)
        assert client.sismember(WatchKeys.PENDING_OUTCOMES, watch.watch_id)
        assert len(manager.get_pending_outcomes()) == 1

        # WRONGTYPE path: nothing is mutated, the watch stays retryable.
        client.delete(WatchKeys.PENDING_OUTCOMES)
        client.set(WatchKeys.PENDING_OUTCOMES, "not-a-set")
        manager = WatchManager(redis_client=client, calendar=_CalendarStub())
        watch = _create_already_expired_watch(manager, alert_id="alert-wrongtype")
        assert watch is not None

        with pytest.raises(redis_lib.exceptions.ResponseError):
            BarrierChecker(manager, calendar=_CalendarStub()).check_all()

        assert client.sismember(WatchKeys.ACTIVE_WATCHES, watch.watch_id), (
            "watch must stay active and retryable when its outcome cannot be staged"
        )
        assert client.type(WatchKeys.PENDING_OUTCOMES) == b"string", "the bad key must be left untouched"
    finally:
        for key in client.scan_iter(match=f"{namespaced_watch_keys}:*"):
            client.delete(key)
