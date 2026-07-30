"""One alert must produce one watch, however many times it is delivered.

``create_watch`` mints a fresh ``watch_id`` on every call, so processing the same
``alert_id`` twice produced two independent watches. Both then poll the same
contract on their own schedule and each writes its own Gold label rows — so the
duplication lands in the training data, where Bronze's append-only compaction
cannot see or remove it.

Nothing triggered this while the consumer only ever read new messages. It becomes
reachable the moment anything redelivers: pending-message recovery, a restart
that replays unacknowledged work, or any at-least-once transport. Every one of
those is either already true or planned, so the guard has to exist first.

The claim must be atomic. A check-then-set would let the live path and a
redelivery both observe "no watch yet" and both create one.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon, WatchKeys

pytestmark = pytest.mark.unit

ALERT_TIME = datetime(2026, 1, 15, 14, 30, tzinfo=UTC)


class _FakeRedis:
    """Minimal Redis with real SET NX / EX semantics."""

    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._sets: dict[str, set[bytes]] = {}
        self.ttls: dict[str, int] = {}
        self.set_calls: list[tuple[str, bool]] = []

    @staticmethod
    def _b(value: str | bytes) -> bytes:
        return value if isinstance(value, bytes) else value.encode()

    def set(self, key: str, value: str | bytes, nx: bool = False, ex: int | None = None):
        self.set_calls.append((key, nx))
        if nx and key in self._kv:
            return None
        self._kv[key] = self._b(value)
        if ex is not None:
            self.ttls[key] = ex
        return True

    def get(self, key: str) -> bytes | None:
        return self._kv.get(key)

    def mget(self, keys: list[str]) -> list[bytes | None]:
        return [self._kv.get(k) for k in keys]

    def sadd(self, key: str, *values: str | bytes) -> None:
        self._sets.setdefault(key, set()).update(self._b(v) for v in values)

    def smembers(self, key: str) -> set[bytes]:
        return set(self._sets.get(key, set()))

    def srem(self, key: str, value: str | bytes) -> None:
        self._sets.get(key, set()).discard(self._b(value))

    def delete(self, key: str) -> None:
        self._kv.pop(key, None)
        self._sets.pop(key, None)


class _CalendarStub:
    def add_trading_hours(self, ts: datetime, hours: int) -> datetime:
        return ts + timedelta(hours=hours)


def _create(manager: WatchManager, alert_id: str = "alert-1", **overrides):
    kwargs = {
        "alert_id": alert_id,
        "occ_symbol": "AAPL260116C00200000",
        "underlying": "AAPL",
        "put_call": "C",
        "expiry": "2026-01-16",
        "strike": 200.0,
        "entry_price": 3.25,
        "spot_at_alert": 198.4,
        "alert_time": ALERT_TIME,
        "horizon": WatchHorizon.INTRADAY,
        "tp_threshold": 0.5,
        "sl_threshold": -0.3,
    }
    kwargs.update(overrides)
    return manager.create_watch(**kwargs)


@pytest.fixture
def manager() -> WatchManager:
    return WatchManager(redis_client=_FakeRedis(), calendar=_CalendarStub())


def test_same_alert_twice_yields_one_watch(manager: WatchManager) -> None:
    first = _create(manager)
    second = _create(manager)

    assert second.watch_id == first.watch_id
    assert len(manager.redis.smembers(WatchKeys.ACTIVE_WATCHES)) == 1


def test_distinct_alerts_still_get_distinct_watches(manager: WatchManager) -> None:
    first = _create(manager, alert_id="alert-1")
    second = _create(manager, alert_id="alert-2")

    assert first.watch_id != second.watch_id
    assert len(manager.redis.smembers(WatchKeys.ACTIVE_WATCHES)) == 2


def test_the_claim_is_atomic_not_check_then_set(manager: WatchManager) -> None:
    """A read followed by a write would let two deliveries both win the race."""
    _create(manager)

    claim_key = WatchKeys.by_alert_key("alert-1")
    nx_claims = [key for key, nx in manager.redis.set_calls if key == claim_key and nx]
    assert nx_claims, "the alert claim was not made with SET NX"


def test_the_claim_outlives_the_longest_watch(manager: WatchManager) -> None:
    """A LEAP watch runs 30 days; the guard must not expire before it does."""
    _create(manager, horizon=WatchHorizon.LEAP)

    ttl = manager.redis.ttls[WatchKeys.by_alert_key("alert-1")]
    assert ttl >= 30 * 86400


def test_a_dangling_claim_does_not_block_a_new_watch(manager: WatchManager) -> None:
    """If the watch is gone but the claim lingers, the alert is watchable again."""
    first = _create(manager)
    manager.redis.delete(WatchKeys.watch_key(first.watch_id))

    second = _create(manager)

    assert second.watch_id != first.watch_id
    assert manager.get_watch(second.watch_id) is not None


def test_returned_watch_is_the_stored_one(manager: WatchManager) -> None:
    """The second call returns real stored state, not a fresh in-memory object."""
    first = _create(manager)
    second = _create(manager, entry_price=99.0)

    assert second.entry_price == first.entry_price
    assert manager.get_watch(second.watch_id).entry_price == first.entry_price
