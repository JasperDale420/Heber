from __future__ import annotations

from datetime import UTC, datetime, timedelta

from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon


class _BytesRedis:
    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._sets: dict[str, set[bytes]] = {}

    @staticmethod
    def _to_bytes(value: str | bytes) -> bytes:
        if isinstance(value, bytes):
            return value
        return value.encode("utf-8")

    def set(self, key: str, value: str | bytes) -> None:
        self._kv[key] = self._to_bytes(value)

    def get(self, key: str) -> bytes | None:
        return self._kv.get(key)

    def sadd(self, key: str, *values: str | bytes) -> None:
        bucket = self._sets.setdefault(key, set())
        for value in values:
            bucket.add(self._to_bytes(value))

    def smembers(self, key: str) -> set[bytes]:
        return set(self._sets.get(key, set()))

    def srem(self, key: str, value: str | bytes) -> None:
        bucket = self._sets.get(key)
        if not bucket:
            return
        bucket.discard(self._to_bytes(value))

    def delete(self, key: str) -> None:
        self._kv.pop(key, None)
        self._sets.pop(key, None)


class _CalendarStub:
    def add_trading_hours(self, ts: datetime, hours: int) -> datetime:
        return ts + timedelta(hours=hours)


def _create_watch(manager: WatchManager):
    return manager.create_watch(
        alert_id="alert-1",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.5,
        spot_at_alert=190.0,
        alert_time=datetime.now(UTC),
        horizon=WatchHorizon.SWING,
        tp_threshold=0.25,
        sl_threshold=0.15,
    )


def test_get_active_watches_supports_redis_byte_ids() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)

    active = manager.get_active_watches()

    assert len(active) == 1
    assert active[0].watch_id == watch.watch_id


def test_get_watches_for_symbol_supports_redis_byte_ids() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)

    by_symbol = manager.get_watches_for_symbol("AAPL260220C00100000")

    assert len(by_symbol) == 1
    assert by_symbol[0].watch_id == watch.watch_id
