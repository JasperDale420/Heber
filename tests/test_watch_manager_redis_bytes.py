from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from heber.watch import manager as manager_module
from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon, WatchKeys, WatchStatus


class _BytesRedis:
    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._sets: dict[str, set[bytes]] = {}
        self._ttls: dict[str, int] = {}

    @staticmethod
    def _to_bytes(value: str | bytes) -> bytes:
        if isinstance(value, bytes):
            return value
        return value.encode("utf-8")

    def set(self, key: str, value: str | bytes, nx: bool = False, ex: int | None = None):
        """Mirror redis-py: NX refuses an existing key and returns None."""
        if nx and key in self._kv:
            return None
        self._kv[key] = self._to_bytes(value)
        if ex is not None:
            self._ttls[key] = ex
        return True

    def get(self, key: str) -> bytes | None:
        return self._kv.get(key)

    def mget(self, keys: list[str]) -> list[bytes | None]:
        return [self._kv.get(k) for k in keys]

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


def _create_zero_entry_watch(manager: WatchManager):
    return manager.create_watch(
        alert_id="alert-zero",
        occ_symbol="AAPL260220P00100000",
        underlying="AAPL",
        put_call="P",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=0.0,
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


class _CountingRedis(_BytesRedis):
    """Redis stub that counts mget/get calls for batching assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.mget_calls = 0
        self.get_calls = 0

    def get(self, key: str) -> bytes | None:
        self.get_calls += 1
        return super().get(key)

    def mget(self, keys: list[str]) -> list[bytes | None]:
        self.mget_calls += 1
        return super().mget(keys)


def _create_watch_for_symbol(manager: WatchManager, alert_id: str, occ_symbol: str):
    return manager.create_watch(
        alert_id=alert_id,
        occ_symbol=occ_symbol,
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


def test_get_watches_for_symbol_batches_redis_with_mget() -> None:
    redis_client = _CountingRedis()
    manager = WatchManager(redis_client=redis_client, calendar=_CalendarStub())
    occ_symbol = "AAPL260220C00100000"
    created = [_create_watch_for_symbol(manager, f"alert-{i}", occ_symbol) for i in range(5)]

    redis_client.mget_calls = 0
    redis_client.get_calls = 0

    by_symbol = manager.get_watches_for_symbol(occ_symbol)

    assert redis_client.mget_calls == 1
    assert redis_client.get_calls == 0
    assert len(by_symbol) == len(created)
    assert {watch.watch_id for watch in by_symbol} == {watch.watch_id for watch in created}


def test_get_watches_for_symbol_skips_malformed_payloads() -> None:
    redis_client = _CountingRedis()
    manager = WatchManager(redis_client=redis_client, calendar=_CalendarStub())
    occ_symbol = "AAPL260220C00100000"
    good = _create_watch_for_symbol(manager, "alert-good", occ_symbol)

    bad_id = "not-a-real-watch"
    redis_client.sadd(WatchKeys.by_symbol_key(occ_symbol), bad_id)
    redis_client.set(WatchKeys.watch_key(bad_id), b"{not-json")

    redis_client.mget_calls = 0
    redis_client.get_calls = 0

    by_symbol = manager.get_watches_for_symbol(occ_symbol)

    assert redis_client.mget_calls == 1
    assert redis_client.get_calls == 0
    assert len(by_symbol) == 1
    assert by_symbol[0].watch_id == good.watch_id


def test_get_watches_for_symbol_empty_index_skips_mget() -> None:
    redis_client = _CountingRedis()
    manager = WatchManager(redis_client=redis_client, calendar=_CalendarStub())

    by_symbol = manager.get_watches_for_symbol("AAPL260220C00100000")

    assert by_symbol == []
    assert redis_client.mget_calls == 0
    assert redis_client.get_calls == 0


def test_update_watch_price_handles_zero_entry_price() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_zero_entry_watch(manager)

    updated = manager.update_watch_price(
        watch.watch_id,
        current_price=0.5,
        timestamp=datetime.now(UTC),
    )

    assert updated is not None
    assert updated.current_price == 0.5
    assert updated.current_return is None
    assert updated.snapshot_count == 1


def test_update_watch_price_uses_snapshot_timestamp() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    snapshot_time = datetime(2026, 2, 9, 14, 30, tzinfo=UTC)

    updated = manager.update_watch_price(
        watch.watch_id,
        current_price=1.5,
        timestamp=snapshot_time,
    )

    assert updated is not None
    assert updated.updated_at == snapshot_time

    stored = manager.get_watch(watch.watch_id)
    assert stored is not None
    assert stored.updated_at == snapshot_time


def test_update_watch_price_preserves_zero_mfe_baseline() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    watch.mfe = 0.0
    watch.mae = 0.0
    manager._save_watch(watch)

    updated = manager.update_watch_price(
        watch.watch_id,
        current_price=0.9,
        timestamp=datetime.now(UTC),
    )

    assert updated is not None
    assert updated.current_return == pytest.approx((0.9 - watch.entry_price) / watch.entry_price)
    assert updated.mfe == 0.0
    assert updated.mae == pytest.approx((0.9 - watch.entry_price) / watch.entry_price)


def test_update_watch_price_preserves_zero_mae_baseline() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    watch.mfe = 0.0
    watch.mae = 0.0
    manager._save_watch(watch)

    updated = manager.update_watch_price(
        watch.watch_id,
        current_price=1.65,
        timestamp=datetime.now(UTC),
    )

    assert updated is not None
    assert updated.current_return == pytest.approx(0.1)
    assert updated.mfe == pytest.approx(0.1)
    assert updated.mae == 0.0


def test_cleanup_expired_handles_naive_window_end() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)

    watch.window_end = datetime.now() - timedelta(minutes=1)
    manager._save_watch(watch)

    expired = manager.cleanup_expired()

    assert expired == 1
    stored = manager.get_watch(watch.watch_id)
    assert stored is not None
    assert stored.status == WatchStatus.EXPIRED


def test_delete_watch_with_byte_id_removes_primary_watch_key() -> None:
    redis_client = _BytesRedis()
    manager = WatchManager(redis_client=redis_client, calendar=_CalendarStub())
    watch = _create_watch(manager)

    deleted = manager.delete_watch(watch.watch_id.encode("utf-8"))

    assert deleted is True
    assert manager.get_watch(watch.watch_id) is None
    assert manager.get_watches_for_symbol(watch.occ_symbol) == []


def test_save_watch_non_watching_removes_active_index_membership() -> None:
    redis_client = _BytesRedis()
    manager = WatchManager(redis_client=redis_client, calendar=_CalendarStub())
    watch = _create_watch(manager)

    watch.status = WatchStatus.EXPIRED
    manager._save_watch(watch)

    assert redis_client.smembers(WatchKeys.ACTIVE_WATCHES) == set()


def test_complete_watch_uses_provided_outcome_time() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    outcome_time = datetime(2026, 2, 9, 16, 0, tzinfo=UTC)

    completed = manager.complete_watch(
        watch.watch_id,
        WatchStatus.HIT_TP,
        outcome_return=0.25,
        bars_to_hit=3,
        outcome_time=outcome_time,
    )

    assert completed is not None
    assert completed.outcome_time == outcome_time
    stored = manager.get_watch(watch.watch_id)
    assert stored is not None
    assert stored.outcome_time == outcome_time


def test_complete_watch_non_finite_outcome_return_defaults_to_zero() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)

    completed = manager.complete_watch(
        watch.watch_id,
        WatchStatus.HIT_TP,
        outcome_return=float("nan"),
        bars_to_hit=2,
    )

    assert completed is not None
    assert completed.outcome_return == 0.0
    stored = manager.get_watch(watch.watch_id)
    assert stored is not None
    assert stored.outcome_return == 0.0


def test_cleanup_expired_non_finite_current_return_defaults_to_zero() -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    watch.current_return = float("nan")
    watch.window_end = datetime.now(UTC) - timedelta(minutes=1)
    manager.get_expired_watches = lambda: [watch]  # type: ignore[method-assign]

    expired = manager.cleanup_expired()

    assert expired == 1
    stored = manager.get_watch(watch.watch_id)
    assert stored is not None
    assert stored.status == WatchStatus.EXPIRED
    assert stored.outcome_return == 0.0


def test_complete_watch_expired_skips_per_watch_completion_log(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    info_messages: list[str] = []

    def _capture_info(message: str, **kwargs) -> None:  # noqa: ANN003
        info_messages.append(message)

    monkeypatch.setattr(manager_module.logger, "info", _capture_info)

    completed = manager.complete_watch(
        watch.watch_id,
        WatchStatus.EXPIRED,
        outcome_return=0.0,
    )

    assert completed is not None
    assert "Completed alert watch" not in info_messages


def test_complete_watch_hit_tp_keeps_completion_log(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = WatchManager(redis_client=_BytesRedis(), calendar=_CalendarStub())
    watch = _create_watch(manager)
    info_messages: list[str] = []

    def _capture_info(message: str, **kwargs) -> None:  # noqa: ANN003
        info_messages.append(message)

    monkeypatch.setattr(manager_module.logger, "info", _capture_info)

    completed = manager.complete_watch(
        watch.watch_id,
        WatchStatus.HIT_TP,
        outcome_return=0.25,
    )

    assert completed is not None
    assert "Completed alert watch" in info_messages
