"""Alert-level idempotency for watch creation.

A re-delivered flow alert must not mint a second watch: duplicate watches each
complete independently and each writes its own Gold label row, silently
inflating training sets.
"""

from __future__ import annotations

import math
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from heber.ops import metrics
from heber.watch.manager import WatchManager
from heber.watch.models import (
    ALERT_CLAIM_TTL_SECONDS,
    POLL_CONFIG,
    WatchHorizon,
    WatchKeys,
)


class _FakeRedis:
    """Minimal Redis double with real SET NX/EX semantics, serialized like Redis."""

    def __init__(self) -> None:
        self._kv: dict[str, bytes] = {}
        self._ttls: dict[str, int] = {}
        self._sets: dict[str, set[bytes]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _to_bytes(value: str | bytes) -> bytes:
        return value if isinstance(value, bytes) else value.encode("utf-8")

    def set(
        self,
        key: str,
        value: str | bytes,
        *,
        nx: bool = False,
        ex: int | None = None,
    ) -> bool | None:
        with self._lock:
            if nx and key in self._kv:
                return None
            self._kv[key] = self._to_bytes(value)
            if ex is not None:
                self._ttls[key] = ex
            return True

    def get(self, key: str) -> bytes | None:
        with self._lock:
            return self._kv.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._kv.pop(key, None)
            self._ttls.pop(key, None)
            self._sets.pop(key, None)

    def ttl(self, key: str) -> int:
        with self._lock:
            return self._ttls.get(key, -1)

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


class _CalendarStub:
    def add_trading_hours(self, ts: datetime, hours: int) -> datetime:
        return ts + timedelta(hours=hours)


def _make_manager() -> WatchManager:
    return WatchManager(redis_client=_FakeRedis(), calendar=_CalendarStub())


def _create(manager: WatchManager, alert_id: str = "alert-1"):
    return manager.create_watch(
        alert_id=alert_id,
        occ_symbol="SPY260417C00680000",
        underlying="SPY",
        put_call="C",
        expiry="2026-04-17",
        strike=680.0,
        entry_price=1.5,
        spot_at_alert=670.0,
        alert_time=datetime.now(UTC),
        horizon=WatchHorizon.SWING,
        tp_threshold=0.25,
        sl_threshold=0.15,
    )


def _suppressed_count() -> float:
    return metrics.watch_duplicate_alerts_suppressed_total._value.get()


def test_same_alert_delivered_twice_creates_one_watch() -> None:
    manager = _make_manager()

    first = _create(manager)
    second = _create(manager)

    assert first is not None
    assert second is None
    assert manager.redis.smembers(WatchKeys.ACTIVE_WATCHES) == {first.watch_id.encode()}


def test_duplicate_alert_increments_suppressed_counter() -> None:
    manager = _make_manager()
    before = _suppressed_count()

    _create(manager)
    _create(manager)

    assert _suppressed_count() == before + 1


def test_different_alerts_still_create_separate_watches() -> None:
    manager = _make_manager()

    first = _create(manager, alert_id="alert-a")
    second = _create(manager, alert_id="alert-b")

    assert first is not None
    assert second is not None
    assert first.watch_id != second.watch_id
    assert len(manager.redis.smembers(WatchKeys.ACTIVE_WATCHES)) == 2


def test_concurrent_duplicate_deliveries_create_one_watch() -> None:
    manager = _make_manager()

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: _create(manager), range(64)))

    created = [w for w in results if w is not None]
    assert len(created) == 1
    assert manager.redis.smembers(WatchKeys.ACTIVE_WATCHES) == {created[0].watch_id.encode()}


def test_alert_claim_carries_ttl_beyond_longest_watch_window() -> None:
    manager = _make_manager()
    watch = _create(manager)

    assert watch is not None
    key = WatchKeys.by_alert_key("alert-1")
    assert manager.redis.get(key) == watch.watch_id.encode()
    assert manager.redis.ttl(key) == ALERT_CLAIM_TTL_SECONDS

    # A LEAP watch runs 720 *trading* hours. At 6.5h sessions that is 111 sessions,
    # which spans ~156 calendar days before holidays — far longer than the naive
    # "720 hours is six weeks" reading. The claim must outlive the whole window.
    leap_hours = POLL_CONFIG[WatchHorizon.LEAP]["max_duration_hours"]
    sessions = math.ceil(leap_hours / 6.5)
    leap_window_days = sessions / 5 * 7
    assert ALERT_CLAIM_TTL_SECONDS > leap_window_days * 24 * 3600


def test_failed_watch_save_releases_the_claim_for_retry() -> None:
    """A crash after claiming must not ACK the alert away as a phantom duplicate."""
    manager = _make_manager()
    original_set = manager.redis.set
    calls = {"n": 0}

    def _failing_set(key: str, value, **kwargs):
        if key.startswith("heber:watch:") and "by_alert" not in key:
            calls["n"] += 1
            raise ConnectionError("redis write failed")
        return original_set(key, value, **kwargs)

    manager.redis.set = _failing_set  # type: ignore[method-assign]

    with pytest.raises(ConnectionError):
        _create(manager)

    assert calls["n"] == 1
    assert manager.redis.get(WatchKeys.by_alert_key("alert-1")) is None

    manager.redis.set = original_set  # type: ignore[method-assign]
    assert _create(manager) is not None


def test_partially_saved_watch_is_removed_before_the_claim_is_released() -> None:
    """Releasing a claim while a half-written watch is still active would allow two."""
    manager = _make_manager()
    original_sadd = manager.redis.sadd

    def _failing_sadd(key: str, *values):
        if key.startswith(WatchKeys.BY_SYMBOL.split("{")[0]):
            raise ConnectionError("redis write failed")
        return original_sadd(key, *values)

    manager.redis.sadd = _failing_sadd  # type: ignore[method-assign]

    with pytest.raises(ConnectionError):
        _create(manager)

    assert manager.redis.smembers(WatchKeys.ACTIVE_WATCHES) == set()
    assert manager.redis.get(WatchKeys.by_alert_key("alert-1")) is None

    manager.redis.sadd = original_sadd  # type: ignore[method-assign]
    retried = _create(manager)
    assert retried is not None
    assert manager.redis.smembers(WatchKeys.ACTIVE_WATCHES) == {retried.watch_id.encode()}


def test_claim_is_retained_when_rollback_itself_fails() -> None:
    """Fail closed: an unreleasable claim loses one label, never admits a duplicate."""
    manager = _make_manager()

    def _boom(*args, **kwargs):
        raise ConnectionError("redis unreachable")

    manager.redis.sadd = _boom  # type: ignore[method-assign]
    manager.redis.delete = _boom  # type: ignore[method-assign]

    with pytest.raises(ConnectionError):
        _create(manager)

    assert manager.redis.get(WatchKeys.by_alert_key("alert-1")) is not None


def test_completed_watch_does_not_release_the_alert_claim() -> None:
    """A finished alert must never be re-watched — the claim outlives completion."""
    manager = _make_manager()
    watch = _create(manager)
    assert watch is not None

    from heber.watch.models import WatchStatus

    manager.complete_watch(watch.watch_id, WatchStatus.HIT_TP, 0.3)

    assert _create(manager) is None


def test_duplicate_batch_classifies_as_success_without_retry() -> None:
    from heber.watch.consumer import AlertWatchConsumer

    assert AlertWatchConsumer._classify_alert_results(0, 0, 0, 0, duplicates=2) == (
        True,
        False,
        "duplicate_alert",
    )


def _alert(alert_id: str = "alert-dup") -> dict:
    return {
        "id": alert_id,
        "occ_symbol": "SPY260417C00680000",
        "underlying": "SPY",
        "put_call": "C",
        "expiry": "2026-04-17",
        "strike": 680.0,
        "spot_px": 670.0,
        "dte": 5,
        "ts_event": datetime.now(UTC).isoformat(),
    }


def _stub_consumer(manager: WatchManager, extracted: list[str], priced: list[str]):
    from heber.watch.consumer import AlertWatchConsumer

    consumer = AlertWatchConsumer.__new__(AlertWatchConsumer)
    consumer.manager = manager
    consumer.config = type("_Cfg", (), {"tp_pct": 0.25, "sl_pct": 0.15})()

    async def _resolve_entry_price(alert: dict) -> tuple[float, str]:
        priced.append(alert["id"])
        return 1.5, "contract_px"

    async def _extract(alert: dict, watch_id: str) -> None:
        extracted.append(watch_id)

    consumer._resolve_entry_price = _resolve_entry_price  # type: ignore[method-assign]
    consumer._extract_and_store_features = _extract  # type: ignore[method-assign]
    return consumer


async def test_consumer_returns_duplicate_alert_and_skips_feature_extraction() -> None:
    manager = _make_manager()
    extracted: list[str] = []
    priced: list[str] = []
    consumer = _stub_consumer(manager, extracted, priced)
    alert = _alert()

    assert await consumer._process_parsed_alert(alert) == "watch_created"
    assert await consumer._process_parsed_alert(alert) == "duplicate_alert"
    assert len(extracted) == 1


async def test_duplicate_alert_never_reaches_the_entry_price_resolver() -> None:
    """Storm protection: 13k re-deliveries must not each fire a live quote lookup."""
    manager = _make_manager()
    extracted: list[str] = []
    priced: list[str] = []
    consumer = _stub_consumer(manager, extracted, priced)
    alert = _alert()

    await consumer._process_parsed_alert(alert)
    for _ in range(5):
        assert await consumer._process_parsed_alert(alert) == "duplicate_alert"

    assert priced == ["alert-dup"]


async def test_auth_failure_after_create_keeps_the_claim_so_no_second_watch() -> None:
    """Feature extraction dying leaves one watch, never two.

    The alert keeps its claim, so the redelivery that follows a fatal auth failure is
    suppressed. That costs one training row — a label with no feature row is dropped
    by the join — rather than producing a second label for the same alert.
    """
    from heber.watch.features import EnrichmentAuthFailure

    manager = _make_manager()
    extracted: list[str] = []
    priced: list[str] = []
    consumer = _stub_consumer(manager, extracted, priced)

    async def _failing_extract(alert: dict, watch_id: str) -> None:
        raise EnrichmentAuthFailure("gateway auth rejected")

    consumer._extract_and_store_features = _failing_extract  # type: ignore[method-assign]
    alert = _alert()

    with pytest.raises(EnrichmentAuthFailure):
        await consumer._process_parsed_alert(alert)

    active = manager.redis.smembers(WatchKeys.ACTIVE_WATCHES)
    assert len(active) == 1

    consumer._extract_and_store_features = _stub_consumer(  # type: ignore[method-assign]
        manager, extracted, priced
    )._extract_and_store_features
    assert await consumer._process_parsed_alert(alert) == "duplicate_alert"
    assert manager.redis.smembers(WatchKeys.ACTIVE_WATCHES) == active
