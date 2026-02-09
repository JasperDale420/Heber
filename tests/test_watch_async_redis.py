"""Regression tests for non-blocking watch-service Redis integration."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from heber.watch.consumer import AlertWatchConsumer
from heber.watch.models import AlertWatch, WatchHorizon
from heber.watch.poller import SnapshotPoller


class _SlowRedis:
    def xgroup_create(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    def xreadgroup(self, *args, **kwargs):  # noqa: ANN002, ANN003
        time.sleep(0.2)
        return []

    def xack(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1


class _AsyncOnlyManager:
    def __init__(self) -> None:
        self.created = 0
        self.snapshots = 0
        self.updated = 0
        self.updated_prices: list[float | None] = []

    def create_watch(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("sync create_watch should not be called in async flow")

    async def create_watch_async(self, **kwargs):  # noqa: ANN003
        self.created += 1
        return SimpleNamespace(watch_id="watch-1", horizon=kwargs.get("horizon", WatchHorizon.SWING))

    def get_active_watches(self):
        raise AssertionError("sync get_active_watches should not be called in async flow")

    async def get_active_watches_async(self):
        return [SimpleNamespace(watch_id="watch-1", occ_symbol="AAPL260220C00100000", entry_price=1.0)]

    def add_snapshot(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("sync add_snapshot should not be called in async flow")

    async def add_snapshot_async(self, _snapshot):  # noqa: ANN001
        self.snapshots += 1

    def update_watch_price(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("sync update_watch_price should not be called in async flow")

    async def update_watch_price_async(self, _watch_id, _price, _timestamp):  # noqa: ANN001
        self.updated += 1
        self.updated_prices.append(_price)
        return None


@pytest.mark.asyncio
async def test_consumer_read_messages_does_not_block_event_loop() -> None:
    manager = _AsyncOnlyManager()
    consumer = AlertWatchConsumer(_SlowRedis(), manager)

    start = time.perf_counter()
    read_task = asyncio.create_task(consumer._read_messages())
    await asyncio.sleep(0.02)
    elapsed = time.perf_counter() - start
    assert elapsed < 0.1
    await read_task


@pytest.mark.asyncio
async def test_consumer_process_alert_uses_async_watch_creation() -> None:
    manager = _AsyncOnlyManager()
    consumer = AlertWatchConsumer(_SlowRedis(), manager)

    consumer._get_entry_price = AsyncMock(return_value=1.5)  # type: ignore[method-assign]
    consumer._extract_and_store_features = AsyncMock(return_value=None)  # type: ignore[method-assign]

    payload = {
        "feed": "flow_alerts",
        "payload": {
            "id": "alert-1",
            "occ_symbol": "AAPL260220C00100000",
            "underlying": "AAPL",
            "put_call": "C",
            "expiry": "2026-02-20",
            "strike": 100,
            "spot_px": 200,
            "contract_px": 1.5,
        },
    }
    stream_data = {b"data": json.dumps(payload).encode()}

    await consumer._process_alert("1-0", stream_data)

    assert manager.created == 1


@pytest.mark.asyncio
async def test_poller_uses_async_manager_methods() -> None:
    manager = _AsyncOnlyManager()
    poller = SnapshotPoller(manager)

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

    stats = await poller.poll_once()

    assert stats["watches"] == 1
    assert stats["updated"] == 1
    assert manager.snapshots == 1
    assert manager.updated == 1


def test_alert_watch_timestamps_default_to_aware_utc() -> None:
    now = datetime.now(UTC)
    watch = AlertWatch(
        watch_id="watch-1",
        alert_id="alert-1",
        occ_symbol="AAPL260220C00100000",
        underlying="AAPL",
        put_call="C",
        expiry="2026-02-20",
        strike=100.0,
        entry_price=1.5,
        spot_at_alert=200.0,
        alert_time=now,
        window_end=now + timedelta(hours=1),
        horizon=WatchHorizon.SWING,
        tp_threshold=0.25,
        sl_threshold=0.15,
    )

    assert watch.created_at.tzinfo is not None
    assert watch.updated_at.tzinfo is not None


@pytest.mark.asyncio
async def test_poller_skips_watches_not_due_by_horizon() -> None:
    manager = _AsyncOnlyManager()
    now = datetime.now(UTC)
    manager.get_active_watches_async = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            SimpleNamespace(
                watch_id="watch-intraday-due",
                occ_symbol="AAPL260220C00100000",
                entry_price=1.0,
                horizon=WatchHorizon.INTRADAY,
                updated_at=now - timedelta(minutes=6),
            ),
            SimpleNamespace(
                watch_id="watch-leap-not-due",
                occ_symbol="AAPL260220P00100000",
                entry_price=1.0,
                horizon=WatchHorizon.LEAP,
                updated_at=now - timedelta(minutes=10),
            ),
        ]
    )
    poller = SnapshotPoller(manager)
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

    stats = await poller.poll_once()

    assert stats["watches"] == 2
    assert stats["due_watches"] == 1
    assert stats["quotes"] == 1
    assert manager.snapshots == 1
    assert manager.updated == 1


@pytest.mark.asyncio
async def test_poller_updates_watch_with_zero_mid_price() -> None:
    manager = _AsyncOnlyManager()
    poller = SnapshotPoller(manager)

    poller._fetch_quotes = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "AAPL260220C00100000": {
                "bp": 0.0,
                "ap": 0.0,
                "last_price": 1.1,
                "underlying_price": 200.0,
            }
        }
    )

    stats = await poller.poll_once()

    assert stats["updated"] == 1
    assert manager.updated_prices == [0.0]


@pytest.mark.asyncio
async def test_poller_handles_naive_last_polled_timestamps() -> None:
    manager = _AsyncOnlyManager()
    naive_now = datetime.now()
    manager.get_active_watches_async = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            SimpleNamespace(
                watch_id="watch-naive",
                occ_symbol="AAPL260220C00100000",
                entry_price=1.0,
                horizon=WatchHorizon.INTRADAY,
                updated_at=naive_now - timedelta(minutes=10),
            ),
        ]
    )
    poller = SnapshotPoller(manager)
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

    stats = await poller.poll_once()

    assert stats["due_watches"] == 1
    assert stats["updated"] == 1
    assert manager.updated == 1


@pytest.mark.asyncio
async def test_poller_treats_future_last_polled_timestamp_as_due() -> None:
    manager = _AsyncOnlyManager()
    now = datetime.now(UTC)
    manager.get_active_watches_async = AsyncMock(  # type: ignore[method-assign]
        return_value=[
            SimpleNamespace(
                watch_id="watch-future-polled",
                occ_symbol="AAPL260220C00100000",
                entry_price=1.0,
                horizon=WatchHorizon.INTRADAY,
                updated_at=now + timedelta(hours=1),
            ),
        ]
    )
    poller = SnapshotPoller(manager)
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

    stats = await poller.poll_once()

    assert stats["due_watches"] == 1
    assert stats["updated"] == 1
    assert manager.updated == 1
