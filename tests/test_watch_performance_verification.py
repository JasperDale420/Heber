import asyncio
import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

from heber.watch.manager import WatchManager
from heber.watch.models import AlertWatch, WatchHorizon, WatchStatus
from heber.watch.poller import SnapshotPoller


class TestWatchPerformanceVerification(unittest.IsolatedAsyncioTestCase):
    async def test_manager_uses_mget_for_active_watches(self):
        """Verify get_active_watches uses mget."""
        redis = MagicMock()
        # Mock smembers to return 100 IDs
        ids = [f"watch-{i}" for i in range(100)]
        redis.smembers.return_value = set(ids)

        # Mock mget to return valid JSONs
        watches = [
            AlertWatch(
                watch_id=wid,
                alert_id=f"alert-{wid}",
                occ_symbol="AAPL",
                underlying="AAPL",
                put_call="C",
                expiry="2025-01-01",
                strike=100.0,
                entry_price=1.0,
                spot_at_alert=100.0,
                alert_time=datetime.now(UTC),
                window_end=datetime.now(UTC),
                horizon=WatchHorizon.INTRADAY,
                tp_threshold=0.1,
                sl_threshold=0.1,
                status=WatchStatus.WATCHING,
            ).model_dump_json()
            for wid in ids
        ]
        redis.mget.return_value = watches

        manager = WatchManager(redis)
        active = manager.get_active_watches()

        self.assertEqual(len(active), 100)
        redis.mget.assert_called_once()
        # Ensure keys passed to mget are correct
        call_args = redis.mget.call_args[0][0]
        self.assertEqual(len(call_args), 100)

    async def test_poller_uses_bulk_update(self):
        """Verify poller uses update_watch_prices_bulk_async."""
        # Setup mocks
        AsyncMock()  # Poller uses async methods on manager which might use async redis or threaded sync redis
        # But here we mock the manager directly to check calls to it

        manager = MagicMock(spec=WatchManager)
        # Setup active watches
        watches = [
            AlertWatch(
                watch_id=f"w-{i}",
                alert_id=f"a-{i}",
                occ_symbol="AAPL",
                underlying="AAPL",
                put_call="C",
                expiry="2025-01-01",
                strike=100.0,
                entry_price=1.0,
                spot_at_alert=100.0,
                # Make alert time 1 hour ago so it's due
                alert_time=datetime.now(UTC).replace(
                    hour=datetime.now(UTC).hour - 1 if datetime.now(UTC).hour > 0 else 23
                ),
                window_end=datetime.now(UTC).replace(year=datetime.now(UTC).year + 1),
                horizon=WatchHorizon.INTRADAY,
                tp_threshold=0.1,
                sl_threshold=0.1,
                status=WatchStatus.WATCHING,
            )
            for i in range(10)
        ]
        manager.get_active_watches_async = AsyncMock(return_value=watches)
        manager.update_watch_prices_bulk_async = AsyncMock(return_value=10)
        manager.add_snapshot_async = AsyncMock()

        # Mock httpx response
        poller = SnapshotPoller(manager)

        # We need to mock _fetch_quotes or httpx. Given _fetch_quotes is complex, let's mock it directly
        # to verify the poller logic downstream.
        # But wait, I also want to verify _fetch_quotes uses gather.

        # Sub-test 1: Verify poll_once calls bulk update
        quotes = {
            "AAPL": {"bp": 1.1, "ap": 1.2, "t": datetime.now(UTC).timestamp(), "symbol": "AAPL", "last_price": 1.15}
        }

        with (
            patch.object(poller, "_fetch_quotes", new_callable=AsyncMock) as mock_fetch,
            patch.object(poller, "_is_watch_due", return_value=True),
        ):
            mock_fetch.return_value = quotes

            stats = await poller.poll_once()

            self.assertEqual(stats["updated"], 10)
            manager.update_watch_prices_bulk_async.assert_called_once()
            call_args = manager.update_watch_prices_bulk_async.call_args[0][0]
            self.assertEqual(len(call_args), 10)  # 10 watches for AAPL updated

    async def test_fetch_quotes_parallelism(self):
        """Verify _fetch_quotes uses asyncio.gather."""
        manager = MagicMock(spec=WatchManager)
        poller = SnapshotPoller(manager, batch_size=2)
        symbols = ["A", "B", "C", "D"]

        # Mock _fetch_batch to return a specific dict per batch
        async def mock_fetch_batch_impl(client, batch):
            return {s: {"price": 1} for s in batch}

        with (
            patch.object(poller, "_fetch_batch", side_effect=mock_fetch_batch_impl) as mock_batch,
            patch("asyncio.gather", wraps=asyncio.gather) as spy_gather,
            patch("httpx.AsyncClient"),
        ):  # Mock client context manager
            quotes = await poller._fetch_quotes(symbols)

            # Verify gather was called
            spy_gather.assert_called_once()
            # Verify we got all quotes
            self.assertEqual(len(quotes), 4)
            # Verify _fetch_batch called twice (4 symbols, batch 2)
            self.assertEqual(mock_batch.call_count, 2)
