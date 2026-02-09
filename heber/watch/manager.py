"""Watch Manager - CRUD operations for alert watches in Redis."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

import structlog

from heber.calendar import MarketCalendar
from heber.watch.models import (
    POLL_CONFIG,
    AlertWatch,
    WatchHorizon,
    WatchKeys,
    WatchSnapshot,
    WatchStatus,
)

logger = structlog.get_logger(__name__)


class WatchManager:
    """Manages alert watches in Redis.

    Provides CRUD operations and query methods for the watch list.
    Uses MarketCalendar for trading-hours-aware window calculations.
    """

    def __init__(
        self,
        redis_client: Any,
        calendar: MarketCalendar | None = None,
    ):
        """Initialize with Redis client.

        Args:
            redis_client: Redis client (sync or async)
            calendar: MarketCalendar instance (created if not provided)
        """
        self.redis = redis_client
        self.calendar = calendar or MarketCalendar()

    def create_watch(
        self,
        alert_id: str,
        occ_symbol: str,
        underlying: str,
        put_call: str,
        expiry: str,
        strike: float,
        entry_price: float,
        spot_at_alert: float,
        alert_time: datetime,
        horizon: WatchHorizon,
        tp_threshold: float,
        sl_threshold: float,
        atr_at_alert: float | None = None,
    ) -> AlertWatch:
        """Create a new alert watch.

        Args:
            alert_id: UW alert ID
            occ_symbol: OCC option symbol
            underlying: Stock ticker
            put_call: C or P
            expiry: Expiry date string
            strike: Strike price
            entry_price: Option mid at alert
            spot_at_alert: Underlying price at alert
            alert_time: When alert fired
            horizon: Trading horizon
            tp_threshold: Take profit threshold
            sl_threshold: Stop loss threshold
            atr_at_alert: ATR value (optional)

        Returns:
            Created AlertWatch
        """
        watch_id = str(uuid.uuid4())

        # Calculate window end in trading time (skips non-market hours)
        config = POLL_CONFIG[horizon]
        window_end = self.calendar.add_trading_hours(
            alert_time,
            config["max_duration_hours"],
        )

        watch = AlertWatch(
            watch_id=watch_id,
            alert_id=alert_id,
            occ_symbol=occ_symbol,
            underlying=underlying,
            put_call=put_call,
            expiry=expiry,
            strike=strike,
            entry_price=entry_price,
            spot_at_alert=spot_at_alert,
            atr_at_alert=atr_at_alert,
            alert_time=alert_time,
            window_end=window_end,
            horizon=horizon,
            tp_threshold=tp_threshold,
            sl_threshold=sl_threshold,
            status=WatchStatus.WATCHING,
        )

        # Store in Redis
        self._save_watch(watch)

        logger.info(
            "Created alert watch",
            watch_id=watch_id,
            alert_id=alert_id,
            occ_symbol=occ_symbol,
            horizon=horizon.value,
            window_end=window_end.isoformat(),
        )

        return watch

    async def create_watch_async(
        self,
        alert_id: str,
        occ_symbol: str,
        underlying: str,
        put_call: str,
        expiry: str,
        strike: float,
        entry_price: float,
        spot_at_alert: float,
        alert_time: datetime,
        horizon: WatchHorizon,
        tp_threshold: float,
        sl_threshold: float,
        atr_at_alert: float | None = None,
    ) -> AlertWatch:
        """Async wrapper for create_watch to avoid blocking event loops."""
        return await asyncio.to_thread(
            self.create_watch,
            alert_id,
            occ_symbol,
            underlying,
            put_call,
            expiry,
            strike,
            entry_price,
            spot_at_alert,
            alert_time,
            horizon,
            tp_threshold,
            sl_threshold,
            atr_at_alert,
        )

    @staticmethod
    def _normalize_redis_id(value: str | bytes) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def get_watch(self, watch_id: str | bytes) -> AlertWatch | None:
        """Get a watch by ID."""
        watch_id = self._normalize_redis_id(watch_id)
        key = WatchKeys.watch_key(watch_id)
        data = self.redis.get(key)

        if not data:
            return None

        return AlertWatch.model_validate_json(data)

    def get_active_watches(self) -> list[AlertWatch]:
        """Get all watches with WATCHING status."""
        watch_ids = self.redis.smembers(WatchKeys.ACTIVE_WATCHES)

        watches = []
        for watch_id in watch_ids:
            watch_id = self._normalize_redis_id(watch_id)
            watch = self.get_watch(watch_id)
            if watch and watch.status == WatchStatus.WATCHING:
                watches.append(watch)

        return watches

    async def get_active_watches_async(self) -> list[AlertWatch]:
        """Async wrapper for get_active_watches."""
        return await asyncio.to_thread(self.get_active_watches)

    def get_watches_for_symbol(self, occ_symbol: str) -> list[AlertWatch]:
        """Get all watches for a specific contract."""
        key = WatchKeys.by_symbol_key(occ_symbol)
        watch_ids = self.redis.smembers(key)

        watches = []
        for watch_id in watch_ids:
            watch_id = self._normalize_redis_id(watch_id)
            watch = self.get_watch(watch_id)
            if watch:
                watches.append(watch)

        return watches

    def update_watch_price(
        self,
        watch_id: str,
        current_price: float,
        timestamp: datetime,
    ) -> AlertWatch | None:
        """Update watch with new price and compute metrics.

        Args:
            watch_id: Watch ID
            current_price: Latest option mid price
            timestamp: Snapshot timestamp

        Returns:
            Updated AlertWatch or None if not found
        """
        watch = self.get_watch(watch_id)
        if not watch:
            return None

        # Compute return only when entry price is valid.
        current_return: float | None = None
        mfe = watch.mfe
        mae = watch.mae
        if watch.entry_price > 0:
            current_return = (current_price - watch.entry_price) / watch.entry_price
            prior_mfe = watch.mfe if watch.mfe is not None else -float("inf")
            prior_mae = watch.mae if watch.mae is not None else float("inf")
            mfe = max(prior_mfe, current_return)
            mae = min(prior_mae, current_return)

        # Update watch
        normalized_timestamp = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)

        watch.current_price = current_price
        watch.current_return = current_return
        watch.mfe = mfe
        watch.mae = mae
        watch.snapshot_count += 1
        watch.updated_at = normalized_timestamp.astimezone(UTC)

        self._save_watch(watch)

        return watch

    async def update_watch_price_async(
        self,
        watch_id: str,
        current_price: float,
        timestamp: datetime,
    ) -> AlertWatch | None:
        """Async wrapper for update_watch_price."""
        return await asyncio.to_thread(self.update_watch_price, watch_id, current_price, timestamp)

    def complete_watch(
        self,
        watch_id: str,
        status: WatchStatus,
        outcome_return: float,
        bars_to_hit: int | None = None,
        outcome_time: datetime | None = None,
    ) -> AlertWatch | None:
        """Mark watch as complete with final outcome.

        Args:
            watch_id: Watch ID
            status: Final status (HIT_TP, HIT_SL, EXPIRED)
            outcome_return: Final return at outcome
            bars_to_hit: Number of snapshots until barrier (if any)

        Returns:
            Updated AlertWatch
        """
        watch = self.get_watch(watch_id)
        if not watch:
            return None

        normalized_outcome_time = outcome_time
        if normalized_outcome_time is None:
            normalized_outcome_time = datetime.now(UTC)
        elif normalized_outcome_time.tzinfo is None:
            normalized_outcome_time = normalized_outcome_time.replace(tzinfo=UTC)
        else:
            normalized_outcome_time = normalized_outcome_time.astimezone(UTC)

        watch.status = status
        watch.outcome_time = normalized_outcome_time
        watch.outcome_return = outcome_return
        watch.bars_to_hit = bars_to_hit
        watch.updated_at = datetime.now(UTC)

        self._save_watch(watch)

        # Remove from active set
        self.redis.srem(WatchKeys.ACTIVE_WATCHES, watch_id)

        logger.info(
            "Completed alert watch",
            watch_id=watch_id,
            status=status.value,
            outcome_return=outcome_return,
        )

        return watch

    def add_snapshot(self, snapshot: WatchSnapshot) -> None:
        """Add a price snapshot for a watch."""
        key = WatchKeys.snapshots_key(snapshot.watch_id)
        self.redis.rpush(key, snapshot.model_dump_json())

    async def add_snapshot_async(self, snapshot: WatchSnapshot) -> None:
        """Async wrapper for add_snapshot."""
        await asyncio.to_thread(self.add_snapshot, snapshot)

    def get_snapshots(self, watch_id: str) -> list[WatchSnapshot]:
        """Get all snapshots for a watch."""
        key = WatchKeys.snapshots_key(watch_id)
        data = self.redis.lrange(key, 0, -1)

        return [WatchSnapshot.model_validate_json(d) for d in data]

    def get_expired_watches(self) -> list[AlertWatch]:
        """Get watches that have passed their window_end time."""
        now = datetime.now(UTC)
        active = self.get_active_watches()
        expired: list[AlertWatch] = []
        for watch in active:
            window_end = watch.window_end
            normalized_window_end = window_end if window_end.tzinfo is not None else window_end.replace(tzinfo=UTC)
            if normalized_window_end <= now:
                expired.append(watch)
        return expired

    def cleanup_expired(self) -> int:
        """Mark expired watches as EXPIRED and remove from active.

        Returns:
            Number of watches expired
        """
        expired = self.get_expired_watches()

        for watch in expired:
            final_return = watch.current_return or 0.0
            self.complete_watch(
                watch.watch_id,
                WatchStatus.EXPIRED,
                final_return,
                bars_to_hit=watch.snapshot_count,
                outcome_time=watch.window_end,
            )

        logger.info("Cleaned up expired watches", count=len(expired))

        return len(expired)

    async def cleanup_expired_async(self) -> int:
        """Async wrapper for cleanup_expired."""
        return await asyncio.to_thread(self.cleanup_expired)

    def delete_watch(self, watch_id: str | bytes) -> bool:
        """Delete a watch and its snapshots."""
        watch_id = self._normalize_redis_id(watch_id)
        watch = self.get_watch(watch_id)
        if not watch:
            return False

        # Remove from all indexes
        self.redis.delete(WatchKeys.watch_key(watch_id))
        self.redis.delete(WatchKeys.snapshots_key(watch_id))
        self.redis.srem(WatchKeys.ACTIVE_WATCHES, watch_id)
        self.redis.srem(WatchKeys.by_symbol_key(watch.occ_symbol), watch_id)

        return True

    def _save_watch(self, watch: AlertWatch) -> None:
        """Save watch to Redis."""
        key = WatchKeys.watch_key(watch.watch_id)
        self.redis.set(key, watch.model_dump_json())

        # Keep active index synchronized with status transitions.
        if watch.status == WatchStatus.WATCHING:
            self.redis.sadd(WatchKeys.ACTIVE_WATCHES, watch.watch_id)
        else:
            self.redis.srem(WatchKeys.ACTIVE_WATCHES, watch.watch_id)

        # Add to symbol index
        self.redis.sadd(
            WatchKeys.by_symbol_key(watch.occ_symbol),
            watch.watch_id,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get watch list statistics."""
        active = self.get_active_watches()

        by_horizon = {}
        for watch in active:
            h = watch.horizon
            by_horizon[h] = by_horizon.get(h, 0) + 1

        return {
            "active_count": len(active),
            "by_horizon": by_horizon,
        }
