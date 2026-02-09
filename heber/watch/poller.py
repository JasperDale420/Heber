"""Snapshot Poller - Fetches option quotes for active watches."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from heber.calendar import MarketCalendar
from heber.watch.gateway import gateway_url_candidates
from heber.watch.manager import WatchManager
from heber.watch.models import (
    POLL_CONFIG,
    AlertWatch,
    WatchSnapshot,
)

logger = structlog.get_logger(__name__)

DEFAULT_GATEWAY_URL = "http://localhost:8000"

# Max time to sleep when waiting for market open (check hourly)
MAX_SLEEP_SECONDS = 3600


class SnapshotPoller:
    """Polls option quotes from Data Gateway for active watches.

    Runs as a background service, fetching quotes at intervals
    appropriate for each horizon. Only polls during market hours.
    """

    def __init__(
        self,
        watch_manager: WatchManager,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        batch_size: int = 100,
        calendar: MarketCalendar | None = None,
    ):
        """Initialize the poller.

        Args:
            watch_manager: WatchManager instance
            gateway_url: Data Gateway API URL
            batch_size: Max symbols per API request
            calendar: MarketCalendar instance (created if not provided)
        """
        self.manager = watch_manager
        self.gateway_url = gateway_url
        self.batch_size = batch_size
        self.calendar = calendar or MarketCalendar()
        self._running = False

    async def poll_once(self) -> dict[str, Any]:
        """Run a single poll cycle.

        Returns:
            Stats from the poll cycle
        """
        # Get active watches grouped by symbol
        active = await self.manager.get_active_watches_async()

        if not active:
            return {"watches": 0, "quotes": 0, "errors": 0}

        now = datetime.now(UTC)
        due_watches = [watch for watch in active if self._is_watch_due(watch, now)]
        if not due_watches:
            return {"watches": len(active), "due_watches": 0, "quotes": 0, "errors": 0}

        # Group by unique symbol
        symbol_to_watches: dict[str, list[AlertWatch]] = {}
        for watch in due_watches:
            symbol = watch.occ_symbol
            if symbol not in symbol_to_watches:
                symbol_to_watches[symbol] = []
            symbol_to_watches[symbol].append(watch)

        symbols = list(symbol_to_watches.keys())
        logger.info("Polling quotes", symbols=len(symbols), watches=len(active), due_watches=len(due_watches))

        # Fetch quotes in batches
        quotes = await self._fetch_quotes(symbols)

        # Update watches with new prices
        updated = 0
        for symbol, quote in quotes.items():
            for watch in symbol_to_watches.get(symbol, []):
                snapshot = self._create_snapshot(watch, quote)
                await self.manager.add_snapshot_async(snapshot)
                price_for_watch = snapshot.mid_px if snapshot.mid_px is not None else snapshot.last_px
                await self.manager.update_watch_price_async(
                    watch.watch_id,
                    price_for_watch,
                    snapshot.timestamp,
                )
                updated += 1

        return {
            "watches": len(active),
            "due_watches": len(due_watches),
            "quotes": len(quotes),
            "updated": updated,
        }

    async def run(self) -> None:
        """Run the poller as a continuous service.

        Uses the minimum interval from POLL_CONFIG (5 min for intraday).
        Only polls during market hours to avoid wasted API calls.
        """
        self._running = True
        min_interval = min(c["interval_seconds"] for c in POLL_CONFIG.values())

        logger.info("Starting snapshot poller", interval_seconds=min_interval)

        while self._running:
            try:
                # Check if market is open
                if not self.calendar.is_market_open():
                    seconds_until = self.calendar.seconds_until_open()
                    sleep_time = min(seconds_until, MAX_SLEEP_SECONDS)

                    logger.info(
                        "Market closed, sleeping until open",
                        seconds_until_open=int(seconds_until),
                        sleep_seconds=int(sleep_time),
                    )

                    await asyncio.sleep(sleep_time)
                    continue

                # Poll for quotes
                stats = await self.poll_once()
                logger.info("Poll cycle complete", **stats)

                # Cleanup expired watches
                expired = await self.manager.cleanup_expired_async()
                if expired:
                    logger.info("Expired watches cleaned", count=expired)

            except Exception as e:
                logger.error("Poll cycle failed", error=str(e))

            await asyncio.sleep(min_interval)

    def stop(self) -> None:
        """Stop the poller."""
        self._running = False
        logger.info("Snapshot poller stopped")

    async def _fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """Fetch latest quotes from Data Gateway.

        Args:
            symbols: List of OCC symbols

        Returns:
            Dict mapping symbol to quote data
        """
        quotes = {}

        async with httpx.AsyncClient(timeout=30.0) as client:
            for i in range(0, len(symbols), self.batch_size):
                batch = symbols[i : i + self.batch_size]
                symbols_param = ",".join(batch)
                routes = gateway_url_candidates(
                    self.gateway_url,
                    "/alpaca/options/quotes",
                )
                batch_data: dict | None = None
                last_status: int | None = None

                try:
                    for route in routes:
                        response = await client.get(
                            route,
                            params={"symbols": symbols_param},
                        )
                        last_status = response.status_code
                        if response.status_code == 200:
                            batch_data = response.json()
                            break

                    if batch_data is not None:
                        for symbol, quote in batch_data.get("data", {}).get("quotes", {}).items():
                            quotes[symbol] = quote
                    else:
                        logger.warning(
                            "Quote fetch failed",
                            status=last_status,
                            batch_size=len(batch),
                            routes=routes,
                        )

                except Exception as e:
                    logger.error(
                        "Quote fetch error",
                        error=str(e),
                        batch_size=len(batch),
                        routes=routes,
                    )

        return quotes

    def _create_snapshot(
        self,
        watch: AlertWatch,
        quote: dict,
    ) -> WatchSnapshot:
        """Create a snapshot from quote data."""
        bid = self._coerce_optional_float(quote.get("bp"))
        if bid is None:
            bid = self._coerce_optional_float(quote.get("bid_price"))

        ask = self._coerce_optional_float(quote.get("ap"))
        if ask is None:
            ask = self._coerce_optional_float(quote.get("ask_price"))

        last_price = self._coerce_optional_float(quote.get("last_price"))
        underlying_price = self._coerce_optional_float(quote.get("underlying_price"))

        if bid is not None and ask is not None:
            mid = (bid + ask) / 2
        else:
            mid = last_price

        return_pct = None
        if mid is not None and watch.entry_price > 0:
            return_pct = (mid - watch.entry_price) / watch.entry_price

        return WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=datetime.now(UTC),
            bid_px=bid,
            ask_px=ask,
            mid_px=mid,
            last_px=last_price,
            underlying_price=underlying_price,
            return_pct=return_pct,
        )

    @staticmethod
    def _coerce_optional_float(value: Any) -> float | None:
        """Convert quote payload values to float when possible."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _horizon_interval_seconds(horizon: Any) -> int:
        """Resolve polling interval from watch horizon value."""
        if horizon in POLL_CONFIG:
            return int(POLL_CONFIG[horizon]["interval_seconds"])

        horizon_str = str(horizon)
        for config_horizon, config in POLL_CONFIG.items():
            if horizon_str == getattr(config_horizon, "value", str(config_horizon)):
                return int(config["interval_seconds"])

        # Fail-safe: use the shortest interval for unknown horizon values.
        return min(int(config["interval_seconds"]) for config in POLL_CONFIG.values())

    def _is_watch_due(self, watch: AlertWatch, now: datetime) -> bool:
        """Return True when the watch should be polled at current time."""
        interval_seconds = self._horizon_interval_seconds(getattr(watch, "horizon", None))
        last_polled = getattr(watch, "updated_at", None) or getattr(watch, "alert_time", None)
        if not isinstance(last_polled, datetime):
            return True
        normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
        normalized_last_polled = last_polled if last_polled.tzinfo is not None else last_polled.replace(tzinfo=UTC)
        # Clock skew or bad upstream timestamps should not stall polling indefinitely.
        if normalized_last_polled > normalized_now:
            return True
        return (normalized_now - normalized_last_polled).total_seconds() >= interval_seconds
