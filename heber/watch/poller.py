"""Snapshot Poller - Fetches option quotes for active watches."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from heber.watch.manager import WatchManager
from heber.watch.models import (
    POLL_CONFIG,
    AlertWatch,
    WatchSnapshot,
)

logger = structlog.get_logger(__name__)

DEFAULT_GATEWAY_URL = "http://localhost:8000"


class SnapshotPoller:
    """Polls option quotes from Data Gateway for active watches.

    Runs as a background service, fetching quotes at intervals
    appropriate for each horizon.
    """

    def __init__(
        self,
        watch_manager: WatchManager,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        batch_size: int = 100,
    ):
        """Initialize the poller.

        Args:
            watch_manager: WatchManager instance
            gateway_url: Data Gateway API URL
            batch_size: Max symbols per API request
        """
        self.manager = watch_manager
        self.gateway_url = gateway_url
        self.batch_size = batch_size
        self._running = False

    async def poll_once(self) -> dict[str, Any]:
        """Run a single poll cycle.

        Returns:
            Stats from the poll cycle
        """
        # Get active watches grouped by symbol
        active = self.manager.get_active_watches()

        if not active:
            return {"watches": 0, "quotes": 0, "errors": 0}

        # Group by unique symbol
        symbol_to_watches: dict[str, list[AlertWatch]] = {}
        for watch in active:
            symbol = watch.occ_symbol
            if symbol not in symbol_to_watches:
                symbol_to_watches[symbol] = []
            symbol_to_watches[symbol].append(watch)

        symbols = list(symbol_to_watches.keys())
        logger.info("Polling quotes", symbols=len(symbols), watches=len(active))

        # Fetch quotes in batches
        quotes = await self._fetch_quotes(symbols)

        # Update watches with new prices
        updated = 0
        for symbol, quote in quotes.items():
            for watch in symbol_to_watches.get(symbol, []):
                snapshot = self._create_snapshot(watch, quote)
                self.manager.add_snapshot(snapshot)
                self.manager.update_watch_price(
                    watch.watch_id,
                    snapshot.mid_px or snapshot.last_px,
                    snapshot.timestamp,
                )
                updated += 1

        return {
            "watches": len(active),
            "quotes": len(quotes),
            "updated": updated,
        }

    async def run(self) -> None:
        """Run the poller as a continuous service.

        Uses the minimum interval from POLL_CONFIG (5 min for intraday).
        """
        self._running = True
        min_interval = min(c["interval_seconds"] for c in POLL_CONFIG.values())

        logger.info("Starting snapshot poller", interval_seconds=min_interval)

        while self._running:
            try:
                stats = await self.poll_once()
                logger.info("Poll cycle complete", **stats)

                # Cleanup expired watches
                expired = self.manager.cleanup_expired()
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

                try:
                    response = await client.get(
                        f"{self.gateway_url}/api/v1/alpaca/options/quotes",
                        params={"symbols": symbols_param},
                    )

                    if response.status_code == 200:
                        data = response.json()
                        for symbol, quote in data.get("data", {}).get("quotes", {}).items():
                            quotes[symbol] = quote
                    else:
                        logger.warning(
                            "Quote fetch failed",
                            status=response.status_code,
                            batch_size=len(batch),
                        )

                except Exception as e:
                    logger.error(
                        "Quote fetch error",
                        error=str(e),
                        batch_size=len(batch),
                    )

        return quotes

    def _create_snapshot(
        self,
        watch: AlertWatch,
        quote: dict,
    ) -> WatchSnapshot:
        """Create a snapshot from quote data."""
        bid = quote.get("bp") or quote.get("bid_price")
        ask = quote.get("ap") or quote.get("ask_price")

        if bid and ask:
            mid = (bid + ask) / 2
        else:
            mid = quote.get("last_price")

        return_pct = None
        if mid and watch.entry_price > 0:
            return_pct = (mid - watch.entry_price) / watch.entry_price

        return WatchSnapshot(
            watch_id=watch.watch_id,
            occ_symbol=watch.occ_symbol,
            timestamp=datetime.now(UTC),
            bid_px=bid,
            ask_px=ask,
            mid_px=mid,
            last_px=quote.get("last_price"),
            underlying_price=quote.get("underlying_price"),
            return_pct=return_pct,
        )
