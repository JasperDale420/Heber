"""Snapshot Poller - Fetches option quotes for active watches."""

from __future__ import annotations

import asyncio
import math
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog

from heber.calendar import MarketCalendar
from heber.watch.gateway import (
    DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS,
    extract_quote_timestamp,
    gateway_url_candidates,
    quote_age_seconds,
    route_failure_for_exception,
    route_failure_for_http_status,
    route_failure_for_partial_quotes,
    route_failure_for_payload_shape,
)
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
                if price_for_watch is None:
                    logger.warning(
                        "Skipping watch update due to missing price",
                        watch_id=watch.watch_id,
                        occ_symbol=watch.occ_symbol,
                    )
                    continue
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
                batch_result = await self._fetch_batch(client, batch)
                quotes.update(batch_result)

        return quotes

    async def _fetch_batch(
        self,
        client: httpx.AsyncClient,
        batch: list[str],
    ) -> dict[str, dict]:
        """Fetch quotes for a single batch of symbols, trying multiple routes."""
        symbols_param = ",".join(batch)
        routes = gateway_url_candidates(self.gateway_url, "/alpaca/options/quotes")
        now_utc = datetime.now(UTC)
        route_failures: list[dict[str, Any]] = []
        batch_quotes: dict[str, dict] | None = None
        best_partial_quotes: dict[str, dict] = {}
        best_partial_failure: dict[str, Any] | None = None
        best_stale_quotes: dict[str, dict] = {}
        best_stale_failure: dict[str, Any] | None = None
        best_stale_avg_age: float | None = None

        try:
            for route in routes:
                result = await self._try_route_for_batch(
                    client,
                    route,
                    symbols_param,
                    batch,
                    now_utc,
                    route_failures,
                )
                if result is None:
                    continue

                valid_quotes, stale_quotes, stale_ages, partial_failure = result

                if len(valid_quotes) == len(batch) and partial_failure is None:
                    batch_quotes = valid_quotes
                    break

                route_failures.append(partial_failure)
                if len(valid_quotes) > len(best_partial_quotes):
                    best_partial_quotes = valid_quotes
                    best_partial_failure = partial_failure

                best_stale_quotes, best_stale_failure, best_stale_avg_age = self._update_stale_best(
                    route,
                    stale_quotes,
                    stale_ages,
                    best_stale_quotes,
                    best_stale_failure,
                    best_stale_avg_age,
                )

                logger.warning(
                    "Quote response partial coverage",
                    route=route,
                    requested_count=partial_failure["requested_count"],
                    available_count=partial_failure["available_count"],
                    invalid_count=partial_failure["invalid_count"],
                    stale_count=partial_failure["stale_count"],
                    missing_count=partial_failure["missing_count"],
                )

            return self._resolve_batch_quotes(
                batch,
                routes,
                route_failures,
                batch_quotes,
                best_partial_quotes,
                best_partial_failure,
                best_stale_quotes,
                best_stale_failure,
            )

        except Exception as e:
            logger.error("Quote fetch error", error=str(e), batch_size=len(batch), routes=routes)
            return {}

    async def _try_route_for_batch(
        self,
        client: httpx.AsyncClient,
        route: str,
        symbols_param: str,
        batch: list[str],
        now_utc: datetime,
        route_failures: list[dict[str, Any]],
    ) -> tuple[dict[str, dict], dict[str, dict], list[float], dict[str, Any]] | None:
        """Try fetching from one route; return (valid, stale, stale_ages, partial_failure) or None."""
        try:
            response = await client.get(route, params={"symbols": symbols_param})
        except httpx.HTTPError as request_error:
            route_failures.append(route_failure_for_exception(route, request_error))
            logger.warning("Quote route request failed", route=route, error=str(request_error))
            return None

        if response.status_code != 200:
            route_failures.append(route_failure_for_http_status(route, response.status_code))
            return None

        quotes_payload = self._decode_quotes_payload(response, route, route_failures)
        if quotes_payload is None:
            return None

        return self._validate_route_quotes(route, batch, quotes_payload, now_utc)

    def _decode_quotes_payload(
        self,
        response: httpx.Response,
        route: str,
        route_failures: list[dict[str, Any]],
    ) -> dict | None:
        """Decode and validate the JSON response structure down to the quotes dict."""
        try:
            decoded = response.json()
        except (ValueError, TypeError) as decode_error:
            route_failures.append(route_failure_for_exception(route, decode_error, failure="json_decode"))
            logger.warning("Quote response JSON decode failed", route=route, error=str(decode_error))
            return None

        for label, payload_part in [
            ("payload_shape", decoded),
            ("data_payload_shape", decoded.get("data", {}) if isinstance(decoded, dict) else None),
        ]:
            if not isinstance(payload_part, dict):
                route_failures.append(route_failure_for_payload_shape(route, label, payload_part))
                logger.warning(
                    "Quote response payload shape invalid",
                    route=route,
                    payload_type=type(payload_part).__name__,
                )
                return None

        quotes_payload = decoded["data"].get("quotes", {})
        if not isinstance(quotes_payload, dict):
            route_failures.append(route_failure_for_payload_shape(route, "quotes_payload_shape", quotes_payload))
            logger.warning(
                "Quote response quotes payload shape invalid",
                route=route,
                payload_type=type(quotes_payload).__name__,
            )
            return None

        return quotes_payload

    def _validate_route_quotes(
        self,
        route: str,
        batch: list[str],
        quotes_payload: dict,
        now_utc: datetime,
    ) -> tuple[dict[str, dict], dict[str, dict], list[float], dict[str, Any] | None]:
        """Validate individual quotes from a decoded payload, separating valid from stale."""
        valid_quotes: dict[str, dict] = {}
        stale_quotes: dict[str, dict] = {}
        invalid_symbols: list[str] = []
        stale_symbols: list[str] = []
        stale_ages: list[float] = []

        for symbol in batch:
            quote_payload = quotes_payload.get(symbol)
            if quote_payload is None:
                continue
            if not isinstance(quote_payload, dict):
                invalid_symbols.append(symbol)
                continue
            age = quote_age_seconds(quote_payload, now_utc)
            if age is not None and age > DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS:
                stale_symbols.append(symbol)
                stale_ages.append(age)
                stale_quotes[symbol] = quote_payload
                continue
            valid_quotes[symbol] = quote_payload

        if len(valid_quotes) == len(batch) and not invalid_symbols and not stale_symbols:
            return valid_quotes, stale_quotes, stale_ages, None

        partial_failure = route_failure_for_partial_quotes(
            route=route,
            requested_symbols=batch,
            available_symbols=list(valid_quotes.keys()),
            invalid_symbols=invalid_symbols,
            stale_symbols=stale_symbols,
        )
        return valid_quotes, stale_quotes, stale_ages, partial_failure

    @staticmethod
    def _update_stale_best(
        route: str,
        stale_quotes: dict[str, dict],
        stale_ages: list[float],
        best_stale_quotes: dict[str, dict],
        best_stale_failure: dict[str, Any] | None,
        best_stale_avg_age: float | None,
    ) -> tuple[dict[str, dict], dict[str, Any] | None, float | None]:
        """Update best-stale tracking if this route has better stale coverage."""
        if not stale_quotes:
            return best_stale_quotes, best_stale_failure, best_stale_avg_age

        avg_age = sum(stale_ages) / len(stale_ages)
        is_better = len(stale_quotes) > len(best_stale_quotes) or (
            len(stale_quotes) == len(best_stale_quotes) and (best_stale_avg_age is None or avg_age < best_stale_avg_age)
        )
        if is_better:
            return (
                stale_quotes,
                {
                    "route": route,
                    "failure": "quote_coverage_stale",
                    "stale_count": len(stale_ages),
                    "avg_stale_age_seconds": avg_age,
                    "stale_symbols": list(stale_quotes.keys())[:5],
                },
                avg_age,
            )
        return best_stale_quotes, best_stale_failure, best_stale_avg_age

    def _resolve_batch_quotes(
        self,
        batch: list[str],
        routes: list[str],
        route_failures: list[dict[str, Any]],
        batch_quotes: dict[str, dict] | None,
        best_partial_quotes: dict[str, dict],
        best_partial_failure: dict[str, Any] | None,
        best_stale_quotes: dict[str, dict],
        best_stale_failure: dict[str, Any] | None,
    ) -> dict[str, dict]:
        """Pick the best available quotes for a batch from full / partial / stale results."""
        if batch_quotes is not None:
            return batch_quotes

        if best_partial_quotes:
            logger.warning(
                "Quote fetch using partial coverage",
                batch_size=len(batch),
                returned_quotes=len(best_partial_quotes),
                routes=routes,
                best_partial_failure=best_partial_failure,
                failures=route_failures,
            )
            return best_partial_quotes

        if best_stale_quotes:
            logger.warning(
                "Quote fetch using stale fallback coverage",
                batch_size=len(batch),
                returned_quotes=len(best_stale_quotes),
                routes=routes,
                best_stale_failure=best_stale_failure,
                failures=route_failures,
            )
            return best_stale_quotes

        logger.warning(
            "Quote fetch failed across routes",
            batch_size=len(batch),
            routes=routes,
            failures=route_failures,
        )
        return {}

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
            timestamp=self._parse_quote_timestamp(quote),
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
        if isinstance(value, bool):
            return None
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return numeric
        except (TypeError, ValueError):
            return None

    def _parse_quote_timestamp(self, quote: dict[str, Any]) -> datetime:
        """Resolve snapshot timestamp from quote payload with UTC fallback."""
        parsed = extract_quote_timestamp(quote)
        if parsed is not None:
            return parsed
        return datetime.now(UTC)

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
