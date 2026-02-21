"""Alert Feature Extraction for Meta-Labeling.

Captures features at alert time for training meta-models that predict
which flow alerts will hit their target price before stop loss.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import deque
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import polars as pl
import structlog

from heber.config import settings
from heber.ml.datasets import persist_features_to_gold as persist_features_frame_to_gold
from heber.ops.metrics import (
    record_watch_gateway_request,
    record_watch_gateway_success,
)
from heber.watch.gateway import (
    coerce_optional_float,
    gateway_auth_headers,
    gateway_url_candidates,
    is_retryable_http_status,
    parse_retry_after,
    should_try_legacy_fallback_for_status,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from heber.models.silver import FlowAlertRecord

logger = structlog.get_logger(__name__)

_LOG_ENRICHMENT_FAILED = "Feature enrichment request failed"


def _unwrap_data_payload(response: dict) -> object:
    """Extract the 'data' value from an API response, unwrapping single-element lists."""
    data = response.get("data", {})
    if isinstance(data, list) and data:
        data = data[0]
    return data


def _coalesce_first(data: dict, *keys: str) -> float | None:
    """Return the first non-None coerced float among *keys*."""
    for key in keys:
        value = coerce_optional_float(data.get(key))
        if value is not None:
            return value
    return None


def _coalesce_split_or_fallback(
    data: dict,
    *,
    call_key: str,
    put_key: str,
    fallbacks: tuple[str, ...],
) -> float | None:
    """Sum call+put split fields if present; otherwise try fallback keys in order."""
    call_val = coerce_optional_float(data.get(call_key))
    put_val = coerce_optional_float(data.get(put_key))
    if call_val is not None or put_val is not None:
        return (call_val or 0.0) + (put_val or 0.0)
    return _coalesce_first(data, *fallbacks)


def _classify_direction(value: float) -> str:
    """Classify a numeric value as 'bullish', 'bearish', or 'neutral'."""
    if value > 0:
        return "bullish"
    if value < 0:
        return "bearish"
    return "neutral"


class EnrichmentAuthFailure(RuntimeError):
    """Raised when repeated upstream authentication failures require fail-fast behavior."""


@dataclass
class AlertFeatures:
    """Features captured at alert arrival time for meta-model training.

    These features must only use information available at the moment
    the alert is received - no lookahead allowed.
    """

    # Identifiers
    alert_id: str
    alert_time: datetime
    symbol: str

    # Contract info
    occ_symbol: str | None
    underlying: str
    strike: float
    expiry: date
    put_call: str  # C or P
    days_to_expiry: int

    # Alert characteristics
    premium: float
    volume: float
    open_interest: float | None
    volume_oi_ratio: float | None
    alert_type: str  # SWEEP, BLOCK, etc
    side: str | None  # bid/ask/mid
    aggressor: str | None

    # Prices at alert time
    spot_price: float | None  # Underlying price
    contract_price: float | None  # Option mid

    # Moneyness (requires spot)
    moneyness: float | None = None  # strike / spot (>1 = OTM for calls)
    log_moneyness: float | None = None

    # Greeks (if available from alert or lookup)
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    iv: float | None = None

    # Market context (from underlying quotes/bars)
    underlying_30d_return: float | None = None
    underlying_5d_return: float | None = None
    underlying_1d_return: float | None = None
    realized_vol_20d: float | None = None
    iv_rank: float | None = None  # IV percentile over past year

    # GEX / Market Structure (from UW greek-exposure)
    gex: float | None = None  # Net gamma exposure
    vex: float | None = None  # Net vanna exposure

    # Max Pain (from UW max-pain)
    max_pain_strike: float | None = None
    max_pain_distance_pct: float | None = None  # (spot - max_pain) / spot

    # Market Tide (from UW market-tide)
    market_tide_net_premium: float | None = None
    market_tide_direction: str | None = None  # bullish / bearish / neutral

    # Timing features
    hour_of_day: int = 0
    minute_of_hour: int = 0
    day_of_week: int = 0  # Monday is zero
    minutes_since_open: int = 0
    minutes_to_close: int = 0

    # Tags/sentiment (from UW)
    is_bullish: int = 0
    is_bearish: int = 0
    is_sweep: int = 0
    is_block: int = 0
    is_unusual: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, date):
                result[k] = v.isoformat()
            else:
                result[k] = v
        return result

    @classmethod
    def from_dict(cls, data: dict) -> AlertFeatures:
        """Reconstruct from dictionary."""
        parsed_data = dict(data)
        # Convert date/time strings back
        if isinstance(parsed_data.get("alert_time"), str):
            alert_time = datetime.fromisoformat(parsed_data["alert_time"])
            if alert_time.tzinfo is None:
                alert_time = alert_time.replace(tzinfo=UTC)
            else:
                alert_time = alert_time.astimezone(UTC)
            parsed_data["alert_time"] = alert_time
        if isinstance(parsed_data.get("expiry"), str):
            parsed_data["expiry"] = date.fromisoformat(parsed_data["expiry"])
        return cls(**parsed_data)

    def to_feature_array(self, feature_names: list[str] | None = None) -> list[float]:
        """Convert to numeric array for model input.

        Args:
            feature_names: Specific features to include. If None, uses all numeric.

        Returns:
            List of float values for model input.
        """
        if feature_names is None:
            feature_names = self.numeric_feature_names()

        values = []
        for name in feature_names:
            val = getattr(self, name, None)
            # Convert None to 0 or NaN handling as needed
            if val is None:
                values.append(0.0)
            elif isinstance(val, bool):
                values.append(float(val))
            elif isinstance(val, int | float):
                values.append(float(val))
            else:
                values.append(0.0)
        return values

    @classmethod
    def numeric_feature_names(cls) -> list[str]:
        """Return list of numeric feature names for model training."""
        return [
            "days_to_expiry",
            "premium",
            "volume",
            "open_interest",
            "volume_oi_ratio",
            "spot_price",
            "contract_price",
            "moneyness",
            "log_moneyness",
            "delta",
            "gamma",
            "theta",
            "vega",
            "iv",
            "underlying_30d_return",
            "underlying_5d_return",
            "underlying_1d_return",
            "realized_vol_20d",
            "iv_rank",
            "hour_of_day",
            "minute_of_hour",
            "day_of_week",
            "minutes_since_open",
            "minutes_to_close",
            "is_bullish",
            "is_bearish",
            "is_sweep",
            "is_block",
            "is_unusual",
        ]


class AlertFeatureExtractor:
    """Extracts features from flow alerts at arrival time.

    Integrates with market data sources to enrich alerts with
    context features (returns, volatility, IV rank, etc.).
    """

    # Market hours (Eastern Time)
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MIN = 30
    MARKET_CLOSE_HOUR = 16
    MARKET_CLOSE_MIN = 0
    MARKET_TIMEZONE = ZoneInfo("America/New_York")

    def __init__(
        self,
        redis: Redis | None = None,
        gateway_url: str | None = None,
        gateway_api_key: str | None = None,
        legacy_route_fallback_enabled: bool | None = None,
        request_max_attempts: int = 3,
        retry_base_delay_seconds: float = 0.25,
        retry_jitter_seconds: float = 0.1,
        max_concurrent_requests: int = 8,
        cache_ttl_seconds: float = 30.0,
        auth_failure_threshold: int = 5,
        auth_failure_window_seconds: float = 120.0,
    ):
        """Initialize extractor.

        Args:
            redis: Redis client for caching/lookups.
            gateway_url: Data Gateway URL for market data enrichment.
            legacy_route_fallback_enabled: Whether to try legacy unprefixed route fallback.
            request_max_attempts: Number of attempts per route for retryable statuses.
            retry_base_delay_seconds: Base backoff delay between retries.
            retry_jitter_seconds: Random jitter added to backoff delays.
            max_concurrent_requests: Max in-flight upstream requests.
            cache_ttl_seconds: TTL for in-process request cache.
            auth_failure_threshold: Fail-fast threshold for HTTP 401 in rolling window.
            auth_failure_window_seconds: Rolling window for auth failure counting.
        """
        self.redis = redis
        self.gateway_url = gateway_url
        self.gateway_headers = gateway_auth_headers(gateway_api_key)
        if legacy_route_fallback_enabled is None:
            legacy_route_fallback_enabled = settings.watch_gateway_legacy_fallback_enabled
        self.legacy_route_fallback_enabled = bool(legacy_route_fallback_enabled)
        self.request_max_attempts = max(1, int(request_max_attempts))
        self.retry_base_delay_seconds = max(0.0, float(retry_base_delay_seconds))
        self.retry_jitter_seconds = max(0.0, float(retry_jitter_seconds))
        self.max_concurrent_requests = max(1, int(max_concurrent_requests))
        self.cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self.auth_failure_threshold = max(1, int(auth_failure_threshold))
        self.auth_failure_window_seconds = max(1.0, float(auth_failure_window_seconds))
        self._request_semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        self._response_cache: dict[str, tuple[float, dict]] = {}
        self._auth_failure_timestamps: deque[float] = deque()

    async def extract(self, alert: FlowAlertRecord) -> AlertFeatures:
        """Extract features from a flow alert.

        Args:
            alert: The flow alert record.

        Returns:
            AlertFeatures with all available features populated.
        """
        import math

        # Parse alert time
        alert_time = alert.ts_event
        market_time = self._to_market_time(alert_time)

        # Compute days to expiry
        dte = (alert.expiry - alert_time.date()).days

        # Compute volume/OI ratio
        vol_oi = None
        if alert.open_interest and alert.open_interest > 0:
            vol_oi = alert.volume / alert.open_interest

        # Compute moneyness
        moneyness = None
        log_moneyness = None
        if alert.spot_px and alert.spot_px > 0:
            moneyness = alert.strike / alert.spot_px
            if moneyness > 0:
                log_moneyness = math.log(moneyness)

        # Extract timing features
        hour = market_time.hour
        minute = market_time.minute
        day_of_week = market_time.weekday()

        # Minutes since open (9:30 AM ET)
        market_open_minutes = self.MARKET_OPEN_HOUR * 60 + self.MARKET_OPEN_MIN
        current_minutes = hour * 60 + minute
        mins_since_open = max(0, current_minutes - market_open_minutes)

        # Minutes to close (4:00 PM ET)
        market_close_minutes = self.MARKET_CLOSE_HOUR * 60 + self.MARKET_CLOSE_MIN
        mins_to_close = max(0, market_close_minutes - current_minutes)

        # Parse tags for sentiment/type features
        tags = alert.tags or []
        tags_lower = [t.lower() for t in tags]

        is_bullish = 1 if "bullish" in tags_lower else 0
        is_bearish = 1 if "bearish" in tags_lower else 0
        is_unusual = 1 if "unusual" in tags_lower else 0

        alert_type = (alert.alert_type or "").upper()
        is_sweep = 1 if alert_type == "SWEEP" or "sweep" in tags_lower else 0
        is_block = 1 if alert_type == "BLOCK" or "block" in tags_lower else 0

        features = AlertFeatures(
            # Identifiers
            alert_id=alert.event_id,
            alert_time=alert_time,
            symbol=alert.underlying,
            # Contract
            occ_symbol=alert.occ_symbol,
            underlying=alert.underlying,
            strike=alert.strike,
            expiry=alert.expiry,
            put_call=alert.put_call,
            days_to_expiry=dte,
            # Alert chars
            premium=alert.premium,
            volume=alert.volume,
            open_interest=alert.open_interest,
            volume_oi_ratio=vol_oi,
            alert_type=alert.alert_type,
            side=alert.side,
            aggressor=alert.aggressor,
            # Prices
            spot_price=alert.spot_px,
            contract_price=alert.contract_px,
            moneyness=moneyness,
            log_moneyness=log_moneyness,
            # Timing
            hour_of_day=hour,
            minute_of_hour=minute,
            day_of_week=day_of_week,
            minutes_since_open=mins_since_open,
            minutes_to_close=mins_to_close,
            # Sentiment/type
            is_bullish=is_bullish,
            is_bearish=is_bearish,
            is_sweep=is_sweep,
            is_block=is_block,
            is_unusual=is_unusual,
        )

        # Enrich with market context (async lookups)
        features = await self._enrich_market_context(features)

        # Enrich with Greeks from Alpaca option chain
        features = await self._enrich_greeks(features)

        # Enrich with IV rank from UW
        features = await self._enrich_iv_rank(features)

        # Enrich with GEX from UW greek-exposure
        features = await self._enrich_gex(features)

        # Enrich with max pain from UW
        features = await self._enrich_max_pain(features)

        # Enrich with market tide from UW
        features = await self._enrich_market_tide(features)

        return features

    def _to_market_time(self, dt: datetime) -> datetime:
        """Normalize timestamps to market timezone for time-of-day features.

        Naive datetimes are treated as UTC to match calendar normalization.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(self.MARKET_TIMEZONE)

    def _cache_key(self, endpoint: str, symbol: str, params: dict | None) -> str:
        serialized_params = json.dumps(params or {}, sort_keys=True, default=str)
        return f"{endpoint}|{symbol}|{serialized_params}"

    def _cache_get(self, key: str) -> dict | None:
        if self.cache_ttl_seconds <= 0:
            return None
        cached = self._response_cache.get(key)
        if cached is None:
            return None
        cached_at, payload = cached
        if time.monotonic() - cached_at > self.cache_ttl_seconds:
            self._response_cache.pop(key, None)
            return None
        return payload

    def _cache_set(self, key: str, payload: dict) -> None:
        if self.cache_ttl_seconds <= 0:
            return
        self._response_cache[key] = (time.monotonic(), payload)

    def _record_auth_failure(self) -> int:
        now = time.monotonic()
        cutoff = now - self.auth_failure_window_seconds
        while self._auth_failure_timestamps and self._auth_failure_timestamps[0] < cutoff:
            self._auth_failure_timestamps.popleft()
        self._auth_failure_timestamps.append(now)
        return len(self._auth_failure_timestamps)

    def _build_enrichment_routes(self, *route_patterns: str) -> list[str]:
        """Build de-duplicated URL list from one or more route patterns."""
        routes: list[str] = []
        for pattern in route_patterns:
            for candidate in gateway_url_candidates(
                self.gateway_url,
                pattern,
                include_legacy_fallback=self.legacy_route_fallback_enabled,
            ):
                if candidate not in routes:
                    routes.append(candidate)
        return routes

    def _log_and_record_failure(
        self,
        *,
        endpoint: str,
        status_code: int | None,
        symbol: str,
        alert_id: str | None,
        attempt: int,
        retryable: bool,
        duration_ms: int,
        route: str,
        error: str | None = None,
        extra: dict | None = None,
    ) -> None:
        """Log a warning and record a gateway request metric for a failed enrichment call."""
        log_kwargs: dict = {
            "endpoint": endpoint,
            "status_code": status_code,
            "symbol": symbol,
            "alert_id": alert_id,
            "attempt": attempt,
            "retryable": retryable,
            "duration_ms": duration_ms,
            "route": route,
        }
        if error is not None:
            log_kwargs["error"] = error
        if extra:
            log_kwargs.update(extra)
        logger.warning(_LOG_ENRICHMENT_FAILED, **log_kwargs)
        record_watch_gateway_request(
            component="features",
            endpoint=endpoint,
            outcome="transport_error" if status_code is None else "http_error",
            status_code=status_code,
            duration_seconds=duration_ms / 1000.0,
        )

    async def _sleep_before_retry(self, attempt: int, minimum_delay: float | None = None) -> None:
        delay = self.retry_base_delay_seconds * attempt
        if self.retry_jitter_seconds > 0:
            delay += random.uniform(0.0, self.retry_jitter_seconds)
        if minimum_delay is not None:
            delay = max(delay, minimum_delay)
        if delay > 0:
            await asyncio.sleep(delay)

    async def _request_json_with_retry(
        self,
        *,
        endpoint: str,
        routes: list[str],
        symbol: str,
        params: dict | None = None,
        alert_id: str | None = None,
    ) -> dict | None:
        """Fetch JSON payload with route fallback, retries, throttling, and in-process caching."""
        cache_key = self._cache_key(endpoint=endpoint, symbol=symbol, params=params)
        cached_payload = self._cache_get(cache_key)
        if cached_payload is not None:
            return cached_payload

        import httpx

        ctx = {"endpoint": endpoint, "symbol": symbol, "alert_id": alert_id}
        async with httpx.AsyncClient(timeout=10.0) as client:
            for route in routes:
                result = await self._try_route(client, route, params, ctx)
                if result is False:
                    return None  # hard stop (e.g. rate-limited)
                if result is not None:
                    self._cache_set(cache_key, result)
                    return result

        return None

    async def _try_route(
        self,
        client: object,
        route: str,
        params: dict | None,
        ctx: dict,
    ) -> dict | None | bool:
        """Attempt all retries for a single route.

        Returns:
            dict — success payload
            None — route exhausted, try next route
            False — stop all routes and return None to caller (e.g. rate-limited)
        """
        for attempt in range(1, self.request_max_attempts + 1):
            started = time.perf_counter()
            response = await self._send_request(client, route, params, started, attempt, ctx)
            if response is None:
                continue  # transport error, already logged — retry handled inside _send_request
            if response == "break":
                return None

            duration_ms = int((time.perf_counter() - started) * 1000)
            result = await self._process_response(response, route, duration_ms, attempt, ctx)
            if isinstance(result, dict):
                return result
            if result == "continue":
                continue
            if result == "return_none":
                return False  # stop all routes
            # result == "break" -> try next route
            break
        return None

    async def _send_request(
        self, client: object, route: str, params: dict | None, started: float, attempt: int, ctx: dict
    ) -> object | None | str:
        """Send HTTP request with semaphore. Returns response, None to retry, or 'break' to stop."""
        import httpx

        try:
            async with self._request_semaphore:
                return await client.get(route, params=params, headers=self.gateway_headers or None)
        except httpx.HTTPError as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            retryable = attempt < self.request_max_attempts
            self._log_and_record_failure(
                endpoint=ctx["endpoint"],
                status_code=None,
                symbol=ctx["symbol"],
                alert_id=ctx["alert_id"],
                attempt=attempt,
                retryable=retryable,
                duration_ms=duration_ms,
                route=route,
                error=str(exc),
            )
            if retryable:
                await self._sleep_before_retry(attempt)
                return None  # signals retry
            return "break"

    async def _process_response(
        self, response: object, route: str, duration_ms: int, attempt: int, ctx: dict
    ) -> dict | str:
        """Handle an HTTP response. Returns payload dict, or 'continue'/'break'/'return_none' sentinel."""
        status_code = response.status_code
        fail_kwargs = {
            "endpoint": ctx["endpoint"],
            "status_code": status_code,
            "symbol": ctx["symbol"],
            "alert_id": ctx["alert_id"],
            "attempt": attempt,
            "duration_ms": duration_ms,
            "route": route,
        }

        if status_code == 200:
            return self._parse_success(response, fail_kwargs)

        if status_code == 401:
            return self._handle_auth_failure(fail_kwargs)

        return await self._handle_retryable_failure(response, attempt, fail_kwargs)

    def _parse_success(self, response: object, fail_kwargs: dict) -> dict | str:
        """Parse a 200 response. Returns payload dict or 'break' on decode error."""
        try:
            payload = response.json()
        except (TypeError, ValueError) as decode_error:
            self._log_and_record_failure(**fail_kwargs, retryable=False, error=f"json_decode:{decode_error}")
            return "break"
        if not isinstance(payload, dict):
            self._log_and_record_failure(**fail_kwargs, retryable=False, error=f"payload_type:{type(payload).__name__}")
            return "break"
        record_watch_gateway_request(
            component="features",
            endpoint=fail_kwargs["endpoint"],
            outcome="success",
            status_code=200,
            duration_seconds=fail_kwargs["duration_ms"] / 1000.0,
        )
        record_watch_gateway_success(component="features", endpoint=fail_kwargs["endpoint"])
        return payload

    def _handle_auth_failure(self, fail_kwargs: dict) -> str:
        """Handle 401 responses. Returns 'break' or raises EnrichmentAuthFailure."""
        auth_failures = self._record_auth_failure()
        self._log_and_record_failure(
            **fail_kwargs,
            retryable=False,
            error="unauthorized",
            extra={"auth_failures_window": auth_failures},
        )
        if auth_failures >= self.auth_failure_threshold:
            raise EnrichmentAuthFailure(
                f"Repeated enrichment authorization failures for {fail_kwargs['endpoint']} ({auth_failures} in window)"
            )
        return "break"

    async def _handle_retryable_failure(self, response: object, attempt: int, fail_kwargs: dict) -> str:
        """Handle non-200/non-401 responses. Returns 'continue', 'break', or 'return_none'."""
        status_code = fail_kwargs["status_code"]
        retryable = is_retryable_http_status(status_code)
        retry_after_hint = parse_retry_after(getattr(response, "headers", {}))
        self._log_and_record_failure(**fail_kwargs, retryable=retryable)
        if retryable and attempt < self.request_max_attempts:
            await self._sleep_before_retry(attempt, minimum_delay=retry_after_hint)
            return "continue"
        if not should_try_legacy_fallback_for_status(status_code):
            return "return_none"
        return "break"

    async def _enrich_iv_rank(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with IV rank from Unusual Whales.

        IV rank indicates where current IV is relative to the past year's range.
        0 = lowest IV of the year, 100 = highest IV of the year.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping IV rank enrichment")
            return features

        try:
            routes = self._build_enrichment_routes(
                f"/uw/{features.underlying}/iv-rank",
                f"/uw/options/{features.underlying}/iv-rank",
            )
            data = await self._request_json_with_retry(
                endpoint="uw_iv_rank",
                routes=routes,
                symbol=features.underlying,
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            iv_data = data.get("data", {})
            if iv_data:
                parsed_iv_rank = coerce_optional_float(iv_data.get("iv_rank"))
                if parsed_iv_rank is not None:
                    features.iv_rank = parsed_iv_rank
                logger.debug(
                    "Enriched IV rank",
                    symbol=features.underlying,
                    iv_rank=features.iv_rank,
                )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich IV rank",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    async def _enrich_gex(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with GEX/VEX from Unusual Whales greek-exposure endpoint.

        GEX (gamma exposure) and VEX (vanna exposure) indicate market maker
        hedging pressure on the underlying.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping GEX enrichment")
            return features

        try:
            routes = self._build_enrichment_routes(
                f"/uw/gex/{features.underlying}",
                f"/uw/{features.underlying}/greek-exposure",
            )
            data = await self._request_json_with_retry(
                endpoint="uw_gex",
                routes=routes,
                symbol=features.underlying,
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            gex_data = _unwrap_data_payload(data)
            if isinstance(gex_data, dict):
                parsed_gex = _coalesce_split_or_fallback(
                    gex_data,
                    call_key="call_gamma",
                    put_key="put_gamma",
                    fallbacks=("gamma_exposure", "gex_oi", "gex"),
                )
                if parsed_gex is not None:
                    features.gex = parsed_gex
                parsed_vex = _coalesce_split_or_fallback(
                    gex_data,
                    call_key="call_vanna",
                    put_key="put_vanna",
                    fallbacks=("vanna_exposure", "vex_oi", "vex"),
                )
                if parsed_vex is not None:
                    features.vex = parsed_vex
                logger.debug(
                    "Enriched GEX",
                    symbol=features.underlying,
                    gex=features.gex,
                    vex=features.vex,
                )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich GEX",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    async def _enrich_max_pain(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with max pain strike and distance from UW.

        Max pain is the strike price where option holders would experience
        the greatest financial loss at expiration. Distance to max pain
        indicates how far the current price is from this level.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping max pain enrichment")
            return features

        try:
            routes = self._build_enrichment_routes(
                f"/uw/options/{features.underlying}/max-pain",
                f"/uw/{features.underlying}/max-pain",
            )
            data = await self._request_json_with_retry(
                endpoint="uw_max_pain",
                routes=routes,
                symbol=features.underlying,
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            mp_data = _unwrap_data_payload(data)
            if isinstance(mp_data, dict):
                strike = _coalesce_first(mp_data, "max_pain_strike", "max_pain", "price")
                if strike is not None:
                    features.max_pain_strike = strike
                    if features.spot_price and features.spot_price > 0:
                        features.max_pain_distance_pct = (features.spot_price - strike) / features.spot_price
                logger.debug(
                    "Enriched max pain",
                    symbol=features.underlying,
                    max_pain_strike=features.max_pain_strike,
                    max_pain_distance_pct=features.max_pain_distance_pct,
                )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich max pain",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    async def _enrich_market_tide(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with market tide sentiment from UW.

        Market tide aggregates net premium across the options market,
        indicating overall bullish/bearish positioning.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping market tide enrichment")
            return features

        try:
            routes = self._build_enrichment_routes("/uw/market/tide")
            data = await self._request_json_with_retry(
                endpoint="uw_market_tide",
                routes=routes,
                symbol="MARKET",
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            tide_data = _unwrap_data_payload(data)
            if isinstance(tide_data, dict):
                net_premium = _coalesce_first(tide_data, "net_premium", "net_call_premium")
                if net_premium is not None:
                    features.market_tide_net_premium = net_premium
                    features.market_tide_direction = _classify_direction(net_premium)
                logger.debug(
                    "Enriched market tide",
                    net_premium=features.market_tide_net_premium,
                    direction=features.market_tide_direction,
                )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich market tide",
                error=str(e),
            )

        return features

    async def _enrich_greeks(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with Greeks from Alpaca option chain."""
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping Greeks enrichment")
            return features

        try:
            routes = gateway_url_candidates(
                self.gateway_url,
                f"/alpaca/options/chain/{features.underlying}",
                include_legacy_fallback=self.legacy_route_fallback_enabled,
            )
            params = {
                "expiration_date": features.expiry.isoformat(),
                "strike_price_gte": features.strike - 0.01,
                "strike_price_lte": features.strike + 0.01,
                "option_type": "call" if features.put_call == "C" else "put",
            }
            data = await self._request_json_with_retry(
                endpoint="alpaca_options_chain",
                routes=routes,
                params=params,
                symbol=features.underlying,
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            contracts = data.get("data", {}).get("contracts", [])
            if not contracts:
                logger.debug("No contracts found for Greeks", symbol=features.underlying)
                return features

            contract = self._find_matching_contract(contracts, features.strike)
            if not contract:
                logger.debug("No valid contracts found for Greeks", symbol=features.underlying)
                return features

            features.delta = coerce_optional_float(contract.get("delta"))
            features.gamma = coerce_optional_float(contract.get("gamma"))
            features.theta = coerce_optional_float(contract.get("theta"))
            features.vega = coerce_optional_float(contract.get("vega"))
            features.iv = coerce_optional_float(contract.get("implied_volatility", contract.get("iv")))

            logger.debug(
                "Enriched Greeks",
                symbol=features.underlying,
                delta=features.delta,
                gamma=features.gamma,
                iv=features.iv,
            )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich Greeks",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    def _find_matching_contract(self, contracts: list[dict], target_strike: float) -> dict | None:
        """Find the contract matching the target strike, or fall back to the first valid one."""
        for c in contracts:
            strike_price = coerce_optional_float(c.get("strike_price", c.get("strike")))
            if strike_price is not None and abs(strike_price - target_strike) < 0.01:
                return c

        for c in contracts:
            if coerce_optional_float(c.get("strike_price", c.get("strike"))) is not None:
                return c

        return None

    async def _enrich_market_context(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with market context data from Data Gateway."""
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping market enrichment")
            return features

        try:
            from datetime import timedelta

            symbol = features.underlying
            end_date = features.alert_time.date()
            start_date = end_date - timedelta(days=50)

            routes = gateway_url_candidates(
                self.gateway_url,
                f"/alpaca/stocks/{symbol}/bars",
                include_legacy_fallback=self.legacy_route_fallback_enabled,
            )
            params = {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "timeframe": "1Day",
                "limit": 50,
            }
            data = await self._request_json_with_retry(
                endpoint="alpaca_stock_bars",
                routes=routes,
                params=params,
                symbol=symbol,
                alert_id=features.alert_id,
            )
            if data is None:
                return features

            bars = data.get("data", {}).get("bars", [])
            if not bars or len(bars) < 2:
                logger.debug("Insufficient bars for enrichment", symbol=symbol, count=len(bars))
                return features

            bars = sorted(bars, key=lambda b: b.get("t", ""), reverse=True)
            closes: list[float | None] = [coerce_optional_float(bar.get("c", bar.get("close"))) for bar in bars]

            if len(closes) < 2 or closes[0] is None or closes[0] <= 0:
                return features

            self._compute_returns(features, closes)
            self._compute_realized_vol_20d(features, closes)

            logger.debug(
                "Enriched market context",
                symbol=symbol,
                return_1d=features.underlying_1d_return,
                return_5d=features.underlying_5d_return,
                return_30d=features.underlying_30d_return,
                vol_20d=features.realized_vol_20d,
            )

        except EnrichmentAuthFailure:
            raise
        except Exception as e:
            logger.warning(
                "Failed to enrich market context",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    @staticmethod
    def _compute_returns(features: AlertFeatures, closes: list[float | None]) -> None:
        """Compute 1d, 5d, 30d returns from close prices."""
        current_close = closes[0]
        if len(closes) >= 2 and closes[1] is not None and closes[1] > 0:
            features.underlying_1d_return = (current_close / closes[1]) - 1.0
        if len(closes) >= 6 and closes[5] is not None and closes[5] > 0:
            features.underlying_5d_return = (current_close / closes[5]) - 1.0
        if len(closes) >= 31 and closes[30] is not None and closes[30] > 0:
            features.underlying_30d_return = (current_close / closes[30]) - 1.0

    @staticmethod
    def _compute_realized_vol_20d(features: AlertFeatures, closes: list[float | None]) -> None:
        """Compute 20-day realized volatility (annualized)."""
        import math

        if len(closes) < 21:
            return
        daily_returns = []
        for i in range(20):
            if closes[i] is not None and closes[i + 1] is not None and closes[i] > 0 and closes[i + 1] > 0:
                daily_returns.append(math.log(closes[i] / closes[i + 1]))
        if not daily_returns:
            return
        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(daily_returns)
        features.realized_vol_20d = math.sqrt(variance) * math.sqrt(252)


# Redis key pattern for feature storage
FEATURES_KEY = "heber:watch:features:{alert_id}"
DEFAULT_FEATURES_OUTPUT_PATH = settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"


async def store_features(redis: Redis, features: AlertFeatures) -> None:
    """Store extracted features in Redis.

    Args:
        redis: Redis client.
        features: Extracted features.
    """
    import json

    key = FEATURES_KEY.format(alert_id=features.alert_id)
    await redis.set(key, json.dumps(features.to_dict()), ex=86400 * 7)  # 7 day TTL


async def get_features(redis: Redis, alert_id: str) -> AlertFeatures | None:
    """Retrieve stored features from Redis.

    Args:
        redis: Redis client.
        alert_id: Alert identifier.

    Returns:
        AlertFeatures if found, None otherwise.
    """
    import json

    key = FEATURES_KEY.format(alert_id=alert_id)
    data = await redis.get(key)
    if data is None:
        return None
    return AlertFeatures.from_dict(json.loads(data))


def persist_features_to_gold(features: AlertFeatures, output_path: Path | None = None) -> None:
    """Persist one feature row into Gold meta-label feature partitions."""
    row = dict(features.__dict__)
    features_df = pl.DataFrame([row])
    persist_features_frame_to_gold(
        features_df=features_df,
        output_path=output_path or DEFAULT_FEATURES_OUTPUT_PATH,
        partition_col="alert_time",
    )
