"""Alert Watch Consumer - Creates watches from flow_alerts stream.

Listens to the flow_alerts Redis stream and automatically creates
watches for each incoming alert.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from typing import Any

import httpx
import redis
import structlog

from heber.config import settings
from heber.core.http_client import create_async_http_client
from heber.features.templates.alert_labels import (
    AlertHorizon,
    ContractBarrierConfig,
    classify_horizon,
)
from heber.ops.metrics import (
    record_watch_alert_parse,
    record_watch_gateway_request,
    record_watch_watch_created,
)
from heber.ops.runtime_retry import calculate_retry_delay, classify_runtime_error
from heber.watch.features import (
    FEATURES_KEY,
    AlertFeatureExtractor,
    EnrichmentAuthFailure,
    persist_features_to_gold,
    store_features,
)
from heber.watch.gateway import (
    DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS,
    coerce_optional_float,
    coerce_utc_timestamp,
    gateway_auth_headers,
    gateway_url_candidates,
    quote_age_seconds,
    route_failure_for_exception,
    route_failure_for_http_status,
    route_failure_for_payload_shape,
    route_failure_for_symbol_missing,
    route_failure_for_symbol_shape,
    should_continue_route_fallback,
)
from heber.watch.manager import WatchManager
from heber.watch.models import POLL_CONFIG, WatchHorizon

logger = structlog.get_logger(__name__)

# Stream configuration
DEFAULT_EVENTS_STREAM = settings.redis_stream_name
FLOW_ALERTS_FEED = "flow_alerts"  # Filter by this feed type
CONSUMER_GROUP = "watch-consumer"
CONSUMER_NAME = "watch-consumer-1"
DATA_GATEWAY_URL = "http://localhost:8000"
FEATURE_STORAGE_TTL_SECONDS = 86400 * 7


def _alert_horizon_to_watch_horizon(horizon: AlertHorizon) -> WatchHorizon:
    """Convert AlertHorizon enum to WatchHorizon enum."""
    mapping = {
        AlertHorizon.INTRADAY: WatchHorizon.INTRADAY,
        AlertHorizon.SWING: WatchHorizon.SWING,
        AlertHorizon.LEAP: WatchHorizon.LEAP,
    }
    return mapping.get(horizon, WatchHorizon.SWING)


class AlertWatchConsumer:
    """Consumes flow alerts and creates watches.

    Runs as a background service, listening to the events stream and filtering
    for the flow_alerts feed.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        watch_manager: WatchManager,
        contract_config: ContractBarrierConfig | None = None,
        gateway_url: str = DATA_GATEWAY_URL,
        gateway_api_key: str | None = None,
        legacy_route_fallback_enabled: bool | None = None,
        async_redis: redis.asyncio.Redis | None = None,
        stream_name: str | None = None,
        dlq_stream_name: str | None = None,
        max_process_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
    ):
        """Initialize the consumer.

        Args:
            redis_client: Redis client (sync)
            watch_manager: WatchManager instance
            contract_config: Barrier configuration for contracts
            gateway_url: Data Gateway URL for fetching entry prices
            async_redis: Async Redis client for feature storage (optional)
            stream_name: Redis stream to consume events from
            dlq_stream_name: Redis stream for watch processing failures
            max_process_retries: Retry attempts before dead-lettering
            retry_backoff_seconds: Base retry backoff delay in seconds
        """
        self.redis = redis_client
        self.async_redis = async_redis
        self.manager = watch_manager
        self.config = contract_config or ContractBarrierConfig.moderate()
        self.gateway_url = gateway_url
        self.gateway_headers = gateway_auth_headers(gateway_api_key)
        if legacy_route_fallback_enabled is None:
            legacy_route_fallback_enabled = settings.watch_gateway_legacy_fallback_enabled
        self.legacy_route_fallback_enabled = bool(legacy_route_fallback_enabled)
        self.stream_name = stream_name or DEFAULT_EVENTS_STREAM
        self.dlq_stream_name = dlq_stream_name or settings.redis_dlq_stream_name
        configured_retries = settings.redis_process_max_retries if max_process_retries is None else max_process_retries
        self.max_process_retries = max(1, int(configured_retries))
        configured_backoff = (
            settings.redis_retry_backoff_seconds if retry_backoff_seconds is None else retry_backoff_seconds
        )
        self.retry_backoff_seconds = max(0.0, configured_backoff)
        self._running = False

        # Feature extractor for meta-labeling
        self.feature_extractor = AlertFeatureExtractor(
            redis=async_redis,
            gateway_url=gateway_url,
            gateway_api_key=gateway_api_key,
            legacy_route_fallback_enabled=self.legacy_route_fallback_enabled,
        )

    def setup_consumer_group(self) -> None:
        """Create consumer group if it doesn't exist."""
        try:
            self.redis.xgroup_create(
                self.stream_name,
                CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Created consumer group",
                stream=self.stream_name,
                group=CONSUMER_GROUP,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                # Group already exists
                pass
            else:
                raise

    async def _setup_consumer_group_async(self) -> None:
        """Async wrapper for consumer group setup."""
        await asyncio.to_thread(self.setup_consumer_group)

    async def _read_messages(self):
        """Read stream messages without blocking the event loop."""
        return await asyncio.to_thread(
            self.redis.xreadgroup,
            CONSUMER_GROUP,
            CONSUMER_NAME,
            {self.stream_name: ">"},
            count=100,
            block=5000,
        )

    async def _ack_message(self, msg_id: str) -> None:
        """Acknowledge stream message without blocking the event loop."""
        await asyncio.to_thread(self.redis.xack, self.stream_name, CONSUMER_GROUP, msg_id)

    @staticmethod
    def _normalize_stream_data(data: dict[Any, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in data.items():
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            if isinstance(value, bytes):
                normalized[key_str] = value.decode()
            else:
                normalized[key_str] = value
        return normalized

    async def _dead_letter_message(self, msg_id: str, data: dict, attempts: int, error: str) -> bool:
        """Write failed message to DLQ stream."""
        dlq_payload = {
            "origin_stream": self.stream_name,
            "origin_message_id": msg_id,
            "attempts": str(attempts),
            "error": error,
            "failed_at": datetime.now(UTC).isoformat(),
            "payload": json.dumps(self._normalize_stream_data(data), default=str),
        }
        try:
            await asyncio.to_thread(self.redis.xadd, self.dlq_stream_name, dlq_payload)
            logger.error(
                "Watch message dead-lettered",
                stream=self.dlq_stream_name,
                msg_id=msg_id,
                attempts=attempts,
                error=error,
            )
            return True
        except Exception as dlq_error:
            logger.error(
                "Failed to dead-letter watch message",
                stream=self.dlq_stream_name,
                msg_id=msg_id,
                attempts=attempts,
                error=str(dlq_error),
            )
            return False

    async def _process_flow_alert_with_retries(self, msg_id: str, data: dict) -> bool:
        """Process one flow alert with retry + DLQ behavior."""
        msg_id_text = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
        last_reason = "processing_failed"
        for attempt in range(1, self.max_process_retries + 1):
            success, retryable, reason = self._normalize_process_result(await self._process_alert(msg_id_text, data))
            if success:
                return True
            last_reason = reason
            if not retryable:
                logger.warning(
                    "Non-retriable flow alert failure",
                    msg_id=msg_id_text,
                    reason=reason,
                )
                return True
            if attempt < self.max_process_retries:
                await asyncio.sleep(max(0.0, self.retry_backoff_seconds * attempt))

        return await self._dead_letter_message(
            msg_id=msg_id_text,
            data=data,
            attempts=self.max_process_retries,
            error=f"processing_failed_after_retries:{last_reason}",
        )

    @staticmethod
    def _normalize_process_result(result: Any) -> tuple[bool, bool, str]:
        """Normalize process results from bool or tuple return conventions."""
        if not isinstance(result, tuple):
            success = bool(result)
            return success, not success, "processed" if success else "processing_failed"

        success = bool(result[0])
        default_reason = "processed" if success else "processing_failed"

        if len(result) >= 3:
            reason = str(result[2]) if result[2] else default_reason
            return success, bool(result[1]), reason
        if len(result) == 2:
            return success, bool(result[1]), default_reason

        return success, not success, default_reason

    async def _handle_message(self, msg_id: str, data: dict) -> bool:
        """Handle one stream entry and indicate whether it should be ACKed."""
        if not self._is_flow_alert(data):
            return True
        return await self._process_flow_alert_with_retries(msg_id, data)

    async def run(self) -> None:
        """Run the consumer as a continuous service."""
        self._running = True
        await self._setup_consumer_group_async()
        error_streak = 0

        logger.info(
            "Starting alert watch consumer",
            stream=self.stream_name,
            group=CONSUMER_GROUP,
        )

        while self._running:
            try:
                messages = await self._read_messages()
                if messages:
                    await self._dispatch_messages(messages)
                error_streak = 0
            except EnrichmentAuthFailure as auth_error:
                logger.error("Fatal enrichment auth failure", error=str(auth_error), exc_info=True)
                raise
            except Exception as e:
                error_streak += 1
                delay = calculate_retry_delay(
                    attempt=error_streak,
                    base_seconds=self.retry_backoff_seconds,
                    max_seconds=30.0,
                    jitter_ratio=0.2,
                )
                is_transient, error_kind = classify_runtime_error(e)
                if is_transient:
                    logger.warning(
                        "Alert watch transient runtime error",
                        error=str(e),
                        error_kind=error_kind,
                        consecutive_errors=error_streak,
                        retry_delay_seconds=round(delay, 3),
                    )
                else:
                    logger.error(
                        "Consumer error",
                        error=str(e),
                        error_kind=error_kind,
                        consecutive_errors=error_streak,
                        retry_delay_seconds=round(delay, 3),
                    )
                await asyncio.sleep(delay)

    async def _dispatch_messages(self, messages: list) -> None:
        """Dispatch a batch of stream messages."""
        for _stream, entries in messages:
            for msg_id, data in entries:
                should_ack = await self._handle_message(msg_id, data)
                if should_ack:
                    await self._ack_message(msg_id)

    def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        logger.info("Alert watch consumer stopped")

    def _is_flow_alert(self, data: dict) -> bool:
        """Check if the event is a flow_alerts feed event.

        The configured events stream contains multiple feed types
        (flow_alerts, darkpool, market_tide, etc.). We only process flow_alerts.
        """
        # The 'data' field contains JSON string with event envelope.
        payload = data.get(b"data")
        if payload is None:
            payload = data.get("data")
        if payload is None:
            return False

        try:
            if isinstance(payload, bytes):
                payload = payload.decode()
            if isinstance(payload, str):
                envelope = json.loads(payload)
            elif isinstance(payload, dict):
                envelope = payload
            else:
                return False
            return envelope.get("feed") == FLOW_ALERTS_FEED
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
            pass
        return False

    @staticmethod
    def _classify_alert_results(
        created: int,
        missing_occ: int,
        stale_window: int,
        exceptions: int,
    ) -> tuple[bool, bool, str]:
        """Classify batch processing results into (success, retryable, reason)."""
        if created > 0:
            return True, False, "watch_created"
        if stale_window > 0 and missing_occ == 0 and exceptions == 0:
            return True, False, "stale_alert_window"
        if missing_occ > 0 and exceptions == 0:
            return True, False, "missing_occ_symbol"
        if stale_window > 0 and missing_occ > 0 and exceptions == 0:
            return True, False, "stale_or_missing_occ_symbol"
        if exceptions > 0:
            return False, True, "processing_exception"
        return False, False, "alert_parse_failed"

    async def _process_one_alert_safe(
        self,
        alert: dict,
        msg_id: str,
        batch_index: int,
    ) -> str:
        """Process a single alert, returning status string or raising on fatal auth errors."""
        try:
            return await self._process_parsed_alert(alert)
        except EnrichmentAuthFailure:
            logger.error(
                "Fatal enrichment auth failure while processing parsed alert",
                msg_id=msg_id,
                alert_id=alert.get("id"),
                batch_index=batch_index,
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.error(
                "Failed to process parsed alert",
                msg_id=msg_id,
                alert_id=alert.get("id"),
                batch_index=batch_index,
                error=str(e),
            )
            return "processing_exception"

    async def _process_alert(self, msg_id: str, data: dict) -> tuple[bool, bool, str]:
        """Process one stream message that may contain one or many alerts."""
        alerts = self._parse_alerts(data)
        if not alerts:
            parser_override = self.__dict__.get("_parse_alert")
            if callable(parser_override):
                parsed = parser_override(data)
                if parsed:
                    alerts = [parsed]
        if not alerts:
            logger.warning("Could not parse alert", msg_id=msg_id)
            return False, False, "alert_parse_failed"

        created_count = 0
        skipped_missing_occ = 0
        skipped_stale_window = 0
        processing_exceptions = 0

        for batch_index, alert in enumerate(alerts):
            result = await self._process_one_alert_safe(alert, msg_id, batch_index)
            if result == "watch_created":
                created_count += 1
            elif result == "missing_occ_symbol":
                skipped_missing_occ += 1
            elif result == "stale_alert_window":
                skipped_stale_window += 1
            elif result == "processing_exception":
                processing_exceptions += 1

        return self._classify_alert_results(
            created_count,
            skipped_missing_occ,
            skipped_stale_window,
            processing_exceptions,
        )

    async def _process_parsed_alert(self, alert: dict[str, Any]) -> str:
        """Process one parsed alert and create its watch."""
        # Skip if no OCC symbol
        if not alert.get("occ_symbol"):
            logger.debug("Alert has no OCC symbol, skipping", alert_id=alert.get("id"))
            return "missing_occ_symbol"

        # Determine horizon based on DTE
        dte = alert.get("dte", 5)
        alert_horizon = classify_horizon(dte)
        watch_horizon = _alert_horizon_to_watch_horizon(alert_horizon)
        alert_time = self._normalize_alert_time(alert.get("ts_event"))
        calendar = getattr(self.manager, "calendar", None)
        if calendar is not None:
            window_end = calendar.add_trading_hours(
                alert_time,
                POLL_CONFIG[watch_horizon]["max_duration_hours"],
            )
            if window_end <= datetime.now(UTC):
                logger.info(
                    "Skipping stale alert window",
                    alert_id=alert.get("id"),
                    occ_symbol=alert.get("occ_symbol"),
                    horizon=watch_horizon.value,
                    alert_time=alert_time.isoformat(),
                    window_end=window_end.isoformat(),
                )
                return "stale_alert_window"

        # Prefer price carried with the alert to avoid stale quote lookups.
        entry_price = coerce_optional_float(alert.get("contract_px"))
        if entry_price is None or entry_price <= 0:
            entry_price = await self._get_entry_price(alert["occ_symbol"])

        if not entry_price or entry_price <= 0:
            logger.warning(
                "Could not get entry price",
                occ_symbol=alert["occ_symbol"],
            )
            entry_price = 1.0

        # Create watch
        watch = await self.manager.create_watch_async(
            alert_id=alert["id"],
            occ_symbol=alert["occ_symbol"],
            underlying=alert["underlying"],
            put_call=alert["put_call"],
            expiry=alert.get("expiry", ""),
            strike=alert.get("strike", 0.0),
            entry_price=entry_price,
            spot_at_alert=alert.get("spot_px", 0.0),
            alert_time=alert_time,
            horizon=watch_horizon,
            tp_threshold=self.config.tp_pct,
            sl_threshold=self.config.sl_pct,
        )

        # Extract and store features for meta-labeling
        await self._extract_and_store_features(alert, watch.watch_id)

        logger.info(
            "Created watch from alert",
            watch_id=watch.watch_id,
            alert_id=alert["id"],
            occ_symbol=alert["occ_symbol"],
            horizon=watch_horizon.value,
        )
        record_watch_watch_created()
        return "watch_created"

    def _parse_alert(self, data: dict) -> dict | None:
        """Parse one alert from a stream message for compatibility callers."""
        parsed_alerts = self._parse_alerts(data)
        if not parsed_alerts:
            return None
        return parsed_alerts[0]

    @staticmethod
    def _normalize_alert_time(raw: Any) -> datetime:
        return coerce_utc_timestamp(raw) or datetime.now(UTC)

    def _parse_alerts(self, data: dict) -> list[dict[str, Any]]:
        """Parse alert(s) from stream message supporting single and batched payloads."""
        try:
            parsed = self._decode_stream_data(data)
            maybe_items = parsed.get("items")

            if maybe_items is None:
                alert = self._build_alert_record(parsed, batch_index=None)
                alerts = [alert] if alert else []
                self._log_parse_summary(
                    alert_parse_success=len(alerts),
                    alert_parse_failed=0 if alerts else 1,
                    batch_items_total=0,
                    batch_items_failed=0,
                )
                return alerts

            if not isinstance(maybe_items, list):
                self._log_parse_summary(
                    alert_parse_success=0,
                    alert_parse_failed=1,
                    batch_items_total=0,
                    batch_items_failed=1,
                )
                return []

            parsed_alerts: list[dict[str, Any]] = []
            batch_items_failed = 0
            for batch_index, item in enumerate(maybe_items):
                if not isinstance(item, dict):
                    batch_items_failed += 1
                    logger.warning(
                        "Alert batch item invalid shape",
                        batch_index=batch_index,
                        payload_type=type(item).__name__,
                    )
                    continue
                item_payload = {**parsed, **item}
                nested_payload = item.get("payload")
                if isinstance(nested_payload, dict):
                    item_payload = {**item_payload, **nested_payload}
                alert = self._build_alert_record(item_payload, batch_index=batch_index)
                if alert is None:
                    batch_items_failed += 1
                    continue
                parsed_alerts.append(alert)

            self._log_parse_summary(
                alert_parse_success=len(parsed_alerts),
                alert_parse_failed=batch_items_failed,
                batch_items_total=len(maybe_items),
                batch_items_failed=batch_items_failed,
            )
            return parsed_alerts

        except Exception as e:
            logger.error("Failed to parse alert", error=str(e))
            self._log_parse_summary(
                alert_parse_success=0,
                alert_parse_failed=1,
                batch_items_total=0,
                batch_items_failed=0,
            )
            return []

    def _build_alert_record(self, parsed: dict[str, Any], batch_index: int | None) -> dict[str, Any] | None:
        """Build normalized alert record from parsed payload."""
        result = self._map_alert_fields(parsed)
        if not result.get("id") or not result.get("underlying"):
            logger.warning(
                "Alert missing required fields",
                has_id=bool(result.get("id")),
                has_underlying=bool(result.get("underlying")),
                has_occ_symbol=bool(result.get("occ_symbol")),
                batch_index=batch_index,
            )
            return None
        result["dte"] = self._calculate_dte(result.get("expiry"))
        result["ts_event"] = self._parse_timestamp(parsed)
        return result

    @staticmethod
    def _log_parse_summary(
        *,
        alert_parse_success: int,
        alert_parse_failed: int,
        batch_items_total: int,
        batch_items_failed: int,
    ) -> None:
        if alert_parse_success > 0:
            record_watch_alert_parse(status="success", count=alert_parse_success)
        if alert_parse_failed > 0:
            record_watch_alert_parse(status="failed", count=alert_parse_failed)
        logger.info(
            "Alert parsing summary",
            alert_parse_success=alert_parse_success,
            alert_parse_failed=alert_parse_failed,
            batch_items_total=batch_items_total,
            batch_items_failed=batch_items_failed,
        )

    def _decode_stream_data(self, data: dict) -> dict:
        """Decode bytes and parse nested JSON from stream message."""
        parsed = {}
        for k, v in data.items():
            key = k.decode(errors="replace") if isinstance(k, bytes) else k
            val = v.decode(errors="replace") if isinstance(v, bytes) else v

            if isinstance(val, str) and val.lstrip().startswith(("{", "[")):
                try:
                    val = json.loads(val)
                except json.JSONDecodeError:
                    pass

            parsed[key] = val

        # Flatten 'data' envelope
        if "data" in parsed and isinstance(parsed["data"], dict):
            parsed = {**parsed, **parsed["data"]}

        # Flatten 'payload' - this is where UW alert fields like option_chain live
        if "payload" in parsed and isinstance(parsed["payload"], dict):
            parsed = {**parsed, **parsed["payload"]}

        return parsed

    @staticmethod
    def _first_present_value(parsed: dict[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            if key in parsed and parsed[key] is not None:
                return parsed[key]
        return None

    @staticmethod
    def _normalize_optional_text(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_tags(value: Any) -> list[str] | None:
        if value is None:
            return None
        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                decoded = json.loads(raw)
                if isinstance(decoded, list):
                    tags = [str(item).strip() for item in decoded if str(item).strip()]
                    return tags or None
            except json.JSONDecodeError:
                pass
            tags = [part.strip() for part in raw.replace("|", ",").split(",") if part.strip()]
            return tags or None
        if isinstance(value, list | tuple | set):
            tags = [str(item).strip() for item in value if str(item).strip()]
            return tags or None
        return None

    @staticmethod
    def _coerce_float_or_default(value: Any, default: float = 0.0) -> float:
        """Coerce value to float, returning default if None."""
        result = coerce_optional_float(value)
        return result if result is not None else default

    def _resolve_put_call(self, parsed: dict) -> str:
        """Resolve put/call from available parsed fields."""
        put_call_raw = self._first_present_value(parsed, ("put_call", "option_type", "call_put"))
        type_raw = parsed.get("type")
        if put_call_raw is None and isinstance(type_raw, str):
            normalized_type = type_raw.strip().upper()
            if normalized_type.startswith("C") or normalized_type.startswith("P"):
                put_call_raw = normalized_type
        return self._normalize_put_call(put_call_raw or "C")

    def _resolve_alert_type(self, parsed: dict) -> str:
        """Resolve alert_type from available parsed fields."""
        alert_type = self._normalize_optional_text(
            self._first_present_value(parsed, ("alert_type", "flow_type", "order_type"))
        )
        if alert_type is not None:
            return alert_type
        type_raw = parsed.get("type")
        if isinstance(type_raw, str):
            normalized_type = type_raw.strip().upper()
            if not (normalized_type.startswith("C") or normalized_type.startswith("P")):
                return normalized_type
        return "UNKNOWN"

    def _map_alert_fields(self, parsed: dict) -> dict:
        """Map various field name conventions to standard fields."""
        put_call = self._resolve_put_call(parsed)
        strike = self._coerce_float_or_default(self._first_present_value(parsed, ("strike",)))

        normalized_spot_px = self._coerce_float_or_default(
            self._first_present_value(parsed, ("spot_px", "underlying_price", "spot", "underlying_px"))
        )
        normalized_contract_px = self._coerce_float_or_default(
            self._first_present_value(parsed, ("contract_px", "price", "option_price", "mid", "mid_price"))
        )
        normalized_premium = self._coerce_float_or_default(
            self._first_present_value(
                parsed, ("premium", "total_premium", "premium_amount", "notional_premium", "notional")
            )
        )
        normalized_volume = self._coerce_float_or_default(
            self._first_present_value(parsed, ("volume", "size", "contracts", "contract_volume", "total_size"))
        )
        normalized_open_interest = coerce_optional_float(
            self._first_present_value(parsed, ("open_interest", "oi", "openInterest"))
        )

        normalized_volume_oi_ratio = coerce_optional_float(
            self._first_present_value(parsed, ("volume_oi_ratio", "vol_oi_ratio"))
        )
        if normalized_volume_oi_ratio is None and normalized_open_interest is not None and normalized_open_interest > 0:
            normalized_volume_oi_ratio = normalized_volume / normalized_open_interest

        return {
            "id": parsed.get("id") or parsed.get("event_id") or parsed.get("alert_id"),
            "occ_symbol": parsed.get("occ_symbol") or parsed.get("option_chain"),
            "underlying": parsed.get("underlying") or parsed.get("ticker") or parsed.get("symbol"),
            "put_call": put_call,
            "expiry": parsed.get("expiry"),
            "strike": strike,
            "spot_px": normalized_spot_px,
            "contract_px": normalized_contract_px,
            "premium": normalized_premium,
            "volume": normalized_volume,
            "open_interest": normalized_open_interest,
            "alert_type": self._resolve_alert_type(parsed),
            "side": self._normalize_optional_text(self._first_present_value(parsed, ("side", "execution_side"))),
            "aggressor": self._normalize_optional_text(self._first_present_value(parsed, ("aggressor",))),
            "tags": self._normalize_tags(self._first_present_value(parsed, ("tags", "tag_list", "flags"))),
            "is_sweep": parsed.get("is_sweep"),
            "is_unusual": parsed.get("is_unusual"),
            "sentiment": self._normalize_optional_text(parsed.get("sentiment")),
            "trade_count": parsed.get("trade_count"),
            "volume_oi_ratio": normalized_volume_oi_ratio,
        }

    @staticmethod
    def _normalize_put_call(value: Any) -> str:
        """Normalize put/call values to C/P with a stable default."""
        if isinstance(value, str):
            normalized = value.strip().upper()
            if normalized.startswith("P"):
                return "P"
            if normalized.startswith("C"):
                return "C"
        return "C"

    def _calculate_dte(self, expiry: str | None) -> int:
        """Calculate days to expiry from expiry string."""
        if not expiry:
            return 5
        try:
            from datetime import date

            exp_date = datetime.strptime(str(expiry)[:10], "%Y-%m-%d").date()
            return (exp_date - date.today()).days
        except Exception:
            return 5

    def _parse_timestamp(self, parsed: dict) -> datetime:
        """Parse timestamp from various field formats."""
        for key in ("ts_event", "created_at", "timestamp"):
            result = coerce_utc_timestamp(parsed.get(key))
            if result is not None:
                return result
        return datetime.now(UTC)

    async def _get_entry_price(self, occ_symbol: str) -> float | None:
        """Get latest option quote from Data Gateway.

        Args:
            occ_symbol: OCC option symbol

        Returns:
            Mid price or None
        """
        try:
            async with create_async_http_client(timeout=10.0) as client:
                routes = gateway_url_candidates(
                    self.gateway_url,
                    "/alpaca/options/quotes",
                    include_legacy_fallback=self.legacy_route_fallback_enabled,
                )
                now_utc = datetime.now(UTC)
                best_stale_price: float | None = None
                best_stale_age_seconds: float | None = None
                route_failures: list[dict[str, Any]] = []

                for route in routes:
                    result = await self._try_route(
                        client,
                        route,
                        occ_symbol,
                        now_utc,
                        route_failures,
                    )
                    if result is None:
                        if not should_continue_route_fallback(route_failures):
                            break
                        continue
                    price, is_stale, age = result
                    if not is_stale:
                        return price
                    if best_stale_age_seconds is None or age < best_stale_age_seconds:
                        best_stale_price = price
                        best_stale_age_seconds = age

                return self._resolve_stale_fallback(
                    occ_symbol,
                    routes,
                    route_failures,
                    best_stale_price,
                    best_stale_age_seconds,
                )

        except Exception as e:
            logger.error("Failed to get entry price", occ_symbol=occ_symbol, error=str(e))
            return None

    async def _try_route(
        self,
        client: httpx.AsyncClient,
        route: str,
        occ_symbol: str,
        now_utc: datetime,
        route_failures: list[dict[str, Any]],
    ) -> tuple[float, bool, float] | None:
        """Try a single gateway route. Returns (price, is_stale, age_seconds) or None."""
        started = time.perf_counter()
        try:
            response = await client.get(
                route,
                params={"symbols": occ_symbol},
                headers=self.gateway_headers or None,
            )
        except httpx.HTTPError as request_error:
            route_failures.append(route_failure_for_exception(route, request_error))
            logger.warning("Entry price route request failed", route=route, error=str(request_error))
            record_watch_gateway_request(
                component="consumer_entry_price",
                endpoint="alpaca_options_quotes",
                outcome="transport_error",
                status_code=None,
                duration_seconds=max(0.0, time.perf_counter() - started),
            )
            return None

        if response.status_code != 200:
            route_failures.append(route_failure_for_http_status(route, response.status_code))
            record_watch_gateway_request(
                component="consumer_entry_price",
                endpoint="alpaca_options_quotes",
                outcome="http_error",
                status_code=response.status_code,
                duration_seconds=max(0.0, time.perf_counter() - started),
            )
            return None

        quote_payload = self._validate_route_response(response, route, occ_symbol, route_failures)
        if quote_payload is None:
            return None

        computed_price = self._extract_price_from_quote(quote_payload, route, occ_symbol, route_failures)
        if computed_price is None:
            return None

        age = quote_age_seconds(quote_payload, now_utc)
        if age is not None and age > DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS:
            route_failures.append(
                {
                    "route": route,
                    "failure": "quote_stale",
                    "symbol": occ_symbol,
                    "quote_age_seconds": age,
                    "max_quote_age_seconds": DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS,
                }
            )
            return computed_price, True, age

        record_watch_gateway_request(
            component="consumer_entry_price",
            endpoint="alpaca_options_quotes",
            outcome="success",
            status_code=200,
            duration_seconds=max(0.0, time.perf_counter() - started),
        )
        return computed_price, False, 0.0

    def _validate_route_response(
        self,
        response: httpx.Response,
        route: str,
        occ_symbol: str,
        route_failures: list[dict[str, Any]],
    ) -> dict | None:
        """Validate and drill into gateway response to get the symbol's quote dict."""
        try:
            decoded = response.json()
        except (TypeError, ValueError) as decode_error:
            route_failures.append(route_failure_for_exception(route, decode_error, failure="json_decode"))
            logger.warning("Entry price response JSON decode failed", route=route, error=str(decode_error))
            return None

        for label, payload_part in [
            ("payload_shape", decoded),
            ("data_payload_shape", decoded.get("data", {}) if isinstance(decoded, dict) else None),
        ]:
            if not isinstance(payload_part, dict):
                route_failures.append(route_failure_for_payload_shape(route, label, payload_part))
                logger.warning(
                    "Entry price payload shape invalid",
                    route=route,
                    payload_type=type(payload_part).__name__,
                )
                return None

        data_payload = decoded["data"]
        quotes_payload = data_payload.get("quotes", {})
        if not isinstance(quotes_payload, dict):
            route_failures.append(route_failure_for_payload_shape(route, "quotes_payload_shape", quotes_payload))
            logger.warning(
                "Entry price quotes payload shape invalid",
                route=route,
                payload_type=type(quotes_payload).__name__,
            )
            return None

        quote_payload = quotes_payload.get(occ_symbol)
        if quote_payload is None:
            route_failures.append(route_failure_for_symbol_missing(route, occ_symbol))
            return None
        if not isinstance(quote_payload, dict):
            route_failures.append(route_failure_for_symbol_shape(route, occ_symbol, quote_payload))
            return None

        return quote_payload

    def _extract_price_from_quote(
        self,
        quote_payload: dict,
        route: str,
        occ_symbol: str,
        route_failures: list[dict[str, Any]],
    ) -> float | None:
        """Extract mid or last price from a validated quote payload."""
        bid = coerce_optional_float(quote_payload.get("bp"))
        if bid is None:
            bid = coerce_optional_float(quote_payload.get("bid_price"))

        ask = coerce_optional_float(quote_payload.get("ap"))
        if ask is None:
            ask = coerce_optional_float(quote_payload.get("ask_price"))

        if bid is not None and ask is not None:
            return (bid + ask) / 2

        last_price = coerce_optional_float(quote_payload.get("last_price"))
        if last_price is not None:
            return last_price

        route_failures.append({"route": route, "failure": "quote_price_unusable", "symbol": occ_symbol})
        return None

    @staticmethod
    def _resolve_stale_fallback(
        occ_symbol: str,
        routes: list[str],
        route_failures: list[dict[str, Any]],
        best_stale_price: float | None,
        best_stale_age_seconds: float | None,
    ) -> float | None:
        """Return stale price fallback or None after logging."""
        if best_stale_price is not None:
            logger.warning(
                "Entry price using stale quote fallback",
                occ_symbol=occ_symbol,
                quote_age_seconds=best_stale_age_seconds,
                max_quote_age_seconds=DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS,
            )
            record_watch_gateway_request(
                component="consumer_entry_price",
                endpoint="alpaca_options_quotes",
                outcome="stale_fallback",
                status_code=None,
                duration_seconds=0.0,
            )
            return best_stale_price
        if route_failures:
            logger.warning(
                "Entry price fetch failed across routes",
                occ_symbol=occ_symbol,
                routes=routes,
                failures=route_failures,
            )
        return None

    async def _extract_and_store_features(self, alert: dict, watch_id: str) -> None:
        """Extract features from alert and store for meta-labeling.

        Args:
            alert: Parsed alert data dict
            watch_id: Associated watch ID
        """
        try:
            # Build a FlowAlertRecord-like object for feature extraction
            from heber.models.silver import FlowAlertRecord

            # Create minimal record for feature extraction
            record = FlowAlertRecord(
                event_id=alert["id"],
                ts_event=alert.get("ts_event", datetime.now(UTC)),
                ts_ingest=datetime.now(UTC),
                ts_available=datetime.now(UTC),
                instrument_type="option",
                instrument_key=f"option:{alert.get('occ_symbol', '')}",
                symbol=alert.get("occ_symbol") or alert["underlying"],
                provider="unusual_whales",
                feed="flow_alerts",
                source="watch_consumer",
                underlying=alert["underlying"],
                occ_symbol=alert.get("occ_symbol"),
                expiry=alert.get("expiry")
                if isinstance(alert.get("expiry"), type(None)) or hasattr(alert.get("expiry"), "year")
                else datetime.strptime(str(alert["expiry"])[:10], "%Y-%m-%d").date(),
                strike=alert.get("strike", 0.0),
                put_call=alert["put_call"],
                premium=alert.get("premium", 0.0),
                volume=alert.get("volume", 0.0),
                open_interest=alert.get("open_interest"),
                spot_px=alert.get("spot_px"),
                contract_px=alert.get("contract_px"),
                alert_type=alert.get("alert_type", "UNKNOWN"),
                side=alert.get("side"),
                aggressor=alert.get("aggressor"),
                tags=alert.get("tags"),
            )

            # Extract features
            features = await self.feature_extractor.extract(record)

            # Store in Redis with async client when available.
            if self.async_redis:
                await store_features(self.async_redis, features)
                logger.debug(
                    "Stored alert features",
                    alert_id=alert["id"],
                    watch_id=watch_id,
                    feature_count=len(features.numeric_feature_names()),
                )
            elif self.redis is not None and hasattr(self.redis, "set"):
                key = FEATURES_KEY.format(alert_id=features.alert_id)
                payload = json.dumps(features.to_dict())
                await asyncio.to_thread(
                    self.redis.set,
                    key,
                    payload,
                    FEATURE_STORAGE_TTL_SECONDS,
                )
                logger.debug(
                    "Stored alert features",
                    alert_id=alert["id"],
                    watch_id=watch_id,
                    feature_count=len(features.numeric_feature_names()),
                    storage_mode="sync_fallback",
                )
            else:
                logger.debug(
                    "Skipping feature storage (no async redis)",
                    alert_id=alert["id"],
                    sync_redis_has_set=bool(getattr(self.redis, "set", None)),
                )

            # Persist feature row to Gold dataset for training-set assembly.
            await asyncio.to_thread(persist_features_to_gold, features)

        except EnrichmentAuthFailure:
            logger.error(
                "Feature extraction failed due to repeated auth failures",
                alert_id=alert.get("id"),
                watch_id=watch_id,
                exc_info=True,
            )
            raise
        except Exception as e:
            # Don't fail watch creation if feature extraction fails
            logger.warning(
                "Failed to extract/store features",
                alert_id=alert.get("id"),
                error=str(e),
            )
