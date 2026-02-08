"""Alert Watch Consumer - Creates watches from flow_alerts stream.

Listens to the flow_alerts Redis stream and automatically creates
watches for each incoming alert.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx
import redis
import structlog

from heber.config import settings
from heber.features.templates.alert_labels import (
    AlertHorizon,
    ContractBarrierConfig,
    classify_horizon,
)
from heber.watch.features import AlertFeatureExtractor, persist_features_to_gold, store_features
from heber.watch.gateway import gateway_url_candidates
from heber.watch.manager import WatchManager
from heber.watch.models import WatchHorizon

logger = structlog.get_logger(__name__)

# Stream configuration
DEFAULT_EVENTS_STREAM = settings.redis_stream_name
FLOW_ALERTS_FEED = "flow_alerts"  # Filter by this feed type
CONSUMER_GROUP = "watch-consumer"
CONSUMER_NAME = "watch-consumer-1"
DATA_GATEWAY_URL = "http://localhost:8000"


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
        self.stream_name = stream_name or DEFAULT_EVENTS_STREAM
        self.dlq_stream_name = dlq_stream_name or settings.redis_dlq_stream_name
        self.max_process_retries = max_process_retries or settings.redis_process_max_retries
        self.retry_backoff_seconds = retry_backoff_seconds or settings.redis_retry_backoff_seconds
        self._running = False

        # Feature extractor for meta-labeling
        self.feature_extractor = AlertFeatureExtractor(
            redis=async_redis,
            gateway_url=gateway_url,
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
        for attempt in range(1, self.max_process_retries + 1):
            if await self._process_alert(msg_id_text, data):
                return True
            if attempt < self.max_process_retries:
                await asyncio.sleep(self.retry_backoff_seconds * attempt)

        return await self._dead_letter_message(
            msg_id=msg_id_text,
            data=data,
            attempts=self.max_process_retries,
            error="processing_failed_after_retries",
        )

    async def _handle_message(self, msg_id: str, data: dict) -> bool:
        """Handle one stream entry and indicate whether it should be ACKed."""
        if not self._is_flow_alert(data):
            return True
        return await self._process_flow_alert_with_retries(msg_id, data)

    async def run(self) -> None:
        """Run the consumer as a continuous service."""
        self._running = True
        await self._setup_consumer_group_async()

        logger.info(
            "Starting alert watch consumer",
            stream=self.stream_name,
            group=CONSUMER_GROUP,
        )

        while self._running:
            try:
                # Read new messages from stream
                messages = await self._read_messages()

                if messages:
                    for _stream, entries in messages:
                        for msg_id, data in entries:
                            should_ack = await self._handle_message(msg_id, data)
                            if should_ack:
                                await self._ack_message(msg_id)

            except Exception as e:
                logger.error("Consumer error", error=str(e))
                await asyncio.sleep(1)

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

    async def _process_alert(self, msg_id: str, data: dict) -> bool:
        """Process a single alert and create a watch.

        Args:
            msg_id: Redis message ID
            data: Alert data from stream
        """
        try:
            # Parse alert data
            alert = self._parse_alert(data)

            if not alert:
                logger.warning("Could not parse alert", msg_id=msg_id)
                return False

            # Skip if no OCC symbol
            if not alert.get("occ_symbol"):
                logger.debug("Alert has no OCC symbol, skipping", alert_id=alert.get("id"))
                return True

            # Determine horizon based on DTE
            dte = alert.get("dte", 5)
            alert_horizon = classify_horizon(dte)
            watch_horizon = _alert_horizon_to_watch_horizon(alert_horizon)

            # Get entry price (option quote at alert time)
            entry_price = await self._get_entry_price(alert["occ_symbol"])

            if not entry_price or entry_price <= 0:
                logger.warning(
                    "Could not get entry price",
                    occ_symbol=alert["occ_symbol"],
                )
                # Use contract_px from alert if available
                entry_price = alert.get("contract_px", 1.0)

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
                alert_time=alert.get("ts_event", datetime.now(UTC)),
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
            return True

        except Exception as e:
            logger.error(
                "Failed to process alert",
                msg_id=msg_id,
                error=str(e),
            )
            return False

    def _parse_alert(self, data: dict) -> dict | None:
        """Parse alert data from stream message.

        Args:
            data: Raw message data (may be bytes or nested JSON)

        Returns:
            Parsed alert dict or None
        """
        try:
            parsed = self._decode_stream_data(data)
            result = self._map_alert_fields(parsed)
            result["dte"] = self._calculate_dte(result.get("expiry"))
            result["ts_event"] = self._parse_timestamp(parsed)
            return result

        except Exception as e:
            logger.error("Failed to parse alert", error=str(e))
            return None

    def _decode_stream_data(self, data: dict) -> dict:
        """Decode bytes and parse nested JSON from stream message."""
        parsed = {}
        for k, v in data.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v

            if isinstance(val, str) and val.startswith("{"):
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

    def _map_alert_fields(self, parsed: dict) -> dict:
        """Map various field name conventions to standard fields."""
        put_call_raw = parsed.get("put_call") or parsed.get("type", "C")
        put_call = put_call_raw[0].upper() if put_call_raw else "C"

        return {
            "id": parsed.get("id") or parsed.get("event_id") or parsed.get("alert_id"),
            "occ_symbol": parsed.get("occ_symbol") or parsed.get("option_chain"),
            "underlying": parsed.get("underlying") or parsed.get("ticker") or parsed.get("symbol"),
            "put_call": put_call,
            "expiry": parsed.get("expiry"),
            "strike": float(parsed.get("strike", 0) or 0),
            "spot_px": float(parsed.get("spot_px") or parsed.get("underlying_price", 0) or 0),
            "contract_px": float(parsed.get("contract_px") or parsed.get("price", 0) or 0),
        }

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
        ts = parsed.get("ts_event") or parsed.get("created_at") or parsed.get("timestamp")
        if not ts:
            return datetime.now(UTC)

        if isinstance(ts, str):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if isinstance(ts, int | float):
            return datetime.fromtimestamp(ts, tz=UTC)
        return datetime.now(UTC)

    async def _get_entry_price(self, occ_symbol: str) -> float | None:
        """Get latest option quote from Data Gateway.

        Args:
            occ_symbol: OCC option symbol

        Returns:
            Mid price or None
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                data: dict | None = None
                routes = gateway_url_candidates(
                    self.gateway_url,
                    "/alpaca/options/quotes",
                )
                for route in routes:
                    response = await client.get(
                        route,
                        params={"symbols": occ_symbol},
                    )
                    if response.status_code == 200:
                        data = response.json()
                        break

                if data is not None:
                    quotes = data.get("data", {}).get("quotes", {})
                    quote = quotes.get(occ_symbol)
                    if quote:
                        bid = quote.get("bp")
                        if bid is None:
                            bid = quote.get("bid_price")

                        ask = quote.get("ap")
                        if ask is None:
                            ask = quote.get("ask_price")

                        if bid is not None and ask is not None:
                            return (float(bid) + float(ask)) / 2

                        last_price = quote.get("last_price")
                        if last_price is not None:
                            return float(last_price)
                        return None

                return None

        except Exception as e:
            logger.error("Failed to get entry price", occ_symbol=occ_symbol, error=str(e))
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

            # Store in Redis if async client available
            if self.async_redis:
                await store_features(self.async_redis, features)
                logger.debug(
                    "Stored alert features",
                    alert_id=alert["id"],
                    watch_id=watch_id,
                    feature_count=len(features.numeric_feature_names()),
                )
            else:
                logger.debug(
                    "Skipping feature storage (no async redis)",
                    alert_id=alert["id"],
                )

            # Persist feature row to Gold dataset for training-set assembly.
            await asyncio.to_thread(persist_features_to_gold, features)

        except Exception as e:
            # Don't fail watch creation if feature extraction fails
            logger.warning(
                "Failed to extract/store features",
                alert_id=alert.get("id"),
                error=str(e),
            )
