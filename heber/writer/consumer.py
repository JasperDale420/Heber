"""Redis Streams consumer for incoming events.

Subscribes to the event stream from Data Gateway and routes to Bronze/Silver writers.

This module uses ``redis.asyncio`` directly rather than the ``EventBus`` abstraction
because it requires low-level stream control (``XREADGROUP``, ``XACK``, ``XCLAIM``,
``XADD`` for DLQ) that the ``EventBus`` interface does not expose.
"""

import asyncio
import json
import signal
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis
import structlog
from pydantic import ValidationError

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.logging import configure_logging
from heber.ops.metrics import (
    record_batch_processed,
    record_dlq_event,
    record_event_processed,
    record_event_received,
    record_ingest_latency,
    start_metrics_server_from_env,
)
from heber.ops.runtime_retry import calculate_retry_delay, classify_runtime_error
from heber.writer.bronze import BronzeWriter
from heber.writer.ingest_contracts import (
    DLQ_REASON_UNCONTRACTED,
    PAYLOAD_ALLOWED_FIELDS,
    PAYLOAD_REQUIRED_FIELDS,
    UnmappedFeedError,
    is_bronze_only_feed,
    is_contracted_feed,
    resolve_feed_alias,
    resolve_silver_feed,
)
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.silver import SilverWriter
from heber.writer.utils import build_silver_candidates

logger = structlog.get_logger(__name__)


class EventConsumer:
    """Consumes events from Redis Streams and writes to Lake layers."""

    def __init__(self):
        self.redis: redis.Redis | None = None
        self.bronze_writer = BronzeWriter()
        self.silver_writer = SilverWriter()
        self.running = False
        self.consumer_name = f"consumer-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
        self._payload_required = PAYLOAD_REQUIRED_FIELDS
        self._payload_allowed = PAYLOAD_ALLOWED_FIELDS

    async def connect(self):
        """Connect to Redis."""
        self.redis = redis.from_url(settings.redis_url)
        logger.info("Connected to Redis", url=settings.redis_url)

        # Create consumer group if it doesn't exist
        try:
            await self.redis.xgroup_create(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Created consumer group",
                stream=settings.redis_stream_name,
                group=settings.redis_consumer_group,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug("Consumer group already exists")
            else:
                raise

        recovered = await self._recover_pending_messages()
        if recovered > 0:
            logger.info(
                "Recovered pending messages",
                recovered=recovered,
                stream=settings.redis_stream_name,
                group=settings.redis_consumer_group,
            )

    @staticmethod
    def _decode_string(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _serialize_message_data(self, message_data: dict) -> str:
        """Serialize Redis stream payload for DLQ auditing."""
        normalized: dict[str, str] = {}
        for key, value in message_data.items():
            key_str = self._decode_string(key)
            if isinstance(value, bytes):
                normalized[key_str] = value.decode("utf-8", errors="replace")
            elif isinstance(value, dict | list):
                normalized[key_str] = json.dumps(value, default=str)
            else:
                normalized[key_str] = str(value)
        return json.dumps(normalized, default=str)

    async def _send_to_dlq(
        self,
        message_id: str | bytes,
        message_data: dict,
        error: str,
        attempts: int,
        feed: str = "unknown",
    ) -> bool:
        """Write failed message details to DLQ stream."""
        try:
            dlq_event = {
                "source_stream": settings.redis_stream_name,
                "source_group": settings.redis_consumer_group,
                "source_message_id": self._decode_string(message_id),
                "consumer_name": self.consumer_name,
                "attempts": str(attempts),
                "error": error,
                "failed_at": datetime.now(UTC).isoformat(),
                "payload": self._serialize_message_data(message_data),
            }
            dlq_id = await self.redis.xadd(settings.redis_dlq_stream_name, dlq_event)
            logger.warning(
                "Message sent to DLQ",
                dlq_stream=settings.redis_dlq_stream_name,
                source_message_id=dlq_event["source_message_id"],
                dlq_message_id=self._decode_string(dlq_id),
            )
            error_type = error.split(":", 1)[0] if error else "unknown_error"
            record_dlq_event(feed=feed, error_type=error_type)
            return True
        except Exception as exc:
            logger.error(
                "Failed to write message to DLQ",
                source_message_id=self._decode_string(message_id),
                error=str(exc),
                exc_info=True,
            )
            return False

    def _process_event_once(self, event_data: dict) -> tuple[bool, str | None, bool]:
        """Process an event one time and return `(success, error, retryable)`."""
        feed = "unknown"
        provider = "unknown"
        bronze_written = False
        try:
            # Parse envelope - Data Gateway sends 'data', legacy uses 'payload'
            payload_str = (
                event_data.get(b"data")
                or event_data.get("data")
                or event_data.get(b"payload")
                or event_data.get("payload")
            )
            if payload_str is None:
                raise ValueError(f"No 'data' or 'payload' field in event: {list(event_data.keys())}")

            if isinstance(payload_str, bytes):
                payload_str = payload_str.decode("utf-8")

            event_dict = json.loads(payload_str)
            envelope = EventEnvelope.model_validate(event_dict)
            feed = envelope.feed
            provider = envelope.provider
            record_event_received(feed=feed, provider=provider)

            # Set ts_available if not present
            if envelope.ts_available is None:
                envelope = envelope.with_ts_available(datetime.now(UTC))

            ingest_lag = max((envelope.ts_ingest - envelope.ts_event).total_seconds(), 0.0)
            availability_lag = max((envelope.ts_available - envelope.ts_event).total_seconds(), 0.0)
            record_ingest_latency(
                feed=feed,
                provider=provider,
                ingest_lag=ingest_lag,
                availability_lag=availability_lag,
            )

            self._validate_payload_schema(envelope)

            # Bronze-first policy: persist envelope before Silver normalization/validation.
            self.bronze_writer.write(envelope)
            bronze_written = True
            logger.info(
                "bronze_write_success",
                event_id=envelope.event_id,
                feed=envelope.feed,
                instrument_key=envelope.instrument_key,
            )

            if not is_contracted_feed(envelope.feed):
                logger.warning(
                    "silver_feed_uncontracted",
                    source_feed=envelope.feed,
                    provider=envelope.provider,
                    event_id=envelope.event_id,
                )
                raise UnmappedFeedError(DLQ_REASON_UNCONTRACTED)
            if is_bronze_only_feed(envelope.feed):
                logger.info(
                    "silver_write_skipped_policy",
                    event_id=envelope.event_id,
                    source_feed=envelope.feed,
                    canonical_feed=resolve_feed_alias(envelope.feed),
                    reason="bronze_only_feed_policy",
                )
                record_event_processed(feed=feed, provider=provider, status="success")
                return True, None, True

            silver_candidates = build_silver_candidates(envelope)
            if not silver_candidates:
                logger.info(
                    "silver_write_skipped_empty_aggregate",
                    event_id=envelope.event_id,
                    feed=envelope.feed,
                    provider=envelope.provider,
                )
                record_event_processed(feed=feed, provider=provider, status="success")
                return True, None, True

            aggregate_mode = len(silver_candidates) > 1
            silver_success_count = 0
            silver_failure_count = 0
            last_error: Exception | None = None

            for candidate in silver_candidates:
                try:
                    self._write_silver_candidate(envelope, candidate)
                    silver_success_count += 1
                except (UnmappedFeedError, ValueError, ValidationError) as exc:
                    last_error = exc
                    silver_failure_count += 1
                    if aggregate_mode:
                        logger.warning(
                            "silver_aggregate_item_failed",
                            source_event_id=envelope.event_id,
                            item_event_id=candidate.event_id,
                            feed=candidate.feed,
                            error=str(exc),
                        )
                        continue
                    raise

            if silver_failure_count > 0:
                logger.warning(
                    "silver_aggregate_write_summary",
                    source_event_id=envelope.event_id,
                    feed=envelope.feed,
                    success_count=silver_success_count,
                    failed_count=silver_failure_count,
                )

            if silver_success_count == 0:
                if last_error is not None:
                    raise last_error
                raise ValueError("all_aggregate_items_failed")

            logger.debug(
                "Processed event",
                event_id=envelope.event_id,
                feed=envelope.feed,
                silver_writes=silver_success_count,
            )
            record_event_processed(feed=feed, provider=provider, status="success")
            return True, None, True
        except UnmappedFeedError as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            return False, str(exc), False
        except (json.JSONDecodeError, ValidationError, ValueError) as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            if bronze_written:
                logger.warning(
                    "silver_normalization_failed",
                    feed=feed,
                    provider=provider,
                    error=str(exc),
                )
            else:
                logger.error(
                    "Failed to parse event",
                    error=str(exc),
                    event_data=str(event_data)[:200],
                )
            return False, str(exc), False
        except Exception as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            logger.error(
                "Failed to process event",
                error=str(exc),
                event_data=str(event_data)[:200],
                exc_info=True,
            )
            return False, str(exc), not bronze_written

    def process_event(self, event_data: dict) -> bool:
        """Process a single event through Bronze and Silver layers.

        Returns True if successful, False otherwise.
        """
        success, _, _ = self._process_event_once(event_data)
        return success

    async def _process_with_retry(self, event_data: dict) -> tuple[bool, str, int]:
        """Process message with retry/backoff before DLQ."""
        max_retries = max(1, settings.redis_process_max_retries)
        backoff = max(0.0, settings.redis_retry_backoff_seconds)
        last_error = "unknown_error"

        for attempt in range(1, max_retries + 1):
            success, error, retryable = self._process_event_once(event_data)
            if success:
                return True, "", attempt

            if error:
                last_error = error

            if not retryable:
                return False, last_error, attempt

            if attempt < max_retries and backoff > 0:
                await asyncio.sleep(backoff * attempt)

        return False, last_error, max_retries

    @staticmethod
    def _extract_feed_from_message(message_data: dict) -> str:
        payload_str = (
            message_data.get(b"data")
            or message_data.get("data")
            or message_data.get(b"payload")
            or message_data.get("payload")
        )
        if payload_str is None:
            return "unknown"
        if isinstance(payload_str, bytes):
            payload_str = payload_str.decode("utf-8", errors="replace")
        if isinstance(payload_str, dict):
            return str(payload_str.get("feed") or "unknown")
        try:
            event_dict = json.loads(payload_str)
        except Exception:
            return "unknown"
        return str(event_dict.get("feed") or "unknown")

    async def _process_stream_messages(self, stream_messages: list[tuple[Any, dict]]) -> tuple[list[str], list[str]]:
        """Process a list of stream messages and return `(acked_ids, failed_ids)`."""
        processed_ids: list[str] = []
        failed_ids: list[str] = []

        for message_id, message_data in stream_messages:
            success, error, attempts = await self._process_with_retry(message_data)
            message_id_str = self._decode_string(message_id)
            if success:
                processed_ids.append(message_id_str)
                continue

            feed = self._extract_feed_from_message(message_data)
            moved_to_dlq = await self._send_to_dlq(
                message_id=message_id,
                message_data=message_data,
                error=error,
                attempts=attempts,
                feed=feed,
            )
            if moved_to_dlq:
                # Ack after durable DLQ write so poison messages don't block group progress.
                processed_ids.append(message_id_str)
            else:
                failed_ids.append(message_id_str)

        return processed_ids, failed_ids

    async def _recover_pending_messages(self) -> int:
        """Claim and process idle pending messages for this consumer group."""
        pending = await self.redis.xpending_range(
            settings.redis_stream_name,
            settings.redis_consumer_group,
            "-",
            "+",
            settings.redis_claim_batch_size,
            idle=settings.redis_claim_idle_ms,
        )
        if not pending:
            return 0

        message_ids: list[str] = []
        for entry in pending:
            if isinstance(entry, dict):
                msg_id = entry.get("message_id") or entry.get(b"message_id")
                if msg_id is not None:
                    message_ids.append(self._decode_string(msg_id))

        if not message_ids:
            return 0

        claimed = await self.redis.xclaim(
            settings.redis_stream_name,
            settings.redis_consumer_group,
            self.consumer_name,
            settings.redis_claim_idle_ms,
            message_ids,
        )
        if not claimed:
            return 0

        ack_ids, failed_ids = await self._process_stream_messages(claimed)

        # Flush to disk before acking claimed messages.
        self._flush_layers()

        if ack_ids:
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *ack_ids,
            )

        if failed_ids:
            logger.warning(
                "Pending messages could not be recovered",
                failed_count=len(failed_ids),
                failed_ids=failed_ids[:10],
            )

        return len(ack_ids)

    def _validate_payload_schema(self, envelope: EventEnvelope) -> None:
        """Warn on missing/unknown payload keys for selected feeds."""
        feed = envelope.feed
        if feed not in self._payload_required:
            return
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        required = self._payload_required[feed]
        allowed = self._payload_allowed.get(feed, required)

        missing = required - set(payload.keys())
        unexpected = set(payload.keys()) - allowed

        if missing:
            logger.warning(
                "payload_missing_keys",
                feed=feed,
                missing=sorted(missing),
            )
        if unexpected:
            logger.warning(
                "payload_unexpected_keys",
                feed=feed,
                unexpected=sorted(unexpected),
            )

    @staticmethod
    def _validate_instrument_key(envelope: EventEnvelope) -> None:
        """Enforce canonical instrument-key format before writes."""
        if envelope.is_valid_instrument_key():
            return
        raise ValueError(
            f"Invalid instrument_key format for instrument_type {envelope.instrument_type}: {envelope.instrument_key}"
        )

    def _write_silver_candidate(self, source_envelope: EventEnvelope, candidate: EventEnvelope) -> None:
        """Normalize and persist one candidate event to Silver."""
        normalized = normalize_envelope_for_silver(candidate)
        silver_feed = resolve_silver_feed(normalized.feed)
        if silver_feed is None:
            logger.warning(
                "silver_schema_unmapped",
                source_feed=source_envelope.feed,
                canonical_feed=normalized.feed,
                event_id=source_envelope.event_id,
            )
            raise UnmappedFeedError("unmapped_feed")

        normalized = normalized.model_copy(update={"feed": silver_feed})
        self._validate_instrument_key(normalized)
        self.silver_writer.write(normalized)

    async def run(self):
        """Main consumer loop."""
        await self.connect()
        self.running = True
        error_streak = 0

        logger.info(
            "Starting consumer",
            stream=settings.redis_stream_name,
            group=settings.redis_consumer_group,
            consumer=self.consumer_name,
        )

        while self.running:
            try:
                await self._consume_iteration()
                error_streak = 0
            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise  # Re-raise per best practice
            except Exception as e:
                error_streak += 1
                delay = calculate_retry_delay(
                    attempt=error_streak,
                    base_seconds=settings.redis_retry_backoff_seconds,
                    max_seconds=30.0,
                    jitter_ratio=0.2,
                )
                is_transient, error_kind = classify_runtime_error(e)
                if is_transient:
                    logger.warning(
                        "Consumer transient runtime error",
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
                        exc_info=True,
                    )
                await asyncio.sleep(delay)

        self._final_flush()

    def _flush_layers(self) -> None:
        """Flush Bronze and Silver independently so one failure doesn't block the other."""
        try:
            self.bronze_writer.flush_if_needed()
        except Exception as e:
            logger.error("Bronze flush failed", error=str(e), exc_info=True)

        try:
            self.silver_writer.flush_if_needed()
        except Exception as e:
            logger.error("Silver flush failed", error=str(e), exc_info=True)

    async def _consume_iteration(self) -> None:
        """Execute a single iteration of the consumer loop.

        Flow: read batch → process all → flush to disk → ACK all.
        This ensures data is persisted before we acknowledge to Redis,
        preventing data loss on crash.
        """
        messages = await self.redis.xreadgroup(
            groupname=settings.redis_consumer_group,
            consumername=self.consumer_name,
            streams={settings.redis_stream_name: ">"},
            count=settings.redis_read_batch_size,
            block=settings.redis_read_block_ms,
        )
        # Always check for flush even with no messages (handles idle periods)
        self._flush_layers()

        if not messages:
            return

        # Collect successfully processed message IDs
        processed_ids: list[str] = []
        failed_ids: list[str] = []

        for _stream_name, stream_messages in messages:
            record_batch_processed(feed="mixed", batch_size=len(stream_messages))
            ack_ids, stream_failed_ids = await self._process_stream_messages(stream_messages)
            processed_ids.extend(ack_ids)
            failed_ids.extend(stream_failed_ids)

        # Flush to disk BEFORE acknowledging
        self._flush_layers()

        # Only ACK after successful flush
        if processed_ids:
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *processed_ids,
            )
            logger.debug("Acknowledged batch", count=len(processed_ids))

        if failed_ids:
            logger.warning(
                "Messages left pending after DLQ failures",
                failed_count=len(failed_ids),
                failed_ids=failed_ids[:10],
            )

    def _final_flush(self) -> None:
        """Perform final flush when consumer stops."""
        self.bronze_writer.flush()
        self.silver_writer.flush()
        logger.info("Consumer stopped")

    async def stop(self):
        """Stop the consumer."""
        self.running = False
        if self.redis:
            await self.redis.close()


async def main():
    """Entry point for the consumer."""
    configure_logging(service_name="heber-consumer", log_level=settings.log_level, json_output=True)
    start_metrics_server_from_env(default_port=9090)
    consumer = EventConsumer()

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
