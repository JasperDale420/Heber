"""Redis Streams consumer for incoming events.

Subscribes to the event stream from Data Gateway and routes to Bronze/Silver writers.

This module uses ``redis.asyncio`` directly rather than the ``EventBus`` abstraction
because it requires low-level stream control (``XREADGROUP``, ``XACK``, ``XCLAIM``,
``XADD`` for DLQ) that the ``EventBus`` interface does not expose.
"""

import asyncio
import json
import signal
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis
import structlog
from pydantic import ValidationError

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.logging import configure_logging
from heber.ops.metrics import (
    consumer_loop_heartbeat_unixtime,
    record_batch_processed,
    record_dedupe_drop,
    record_dlq_event,
    record_event_processed,
    record_event_received,
    record_ingest_latency,
    start_metrics_server_from_env,
)
from heber.ops.reliability import EventDeduplicator, RedisDedupeStore
from heber.ops.runtime_retry import calculate_retry_delay, classify_runtime_error
from heber.writer.bronze import BronzeWriter
from heber.writer.dlq_fallback import (
    log_fallback_backlog,
    try_xadd_with_retry,
    write_dlq_fallback_file,
)
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
from heber.writer.key_normalization import (
    InvalidInstrumentKeyError,
    SilverNormalizationError,
    normalize_envelope_for_silver,
)
from heber.writer.normalizer import (
    MissingRequiredFieldsError,
    enforce_required_non_null_fields,
    envelope_to_silver_row,
)
from heber.writer.silver import SilverWriter
from heber.writer.utils import build_silver_candidates, get_partition_key

logger = structlog.get_logger(__name__)

# ponytail: safety bound on a single recovery drain (× redis_claim_batch_size
# messages). Caps worst-case round-trips if Redis kept returning claimable
# pending; raise only if a real backlog can exceed this many batches.
_MAX_RECOVERY_BATCHES = 100


class EventConsumer:
    """Consumes events from Redis Streams and writes to Lake layers."""

    def __init__(self, event_deduplicator: EventDeduplicator | None = None):
        self.redis: redis.Redis | None = None
        self.bronze_writer = BronzeWriter()
        self.silver_writer = SilverWriter()
        self.running = False
        self.consumer_name = f"consumer-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
        self.event_deduplicator = event_deduplicator or EventDeduplicator(
            backing_store=self._build_dedupe_store(),
        )
        self._inflight_event_ids: set[str] = set()
        self._payload_required = PAYLOAD_REQUIRED_FIELDS
        self._payload_allowed = PAYLOAD_ALLOWED_FIELDS
        self._silver_validation_warning_counts: dict[tuple[str, str, str, str], int] = {}
        self._last_recovery_monotonic = 0.0

    @staticmethod
    def _build_dedupe_store() -> RedisDedupeStore | None:
        """Build the exact-match dedupe backing store when enabled."""
        if not settings.dedupe_redis_enabled:
            return None
        return RedisDedupeStore(
            redis_url=settings.redis_url,
            ttl_seconds=settings.dedupe_redis_ttl_seconds,
        )

    def _claim_event_id(self, event_id: str) -> str | None:
        """Claim an event_id for processing or return a duplicate reason."""
        if event_id in self._inflight_event_ids:
            return "inflight_duplicate"

        dedupe_result = self.event_deduplicator.check(event_id)
        if dedupe_result.is_duplicate:
            return dedupe_result.reason

        self._inflight_event_ids.add(event_id)
        return None

    async def connect(self):
        """Connect to Redis."""
        self.redis = redis.from_url(settings.redis_url)
        logger.info("Connected to Redis", url=settings.redis_url)

        log_fallback_backlog(settings.dlq_fallback_path, service="heber-consumer")

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

    @staticmethod
    def _extract_payload_value(message_data: dict) -> Any | None:
        return (
            message_data.get(b"data")
            or message_data.get("data")
            or message_data.get(b"payload")
            or message_data.get("payload")
        )

    async def _send_to_dlq(
        self,
        message_id: str | bytes,
        message_data: dict,
        error: str,
        attempts: int,
        feed: str = "unknown",
    ) -> bool:
        """Dead-letter a failed message durably.

        The event is ALWAYS persisted to a JSON file under
        ``settings.dlq_fallback_path`` first — that file is the durable audit
        record, because the DLQ stream lives on a cache-mode Redis that evicts
        entries (LRU/TTL). A best-effort ``XADD`` to the Redis DLQ stream then
        enqueues it for convenient reprocessing. Returns ``True`` when the event
        is captured by the file or Redis (caller may ACK); ``False`` only if
        both fail (caller must not ACK).
        """
        source_message_id = self._decode_string(message_id)
        dlq_event = {
            "source_stream": settings.redis_stream_name,
            "source_group": settings.redis_consumer_group,
            "source_message_id": source_message_id,
            "consumer_name": self.consumer_name,
            "attempts": str(attempts),
            "error": error,
            "failed_at": datetime.now(UTC).isoformat(),
            "payload": self._serialize_message_data(message_data),
        }
        error_type = error.split(":", 1)[0] if error else "unknown_error"

        # Durability first: always persist the event to the on-disk fallback
        # store. The DLQ stream lives on a cache-mode Redis (LRU/TTL eviction),
        # so the file — not the stream — is the audit record of record. The
        # Redis XADD below is a best-effort reprocessing queue on top of it.
        fallback_path = await self._write_dlq_fallback(
            source_message_id=source_message_id,
            dlq_event=dlq_event,
            redis_error=None,
        )

        success, dlq_id, last_exc = await try_xadd_with_retry(
            lambda: self.redis.xadd(
                settings.redis_dlq_stream_name,
                dlq_event,
                maxlen=settings.redis_dlq_max_stream_len,
                approximate=True,
            ),
        )

        if fallback_path is None and not success:
            # Neither the durable file nor Redis captured the event — do not ACK.
            logger.error(
                "Failed to write message to DLQ",
                source_message_id=source_message_id,
                error=str(last_exc) if last_exc else "unknown",
                exc_info=last_exc,
            )
            return False

        if success:
            logger.warning(
                "Message sent to DLQ",
                dlq_stream=settings.redis_dlq_stream_name,
                source_message_id=source_message_id,
                dlq_message_id=self._decode_string(dlq_id) if dlq_id is not None else None,
                durable_path=str(fallback_path) if fallback_path is not None else None,
            )
        else:
            # Redis enqueue failed but the durable file was written — not a loss.
            logger.warning(
                "dlq_redis_enqueue_failed_durable_file_written",
                source_message_id=source_message_id,
                fallback_path=str(fallback_path),
                redis_error=str(last_exc) if last_exc else None,
            )
        record_dlq_event(feed=feed, error_type=error_type)
        return True

    async def _write_dlq_fallback(
        self,
        source_message_id: str,
        dlq_event: dict[str, Any],
        redis_error: Exception | None,
    ) -> Any | None:
        """Persist a DLQ event to the on-disk fallback dir; return the path or None."""
        try:
            return await asyncio.to_thread(
                write_dlq_fallback_file,
                settings.dlq_fallback_path,
                settings.redis_dlq_stream_name,
                source_message_id,
                dlq_event,
            )
        except Exception as fallback_exc:  # noqa: BLE001 — last-resort fallback must not crash consumer
            logger.error(
                "dlq_file_fallback_failed",
                stream=settings.redis_dlq_stream_name,
                source_message_id=source_message_id,
                redis_error=str(redis_error) if redis_error else None,
                fallback_error=str(fallback_exc),
                exc_info=True,
            )
            return None

    def _parse_and_validate_envelope(self, event_data: dict) -> EventEnvelope:
        """Parse event data dict into a validated EventEnvelope."""
        payload_str = self._extract_payload_value(event_data)
        if payload_str is None:
            raise ValueError(f"No 'data' or 'payload' field in event: {list(event_data.keys())}")

        if isinstance(payload_str, bytes):
            payload_str = payload_str.decode("utf-8")

        event_dict = json.loads(payload_str)
        envelope = EventEnvelope.model_validate(event_dict)

        if envelope.ts_available is None:
            envelope = envelope.with_ts_available(datetime.now(UTC))

        return envelope

    def _write_silver_candidates(self, envelope: EventEnvelope) -> None:
        """Write silver candidates from an envelope, handling aggregate mode failures."""
        silver_candidates = build_silver_candidates(envelope)
        if not silver_candidates:
            logger.info(
                "silver_write_skipped_empty_aggregate",
                event_id=envelope.event_id,
                feed=envelope.feed,
                provider=envelope.provider,
            )
            return

        aggregate_mode = len(silver_candidates) > 1
        silver_success_count = 0
        silver_failure_count = 0
        last_error: Exception | None = None

        for candidate in silver_candidates:
            try:
                self._write_silver_candidate(envelope, candidate)
                silver_success_count += 1
            except (UnmappedFeedError, MissingRequiredFieldsError, ValidationError) as exc:
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

    def _process_event_once(self, event_data: dict) -> tuple[bool, str | None, bool]:
        """Process an event one time and return `(success, error, retryable)`."""
        feed = "unknown"
        provider = "unknown"
        bronze_written = False
        envelope: EventEnvelope | None = None
        claimed_event_id = False
        register_success = False
        try:
            envelope = self._parse_and_validate_envelope(event_data)
            feed = envelope.feed
            provider = envelope.provider
            record_event_received(feed=feed, provider=provider)

            ingest_lag = max((envelope.ts_ingest - envelope.ts_event).total_seconds(), 0.0)
            availability_lag = max((envelope.ts_available - envelope.ts_event).total_seconds(), 0.0)
            record_ingest_latency(
                feed=feed,
                provider=provider,
                ingest_lag=ingest_lag,
                availability_lag=availability_lag,
            )

            self._validate_payload_schema(envelope)

            dedupe_reason = self._claim_event_id(envelope.event_id)
            if dedupe_reason is not None:
                record_dedupe_drop(feed=feed)
                logger.info(
                    "consumer_dedupe_dropped",
                    event_id=envelope.event_id,
                    feed=envelope.feed,
                    provider=envelope.provider,
                    reason=dedupe_reason,
                )
                record_event_processed(feed=feed, provider=provider, status="dropped")
                return True, None, True
            claimed_event_id = True

            self.bronze_writer.write(envelope)
            bronze_written = True
            logger.debug(
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
                register_success = True
                return True, None, True

            self._write_silver_candidates(envelope)

            logger.debug(
                "Processed event",
                event_id=envelope.event_id,
                feed=envelope.feed,
            )
            record_event_processed(feed=feed, provider=provider, status="success")
            register_success = True
            return True, None, True
        except UnmappedFeedError as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            return False, str(exc), False
        except json.JSONDecodeError as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            logger.error(
                "Failed to parse event",
                error=str(exc),
                event_data=str(event_data)[:200],
            )
            return False, str(exc), False
        except (SilverNormalizationError, MissingRequiredFieldsError) as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            if bronze_written and envelope is not None:
                self._log_silver_validation_failure(envelope, exc)
            else:
                logger.error(
                    "Failed to parse event",
                    error=str(exc),
                    event_data=str(event_data)[:200],
                )
            return False, str(exc), False
        except ValidationError as exc:
            record_event_processed(feed=feed, provider=provider, status="error")
            if bronze_written and envelope is not None:
                self._log_silver_validation_failure(envelope, exc)
            else:
                logger.error(
                    "Failed to parse event",
                    error=str(exc),
                    event_data=str(event_data)[:200],
                )
            return False, str(exc), False
        except Exception as exc:  # noqa: BLE001 — catch-all after specific handlers for unexpected errors
            record_event_processed(feed=feed, provider=provider, status="error")
            logger.error(
                "Failed to process event",
                error=str(exc),
                event_data=str(event_data)[:200],
                exc_info=True,
            )
            return False, str(exc), not bronze_written
        finally:
            if envelope is not None and claimed_event_id:
                self._inflight_event_ids.discard(envelope.event_id)
                if register_success:
                    self.event_deduplicator.register(envelope.event_id)

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
        payload_str = EventConsumer._extract_payload_value(message_data)
        if payload_str is None:
            return "unknown"
        if isinstance(payload_str, bytes):
            payload_str = payload_str.decode("utf-8", errors="replace")
        if isinstance(payload_str, dict):
            return str(payload_str.get("feed") or "unknown")
        try:
            event_dict = json.loads(payload_str)
        except (json.JSONDecodeError, TypeError, ValueError):
            return "unknown"
        return str(event_dict.get("feed") or "unknown")

    async def _process_stream_messages(self, stream_messages: list[tuple[Any, dict]]) -> tuple[list[str], list[str]]:
        """Process a list of stream messages concurrently and return `(acked_ids, failed_ids)`.

        Uses a semaphore to limit concurrency to ``redis_process_concurrency``.
        Processing order is non-deterministic but result ordering is preserved
        for correct ACK/DLQ routing.
        """
        sem = asyncio.Semaphore(settings.redis_process_concurrency)

        async def _process_one(message_id: Any, message_data: dict) -> tuple[str, bool, str | None, int]:
            async with sem:
                success, error, attempts = await self._process_with_retry(message_data)
                return self._decode_string(message_id), success, error, attempts

        results = await asyncio.gather(
            *[_process_one(mid, mdata) for mid, mdata in stream_messages],
            return_exceptions=True,
        )

        processed_ids: list[str] = []
        failed_ids: list[str] = []

        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                mid_str = self._decode_string(stream_messages[i][0])
                logger.error(
                    "Unexpected error processing message",
                    message_id=mid_str,
                    error=str(result),
                    exc_info=result,
                )
                failed_ids.append(mid_str)
                continue

            message_id_str, success, error, attempts = result
            if success:
                processed_ids.append(message_id_str)
                continue

            feed = self._extract_feed_from_message(stream_messages[i][1])
            moved_to_dlq = await self._send_to_dlq(
                message_id=stream_messages[i][0],
                message_data=stream_messages[i][1],
                error=error,
                attempts=attempts,
                feed=feed,
            )
            if moved_to_dlq:
                processed_ids.append(message_id_str)
            else:
                failed_ids.append(message_id_str)

        return processed_ids, failed_ids

    async def _recover_pending_messages(self) -> int:
        """Reclaim idle pending messages for this group, draining until none remain.

        A consumer that dies holding a large pending backlog (e.g. 1,900 messages)
        used to leave all but one batch stranded forever, because recovery claimed a
        single ``redis_claim_batch_size`` batch and ran only at startup. This now
        loops over batches until the idle-pending set is empty (each claim removes
        the messages from the idle window, so the loop terminates naturally) and is
        also invoked periodically from the run loop (``_maybe_recover_pending``).
        """
        total = 0
        for _ in range(_MAX_RECOVERY_BATCHES):
            recovered = await self._recover_pending_batch()
            if recovered == 0:
                break
            total += recovered
        return total

    async def _maybe_recover_pending(self, now_monotonic: float) -> int:
        """Run a recovery drain if the periodic interval has elapsed. Returns count recovered."""
        interval = settings.redis_recover_interval_seconds
        if interval <= 0 or (now_monotonic - self._last_recovery_monotonic) < interval:
            return 0
        self._last_recovery_monotonic = now_monotonic
        return await self._recover_pending_messages()

    async def _recover_pending_batch(self) -> int:
        """Claim and process one batch of idle pending messages for this consumer group."""
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
        flush_ok = self._flush_layers()

        if ack_ids and flush_ok:
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *ack_ids,
            )
        elif ack_ids and not flush_ok:
            logger.warning(
                "Skipping ACK for recovered messages due to flush failure",
                pending_ack_count=len(ack_ids),
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
        raise InvalidInstrumentKeyError(
            f"Invalid instrument_key format for instrument_type {envelope.instrument_type}: {envelope.instrument_key}",
            details={
                "feed": envelope.feed,
                "instrument_type": envelope.instrument_type,
                "instrument_key": envelope.instrument_key,
                "symbol": envelope.symbol,
            },
        )

    @staticmethod
    def _should_emit_validation_warning(occurrence_count: int) -> bool:
        """Emit the first and milestone repeats for a repeated Silver validation failure."""
        return occurrence_count in {1, 10, 100} or occurrence_count % 1000 == 0

    def _log_silver_validation_failure(self, envelope: EventEnvelope, error: Exception) -> None:
        """Log a bounded warning for malformed upstream data rejected from Silver."""
        if len(self._silver_validation_warning_counts) > 10000:
            self._silver_validation_warning_counts.clear()
        signature = (
            envelope.provider,
            envelope.feed,
            type(error).__name__,
            str(error)[:120],
        )
        occurrence_count = self._silver_validation_warning_counts.get(signature, 0) + 1
        self._silver_validation_warning_counts[signature] = occurrence_count

        if not self._should_emit_validation_warning(occurrence_count):
            return

        details = getattr(error, "details", {})
        if not isinstance(details, Mapping):
            details = {}

        logger.warning(
            "silver_validation_failed",
            event_id=envelope.event_id,
            provider=envelope.provider,
            feed=details.get("feed", envelope.feed),
            error_type=type(error).__name__,
            error=str(error),
            occurrence_count=occurrence_count,
            instrument_type=details.get("instrument_type", envelope.instrument_type),
            instrument_key=details.get("instrument_key", envelope.instrument_key),
            symbol=details.get("symbol", envelope.symbol),
            payload_symbol=details.get("payload_symbol"),
            payload_ticker=details.get("payload_ticker"),
        )

    def _write_silver_candidate(self, source_envelope: EventEnvelope, candidate: EventEnvelope) -> None:
        """Normalize and persist one candidate event to Silver.

        Performs normalization once and passes the pre-normalized row
        directly to ``SilverWriter.write_row()`` to avoid the double
        normalization that ``SilverWriter.write()`` would trigger.
        """
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

        # Build the Silver row and buffer it directly, skipping
        # SilverWriter._envelope_to_row() which would re-normalize.
        row = envelope_to_silver_row(normalized)
        enforce_required_non_null_fields(feed=silver_feed, row=row, event_id=normalized.event_id)
        partition_key = get_partition_key(
            feed=normalized.feed,
            instrument_type=normalized.instrument_type,
            ts_event=normalized.ts_event,
        )
        self.silver_writer.write_row(partition_key, row)

    async def run(self):
        """Main consumer loop."""
        await self.connect()
        self.running = True
        error_streak = 0
        # Startup recovery already ran in connect(); start the periodic clock now so
        # the next drain waits a full interval rather than firing immediately.
        self._last_recovery_monotonic = time.monotonic()

        logger.info(
            "Starting consumer",
            stream=settings.redis_stream_name,
            group=settings.redis_consumer_group,
            consumer=self.consumer_name,
        )

        while self.running:
            try:
                # Liveness heartbeat at the top of every iteration — stays fresh while
                # the loop spins (even on idle, no-data cycles); the container
                # healthcheck reads this to detect a stalled-but-running consumer.
                consumer_loop_heartbeat_unixtime.set(time.time())
                await self._consume_iteration()
                error_streak = 0
                recovered = await self._maybe_recover_pending(time.monotonic())
                if recovered:
                    logger.info("Recovered stranded pending messages", recovered=recovered)
            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise  # Re-raise per best practice
            except redis.ResponseError as e:
                if "NOGROUP" in str(e):
                    logger.warning(
                        "Consumer group missing — recreating after Redis restart",
                        stream=settings.redis_stream_name,
                        group=settings.redis_consumer_group,
                    )
                    try:
                        await self.redis.xgroup_create(
                            name=settings.redis_stream_name,
                            groupname=settings.redis_consumer_group,
                            id="0",
                            mkstream=True,
                        )
                        logger.info(
                            "consumer_group_auto_created",
                            stream=settings.redis_stream_name,
                            group=settings.redis_consumer_group,
                        )
                    except redis.ResponseError as create_err:
                        if "BUSYGROUP" not in str(create_err):
                            raise
                    continue
                raise  # Re-raise non-NOGROUP ResponseErrors to the general handler
            except Exception as e:  # noqa: BLE001 — top-level consumer loop must not crash
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

    def _flush_layers(self) -> bool:
        """Flush Bronze and Silver independently.

        Returns True if both flushes succeed. On failure the data remains
        buffered and the caller must NOT acknowledge the messages so that
        Redis can redeliver them on the next iteration.
        """
        ok = True
        try:
            self.bronze_writer.flush_if_needed()
        except Exception as e:  # noqa: BLE001 — flush must not crash consumer
            logger.error("Bronze flush failed", error=str(e), exc_info=True)
            ok = False

        try:
            self.silver_writer.flush_if_needed()
        except Exception as e:  # noqa: BLE001 — flush must not crash consumer
            logger.error("Silver flush failed", error=str(e), exc_info=True)
            ok = False

        return ok

    async def _consume_iteration(self) -> None:
        """Execute a single iteration of the consumer loop.

        Flow: read batch → process all concurrently → flush to disk → ACK all.
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

        if not messages:
            # Flush on idle iterations to respect time-based flush thresholds.
            # Offloaded to a thread so the gzip/parquet I/O does not block the
            # event loop (keeps the Prometheus metrics endpoint and Redis
            # heartbeats responsive). Safe: flush never runs concurrently with
            # buffer appends or another flush within the single consume loop.
            await asyncio.to_thread(self._flush_layers)
            return

        t0 = time.monotonic()

        # Collect successfully processed message IDs
        processed_ids: list[str] = []
        failed_ids: list[str] = []
        total_messages = 0

        for _stream_name, stream_messages in messages:
            total_messages += len(stream_messages)
            record_batch_processed(feed="mixed", batch_size=len(stream_messages))
            ack_ids, stream_failed_ids = await self._process_stream_messages(stream_messages)
            processed_ids.extend(ack_ids)
            failed_ids.extend(stream_failed_ids)

        # Flush to disk BEFORE acknowledging. Offloaded to a thread so the
        # blocking gzip/parquet write does not stall the event loop (and the
        # consumer's metrics endpoint) on large batches.
        flush_ok = await asyncio.to_thread(self._flush_layers)

        # Only ACK after successful flush — on failure, messages stay
        # pending and will be redelivered on the next iteration.
        if processed_ids and flush_ok:
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *processed_ids,
            )
        elif processed_ids and not flush_ok:
            logger.warning(
                "Skipping ACK due to flush failure — messages will be redelivered",
                pending_ack_count=len(processed_ids),
            )

        elapsed = time.monotonic() - t0
        rate = total_messages / elapsed if elapsed > 0 else 0
        logger.info(
            "batch_processed",
            total=total_messages,
            acked=len(processed_ids),
            failed=len(failed_ids),
            elapsed_seconds=round(elapsed, 3),
            messages_per_second=round(rate, 1),
            concurrency=settings.redis_process_concurrency,
        )

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
    try:
        start_metrics_server_from_env(default_port=9090)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))
    consumer = EventConsumer()

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
