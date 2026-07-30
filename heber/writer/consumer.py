"""Redis Streams consumer for incoming events.

Subscribes to the event stream from Data Gateway and routes to Bronze/Silver writers.

This module uses ``redis.asyncio`` directly rather than the ``EventBus`` abstraction
because it requires low-level stream control (``XREADGROUP``, ``XACK``, ``XCLAIM``,
``XADD`` for DLQ) that the ``EventBus`` interface does not expose.
"""

import asyncio
import json
import os
import signal
import tempfile
import time
import uuid
from collections.abc import Awaitable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

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
    record_forced_flush,
    record_ingest_latency,
    record_pending_ack_state,
    start_metrics_server_from_env,
)
from heber.ops.reliability import EventDeduplicator, RedisDedupeStore
from heber.ops.runtime_retry import calculate_retry_delay, classify_runtime_error
from heber.writer.backfill_ack import (
    BackfillAckWriter,
    BackfillEventProof,
    BackfillProofMismatch,
    backfill_event_proof,
)
from heber.writer.bronze import BronzeWriter
from heber.writer.dlq_fallback import (
    log_fallback_backlog,
    try_xadd_with_retry,
    write_dlq_fallback_file,
)
from heber.writer.durability import create_durable_directory
from heber.writer.ingest_contracts import (
    DLQ_REASON_TS_OUT_OF_RANGE,
    DLQ_REASON_UNCONTRACTED,
    PAYLOAD_ALLOWED_FIELDS,
    PAYLOAD_REQUIRED_FIELDS,
    TimestampOutOfRangeError,
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
from heber.writer.utils import (
    build_silver_candidates,
    commit_id_for_message_ids,
    get_partition_key,
    write_batch_commit_marker,
)

logger = structlog.get_logger(__name__)

# Pause when the held set is over the backpressure cap. Without it the loop
# retries a failing flush with no delay, turning a stuck disk into a hot loop.
_BACKPRESSURE_SLEEP_SECONDS = 1.0

# A held batch can reach tens of thousands of ids, which is far too many for one
# XACK command, so the commit sends them in chunks.
_ACK_CHUNK_SIZE = 1000

# Multiple of writer_max_unacked_messages at which the consumer stops reading
# entirely. Past this point the forced flush is already failing, so continuing to
# read only grows a pending list that the stream will trim out from under us —
# turning a recoverable stall into permanent loss.
_BACKPRESSURE_FACTOR = 10
_BACKFILL_READINESS_INTERVAL_SECONDS = 20.0


class EventConsumer:
    """Consumes events from Redis Streams and writes to Lake layers."""

    def __init__(self, event_deduplicator: EventDeduplicator | None = None):
        self.redis: redis.Redis | None = None
        self.bronze_writer = BronzeWriter()
        self.silver_writer = SilverWriter()
        self.running = False
        # uuid suffix makes the group-consumer name unique across sharded
        # containers even when two start within the same second — a bare
        # timestamp would collide and share one identity in the consumer group.
        self.consumer_name = f"consumer-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.event_deduplicator = event_deduplicator or EventDeduplicator(
            backing_store=self._build_dedupe_store(),
        )
        self._inflight_event_ids: set[str] = set()
        # Message ids processed but not yet acknowledged, and event ids not yet
        # in the dedupe store. Both are released by _settle_and_commit() once a
        # flush proves the events are on disk. They must move together: acking
        # without registering permits duplicates, registering without acking
        # makes a redelivery look like one and drops it.
        self._pending_ack_ids: set[str] = set()
        self._pending_register_ids: set[str] = set()
        self._pending_backfill_proofs: dict[str, BackfillEventProof] = {}
        self._pending_backfill_proof_errors: dict[str, str] = {}
        self._pending_message_chunks: dict[str, tuple[str, str] | None] = {}
        self._transport_ack_eligible_chunks: set[tuple[str, str]] = set()
        self._backfill_ack_writer: BackfillAckWriter | None = None
        self._backfill_binding: tuple[str, str, str, str] | None = None
        self._pending_since: float | None = None
        self._payload_required = PAYLOAD_REQUIRED_FIELDS
        self._payload_allowed = PAYLOAD_ALLOWED_FIELDS
        self._silver_validation_warning_counts: dict[tuple[str, str, str, str], int] = {}
        self._last_recovery_monotonic = 0.0
        self._last_backfill_readiness_monotonic = 0.0

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

        # Already processed this cycle and waiting on the flush. Not yet in the
        # dedupe store — registration waits for durability — so it has to be
        # caught here or the same event would be buffered twice.
        if event_id in self._pending_register_ids:
            return "pending_flush_duplicate"

        dedupe_result = self.event_deduplicator.check(event_id)
        if dedupe_result.is_duplicate:
            return dedupe_result.reason

        self._inflight_event_ids.add(event_id)
        return None

    def _stage_backfill_proof(self, envelope: EventEnvelope) -> None:
        """Hold backfill proof data until the same event crosses the fsync boundary."""
        try:
            proof = backfill_event_proof(envelope)
        except BackfillProofMismatch as exc:
            self._pending_backfill_proof_errors[envelope.event_id] = str(exc)
            return
        if proof is None:
            return
        existing = self._pending_backfill_proofs.get(envelope.event_id)
        if existing is not None and existing != proof:
            self._pending_backfill_proof_errors[envelope.event_id] = "backfill proof changed before durable commit"
            return
        self._pending_backfill_proofs[envelope.event_id] = proof

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

        self._backfill_binding = (
            "redis",
            settings.ingest_lane,
            settings.redis_stream_name,
            settings.redis_consumer_group,
        )
        recovered = await self._recover_pending_messages()
        if recovered > 0:
            logger.info(
                "Recovered pending messages",
                recovered=recovered,
                stream=settings.redis_stream_name,
                group=settings.redis_consumer_group,
            )

    def _backfill_consumer_bound(self) -> bool:
        """Whether this transport is bound and able to receive backfill work."""
        return self.redis is not None and self._backfill_binding is not None

    @staticmethod
    def _durability_probe(path: Path, *, root: Path) -> None:
        """Prove a writer directory can persist and remove a small file."""
        create_durable_directory(path, root=root)
        fd, name = tempfile.mkstemp(prefix=".readiness-", dir=path)
        try:
            os.write(fd, b"ready")
            os.fsync(fd)
        finally:
            os.close(fd)
            try:
                os.unlink(name)
            finally:
                directory_fd = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)

    async def _check_backfill_writer_durability(self) -> None:
        """Bounded Bronze/Silver durable-write probes."""
        timeout = settings.backfill_readiness_check_timeout_seconds
        await asyncio.wait_for(
            asyncio.to_thread(
                self._durability_probe,
                settings.bronze_path,
                root=settings.data_root,
            ),
            timeout=timeout,
        )
        await asyncio.wait_for(
            asyncio.to_thread(
                self._durability_probe,
                settings.silver_path,
                root=settings.data_root,
            ),
            timeout=timeout,
        )

    async def _write_backfill_readiness(self) -> None:
        """Publish true readiness only after transport, storage, and ACK checks."""
        if self.redis is None:
            self.redis = redis.from_url(settings.redis_url)

        failure: Exception | None = None
        consumer_ready = self._backfill_consumer_bound()
        binding = self._backfill_binding or ("", "", "", "")
        if not consumer_ready:
            failure = RuntimeError("backfill transport consumer is not bound")
        writer_ready = False
        try:
            await self._check_backfill_writer_durability()
            writer_ready = True
        except Exception as exc:
            failure = failure or exc
        ack_store_ready = False
        try:
            pong = await asyncio.wait_for(
                cast(Awaitable[Any], self.redis.eval("return redis.call('PING')", 0)),
                timeout=settings.backfill_readiness_check_timeout_seconds,
            )
            ack_store_ready = self._decode_string(pong).upper() == "PONG"
            if not ack_store_ready:
                failure = failure or RuntimeError("backfill ACK store Lua probe failed")
        except Exception as exc:
            failure = failure or exc

        await asyncio.wait_for(
            cast(
                Awaitable[Any],
                self.redis.hset(
                    "gateway:backfill:heber:readiness:v1",
                    mapping={
                        "consumer_healthy": str(consumer_ready).lower(),
                        "writer_healthy": str(writer_ready).lower(),
                        "ack_store_ready": str(ack_store_ready).lower(),
                        "protocol_version": "1",
                        "transport": binding[0],
                        "lane": binding[1],
                        "stream": binding[2],
                        "durable_consumer": binding[3],
                        "observed_at": datetime.now(UTC).isoformat(),
                    },
                ),
            ),
            timeout=settings.backfill_readiness_check_timeout_seconds,
        )
        if failure is not None:
            raise failure
        self._last_backfill_readiness_monotonic = time.monotonic()

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
            fallback_root = settings.dlq_fallback_path
            data_root = settings.data_root.resolve(strict=True)
            durable_root = data_root if fallback_root.resolve(strict=False).is_relative_to(data_root) else fallback_root
            return await asyncio.to_thread(
                write_dlq_fallback_file,
                fallback_root,
                settings.redis_dlq_stream_name,
                source_message_id,
                dlq_event,
                durable_root=durable_root,
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

            # Future-dated events must not mint partitions (dt= is derived from
            # ts_event). Past-dated events are legal — historical backfills carry
            # old timestamps — but a *live* source emitting an ancient ts is the
            # poisoned-record signature (the recurring dt=2023-06-23 crypto bar),
            # so log it for tracing without dead-lettering.
            now_utc = datetime.now(UTC)
            if envelope.ts_event > now_utc + timedelta(hours=1):
                logger.warning(
                    "ts_event_out_of_range",
                    event_id=envelope.event_id,
                    feed=feed,
                    provider=provider,
                    ts_event=envelope.ts_event.isoformat(),
                )
                raise TimestampOutOfRangeError(DLQ_REASON_TS_OUT_OF_RANGE)
            if envelope.source != "backfill" and envelope.ts_event < now_utc - timedelta(days=30):
                logger.warning(
                    "stale_ts_event_live_source",
                    event_id=envelope.event_id,
                    feed=feed,
                    provider=provider,
                    source=envelope.source,
                    ts_event=envelope.ts_event.isoformat(),
                )

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
                self._stage_backfill_proof(envelope)
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
        except (UnmappedFeedError, TimestampOutOfRangeError) as exc:
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
                    # Held, not registered: the event is only buffered at this
                    # point. Registering now would make a redelivery of data that
                    # was never written look like a duplicate, and it would be
                    # dropped — the loss this deferral exists to prevent.
                    # _settle_and_commit() registers these once they are durable.
                    self._pending_register_ids.add(envelope.event_id)
                    self._stage_backfill_proof(envelope)

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

    def _backfill_chunk_for_message(self, message_data: dict[Any, Any]) -> tuple[str, str] | None:
        """Return the staged proof chunk for a successfully processed message."""
        payload = self._extract_payload_value(message_data)
        if payload is None:
            return None
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")
        if not isinstance(payload, dict):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError, ValueError):
                return None
        event_id = payload.get("event_id")
        proof = self._pending_backfill_proofs.get(str(event_id))
        if proof is None:
            return None
        return proof.job_id, proof.chunk_id

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
                # Forward-progress heartbeat: a batch fanned out across many
                # historical partitions can outlast the healthcheck window between
                # loop-top ticks, reading as "stalled" and getting restarted
                # mid-drain. Ticking per message keeps liveness honest.
                consumer_loop_heartbeat_unixtime.set(time.time())
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
                self._pending_message_chunks[message_id_str] = self._backfill_chunk_for_message(stream_messages[i][1])
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
                self._pending_message_chunks[message_id_str] = None
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
        for _ in range(self._recovery_page_limit()):
            held_before = len(self._pending_ack_ids)
            recovered = await self._recover_pending_batch()
            newly_held = max(len(self._pending_ack_ids) - held_before, 0)
            if recovered == 0 and newly_held == 0:
                break
            total += recovered
        return total

    @staticmethod
    def _recovery_page_limit() -> int:
        """Bound one recovery drain by the largest accepted proof chunk."""
        batch_size = max(settings.redis_claim_batch_size, 1)
        ceiling = settings.backfill_proof_max_expected_records
        return max((ceiling + batch_size - 1) // batch_size, 1)

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

        # Skip what this consumer is already holding for commit: its event is
        # buffered here, so reclaiming it would process and write the same event
        # twice. XPENDING is oldest-first with no cursor, so if a whole page is
        # ours the candidate list comes back empty — and an empty batch reads as
        # "drain complete" to the caller, stranding the reclaimable entries
        # behind it. Page forward past our own ids instead of giving up.
        message_ids: list[str] = []
        cursor = "-"
        # One extra page is needed to look immediately beyond a complete held
        # chunk whose size equals the configured proof ceiling.
        for _ in range(self._recovery_page_limit() + 1):
            last_seen: str | None = None
            for entry in pending:
                if not isinstance(entry, dict):
                    continue
                msg_id = entry.get("message_id") or entry.get(b"message_id")
                if msg_id is None:
                    continue
                last_seen = self._decode_string(msg_id)
                if last_seen not in self._pending_ack_ids:
                    message_ids.append(last_seen)

            if message_ids or last_seen is None or len(pending) < settings.redis_claim_batch_size:
                break

            # Whole page was ours and the page was full — there may be more
            # behind it. "(" makes the next scan exclusive of the last id seen.
            cursor = f"({last_seen}"
            pending = await self.redis.xpending_range(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                cursor,
                "+",
                settings.redis_claim_batch_size,
                idle=settings.redis_claim_idle_ms,
            )
            if not pending:
                break

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

        # Commit through the same path as the main loop. Going straight to XACK
        # here would leave the recovered events' ids stuck in
        # _pending_register_ids — never reaching the dedupe store and never
        # released — because registration is what the commit performs.
        held_before = len(self._pending_ack_ids)
        self._hold_for_commit(ack_ids)
        await self._settle_and_commit()
        released = held_before + len(ack_ids) - len(self._pending_ack_ids)

        if failed_ids:
            logger.warning(
                "Pending messages could not be recovered",
                failed_count=len(failed_ids),
                failed_ids=failed_ids[:10],
            )

        # Only what was actually acknowledged counts as progress. Reporting
        # claimed-but-unacknowledged messages would let the drain loop advance on
        # work it has not finished — and the claim has already reset their idle
        # timers, so they will not resurface for another claim-idle window.
        return max(released, 0)

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

        try:
            await self._run_loop(error_streak)
        finally:
            await self._shutdown()

    async def _run_loop(self, error_streak: int) -> None:
        """The consume loop proper. Exceptions propagate to ``run``'s finally."""
        while self.running:
            try:
                # Liveness heartbeat at the top of every iteration — stays fresh while
                # the loop spins (even on idle, no-data cycles); the container
                # healthcheck reads this to detect a stalled-but-running consumer.
                consumer_loop_heartbeat_unixtime.set(time.time())
                await self._consume_iteration()
                now_monotonic = time.monotonic()
                if (
                    settings.ingest_lane == "backfill"
                    and now_monotonic - self._last_backfill_readiness_monotonic >= _BACKFILL_READINESS_INTERVAL_SECONDS
                ):
                    await self._write_backfill_readiness()
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

    async def _shutdown(self) -> None:
        """Flush, commit held acknowledgements, then close Redis — in that order.

        Runs from ``run``'s ``finally``, so it covers the exits that re-raise
        (cancellation, a fatal ``ResponseError``) as well as a clean stop. Each
        step is isolated: a failure here must never replace the exception that
        caused the shutdown, which is the actual diagnostic.

        The unconditional flush comes first and is synchronous, so it always
        completes. The commit that follows is best-effort — under cancellation
        its ``await`` re-raises immediately and the acknowledgements are skipped.
        That is the safe direction: unacknowledged means redelivered, never lost.
        """
        try:
            self._final_flush()
        except Exception as e:  # noqa: BLE001 — must not mask the shutdown cause
            logger.error("final_flush_failed", error=str(e), exc_info=True)

        try:
            await self._settle_and_commit()
        except Exception as e:  # noqa: BLE001 — must not mask the shutdown cause
            logger.error("final_commit_failed", error=str(e), exc_info=True)
        except asyncio.CancelledError:
            logger.info(
                "final_commit_skipped_cancelled",
                held_messages=len(self._pending_ack_ids),
            )

        if self.redis is not None:
            try:
                # aclose(), not close(): redis-py deprecated close() in 5.0.1 in
                # favour of aclose(). The bundled type stubs still only describe
                # the sync client's surface, hence the narrow ignore.
                await self.redis.aclose()  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001 — must not mask the shutdown cause
                logger.error("redis_close_failed", error=str(e), exc_info=True)

        logger.info("Consumer stopped", held_messages=len(self._pending_ack_ids))

    def _flush_layers(self) -> bool:
        """Flush Bronze and Silver, reporting whether everything reached storage.

        Returns True only when no buffered event remains in either writer. A
        successful call to ``flush_if_needed()`` is not enough on its own: it
        writes only the partitions past a size or elapsed-time threshold, so on a
        quiet iteration it writes nothing and still returns normally. Treating
        that as success is what allowed a batch to be acknowledged while its
        events were still in RAM, where a container kill lost them for good —
        an acknowledged message is never redelivered.

        Callers must not acknowledge when this returns False; the events stay
        buffered and the messages stay pending for a later attempt.

        Each writer fsyncs the completed file and its parent directory around
        the atomic rename, so a drained buffer is safe across process, container,
        and intact-filesystem power loss.
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

        if not ok:
            return False

        return not (self.bronze_writer.has_buffered() or self.silver_writer.has_buffered())

    def _force_flush_layers(self, reason: str) -> bool:
        """Flush every partition regardless of threshold. The durability backstop.

        ``_flush_layers`` only writes what the size/time thresholds make due, so
        a slow trickle of events can stay buffered indefinitely while their
        messages accumulate unacknowledged. This writes everything, at the cost
        of smaller files, and is expected to be rare — in steady state the
        thresholds fire first.
        """
        ok = True
        for writer, layer in ((self.bronze_writer, "bronze"), (self.silver_writer, "silver")):
            try:
                writer.flush()
            except Exception as e:  # noqa: BLE001 — a failed layer must not skip the other
                logger.error("forced_flush_failed", layer=layer, reason=reason, error=str(e), exc_info=True)
                ok = False

        drained = ok and not (self.bronze_writer.has_buffered() or self.silver_writer.has_buffered())
        record_forced_flush(reason=reason, drained=drained)
        logger.info("forced_flush", reason=reason, drained=drained, held_messages=len(self._pending_ack_ids))
        return drained

    def _deferral_bound_exceeded(self) -> str | None:
        """Name the exceeded hold bound, or None while both are satisfied."""
        if len(self._pending_ack_ids) >= settings.writer_max_unacked_messages:
            return "count_bound"
        if (
            self._pending_since is not None
            and (time.monotonic() - self._pending_since) >= settings.writer_max_unacked_seconds
        ):
            return "age_bound"
        return None

    async def _prepare_durable_commit(
        self,
        *,
        stream: str,
        group: str,
    ) -> set[tuple[str, str]] | None:
        """Make held events durable and register them before a transport ACK.

        Redis and JetStream share this boundary so neither transport can
        acknowledge before Bronze, Silver, and the batch receipt are fsynced.
        """
        drained = await asyncio.to_thread(self._flush_layers)

        if not drained:
            bound = self._deferral_bound_exceeded()
            if bound is not None:
                drained = await asyncio.to_thread(self._force_flush_layers, bound)

        self._record_pending_ack_gauges()

        if not drained:
            if self._pending_ack_ids:
                logger.warning(
                    "ack_deferred_flush_incomplete",
                    held_messages=len(self._pending_ack_ids),
                    held_seconds=round(time.monotonic() - self._pending_since, 1) if self._pending_since else 0.0,
                )
            return None

        ids = list(self._pending_ack_ids)
        if ids:
            await asyncio.to_thread(
                write_batch_commit_marker,
                settings.data_root,
                stream=stream,
                group=group,
                consumer=self.consumer_name,
                message_ids=ids,
            )

        finalized_chunks: set[tuple[str, str]] = set()
        if self._pending_backfill_proof_errors:
            event_id, error = next(iter(self._pending_backfill_proof_errors.items()))
            raise BackfillProofMismatch(f"{event_id}: {error}")
        if self._pending_backfill_proofs:
            if self.redis is None:
                self.redis = redis.from_url(settings.redis_url)
            if self._backfill_ack_writer is None:
                self._backfill_ack_writer = BackfillAckWriter(
                    self.redis,
                    proof_ttl_seconds=settings.backfill_proof_ttl_seconds,
                )
            finalized_chunks = await self._backfill_ack_writer.record_committed(
                list(self._pending_backfill_proofs.values()),
                commit_id=commit_id_for_message_ids(ids),
                committed_at=datetime.now(UTC),
            )
            self._pending_backfill_proofs.clear()

        for event_id in self._pending_register_ids:
            self.event_deduplicator.register(event_id)
        self._pending_register_ids.clear()
        return finalized_chunks

    async def _settle_and_commit(self) -> None:
        """Make buffered events durable, then register and acknowledge them.

        This is the only place the Redis consumer acknowledges. Nothing is
        released until the shared durability boundary confirms both storage
        layers and the commit receipt reached disk.
        """
        prepared = await self._prepare_durable_commit(
            stream=settings.redis_stream_name,
            group=settings.redis_consumer_group,
        )
        if prepared is None:
            return
        self._transport_ack_eligible_chunks.update(prepared)

        # Chunked: a held batch can be tens of thousands of ids. On failure the
        # un-acked remainder stays held so the next commit retries it.
        ids = [
            message_id
            for message_id in self._pending_ack_ids
            if self._pending_message_chunks.get(message_id) is None
            or self._pending_message_chunks[message_id] in self._transport_ack_eligible_chunks
        ]
        for start in range(0, len(ids), _ACK_CHUNK_SIZE):
            chunk = ids[start : start + _ACK_CHUNK_SIZE]
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *chunk,
            )
            self._pending_ack_ids.difference_update(chunk)
            for message_id in chunk:
                self._pending_message_chunks.pop(message_id, None)

        if not self._pending_ack_ids:
            self._pending_since = None
        self._prune_transport_ack_eligible_chunks()
        self._record_pending_ack_gauges()

    def _prune_transport_ack_eligible_chunks(self) -> None:
        """Forget finalized chunks only after all mapped transport ACKs succeed."""
        pending_chunks = {
            chunk
            for message_id in self._pending_ack_ids
            if (chunk := self._pending_message_chunks.get(message_id)) is not None
        }
        self._transport_ack_eligible_chunks.intersection_update(pending_chunks)

    def commit_pending_registrations(self) -> int:
        """Register held event ids after a caller has flushed the writers itself.

        For synchronous users of ``process_event`` (the DLQ reprocessor) that
        flush directly rather than going through ``_settle_and_commit``. Without
        this their events reach disk but never enter the dedupe store, so a later
        replay writes them a second time, and the held set grows for the life of
        the process.

        Call only after the writers have actually been flushed.
        """
        count = len(self._pending_register_ids)
        for event_id in self._pending_register_ids:
            self.event_deduplicator.register(event_id)
        self._pending_register_ids.clear()
        return count

    def _record_pending_ack_gauges(self) -> None:
        held_seconds = time.monotonic() - self._pending_since if self._pending_since else 0.0
        record_pending_ack_state(count=len(self._pending_ack_ids), age_seconds=held_seconds)

    def _hold_for_commit(self, message_ids: list[str]) -> None:
        """Take ownership of processed message ids until the next commit."""
        if not message_ids:
            return
        self._pending_ack_ids.update(message_ids)
        if self._pending_since is None:
            self._pending_since = time.monotonic()

    async def _consume_iteration(self) -> None:
        """Execute a single iteration of the consumer loop.

        Flow: read batch → process concurrently → flush → register → ACK.

        Messages are only acknowledged once a flush confirms no buffered event
        remains, so an interrupted iteration leaves them pending and
        redeliverable rather than acknowledged and lost.

        If the held set has grown past the backpressure cap the iteration stops
        reading and only tries to settle. Continuing to read at that point would
        grow the pending list toward the stream's retention limit, where entries
        are trimmed and become permanently unrecoverable — strictly worse than
        pausing.
        """
        hard_cap = settings.writer_max_unacked_messages * _BACKPRESSURE_FACTOR
        if len(self._pending_ack_ids) >= hard_cap:
            logger.critical(
                "consumer_backpressure_stop_reading",
                held_messages=len(self._pending_ack_ids),
                hard_cap=hard_cap,
                reason="flush is not draining; pausing reads to bound the pending list",
            )
            await self._settle_and_commit()
            if len(self._pending_ack_ids) >= hard_cap:
                # Still stuck. Back off rather than spinning on a failing flush,
                # which would flood the logs and add I/O load to whatever is
                # already wedged.
                await asyncio.sleep(_BACKPRESSURE_SLEEP_SECONDS)
            return

        messages = await self.redis.xreadgroup(
            groupname=settings.redis_consumer_group,
            consumername=self.consumer_name,
            streams={settings.redis_stream_name: ">"},
            count=settings.redis_read_batch_size,
            block=settings.redis_read_block_ms,
        )

        if not messages:
            # An idle iteration is where a partly-filled buffer finally crosses
            # its time threshold, so it is also where held messages usually get
            # committed. Settling here (rather than only flushing) is what keeps
            # a quiet stream from holding acknowledgements indefinitely.
            await self._settle_and_commit()
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

        # Split the iteration clock into process (CPU: parse/validate/buffer) vs
        # flush (bind-mount gzip/parquet I/O) so the batch_processed log reveals
        # which half caps drain throughput — the two are addressed by different
        # fixes (more processing parallelism vs faster/overlapped flush I/O).
        process_seconds = time.monotonic() - t0

        # Hold these ids, then try to make the buffers durable and release them.
        # Anything not released stays pending in Redis and is committed by a
        # later iteration — or reclaimed by recovery if this process dies.
        held_before = len(self._pending_ack_ids)
        self._hold_for_commit(processed_ids)
        await self._settle_and_commit()
        flush_seconds = time.monotonic() - t0 - process_seconds
        acked_now = held_before + len(processed_ids) - len(self._pending_ack_ids)

        elapsed = time.monotonic() - t0
        rate = total_messages / elapsed if elapsed > 0 else 0
        logger.info(
            "batch_processed",
            total=total_messages,
            acked=max(acked_now, 0),
            held=len(self._pending_ack_ids),
            failed=len(failed_ids),
            elapsed_seconds=round(elapsed, 3),
            process_seconds=round(process_seconds, 3),
            flush_seconds=round(flush_seconds, 3),
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
        """Write every buffered partition, isolating a failure to its own layer."""
        for writer, layer in ((self.bronze_writer, "bronze"), (self.silver_writer, "silver")):
            try:
                writer.flush()
            except Exception as e:  # noqa: BLE001 — a broken layer must not skip the other
                logger.error("final_flush_layer_failed", layer=layer, error=str(e), exc_info=True)

    async def stop(self):
        """Signal the loop to stop.

        Deliberately does not close Redis: the shutdown path still needs the
        connection to acknowledge whatever it manages to flush. Closing here left
        a final acknowledgement impossible.
        """
        self.running = False


async def main():
    """Entry point for the consumer."""
    configure_logging(service_name="heber-consumer", log_level=settings.log_level, json_output=True)
    try:
        start_metrics_server_from_env(default_port=9090)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))
    consumer = build_ingest_consumer()

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.run()


def build_ingest_consumer() -> EventConsumer:
    """Select the configured transport while keeping Redis as the default."""
    if settings.ingest_transport == "jetstream":
        from heber.writer.jetstream_consumer import JetStreamEventConsumer

        return JetStreamEventConsumer()
    return EventConsumer()


if __name__ == "__main__":
    asyncio.run(main())
