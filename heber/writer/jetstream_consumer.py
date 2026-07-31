"""JetStream delivery adapter for the existing Bronze/Silver writer."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

import nats
import redis.asyncio as redis
import structlog
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from heber.config import settings
from heber.ops.metrics import record_dlq_event, record_jetstream_consumer_state
from heber.ops.reliability import EventDeduplicator
from heber.writer.backfill_ack import BackfillEventProof
from heber.writer.consumer import EventConsumer
from heber.writer.dlq_fallback import log_fallback_backlog

logger = structlog.get_logger(__name__)

_MAX_CONCURRENT_ACKS = 32


class JetStreamEventConsumer(EventConsumer):
    """Pull JetStream messages through ``EventConsumer`` and ACK after fsync."""

    requires_durable_event_receipts = True

    def __init__(self, event_deduplicator: EventDeduplicator | None = None) -> None:
        super().__init__(event_deduplicator=event_deduplicator)
        self.connection: Any | None = None
        self.subscription: Any | None = None
        self._pending_messages: dict[str, Any] = {}
        self._pending_message_proofs: dict[str, BackfillEventProof | None] = {}
        self._stopping = False
        self._readiness_downgrade_tasks: set[asyncio.Task[None]] = set()
        self._last_metrics_refresh = 0.0

    @property
    def subject(self) -> str:
        lane = "backfill" if settings.ingest_lane == "backfill" else "live"
        return f"heber.{lane}.>"

    async def connect(self) -> None:
        """Connect and bind the configured durable pull consumer."""
        record_jetstream_consumer_state(
            stream=settings.jetstream_stream_name,
            consumer=settings.jetstream_durable_name,
            pending=0,
            ack_pending=0,
            redelivered=0,
            bound=False,
        )
        await self._write_backfill_transport_unready()
        password = settings.nats_password.get_secret_value() if settings.nats_password else None
        self.connection = await nats.connect(
            servers=settings.nats_url,
            user=settings.nats_username,
            password=password,
            name=self.consumer_name,
            disconnected_cb=self._on_nats_disconnected,
            closed_cb=self._on_nats_closed,
            reconnected_cb=self._on_nats_reconnected,
        )
        jetstream = self.connection.jetstream()
        config = ConsumerConfig(
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=settings.jetstream_ack_wait_seconds,
            max_ack_pending=settings.jetstream_max_ack_pending,
            filter_subject=self.subject,
        )
        self.subscription = await jetstream.pull_subscribe(
            self.subject,
            durable=settings.jetstream_durable_name,
            stream=settings.jetstream_stream_name,
            config=config,
        )
        await self._record_jetstream_state(jetstream)
        self._backfill_binding = (
            "jetstream",
            settings.ingest_lane,
            settings.jetstream_stream_name,
            settings.jetstream_durable_name,
        )
        log_fallback_backlog(settings.dlq_fallback_path, service="heber-consumer")
        logger.info(
            "jetstream_consumer_connected",
            stream=settings.jetstream_stream_name,
            durable=settings.jetstream_durable_name,
            subject=self.subject,
        )

    async def _record_jetstream_state(self, jetstream: Any) -> None:
        """Publish server-side counters; a pull bind alone does not prove config."""
        info = await jetstream.consumer_info(settings.jetstream_stream_name, settings.jetstream_durable_name)
        config = info.config
        if (
            config.filter_subject != self.subject
            or config.ack_policy != AckPolicy.EXPLICIT
            or config.ack_wait != settings.jetstream_ack_wait_seconds
            or config.max_ack_pending != settings.jetstream_max_ack_pending
        ):
            raise RuntimeError("JetStream durable consumer binding does not match Heber ACK configuration")
        ack_floor = getattr(getattr(info, "ack_floor", None), "stream_seq", None)
        if ack_floor is not None:
            await asyncio.to_thread(
                self.durable_event_receipts.delete_confirmed_stream_sequences,
                int(ack_floor),
            )
            self._record_durable_receipt_metrics()
        record_jetstream_consumer_state(
            stream=settings.jetstream_stream_name,
            consumer=settings.jetstream_durable_name,
            pending=info.num_pending,
            ack_pending=info.num_ack_pending,
            redelivered=info.num_redelivered,
            bound=True,
        )
        self._last_metrics_refresh = time.monotonic()

    async def _refresh_jetstream_state_if_due(self) -> None:
        """Refresh broker counters rather than leaving connect-time values stale."""
        if time.monotonic() - self._last_metrics_refresh < settings.jetstream_metrics_refresh_seconds:
            return
        if self.connection is None or not getattr(self.connection, "is_connected", True):
            return
        jetstream_factory = getattr(self.connection, "jetstream", None)
        if jetstream_factory is None:
            return
        await self._record_jetstream_state(jetstream_factory())

    async def _on_nats_disconnected(self) -> None:
        """Fail readiness closed once for an unexpected broker disconnect."""
        record_jetstream_consumer_state(
            stream=settings.jetstream_stream_name,
            consumer=settings.jetstream_durable_name,
            pending=0,
            ack_pending=0,
            redelivered=0,
            bound=False,
        )
        was_bound = self._backfill_binding is not None
        self._backfill_binding = None
        if was_bound and not self._stopping:
            task = asyncio.create_task(self._write_backfill_transport_unready())
            self._readiness_downgrade_tasks.add(task)
            task.add_done_callback(self._readiness_downgrade_tasks.discard)

    async def _on_nats_closed(self) -> None:
        """Fail readiness closed when NATS exhausts reconnect attempts."""
        await self._on_nats_disconnected()

    async def _on_nats_reconnected(self) -> None:
        """Restore binding identity, then rerun every readiness proof."""
        if self._stopping or self.connection is None or self.subscription is None:
            return
        self._backfill_binding = (
            "jetstream",
            settings.ingest_lane,
            settings.jetstream_stream_name,
            settings.jetstream_durable_name,
        )
        try:
            await self._write_backfill_readiness()
        except Exception as exc:  # noqa: BLE001 — callback failure must not break NATS reconnect
            logger.warning(
                "backfill_readiness_restore_failed",
                error=str(exc),
                exc_info=True,
            )

    @staticmethod
    def _message_id(message: Any) -> str:
        """Return the stable stream sequence used by commit receipts."""
        return str(message.metadata.sequence.stream)

    def _backfill_consumer_bound(self) -> bool:
        return (
            self.connection is not None
            and bool(getattr(self.connection, "is_connected", False))
            and self.subscription is not None
            and self._backfill_binding is not None
        )

    async def _write_backfill_transport_unready(self) -> None:
        """Publish a false transport heartbeat before reconnecting."""
        self._backfill_binding = None
        if settings.ingest_lane != "backfill":
            return
        if self.redis is None:
            self.redis = redis.from_url(settings.redis_url)  # type: ignore[no-untyped-call]
        try:
            await asyncio.wait_for(
                cast(
                    Awaitable[Any],
                    self.redis.hset(
                        "gateway:backfill:heber:readiness:v1",
                        mapping={
                            "consumer_healthy": "false",
                            "observed_at": datetime.now(UTC).isoformat(),
                        },
                    ),
                ),
                timeout=settings.backfill_readiness_check_timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 — broker recovery must continue without Redis
            logger.warning(
                "backfill_readiness_downgrade_failed",
                error=str(exc),
                exc_info=True,
            )

    def _chunk_for_message(self, message: Any) -> tuple[str, str] | None:
        """Return the staged proof chunk associated with a validated message."""
        try:
            event_id = str(json.loads(message.data)["event_id"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None
        proof = self._pending_backfill_proofs.get(event_id)
        if proof is None:
            return None
        return proof.job_id, proof.chunk_id

    @staticmethod
    def _event_id_for_message(message: Any) -> str | None:
        try:
            return str(json.loads(getattr(message, "data", b""))["event_id"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    async def _write_jetstream_dlq(
        self,
        message: Any,
        *,
        message_id: str,
        error: str,
        attempts: int,
    ) -> bool:
        """Persist a poison message to the existing filesystem DLQ."""
        dlq_event = {
            "source_stream": settings.jetstream_stream_name,
            "source_group": settings.jetstream_durable_name,
            "source_message_id": message_id,
            "consumer_name": self.consumer_name,
            "attempts": str(attempts),
            "error": error,
            "failed_at": datetime.now(UTC).isoformat(),
            "payload": self._serialize_message_data({"data": message.data}),
        }
        path = await self._write_dlq_fallback(
            source_message_id=message_id,
            dlq_event=dlq_event,
            redis_error=None,
        )
        if path is None:
            return False
        feed = self._extract_feed_from_message({"data": message.data})
        record_dlq_event(feed=feed, error_type=error.split(":", 1)[0] if error else "unknown_error")
        logger.warning(
            "jetstream_message_sent_to_dlq",
            source_message_id=message_id,
            durable_path=str(path),
        )
        return True

    async def _process_batch(self, messages: list[Any]) -> None:
        """Process one pull batch and settle only durably captured messages."""
        for message in messages:
            message_id = self._message_id(message)
            if message_id in self._pending_messages:
                self._pending_messages[message_id] = message
                await message.in_progress()
                continue
            success, error, attempts = await self._process_with_retry({"data": message.data})
            if not success:
                captured = await self._write_jetstream_dlq(
                    message,
                    message_id=message_id,
                    error=error,
                    attempts=attempts,
                )
                if not captured:
                    await message.nak(delay=settings.redis_retry_backoff_seconds)
                    continue

            self._pending_messages[message_id] = message
            proof = self._backfill_proof_for_message({"data": message.data})
            self._pending_message_proofs[message_id] = proof
            self._pending_message_chunks[message_id] = None if proof is None else (proof.job_id, proof.chunk_id)
            self._hold_for_commit([message_id])

        await self._settle_and_commit()

    async def _settle_and_commit(self) -> None:
        """ACK JetStream messages after the shared writer durability boundary."""
        prepared = await self._prepare_durable_commit(
            stream=settings.jetstream_stream_name,
            group=settings.jetstream_durable_name,
        )
        if prepared is None:
            return
        self._transport_ack_eligible_chunks.update(prepared)
        pending_event_ids = {
            message_id: event_id
            for message_id, message in self._pending_messages.items()
            if (event_id := self._event_id_for_message(message)) is not None
        }
        receipt_sequences = {
            event_id: int(message_id)
            for message_id, event_id in pending_event_ids.items()
            if self.durable_event_receipts.contains(event_id)
        }
        await asyncio.to_thread(
            self.durable_event_receipts.record_stream_sequences,
            receipt_sequences,
        )

        eligible_message_ids: list[str] = []
        for message_id in list(self._pending_ack_ids):
            message = self._pending_messages[message_id]
            chunk = self._pending_message_chunks.get(message_id)
            proof = self._pending_message_proofs.get(message_id)
            if proof is not None and not self.durable_event_receipts.backfill_proof_is_finalized(proof):
                await message.in_progress()
                continue
            if proof is None and chunk is not None and chunk not in self._transport_ack_eligible_chunks:
                await message.in_progress()
                continue
            eligible_message_ids.append(message_id)

        acknowledged: set[str] = set()
        for start in range(0, len(eligible_message_ids), _MAX_CONCURRENT_ACKS):
            message_ids = eligible_message_ids[start : start + _MAX_CONCURRENT_ACKS]
            results = await asyncio.gather(*(self._ack_message(message_id) for message_id in message_ids))
            acknowledged.update(
                message_id for message_id, succeeded in zip(message_ids, results, strict=True) if succeeded
            )

        pending_messages_by_event: dict[str, set[str]] = {}
        for message_id, event_id in pending_event_ids.items():
            pending_messages_by_event.setdefault(event_id, set()).add(message_id)
        confirmed_event_ids = {
            event_id for event_id, message_ids in pending_messages_by_event.items() if message_ids <= acknowledged
        }
        await asyncio.to_thread(self.durable_event_receipts.confirm_broker_acks, confirmed_event_ids)
        for message_id in acknowledged:
            self._pending_messages.pop(message_id, None)
            self._pending_message_proofs.pop(message_id, None)
            self._pending_message_chunks.pop(message_id, None)
            self._pending_ack_ids.discard(message_id)

        if not self._pending_ack_ids:
            self._pending_since = None
        self._prune_transport_ack_eligible_chunks()
        self._record_durable_receipt_metrics()
        self._record_pending_ack_gauges()

    async def _ack_message(self, message_id: str) -> bool:
        """Synchronously confirm one broker ACK without serializing its peers."""
        try:
            await self._pending_messages[message_id].ack_sync(timeout=5)
        except Exception as exc:  # noqa: BLE001 — unacked messages must remain redeliverable
            logger.warning(
                "jetstream_ack_failed",
                message_id=message_id,
                error=str(exc),
                exc_info=True,
            )
            return False
        return True

    async def _consume_iteration(self) -> None:
        """Fetch one batch; an idle timeout is also a flush opportunity."""
        await self._refresh_jetstream_state_if_due()
        if len(self._pending_ack_ids) >= settings.jetstream_max_ack_pending:
            await self._settle_and_commit()
            if len(self._pending_ack_ids) >= settings.jetstream_max_ack_pending:
                await asyncio.sleep(1)
            return

        try:
            if self.subscription is None:
                raise RuntimeError("JetStream consumer is not connected")
            messages = await self.subscription.fetch(
                batch=settings.redis_read_batch_size,
                timeout=settings.redis_read_block_ms / 1000,
            )
        except NatsTimeoutError:
            await self._settle_and_commit()
            return
        except Exception as exc:  # noqa: BLE001 — stale subscriptions must be rebound
            logger.warning("jetstream_fetch_failed", error=str(exc), exc_info=True)
            stale_connection = self.connection
            self.connection = None
            self.subscription = None
            self._backfill_binding = None
            await self._write_backfill_transport_unready()
            if stale_connection is not None:
                try:
                    await stale_connection.close()
                except Exception as close_exc:  # noqa: BLE001 — reconnect must still be attempted
                    logger.warning(
                        "jetstream_stale_connection_close_failed",
                        error=str(close_exc),
                        exc_info=True,
                    )
            await asyncio.sleep(settings.jetstream_reconnect_backoff_seconds)
            await self.connect()
            return

        await self._process_batch(messages)

    async def run(self) -> None:
        """Run the pull loop until stopped."""
        await self.connect()
        self.running = True
        logger.info(
            "starting_jetstream_consumer",
            stream=settings.jetstream_stream_name,
            durable=settings.jetstream_durable_name,
            consumer=self.consumer_name,
        )
        try:
            await self._run_loop(0)
        finally:
            await self._shutdown()

    async def _maybe_recover_pending(self, now_monotonic: float) -> int:
        """JetStream redelivers unacknowledged messages without a claim sweep."""
        return 0

    async def _shutdown(self) -> None:
        """Flush and settle before closing the broker connection."""
        self._stopping = True
        await self._write_backfill_transport_unready()
        await super()._shutdown()
        if self.connection is not None:
            try:
                await self.connection.close()
            except Exception as exc:  # noqa: BLE001 — shutdown diagnostics must survive broker failure
                logger.error("jetstream_close_failed", error=str(exc), exc_info=True)
