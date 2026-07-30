"""JetStream delivery adapter for the existing Bronze/Silver writer."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

import nats
import redis.asyncio as redis
import structlog
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from heber.config import settings
from heber.ops.metrics import record_dlq_event
from heber.ops.reliability import EventDeduplicator
from heber.writer.consumer import EventConsumer
from heber.writer.dlq_fallback import log_fallback_backlog

logger = structlog.get_logger(__name__)


class JetStreamEventConsumer(EventConsumer):
    """Pull JetStream messages through ``EventConsumer`` and ACK after fsync."""

    def __init__(self, event_deduplicator: EventDeduplicator | None = None) -> None:
        super().__init__(event_deduplicator=event_deduplicator)
        self.connection: Any | None = None
        self.subscription: Any | None = None
        self._pending_messages: dict[str, Any] = {}
        self._stopping = False
        self._readiness_downgrade_tasks: set[asyncio.Task[None]] = set()

    @property
    def subject(self) -> str:
        lane = "backfill" if settings.ingest_lane == "backfill" else "live"
        return f"heber.{lane}.>"

    async def connect(self) -> None:
        """Connect and bind the configured durable pull consumer."""
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

    async def _on_nats_disconnected(self) -> None:
        """Fail readiness closed once for an unexpected broker disconnect."""
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
            self._pending_message_chunks[message_id] = self._chunk_for_message(message)
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

        for message_id in list(self._pending_ack_ids):
            message = self._pending_messages[message_id]
            chunk = self._pending_message_chunks.get(message_id)
            if chunk is not None and chunk not in self._transport_ack_eligible_chunks:
                await message.in_progress()
                continue
            try:
                await message.ack()
            except Exception as exc:  # noqa: BLE001 — unacked messages must remain redeliverable
                logger.warning(
                    "jetstream_ack_failed",
                    message_id=message_id,
                    error=str(exc),
                    exc_info=True,
                )
                continue
            self._pending_messages.pop(message_id, None)
            self._pending_message_chunks.pop(message_id, None)
            self._pending_ack_ids.discard(message_id)

        if not self._pending_ack_ids:
            self._pending_since = None
        self._prune_transport_ack_eligible_chunks()
        self._record_pending_ack_gauges()

    async def _consume_iteration(self) -> None:
        """Fetch one batch; an idle timeout is also a flush opportunity."""
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
