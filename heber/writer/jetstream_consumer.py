"""JetStream pull receiver for the Bronze/Silver writer.

Data-Gateway's durable outbox publishes every ``heber:events`` envelope to the
``HEBER_LIVE`` work-queue stream (``heber.live.<feed>``) and every
``heber:events:backfill`` envelope to ``HEBER_BACKFILL``
(``heber.backfill.<feed>``). The message body is the same EventEnvelope JSON the
Redis path carries in its ``data`` field, and the ``Nats-Msg-Id`` header is the
envelope's ``event_id``. The gateway owns and provisions both streams; this
receiver only binds to them, creating its lane's durable pull consumer when it
is missing and refusing to start when an existing one has drifted.

Guarantees:

* **Acknowledge only after the write is durable.** A processed message is held
  until a flush leaves both writers with no buffered rows; only then is it
  acknowledged. A flush that fails or leaves rows behind acknowledges nothing,
  so the broker redelivers. A barrier forces a full flush once a message has
  been held for ``jetstream_ack_barrier_seconds`` or ``jetstream_max_held_messages``
  are waiting, and shutdown always attempts one before closing the connection.
* **Fail closed on malformed messages.** A message that cannot be parsed or
  validated, or whose ``Nats-Msg-Id`` header is missing or disagrees with the
  payload's ``event_id``, is written to the durable on-disk DLQ (fsynced, file
  and directory) and then acknowledged at the next durable barrier. If the DLQ
  write fails the message is NAKed for redelivery, never acknowledged.
* **Idempotent by event_id**, with the same guarantee as the Redis path. A
  redelivery of a message still held (same stream sequence, e.g. ack_wait
  expired during a slow flush) replaces the held delivery without being
  processed again. A different sequence carrying an already-written event_id is
  dropped by the writer's event-id deduplicator and acknowledged. Redelivery
  after a crash between flush and acknowledgement can repeat rows in Bronze;
  the compactor's exact dedupe removes them, exactly as for Redis redelivery.

DLQ records land in ``<dlq_fallback_path>/dt=YYYY-MM-DD/`` with
``source_transport=jetstream`` and the stream/durable as source; there is no
Redis DLQ stream copy on this transport.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import nats
import structlog
from empire_core.errors import EmpireError  # type: ignore[import-untyped]  # empire-core ships no py.typed
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, RetentionPolicy
from nats.js.errors import NotFoundError

from heber.config import settings
from heber.ops.metrics import (
    consumer_acked_total,
    consumer_last_xread_success_unixtime,
    consumer_loop_heartbeat_unixtime,
    jetstream_ack_failures_total,
    jetstream_held_messages,
    record_batch_processed,
    record_dlq_event,
)
from heber.ops.reliability import EventDeduplicator
from heber.ops.runtime_retry import calculate_retry_delay
from heber.writer.consumer import EventConsumer
from heber.writer.dlq_fallback import log_fallback_backlog, write_dlq_fallback_file
from heber.writer.utils import fsync_directory

logger = structlog.get_logger(__name__)

Connector = Callable[..., Awaitable[Any]]

_MSG_ID_HEADER = "Nats-Msg-Id"
_RECOVER_AFTER_CONSECUTIVE_ERRORS = 3
_ACK_FLUSH_TIMEOUT_SECONDS = 5.0


class JetStreamBindingError(EmpireError):  # type: ignore[misc]
    """The broker's streams or durable consumer do not match Heber's configuration."""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, "HEBER_JETSTREAM_BINDING_ERROR", details)


class JetStreamEventConsumer(EventConsumer):
    """Pull one writer lane from JetStream and acknowledge after durable writes."""

    def __init__(
        self,
        event_deduplicator: EventDeduplicator | None = None,
        *,
        connector: Connector = nats.connect,
    ) -> None:
        super().__init__(event_deduplicator=event_deduplicator)
        self._connector = connector
        self._connection: Any | None = None
        self._subscription: Any | None = None
        # Stream sequence -> latest delivery of a processed, not yet acknowledged message.
        self._held: dict[int, Any] = {}
        self._oldest_held_monotonic: float | None = None

    @property
    def _stream(self) -> str:
        return settings.jetstream_stream_name

    @property
    def _durable(self) -> str:
        return settings.jetstream_durable_name

    def _is_backfill_source(self) -> bool:
        return settings.ingest_lane == "backfill"

    # ------------------------------------------------------------------
    # Binding
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect, verify the gateway's stream, and bind the lane's durable consumer."""
        connection = await self._connector(
            servers=settings.nats_url,
            user=settings.nats_username,
            password=settings.nats_password.get_secret_value(),
            name=f"heber-{settings.ingest_lane}-writer-{self.consumer_name}",
            disconnected_cb=self._on_disconnected,
            reconnected_cb=self._on_reconnected,
            error_cb=self._on_error,
        )
        try:
            manager = connection.jsm()
            await self._verify_stream(manager)
            await self._ensure_durable(manager)
            subscription = await connection.jetstream().pull_subscribe_bind(durable=self._durable, stream=self._stream)
        except BaseException:
            with suppress(Exception):
                await connection.close()
            raise
        self._connection = connection
        self._subscription = subscription
        log_fallback_backlog(settings.dlq_fallback_path, service="heber-consumer")
        logger.info(
            "jetstream_consumer_bound",
            stream=self._stream,
            durable=self._durable,
            subject=settings.jetstream_filter_subject,
            lane=settings.ingest_lane,
            nats_url=settings.nats_url,
        )

    async def _verify_stream(self, manager: Any) -> None:
        """Refuse to run against a missing stream or one that is not the gateway's work queue."""
        subject = settings.jetstream_filter_subject
        try:
            info = await manager.stream_info(self._stream)
        except NotFoundError as exc:
            raise JetStreamBindingError(
                f"JetStream stream {self._stream} does not exist; Data-Gateway provisions it",
                {"stream": self._stream},
            ) from exc
        subjects = list(info.config.subjects or [])
        if subject not in subjects:
            raise JetStreamBindingError(
                f"JetStream stream {self._stream} subjects {subjects} do not include {subject}",
                {"stream": self._stream, "subjects": subjects, "expected_subject": subject},
            )
        if info.config.retention != RetentionPolicy.WORK_QUEUE:
            raise JetStreamBindingError(
                f"JetStream stream {self._stream} retention is {info.config.retention}, expected work queue",
                {"stream": self._stream},
            )

    async def _ensure_durable(self, manager: Any) -> None:
        """Create the lane's durable when missing; refuse one whose delivery contract drifted."""
        subject = settings.jetstream_filter_subject
        ack_wait = float(settings.jetstream_ack_wait_seconds)
        try:
            info = await manager.consumer_info(self._stream, self._durable)
        except NotFoundError:
            await manager.add_consumer(
                self._stream,
                config=ConsumerConfig(
                    name=self._durable,
                    durable_name=self._durable,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=ack_wait,
                    max_ack_pending=settings.jetstream_max_ack_pending,
                    filter_subject=subject,
                ),
            )
            logger.info("jetstream_durable_created", stream=self._stream, durable=self._durable, subject=subject)
            return

        config = info.config
        checks = {
            "filter_subject": config.filter_subject == subject and not config.filter_subjects,
            "ack_policy": config.ack_policy == AckPolicy.EXPLICIT,
            "ack_wait": config.ack_wait is not None and abs(float(config.ack_wait) - ack_wait) < 1e-3,
            "max_ack_pending": config.max_ack_pending == settings.jetstream_max_ack_pending,
        }
        drifted = sorted(name for name, matches in checks.items() if not matches)
        if drifted:
            raise JetStreamBindingError(
                f"JetStream durable {self._durable} on {self._stream} does not match Heber's configuration: "
                f"{', '.join(drifted)}",
                {"stream": self._stream, "durable": self._durable, "drifted": drifted},
            )

    async def _on_disconnected(self) -> None:
        logger.warning("jetstream_consumer_disconnected", stream=self._stream, held=len(self._held))

    async def _on_reconnected(self) -> None:
        logger.info("jetstream_consumer_reconnected", stream=self._stream)

    async def _on_error(self, error: Exception) -> None:
        logger.warning("jetstream_connection_error", stream=self._stream, error=f"{type(error).__name__}: {error}")

    def _connection_lost(self) -> bool:
        return self._connection is None or bool(self._connection.is_closed)

    async def _recover_connection(self) -> None:
        """Drop the current connection and bind again.

        Held deliveries keep referencing the old connection, so their
        acknowledgements fail and they are released; the data they carried is
        either durable already (the broker redelivers and dedupe drops the
        repeat) or still buffered and covered by the redelivery.
        """
        stale = self._connection
        self._connection = None
        self._subscription = None
        if stale is not None and not stale.is_closed:
            with suppress(Exception):
                await stale.close()
        await self.connect()

    # ------------------------------------------------------------------
    # Receive
    # ------------------------------------------------------------------

    async def _consume_iteration(self) -> None:
        """Pull one batch, buffer it, and acknowledge whatever a flush made durable."""
        if self._barrier_due():
            await self._ack_barrier(force=True)
            if len(self._held) >= settings.jetstream_max_held_messages:
                # The barrier could not make the held messages durable; pulling
                # more would only grow what is at risk.
                await asyncio.sleep(settings.jetstream_reconnect_backoff_seconds)
                return

        if self._subscription is None:
            raise ConnectionError("JetStream consumer is not bound")
        try:
            messages = await self._subscription.fetch(
                batch=settings.jetstream_fetch_batch_size,
                timeout=settings.jetstream_fetch_timeout_seconds,
            )
        except NatsTimeoutError:
            messages = []
        if self._connection is not None and self._connection.is_connected:
            consumer_last_xread_success_unixtime.set(time.time())

        if messages:
            await self._receive(messages)
        await self._ack_barrier(force=self._barrier_due())

    async def _receive(self, messages: list[Any]) -> None:
        for message in messages:
            consumer_loop_heartbeat_unixtime.set(time.time())
            sequence = int(message.metadata.sequence.stream)
            if sequence in self._held:
                # Redelivered while still waiting for its flush: the rows are
                # already buffered, so keep the newest delivery to acknowledge.
                self._held[sequence] = message
                await self._signal_in_progress(message, sequence)
                continue

            error = self._header_error(message)
            attempts = 1
            if error is None:
                success, error, attempts = await self._process_with_retry({"data": message.data})
                if success:
                    self._hold(sequence, message)
                    continue
            if await self._dead_letter(message, sequence, error, attempts):
                self._hold(sequence, message)
            else:
                await self._nak(message, sequence)
        record_batch_processed(feed="mixed", batch_size=len(messages))
        jetstream_held_messages.labels(stream=self._stream).set(len(self._held))

    @staticmethod
    def _header_error(message: Any) -> str | None:
        """Return a DLQ reason when ``Nats-Msg-Id`` is missing or names another event."""
        headers = message.headers or {}
        header_event_id = headers.get(_MSG_ID_HEADER)
        if not header_event_id:
            return f"missing_nats_msg_id: no {_MSG_ID_HEADER} header"
        try:
            payload_event_id = json.loads(message.data).get("event_id")
        except (AttributeError, TypeError, ValueError):
            # Unparseable or non-object bodies fail in processing with their own reason.
            return None
        if payload_event_id is not None and payload_event_id != header_event_id:
            return f"event_id_header_mismatch: header={header_event_id} payload={payload_event_id}"
        return None

    def _hold(self, sequence: int, message: Any) -> None:
        self._held[sequence] = message
        if self._oldest_held_monotonic is None:
            self._oldest_held_monotonic = time.monotonic()

    async def _signal_in_progress(self, message: Any, sequence: int) -> None:
        try:
            await message.in_progress()
        except Exception as exc:  # noqa: BLE001 — a lost progress signal only risks another redelivery
            logger.warning("jetstream_in_progress_failed", stream=self._stream, sequence=sequence, error=str(exc))

    async def _nak(self, message: Any, sequence: int) -> None:
        try:
            await message.nak(delay=settings.jetstream_reconnect_backoff_seconds)
        except Exception as exc:  # noqa: BLE001 — an unacknowledged message is redelivered after ack_wait anyway
            logger.warning("jetstream_nak_failed", stream=self._stream, sequence=sequence, error=str(exc))

    # ------------------------------------------------------------------
    # Dead letter
    # ------------------------------------------------------------------

    async def _dead_letter(self, message: Any, sequence: int, error: str, attempts: int) -> bool:
        """Persist a failed message to the durable DLQ; True only once it is on disk."""
        source_message_id = f"{self._stream}:{sequence}"
        failed_at = datetime.now(UTC).isoformat()
        record = {
            "source_transport": "jetstream",
            "source_stream": self._stream,
            "source_group": self._durable,
            "source_message_id": source_message_id,
            "subject": str(getattr(message, "subject", "")),
            "nats_msg_id": (message.headers or {}).get(_MSG_ID_HEADER),
            "consumer_name": self.consumer_name,
            "attempts": str(attempts),
            "error": error,
            "failed_at": failed_at,
            "payload": self._serialize_message_data({"data": message.data}),
        }
        try:
            path = await asyncio.to_thread(self._write_dlq_record, f"{source_message_id}:{failed_at}", record)
        except Exception as exc:  # noqa: BLE001 — the caller NAKs instead of acknowledging
            logger.error(
                "jetstream_dlq_write_failed",
                stream=self._stream,
                source_message_id=source_message_id,
                error=str(exc),
                exc_info=True,
            )
            return False
        feed = self._extract_feed_from_message({"data": message.data})
        record_dlq_event(feed=feed, error_type=error.split(":", 1)[0] if error else "unknown_error")
        logger.warning(
            "jetstream_message_dead_lettered",
            stream=self._stream,
            source_message_id=source_message_id,
            feed=feed,
            error=error,
            attempts=attempts,
            durable_path=str(path),
        )
        return True

    def _write_dlq_record(self, key: str, record: dict[str, Any]) -> Path:
        path = write_dlq_fallback_file(settings.dlq_fallback_path, f"jetstream:{self._stream}", key, record)
        fsync_directory(path.parent)
        return path

    # ------------------------------------------------------------------
    # Acknowledge
    # ------------------------------------------------------------------

    def _barrier_due(self) -> bool:
        if not self._held:
            return False
        if len(self._held) >= settings.jetstream_max_held_messages:
            return True
        oldest = self._oldest_held_monotonic
        return oldest is not None and time.monotonic() - oldest >= settings.jetstream_ack_barrier_seconds

    def _writers_hold_rows(self) -> bool:
        return self.bronze_writer.has_buffered() or self.silver_writer.has_buffered()

    async def _ack_barrier(self, *, force: bool) -> None:
        """Flush, and acknowledge the held messages only if nothing is left buffered."""
        if not self._held and not self._writers_hold_rows():
            return
        flushed = await self._flush_layers_with_heartbeat(force)
        if not flushed or self._writers_hold_rows():
            if force:
                logger.warning(
                    "jetstream_ack_deferred_not_durable",
                    stream=self._stream,
                    held=len(self._held),
                    buffered_events=self._buffered_event_count(),
                )
            return
        if self._held:
            await self._ack_held()

    async def _ack_held(self) -> None:
        held = self._held
        self._held = {}
        self._oldest_held_monotonic = None
        acknowledged = 0
        failed = 0
        for sequence, message in held.items():
            try:
                await message.ack()
            except Exception as exc:  # noqa: BLE001 — data is durable; the broker redelivers and dedupe drops it
                failed += 1
                jetstream_ack_failures_total.labels(stream=self._stream).inc()
                logger.warning("jetstream_ack_failed", stream=self._stream, sequence=sequence, error=str(exc))
            else:
                acknowledged += 1
        if acknowledged:
            consumer_acked_total.inc(acknowledged)
        connection = self._connection
        if connection is not None and connection.is_connected:
            try:
                await connection.flush(timeout=_ACK_FLUSH_TIMEOUT_SECONDS)
            except Exception as exc:  # noqa: BLE001 — unflushed acknowledgements are redelivered and deduped
                logger.warning("jetstream_ack_flush_failed", stream=self._stream, error=str(exc))
        jetstream_held_messages.labels(stream=self._stream).set(0)
        logger.info("jetstream_ack_barrier", stream=self._stream, acknowledged=acknowledged, failed=failed)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Pull until stopped; always flush and acknowledge before closing the connection."""
        await self.connect()
        self.running = True
        error_streak = 0
        logger.info(
            "Starting consumer",
            transport="jetstream",
            stream=self._stream,
            durable=self._durable,
            consumer=self.consumer_name,
        )
        try:
            while self.running:
                consumer_loop_heartbeat_unixtime.set(time.time())
                try:
                    await self._consume_iteration()
                    error_streak = 0
                except asyncio.CancelledError:
                    logger.info("Consumer cancelled", transport="jetstream")
                    raise
                except JetStreamBindingError:
                    raise
                except Exception as exc:  # noqa: BLE001 — the pull loop retries; binding errors still fail fast
                    error_streak += 1
                    delay = calculate_retry_delay(
                        attempt=error_streak,
                        base_seconds=settings.jetstream_reconnect_backoff_seconds,
                        max_seconds=30.0,
                        jitter_ratio=0.2,
                    )
                    logger.warning(
                        "jetstream_consumer_error",
                        stream=self._stream,
                        error=f"{type(exc).__name__}: {exc}",
                        consecutive_errors=error_streak,
                        retry_delay_seconds=round(delay, 3),
                        exc_info=True,
                    )
                    await asyncio.sleep(delay)
                    if self._connection_lost() or error_streak >= _RECOVER_AFTER_CONSECUTIVE_ERRORS:
                        await self._recover_connection()
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        try:
            await self._ack_barrier(force=True)
        except Exception as exc:  # noqa: BLE001 — shutdown must still close the connection
            logger.error("jetstream_final_flush_failed", stream=self._stream, error=str(exc), exc_info=True)
        if self._held:
            logger.warning(
                "jetstream_shutdown_left_messages_unacknowledged",
                stream=self._stream,
                held=len(self._held),
                buffered_events=self._buffered_event_count(),
            )
        connection = self._connection
        self._connection = None
        self._subscription = None
        if connection is not None and not connection.is_closed:
            try:
                await connection.close()
            except Exception as exc:  # noqa: BLE001 — nothing left to protect once the flush has run
                logger.error("jetstream_close_failed", stream=self._stream, error=str(exc), exc_info=True)
        logger.info("Consumer stopped", transport="jetstream")
