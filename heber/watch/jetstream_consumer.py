"""JetStream delivery adapter for the existing idempotent watch handler."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import nats
import structlog
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from heber.config import settings
from heber.ops.metrics import consumer_loop_heartbeat_unixtime, record_jetstream_consumer_state
from heber.watch.consumer import AlertWatchConsumer
from heber.writer.event_receipts import DurableEventReceipts

logger = structlog.get_logger(__name__)

_WATCH_SUBJECT = "heber.watch.flow_alerts"


class JetStreamAlertWatchConsumer:
    """Pull flow alerts and ACK only after the Redis handler has settled them.

    ``AlertWatchConsumer._handle_message`` returns true only for an idempotent
    watch creation/skipped result or after its durable DLQ fallback succeeds.
    A false result deliberately leaves the JetStream message unacknowledged.
    """

    def __init__(self, redis_client: Any, manager: Any, **handler_kwargs: Any) -> None:
        self.handler = AlertWatchConsumer(
            redis_client,
            manager,
            dlq_retryable_exhaustion=False,
            **handler_kwargs,
        )
        self.connection: Any | None = None
        self.subscription: Any | None = None
        self._running = False
        self._transport_connected = False
        self._last_metrics_refresh = 0.0
        self.durable_watch_receipts = DurableEventReceipts(settings.data_root, lane="watch")
        self._recovery_watch_messages = self.durable_watch_receipts.load_pending_watch_messages()

    async def connect(self) -> None:
        record_jetstream_consumer_state(
            stream=settings.watch_jetstream_stream_name,
            consumer=settings.watch_jetstream_durable_name,
            pending=0,
            ack_pending=0,
            redelivered=0,
            bound=False,
        )
        password = settings.nats_password.get_secret_value() if settings.nats_password else None
        self.connection = await nats.connect(
            servers=settings.nats_url,
            user=settings.nats_username,
            password=password,
            name="heber-watch",
            disconnected_cb=self._on_disconnected,
            closed_cb=self._on_closed,
            reconnected_cb=self._on_reconnected,
        )
        jetstream = self.connection.jetstream()
        config = ConsumerConfig(
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=settings.jetstream_ack_wait_seconds,
            max_ack_pending=settings.jetstream_max_ack_pending,
            filter_subject=_WATCH_SUBJECT,
        )
        self.subscription = await jetstream.pull_subscribe(
            _WATCH_SUBJECT,
            durable=settings.watch_jetstream_durable_name,
            stream=settings.watch_jetstream_stream_name,
            config=config,
        )
        info = await jetstream.consumer_info(
            settings.watch_jetstream_stream_name,
            settings.watch_jetstream_durable_name,
        )
        if (
            info.config.filter_subject != _WATCH_SUBJECT
            or info.config.ack_policy != AckPolicy.EXPLICIT
            or info.config.ack_wait != settings.jetstream_ack_wait_seconds
            or info.config.max_ack_pending != settings.jetstream_max_ack_pending
        ):
            raise RuntimeError("JetStream watch durable binding does not match explicit-ACK flow-alert configuration")
        self._transport_connected = True
        ack_floor = getattr(getattr(info, "ack_floor", None), "stream_seq", None)
        if ack_floor is not None:
            await asyncio.to_thread(self.durable_watch_receipts.delete_confirmed_watch_messages, int(ack_floor))
        record_jetstream_consumer_state(
            stream=settings.watch_jetstream_stream_name,
            consumer=settings.watch_jetstream_durable_name,
            pending=info.num_pending,
            ack_pending=info.num_ack_pending,
            redelivered=info.num_redelivered,
            bound=True,
        )
        self._last_metrics_refresh = time.monotonic()

    async def _refresh_jetstream_state_if_due(self) -> None:
        if time.monotonic() - self._last_metrics_refresh < settings.jetstream_metrics_refresh_seconds:
            return
        if self.connection is None or not self._transport_connected:
            return
        info = await self.connection.jetstream().consumer_info(
            settings.watch_jetstream_stream_name,
            settings.watch_jetstream_durable_name,
        )
        ack_floor = getattr(getattr(info, "ack_floor", None), "stream_seq", None)
        if ack_floor is not None:
            await asyncio.to_thread(self.durable_watch_receipts.delete_confirmed_watch_messages, int(ack_floor))
        record_jetstream_consumer_state(
            stream=settings.watch_jetstream_stream_name,
            consumer=settings.watch_jetstream_durable_name,
            pending=info.num_pending,
            ack_pending=info.num_ack_pending,
            redelivered=info.num_redelivered,
            bound=True,
        )
        self._last_metrics_refresh = time.monotonic()

    async def _on_disconnected(self) -> None:
        self._transport_connected = False
        record_jetstream_consumer_state(
            stream=settings.watch_jetstream_stream_name,
            consumer=settings.watch_jetstream_durable_name,
            pending=0,
            ack_pending=0,
            redelivered=0,
            bound=False,
        )
        logger.warning("jetstream_watch_disconnected")

    async def _on_closed(self) -> None:
        self._transport_connected = False
        self.connection = None
        self.subscription = None
        logger.warning("jetstream_watch_closed")

    async def _on_reconnected(self) -> None:
        self._transport_connected = True
        logger.info("jetstream_watch_reconnected")

    @staticmethod
    def _message_id(message: Any) -> str:
        return str(message.metadata.sequence.stream)

    async def _dispatch(self, messages: list[Any]) -> None:
        for message in messages:
            message_id = self._message_id(message)
            data = {"data": message.data}
            try:
                event_id = str(json.loads(message.data).get("event_id") or message_id)
            except (TypeError, ValueError, json.JSONDecodeError):
                event_id = message_id
            # A restarted process sees the same durable envelope on redelivery;
            # the INSERT is idempotent and the existing handler retries the
            # Redis watch projection before any confirmed broker ACK.
            self._recovery_watch_messages.pop(event_id, None)
            await asyncio.to_thread(
                self.durable_watch_receipts.store_pending_watch_message,
                event_id,
                message.data,
                int(message.metadata.sequence.stream),
            )
            if not self.handler._is_flow_alert(data):
                captured = await self.handler._dead_letter_message(
                    message_id,
                    data,
                    attempts=getattr(message.metadata, "num_delivered", 1),
                    error="jetstream_flow_subject_payload_invalid",
                )
                if captured:
                    await message.ack_sync(timeout=5)
                    await asyncio.to_thread(self.durable_watch_receipts.delete_pending_watch_message, event_id)
                continue
            # Reuse the mature parser/retry/DLQ/idempotent-manager behavior.
            settled = await self.handler._handle_message(message_id, data)
            if settled:
                await message.ack_sync(timeout=5)
                await asyncio.to_thread(self.durable_watch_receipts.delete_pending_watch_message, event_id)

    async def run(self) -> None:
        await self.connect()
        self._running = True
        try:
            while self._running:
                try:
                    consumer_loop_heartbeat_unixtime.set(time.time())
                    if self.connection is None:
                        await self.connect()
                        continue
                    if self.subscription is None or not self._transport_connected:
                        await asyncio.sleep(settings.jetstream_reconnect_backoff_seconds)
                        continue
                    await self._refresh_jetstream_state_if_due()
                    messages = await self.subscription.fetch(
                        batch=settings.redis_read_batch_size,
                        timeout=settings.redis_read_block_ms / 1000,
                    )
                    await self._dispatch(messages)
                except NatsTimeoutError:
                    continue
                except Exception as exc:  # noqa: BLE001 — transport loop must retain messages for retry
                    logger.error("jetstream_watch_consumer_error", error=str(exc), exc_info=True)
                    await asyncio.sleep(settings.redis_retry_backoff_seconds)
        finally:
            await self.close()

    def stop(self) -> None:
        self._running = False

    async def close(self) -> None:
        """Drain then close NATS after the pull loop has stopped."""
        connection, self.connection = self.connection, None
        self.subscription = None
        self._transport_connected = False
        if connection is None:
            return
        try:
            await connection.drain()
        finally:
            await connection.close()
