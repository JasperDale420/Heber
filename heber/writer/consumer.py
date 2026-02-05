"""Redis Streams consumer for incoming events.

Subscribes to the event stream from Data Gateway and routes to Bronze/Silver writers.
"""

import asyncio
import json
import signal
from datetime import UTC, datetime

import redis.asyncio as redis
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.writer.bronze import BronzeWriter
from heber.writer.silver import SilverWriter

logger = structlog.get_logger(__name__)


class EventConsumer:
    """Consumes events from Redis Streams and writes to Lake layers."""

    def __init__(self):
        self.redis: redis.Redis | None = None
        self.bronze_writer = BronzeWriter()
        self.silver_writer = SilverWriter()
        self.running = False
        self.consumer_name = f"consumer-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
        self._payload_required: dict[str, set[str]] = {
            "flow_alerts": {
                "timestamp",
                "symbol",
                "strike",
                "expiry",
                "put_call",
                "premium",
                "volume",
            },
            "market_tide": {
                "timestamp",
                "date",
                "net_call_premium",
                "net_put_premium",
                "net_volume",
                "sentiment",
            },
        }
        self._payload_allowed: dict[str, set[str]] = {
            "flow_alerts": {
                "timestamp",
                "symbol",
                "strike",
                "expiry",
                "put_call",
                "premium",
                "volume",
                "open_interest",
                "side",
                "is_sweep",
                "is_unusual",
                "sentiment",
                "option_chain",
                "price",
                "underlying_price",
                "alert_rule",
                "alert_type",
                "aggressor",
                "tags",
                "provider",
            },
            "market_tide": {
                "timestamp",
                "date",
                "net_call_premium",
                "net_put_premium",
                "net_volume",
                "sentiment",
                "provider",
            },
        }

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

    async def process_event(self, event_data: dict) -> bool:
        """Process a single event through Bronze and Silver layers.

        Returns True if successful, False otherwise.
        """
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

            # Set ts_available if not present
            if envelope.ts_available is None:
                envelope = envelope.with_ts_available(datetime.now(UTC))

            self._validate_payload_schema(envelope)

            # Write to Bronze (always)
            await self.bronze_writer.write(envelope)

            # Write to Silver (normalized)
            await self.silver_writer.write(envelope)

            logger.debug(
                "Processed event",
                event_id=envelope.event_id,
                feed=envelope.feed,
                instrument_key=envelope.instrument_key,
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to process event",
                error=str(e),
                event_data=str(event_data)[:200],
                exc_info=True,
            )
            return False

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

    async def run(self):
        """Main consumer loop."""
        await self.connect()
        self.running = True

        logger.info(
            "Starting consumer",
            stream=settings.redis_stream_name,
            group=settings.redis_consumer_group,
            consumer=self.consumer_name,
        )

        while self.running:
            try:
                await self._consume_iteration()
            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise  # Re-raise per best practice
            except Exception as e:
                logger.error("Consumer error", error=str(e), exc_info=True)
                await asyncio.sleep(1)  # Back off on error

        await self._final_flush()

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
            count=100,
            block=1000,
        )
        # Always check for flush even with no messages (handles idle periods)
        await self.bronze_writer.flush_if_needed()
        await self.silver_writer.flush_if_needed()

        if not messages:
            return

        # Collect successfully processed message IDs
        processed_ids: list[bytes] = []
        failed_ids: list[bytes] = []

        for _stream_name, stream_messages in messages:
            for message_id, message_data in stream_messages:
                success = await self.process_event(message_data)
                if success:
                    processed_ids.append(message_id)
                else:
                    failed_ids.append(message_id)
                    logger.warning("Event failed, needs DLQ", message_id=message_id)

        # Flush to disk BEFORE acknowledging
        await self.bronze_writer.flush_if_needed()
        await self.silver_writer.flush_if_needed()

        # Only ACK after successful flush
        if processed_ids:
            await self.redis.xack(
                settings.redis_stream_name,
                settings.redis_consumer_group,
                *processed_ids,
            )
            logger.debug("Acknowledged batch", count=len(processed_ids))

    async def _final_flush(self) -> None:
        """Perform final flush when consumer stops."""
        await self.bronze_writer.flush()
        await self.silver_writer.flush()
        logger.info("Consumer stopped")

    async def stop(self):
        """Stop the consumer."""
        self.running = False
        if self.redis:
            await self.redis.close()


async def main():
    """Entry point for the consumer."""
    consumer = EventConsumer()

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
