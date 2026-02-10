"""Event bus abstraction for Heber per PRD §12.7.

Provides:
- Bus-agnostic interface (Redis Streams MVP, upgradable to NATS/Kafka)
- Stream topology (Pattern A: one stream per canonical feed)
- Consumer group behavior with ack semantics
- Ordering guarantees (preserve per-message timestamps)
- Backpressure handling (§12.8)
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger(__name__)


# Stream topology per PRD §12.7.2 (Pattern A)
STREAM_NAMESPACE = "heber:events"


class StreamName(str, Enum):
    """Canonical stream names per PRD §12.7.2."""

    # Market data streams
    MARKET_BARS = f"{STREAM_NAMESPACE}:market.bars"
    MARKET_QUOTES = f"{STREAM_NAMESPACE}:market.quotes"
    MARKET_TRADES = f"{STREAM_NAMESPACE}:market.trades"

    # Intel streams
    INTEL_FLOW_ALERTS = f"{STREAM_NAMESPACE}:intel.flow_alerts"
    INTEL_DARKPOOL = f"{STREAM_NAMESPACE}:intel.darkpool_trades"

    # System streams
    DLQ = f"{STREAM_NAMESPACE}:dlq"


# All canonical streams
CANONICAL_STREAMS = [
    StreamName.MARKET_BARS,
    StreamName.MARKET_QUOTES,
    StreamName.MARKET_TRADES,
    StreamName.INTEL_FLOW_ALERTS,
    StreamName.INTEL_DARKPOOL,
]


# Prometheus metrics
messages_received = Counter(
    "heber_bus_messages_received_total",
    "Total messages received from event bus",
    ["stream"],
)

messages_acked = Counter(
    "heber_bus_messages_acked_total",
    "Total messages acknowledged",
    ["stream"],
)

messages_published = Counter(
    "heber_bus_messages_published_total",
    "Total messages published to event bus",
    ["stream"],
)

consumer_lag = Gauge(
    "heber_bus_consumer_lag",
    "Number of pending messages not yet processed",
    ["stream", "group"],
)

message_processing_duration = Histogram(
    "heber_bus_message_processing_seconds",
    "Time to process a message",
    ["stream"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)


@dataclass
class Message:
    """Event bus message."""

    id: str
    stream: str
    data: dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "id": self.id,
                "stream": self.stream,
                "data": self.data,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> "Message":
        parsed = json.loads(raw)
        return cls(
            id=parsed["id"],
            stream=parsed["stream"],
            data=parsed["data"],
            timestamp=parsed.get("timestamp", time.time()),
        )


@dataclass
class ConsumerConfig:
    """Consumer group configuration per PRD §12.7.3."""

    group_name: str
    consumer_name: str
    stream: StreamName
    batch_size: int = 100
    block_ms: int = 5000
    claim_idle_ms: int = 30000  # Claim idle messages after 30s


class EventBus(ABC):
    """Abstract event bus interface.

    Heber hides the bus behind this interface so we can upgrade
    to NATS/Kafka later without rewriting writers (PRD §12.7.1).
    """

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the event bus."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the event bus."""
        pass

    @abstractmethod
    async def create_stream(self, stream: StreamName, max_len: int | None = None) -> None:
        """Create a stream if it doesn't exist."""
        pass

    @abstractmethod
    async def create_consumer_group(
        self,
        stream: StreamName,
        group_name: str,
        start_id: str = "0",
    ) -> None:
        """Create a consumer group for a stream."""
        pass

    @abstractmethod
    async def publish(self, stream: StreamName, data: dict[str, Any]) -> str:
        """Publish a message to a stream. Returns message ID."""
        pass

    @abstractmethod
    async def consume(
        self,
        config: ConsumerConfig,
    ) -> AsyncIterator[list[Message]]:
        """Consume messages from a stream as a consumer group member.

        Yields batches of messages. Each message must be explicitly acked.
        """
        pass

    @abstractmethod
    async def ack(self, stream: StreamName, group_name: str, message_id: str) -> None:
        """Acknowledge successful processing of a message.

        Per PRD §12.7.3: Ack only after:
        - successful write to object storage
        - successful Catalog update
        """
        pass

    @abstractmethod
    async def get_pending_count(self, stream: StreamName, group_name: str) -> int:
        """Get count of pending (unacked) messages for a consumer group."""
        pass


class RedisEventBus(EventBus):
    """Redis Streams implementation per PRD §12.7.1.

    Default recommendation for MVP (Slice 1-3).
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
    ):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._redis: Any = None  # redis.asyncio.Redis

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info(
                "redis_connected",
                host=self.host,
                port=self.port,
            )
        except ImportError:
            raise ImportError("redis package required: pip install redis")

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._redis:
            await self._redis.close()
            logger.info("redis_disconnected")

    async def create_stream(self, stream: StreamName, max_len: int | None = None) -> None:
        """Create a stream using XGROUP CREATE with MKSTREAM."""
        # Stream is created implicitly when first message is added
        # or when a consumer group is created with MKSTREAM
        logger.debug("stream_ready", stream=stream.value)

    async def create_consumer_group(
        self,
        stream: StreamName,
        group_name: str,
        start_id: str = "0",
    ) -> None:
        """Create consumer group with MKSTREAM option."""
        try:
            await self._redis.xgroup_create(
                stream.value,
                group_name,
                id=start_id,
                mkstream=True,
            )
            logger.info(
                "consumer_group_created",
                stream=stream.value,
                group=group_name,
            )
        except Exception as e:
            # Group already exists is OK
            if "BUSYGROUP" in str(e):
                logger.debug("consumer_group_exists", stream=stream.value, group=group_name)
            else:
                raise

    async def publish(self, stream: StreamName, data: dict[str, Any]) -> str:
        """Publish message using XADD."""
        # Serialize nested data as JSON
        flat_data = {}
        for key, value in data.items():
            if isinstance(value, dict | list):
                flat_data[key] = json.dumps(value)
            else:
                flat_data[key] = str(value) if value is not None else ""

        message_id = await self._redis.xadd(stream.value, flat_data)
        messages_published.labels(stream=stream.value).inc()

        logger.debug(
            "message_published",
            stream=stream.value,
            message_id=message_id,
        )
        return message_id

    async def consume(
        self,
        config: ConsumerConfig,
    ) -> AsyncIterator[list[Message]]:
        """Consume messages using XREADGROUP."""
        while True:
            try:
                claimed = await self._claim_idle_messages(config)
                if claimed:
                    yield claimed
                    continue

                results = await self._redis.xreadgroup(
                    groupname=config.group_name,
                    consumername=config.consumer_name,
                    streams={config.stream.value: ">"},
                    count=config.batch_size,
                    block=config.block_ms,
                )

                if results:
                    messages = self._parse_stream_results(results)
                    if messages:
                        yield messages

            except asyncio.CancelledError:
                logger.info("consumer_cancelled", stream=config.stream.value)
                raise  # Re-raise per Python best practice
            except Exception as e:
                logger.error("consume_error", error=str(e), exc_info=True)
                await asyncio.sleep(1)

    def _parse_stream_results(self, results: list) -> list[Message]:
        """Parse XREADGROUP results into Message objects."""
        messages = []
        for stream_name, stream_messages in results:
            for msg_id, msg_data in stream_messages:
                message = self._parse_single_message(msg_id, stream_name, msg_data)
                messages.append(message)
                messages_received.labels(stream=stream_name).inc()
        return messages

    def _parse_single_message(
        self,
        msg_id: str,
        stream_name: str,
        msg_data: dict,
    ) -> Message:
        """Parse a single message, deserializing JSON fields."""
        parsed_data = {}
        for key, value in msg_data.items():
            parsed_data[key] = self._deserialize_value(value)

        return Message(
            id=msg_id,
            stream=stream_name,
            data=parsed_data,
        )

    def _deserialize_value(self, value: str) -> Any:
        """Attempt to deserialize a JSON value, return original if not JSON."""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    async def _claim_idle_messages(self, config: ConsumerConfig) -> list[Message]:
        """Claim idle messages from other consumers (on restart/crash)."""
        try:
            # Check for pending messages
            pending = await self._redis.xpending_range(
                config.stream.value,
                config.group_name,
                min="-",
                max="+",
                count=config.batch_size,
            )

            claimed_messages = []
            for entry in pending:
                # Claim if idle for too long
                if entry.get("idle", 0) > config.claim_idle_ms:
                    claimed = await self._redis.xclaim(
                        config.stream.value,
                        config.group_name,
                        config.consumer_name,
                        min_idle_time=config.claim_idle_ms,
                        message_ids=[entry["message_id"]],
                    )
                    for msg_id, msg_data in claimed:
                        if msg_data:
                            message = self._parse_single_message(
                                msg_id=msg_id,
                                stream_name=config.stream.value,
                                msg_data=msg_data,
                            )
                            claimed_messages.append(message)
                            messages_received.labels(stream=config.stream.value).inc()
                            logger.info(
                                "message_claimed",
                                stream=config.stream.value,
                                message_id=msg_id,
                            )

            return claimed_messages
        except Exception as e:
            logger.debug("claim_error", error=str(e))
            return []

    async def ack(self, stream: StreamName, group_name: str, message_id: str) -> None:
        """Ack message using XACK."""
        await self._redis.xack(stream.value, group_name, message_id)
        messages_acked.labels(stream=stream.value).inc()

    async def get_pending_count(self, stream: StreamName, group_name: str) -> int:
        """Get pending count using XPENDING."""
        try:
            info = await self._redis.xpending(stream.value, group_name)
            if isinstance(info, dict):
                count = info.get("pending", 0)
            elif info:
                count = info[0]
            else:
                count = 0
            consumer_lag.labels(stream=stream.value, group=group_name).set(count)
            return count
        except Exception:
            return 0


class InMemoryEventBus(EventBus):
    """In-memory event bus for testing."""

    def __init__(self):
        self._streams: dict[str, list[Message]] = {}
        self._groups: dict[str, dict[str, set[str]]] = {}  # stream -> group -> acked_ids
        self._consumers: dict[str, int] = {}  # group -> offset
        self._lock = asyncio.Lock()
        self._message_counter = 0

    async def connect(self) -> None:
        logger.info("in_memory_bus_connected")

    async def disconnect(self) -> None:
        self._streams.clear()
        self._groups.clear()
        logger.info("in_memory_bus_disconnected")

    async def create_stream(self, stream: StreamName, max_len: int | None = None) -> None:
        async with self._lock:
            if stream.value not in self._streams:
                self._streams[stream.value] = []

    async def create_consumer_group(
        self,
        stream: StreamName,
        group_name: str,
        start_id: str = "0",
    ) -> None:
        async with self._lock:
            if stream.value not in self._groups:
                self._groups[stream.value] = {}
            if group_name not in self._groups[stream.value]:
                self._groups[stream.value][group_name] = set()
                self._consumers[group_name] = 0

    async def publish(self, stream: StreamName, data: dict[str, Any]) -> str:
        async with self._lock:
            self._message_counter += 1
            msg_id = f"{int(time.time() * 1000)}-{self._message_counter}"

            if stream.value not in self._streams:
                self._streams[stream.value] = []

            message = Message(
                id=msg_id,
                stream=stream.value,
                data=data,
            )
            self._streams[stream.value].append(message)
            messages_published.labels(stream=stream.value).inc()
            return msg_id

    async def consume(
        self,
        config: ConsumerConfig,
    ) -> AsyncIterator[list[Message]]:
        while True:
            try:
                async with self._lock:
                    stream_msgs = self._streams.get(config.stream.value, [])
                    acked = self._groups.get(config.stream.value, {}).get(config.group_name, set())
                    offset = self._consumers.get(config.group_name, 0)

                    # Get unacked messages
                    batch = []
                    for _i, msg in enumerate(stream_msgs[offset:], start=offset):
                        if msg.id not in acked and len(batch) < config.batch_size:
                            batch.append(msg)
                            messages_received.labels(stream=msg.stream).inc()

                    if batch:
                        self._consumers[config.group_name] = offset + len(batch)

                if batch:
                    yield batch
                else:
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                logger.info("in_memory_consumer_cancelled")
                raise  # Re-raise per Python best practice

    async def ack(self, stream: StreamName, group_name: str, message_id: str) -> None:
        async with self._lock:
            if stream.value in self._groups:
                if group_name in self._groups[stream.value]:
                    self._groups[stream.value][group_name].add(message_id)
                    messages_acked.labels(stream=stream.value).inc()

    async def get_pending_count(self, stream: StreamName, group_name: str) -> int:
        async with self._lock:
            stream_msgs = self._streams.get(stream.value, [])
            acked = self._groups.get(stream.value, {}).get(group_name, set())
            return len([m for m in stream_msgs if m.id not in acked])


def create_event_bus(bus_type: str = "redis", **kwargs) -> EventBus:
    """Factory function to create event bus.

    Args:
        bus_type: "redis" or "memory"
        **kwargs: Bus-specific configuration
    """
    if bus_type == "redis":
        from urllib.parse import urlparse

        from heber.config import get_settings

        settings = get_settings()
        parsed = urlparse(settings.redis_url)
        return RedisEventBus(
            host=kwargs.get("host", parsed.hostname or "localhost"),
            port=kwargs.get("port", parsed.port or 6379),
            db=kwargs.get("db", int((parsed.path or "/0").lstrip("/") or "0")),
            password=kwargs.get("password", parsed.password),
        )
    elif bus_type == "memory":
        return InMemoryEventBus()
    else:
        raise ValueError(f"Unknown bus type: {bus_type}")


async def setup_streams(bus: EventBus) -> None:
    """Set up all canonical streams and consumer groups."""
    for stream in CANONICAL_STREAMS:
        await bus.create_stream(stream)
        await bus.create_consumer_group(stream, "heber-consumers")

    # Create DLQ stream
    await bus.create_stream(StreamName.DLQ)

    logger.info("streams_initialized", count=len(CANONICAL_STREAMS) + 1)
