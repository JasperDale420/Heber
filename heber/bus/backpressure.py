"""Backpressure, retries, and DLQ handling for Heber per PRD §12.8.

Provides:
- Backpressure monitoring and metrics
- Retry policy with exponential backoff + jitter
- Error classification (retryable vs non-retryable)
- Dead Letter Queue (DLQ) handling
- Quarantine storage for schema mismatches
"""

import json
import os
import random
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

from heber.bus import EventBus, StreamName

logger = structlog.get_logger(__name__)


# Prometheus metrics
backpressure_events = Counter(
    "heber_backpressure_events_total",
    "Total backpressure events detected",
    ["stream"],
)

retry_attempts = Counter(
    "heber_retry_attempts_total",
    "Total retry attempts",
    ["stream", "error_type"],
)

dlq_messages = Counter(
    "heber_dlq_messages_total",
    "Total messages sent to DLQ",
    ["stream", "error_type"],
)

quarantine_writes = Counter(
    "heber_quarantine_writes_total",
    "Total writes to quarantine storage",
    ["provider", "feed"],
)

consumer_lag_seconds = Gauge(
    "heber_consumer_lag_seconds",
    "Consumer lag in seconds",
    ["stream", "group"],
)

retry_backoff_seconds = Histogram(
    "heber_retry_backoff_seconds",
    "Retry backoff duration",
    ["stream"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30],
)


class ErrorType(str, Enum):
    """Error classification per PRD §12.8.2."""

    # Retryable errors
    TRANSIENT_STORAGE = "transient_storage"
    TRANSIENT_DB = "transient_db"
    TRANSIENT_NETWORK = "transient_network"
    TRANSIENT_TIMEOUT = "transient_timeout"

    # Non-retryable errors (DLQ / quarantine)
    SCHEMA_MISMATCH = "schema_mismatch"
    MALFORMED_ENVELOPE = "malformed_envelope"
    MISSING_TIMESTAMP = "missing_timestamp"
    VALIDATION_ERROR = "validation_error"
    UNKNOWN = "unknown"


# Retryable error types
RETRYABLE_ERRORS = {
    ErrorType.TRANSIENT_STORAGE,
    ErrorType.TRANSIENT_DB,
    ErrorType.TRANSIENT_NETWORK,
    ErrorType.TRANSIENT_TIMEOUT,
}

# Non-retryable error types (go to DLQ)
NON_RETRYABLE_ERRORS = {
    ErrorType.SCHEMA_MISMATCH,
    ErrorType.MALFORMED_ENVELOPE,
    ErrorType.MISSING_TIMESTAMP,
    ErrorType.VALIDATION_ERROR,
    ErrorType.UNKNOWN,
}


@dataclass
class RetryConfig:
    """Retry policy configuration per PRD §12.8.2."""

    max_retries: int = 10
    initial_delay_ms: int = 100
    max_delay_ms: int = 30000
    jitter_factor: float = 0.1


@dataclass
class DLQPayload:
    """DLQ message payload per PRD §12.8.3."""

    envelope: dict[str, Any]  # Original EventEnvelope
    error_type: str  # ErrorType value
    error_message: str  # Error description
    stack_trace: str  # Full stack trace
    first_seen_ts: float  # When first failure occurred
    retry_count: int  # Number of retry attempts
    stream: str  # Source stream
    message_id: str | None = None  # Original message ID

    def to_dict(self) -> dict[str, Any]:
        return {
            "envelope": self.envelope,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "first_seen_ts": self.first_seen_ts,
            "retry_count": self.retry_count,
            "stream": self.stream,
            "message_id": self.message_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DLQPayload":
        return cls(
            envelope=data["envelope"],
            error_type=data["error_type"],
            error_message=data["error_message"],
            stack_trace=data["stack_trace"],
            first_seen_ts=data["first_seen_ts"],
            retry_count=data["retry_count"],
            stream=data["stream"],
            message_id=data.get("message_id"),
        )


def classify_error(exception: Exception) -> ErrorType:
    """Classify an exception into an error type.

    Args:
        exception: The exception to classify

    Returns:
        ErrorType classification
    """
    error_str = str(exception).lower()
    exception_type = type(exception).__name__.lower()

    # Check for transient errors
    if any(keyword in error_str for keyword in ["timeout", "timed out"]):
        return ErrorType.TRANSIENT_TIMEOUT

    if any(keyword in error_str for keyword in ["connection", "network", "unreachable"]):
        return ErrorType.TRANSIENT_NETWORK

    if any(keyword in error_str for keyword in ["database", "postgres", "sql", "db"]):
        return ErrorType.TRANSIENT_DB

    if any(keyword in error_str for keyword in ["storage", "s3", "bucket", "write failed"]):
        return ErrorType.TRANSIENT_STORAGE

    # Check for non-retryable errors
    if any(keyword in error_str for keyword in ["schema", "type error", "field"]):
        return ErrorType.SCHEMA_MISMATCH

    if any(keyword in error_str for keyword in ["malformed", "invalid json", "parse"]):
        return ErrorType.MALFORMED_ENVELOPE

    if any(keyword in error_str for keyword in ["timestamp", "ts_", "missing required"]):
        return ErrorType.MISSING_TIMESTAMP

    if "validation" in exception_type or "validation" in error_str:
        return ErrorType.VALIDATION_ERROR

    return ErrorType.UNKNOWN


def is_retryable(error_type: ErrorType) -> bool:
    """Check if an error type is retryable."""
    return error_type in RETRYABLE_ERRORS


def calculate_backoff(
    attempt: int,
    config: RetryConfig,
) -> float:
    """Calculate backoff delay with exponential growth and jitter.

    Per PRD §12.8.2: 100ms → 30s with jitter

    Args:
        attempt: Current attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay_ms = config.initial_delay_ms * (2**attempt)

    # Cap at max delay
    delay_ms = min(delay_ms, config.max_delay_ms)

    # Add jitter (±jitter_factor * delay)
    jitter = delay_ms * config.jitter_factor * (2 * random.random() - 1)
    delay_ms = delay_ms + jitter

    return max(delay_ms / 1000.0, 0.001)  # Convert to seconds


class DLQHandler:
    """Dead Letter Queue handler per PRD §12.8.3."""

    def __init__(
        self,
        bus: EventBus,
        quarantine_base_path: str = "quarantine",
    ):
        self.bus = bus
        self.quarantine_base_path = Path(quarantine_base_path)

    @staticmethod
    def _extract_partition_value(envelope: dict[str, Any], field: str) -> str:
        """Resolve partition value from canonical envelope keys with legacy fallback."""
        value = envelope.get(field)
        if value is not None and str(value).strip():
            return str(value)

        legacy_meta = envelope.get("meta")
        if isinstance(legacy_meta, dict):
            legacy_value = legacy_meta.get(field)
            if legacy_value is not None and str(legacy_value).strip():
                return str(legacy_value)

        return "unknown"

    async def send_to_dlq(
        self,
        envelope: dict[str, Any],
        error: Exception,
        stream: str,
        message_id: str | None = None,
        retry_count: int = 0,
    ) -> str:
        """Send a failed message to the DLQ.

        Args:
            envelope: Original EventEnvelope data
            error: The exception that caused the failure
            stream: Source stream name
            message_id: Original message ID (if available)
            retry_count: Number of retry attempts

        Returns:
            DLQ message ID
        """
        error_type = classify_error(error)

        payload = DLQPayload(
            envelope=envelope,
            error_type=error_type.value,
            error_message=str(error),
            stack_trace=traceback.format_exc(),
            first_seen_ts=time.time(),
            retry_count=retry_count,
            stream=stream,
            message_id=message_id,
        )

        dlq_message_id = await self.bus.publish(
            StreamName.DLQ,
            payload.to_dict(),
        )

        dlq_messages.labels(stream=stream, error_type=error_type.value).inc()

        logger.warning(
            "message_sent_to_dlq",
            dlq_message_id=dlq_message_id,
            error_type=error_type.value,
            error=str(error),
            retry_count=retry_count,
            source_stream=stream,
        )

        return dlq_message_id

    def write_to_quarantine(
        self,
        envelope: dict[str, Any],
        error: Exception,
    ) -> Path:
        """Write a failed record to quarantine storage.

        Per PRD §12.8.4: quarantine/provider=.../feed=.../dt=.../

        Args:
            envelope: Original EventEnvelope data
            error: The exception that caused the failure

        Returns:
            Path to quarantine file
        """
        # Extract partition info from envelope
        provider = self._extract_partition_value(envelope, "provider")
        feed = self._extract_partition_value(envelope, "feed")
        dt = datetime.now(UTC).strftime("%Y-%m-%d")
        event_id = envelope.get("event_id", f"unknown_{int(time.time() * 1000)}")

        # Build quarantine path
        quarantine_path = self.quarantine_base_path / f"provider={provider}" / f"feed={feed}" / f"dt={dt}"
        quarantine_path.mkdir(parents=True, exist_ok=True)

        # Write quarantine file
        error_type = classify_error(error)
        quarantine_record = {
            "envelope": envelope,
            "error_type": error_type.value,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
            "quarantined_at": datetime.now(UTC).isoformat(),
        }

        file_path = quarantine_path / f"{event_id}.json"
        with open(file_path, "w") as f:
            json.dump(quarantine_record, f, indent=2, default=str)

        quarantine_writes.labels(provider=provider, feed=feed).inc()

        logger.warning(
            "record_quarantined",
            path=str(file_path),
            error_type=error_type.value,
            event_id=event_id,
        )

        return file_path


class RetryExecutor:
    """Execute operations with retry policy per PRD §12.8.2."""

    def __init__(
        self,
        dlq_handler: DLQHandler,
        config: RetryConfig | None = None,
    ):
        self.dlq_handler = dlq_handler
        self.config = config or RetryConfig()

    async def execute_with_retry(
        self,
        operation: Callable,
        envelope: dict[str, Any],
        stream: str,
        message_id: str | None = None,
        *args,
        **kwargs,
    ) -> Any:
        """Execute an operation with retry policy.

        Args:
            operation: Async callable to execute
            envelope: EventEnvelope for DLQ if all retries fail
            stream: Source stream name
            message_id: Original message ID
            *args, **kwargs: Passed to operation

        Returns:
            Operation result if successful

        Raises:
            Exception if non-retryable or all retries exhausted
        """
        last_error: Exception | None = None

        for attempt in range(self.config.max_retries + 1):
            try:
                return await operation(*args, **kwargs)

            except Exception as e:
                last_error = e
                error_type = classify_error(e)

                retry_attempts.labels(
                    stream=stream,
                    error_type=error_type.value,
                ).inc()

                # Non-retryable: send to DLQ immediately
                if not is_retryable(error_type):
                    logger.warning(
                        "non_retryable_error",
                        error_type=error_type.value,
                        error=str(e),
                    )
                    await self.dlq_handler.send_to_dlq(
                        envelope=envelope,
                        error=e,
                        stream=stream,
                        message_id=message_id,
                        retry_count=attempt,
                    )
                    raise

                # Last attempt: send to DLQ
                if attempt >= self.config.max_retries:
                    logger.error(
                        "max_retries_exceeded",
                        attempts=attempt + 1,
                        error=str(e),
                    )
                    await self.dlq_handler.send_to_dlq(
                        envelope=envelope,
                        error=e,
                        stream=stream,
                        message_id=message_id,
                        retry_count=attempt + 1,
                    )
                    raise

                # Calculate backoff and wait
                backoff = calculate_backoff(attempt, self.config)
                retry_backoff_seconds.labels(stream=stream).observe(backoff)

                logger.info(
                    "retrying_operation",
                    attempt=attempt + 1,
                    max_retries=self.config.max_retries,
                    backoff_seconds=backoff,
                    error_type=error_type.value,
                )

                import asyncio

                await asyncio.sleep(backoff)

        # Should never reach here
        raise last_error or RuntimeError("Unexpected retry state")


class BackpressureMonitor:
    """Monitor and respond to backpressure conditions per PRD §12.8.1."""

    def __init__(
        self,
        bus: EventBus,
        lag_threshold_seconds: float = 60.0,
        check_interval_seconds: float = 10.0,
    ):
        self.bus = bus
        self.lag_threshold = lag_threshold_seconds
        self.check_interval = check_interval_seconds
        self._running = False

    async def check_lag(self, stream: StreamName, group: str) -> float:
        """Check consumer lag for a stream.

        Returns lag in seconds (estimated).
        """
        pending = await self.bus.get_pending_count(stream, group)

        # Estimate lag based on pending count
        # Assume ~1000 messages/second processing rate
        estimated_lag = pending / 1000.0

        consumer_lag_seconds.labels(stream=stream.value, group=group).set(estimated_lag)

        if estimated_lag > self.lag_threshold:
            backpressure_events.labels(stream=stream.value).inc()
            logger.warning(
                "backpressure_detected",
                stream=stream.value,
                group=group,
                lag_seconds=estimated_lag,
                pending_messages=pending,
            )

        return estimated_lag

    async def monitor_loop(self, streams: list[tuple[StreamName, str]]) -> None:
        """Continuously monitor streams for backpressure.

        Args:
            streams: List of (stream, group) tuples to monitor
        """
        import asyncio

        self._running = True

        while self._running:
            for stream, group in streams:
                try:
                    await self.check_lag(stream, group)
                except Exception as e:
                    logger.error("lag_check_failed", stream=stream.value, error=str(e))

            await asyncio.sleep(self.check_interval)

    def stop(self) -> None:
        """Stop the monitoring loop."""
        self._running = False


# Factory functions


def create_dlq_handler(
    bus: EventBus,
    quarantine_path: str | None = None,
) -> DLQHandler:
    """Create a DLQ handler.

    Args:
        bus: Event bus instance
        quarantine_path: Base path for quarantine storage
    """
    path = quarantine_path or os.environ.get("HEBER_QUARANTINE_PATH", "quarantine")
    return DLQHandler(bus, path)


def create_retry_executor(
    dlq_handler: DLQHandler,
    max_retries: int | None = None,
) -> RetryExecutor:
    """Create a retry executor.

    Args:
        dlq_handler: DLQ handler instance
        max_retries: Override default max retries
    """
    config = RetryConfig()
    if max_retries is not None:
        config.max_retries = max_retries
    return RetryExecutor(dlq_handler, config)
