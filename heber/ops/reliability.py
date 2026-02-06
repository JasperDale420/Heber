"""Reliability module for Heber per PRD §12.1-12.4.

Provides:
- Idempotency via event_id deduplication (§12.2)
- Dead-letter queue handling (§12.4)
- Retry with exponential backoff and jitter (§12.4)
"""

import hashlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# Default Bloom filter constants (per PRD §12.2)
BLOOM_FILTER_SIZE = 10_000_000  # 10M bits default
BLOOM_FILTER_HASHES = 7
BLOOM_ROTATION_SECONDS = 3600.0


class BloomFilter:
    """Simple Bloom filter for event_id deduplication (PRD §12.2).

    Used to quickly check if an event_id has been seen recently.
    False positives are possible but false negatives are not.
    """

    def __init__(self, size: int = BLOOM_FILTER_SIZE, num_hashes: int = BLOOM_FILTER_HASHES):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = bytearray((size + 7) // 8)
        self.count = 0

    def _hashes(self, event_id: str) -> list[int]:
        """Generate hash positions for an event_id."""
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.sha256(f"{event_id}:{i}".encode()).hexdigest()
            positions.append(int(h, 16) % self.size)
        return positions

    def add(self, event_id: str) -> None:
        """Add an event_id to the filter."""
        for pos in self._hashes(event_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bit_array[byte_idx] |= 1 << bit_idx
        self.count += 1

    def contains(self, event_id: str) -> bool:
        """Check if an event_id might be in the filter."""
        for pos in self._hashes(event_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True

    def add_if_new(self, event_id: str) -> bool:
        """Add event_id if not seen before. Returns True if new."""
        if self.contains(event_id):
            return False
        self.add(event_id)
        return True


@dataclass
class DeduplicationResult:
    """Result of deduplication check."""

    is_duplicate: bool
    event_id: str
    reason: str | None = None


class EventDeduplicator:
    """Event deduplication using Bloom filter + optional backing store (PRD §12.2).

    Two-tier approach:
    1. Bloom filter for fast in-memory checks
    2. Optional backing store (Redis/DB) for persistence across restarts
    """

    def __init__(
        self,
        bloom_size: int = BLOOM_FILTER_SIZE,
        backing_store: Any = None,
        bloom_rotation_seconds: float = BLOOM_ROTATION_SECONDS,
        now_fn: Callable[[], float] | None = None,
    ):
        self.bloom = BloomFilter(size=bloom_size)
        self._previous_bloom: BloomFilter | None = None
        self.backing_store = backing_store  # Redis client or similar
        self.bloom_rotation_seconds = bloom_rotation_seconds
        self._now_fn = now_fn or time.time
        self._last_rotation_epoch = self._now_fn()
        self._stats = {"checked": 0, "duplicates": 0, "rotations": 0}

    def _rotate_if_needed(self) -> None:
        """Rotate Bloom filters to keep false-positive risk bounded over time."""
        if self.bloom_rotation_seconds <= 0:
            return

        now = self._now_fn()
        elapsed = now - self._last_rotation_epoch
        if elapsed < self.bloom_rotation_seconds:
            return

        windows_elapsed = int(elapsed // self.bloom_rotation_seconds)
        self._previous_bloom = self.bloom if windows_elapsed == 1 else None
        self.bloom = BloomFilter(size=self.bloom.size, num_hashes=self.bloom.num_hashes)
        self._last_rotation_epoch = now
        self._stats["rotations"] += 1

        logger.info(
            "dedupe_bloom_rotated",
            rotations=self._stats["rotations"],
            windows_elapsed=windows_elapsed,
            rotation_seconds=self.bloom_rotation_seconds,
        )

    def check_and_register(self, event_id: str) -> DeduplicationResult:
        """Check if event is duplicate, register if new.

        Returns:
            DeduplicationResult with is_duplicate flag
        """
        self._stats["checked"] += 1
        self._rotate_if_needed()

        # Fast path: Bloom filter check
        bloom_match = self.bloom.contains(event_id)
        if not bloom_match and self._previous_bloom is not None:
            bloom_match = self._previous_bloom.contains(event_id)

        if bloom_match:
            # Possible duplicate - verify with backing store if available
            if self.backing_store:
                if self._backing_contains(event_id):
                    self._stats["duplicates"] += 1
                    return DeduplicationResult(
                        is_duplicate=True,
                        event_id=event_id,
                        reason="backing_store_match",
                    )
            else:
                # No backing store, treat as duplicate (conservative)
                self._stats["duplicates"] += 1
                return DeduplicationResult(
                    is_duplicate=True,
                    event_id=event_id,
                    reason="bloom_filter_match",
                )

        # Not a duplicate - register it
        self.bloom.add(event_id)
        if self.backing_store:
            self._backing_add(event_id)

        return DeduplicationResult(is_duplicate=False, event_id=event_id)

    def _backing_contains(self, event_id: str) -> bool:
        """Check backing store for event_id."""
        # Override in subclass or use Redis client
        return False

    def _backing_add(self, event_id: str) -> None:
        """Add event_id to backing store."""
        # Override in subclass or use Redis client
        pass

    @property
    def stats(self) -> dict:
        return {
            **self._stats,
            "bloom_count": self.bloom.count,
            "previous_bloom_count": self._previous_bloom.count if self._previous_bloom else 0,
            "bloom_rotation_seconds": self.bloom_rotation_seconds,
            "last_rotation_epoch": self._last_rotation_epoch,
        }


@dataclass
class DLQEvent:
    """Event in the Dead Letter Queue (PRD §12.4)."""

    event_id: str
    original_payload: dict
    error_type: str
    error_message: str
    feed: str
    provider: str
    attempts: int = 1
    first_failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "original_payload": self.original_payload,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "feed": self.feed,
            "provider": self.provider,
            "attempts": self.attempts,
            "first_failed_at": self.first_failed_at.isoformat(),
            "last_failed_at": self.last_failed_at.isoformat(),
        }


class DeadLetterQueue:
    """Dead Letter Queue for failed events (PRD §12.4).

    Events that fail processing are sent here for:
    - Manual inspection
    - Delayed retry
    - Alert generation
    """

    def __init__(self, max_size: int = 10000):
        self._queue: list[DLQEvent] = []
        self.max_size = max_size
        self._stats = {"added": 0, "reprocessed": 0, "dropped": 0}

    def add(
        self,
        event_id: str,
        payload: dict,
        error: Exception,
        feed: str,
        provider: str,
    ) -> None:
        """Add a failed event to the DLQ."""
        # Check if already in queue
        for existing in self._queue:
            if existing.event_id == event_id:
                existing.attempts += 1
                existing.last_failed_at = datetime.now(UTC)
                existing.error_message = str(error)
                logger.warning(
                    "dlq_retry_failed",
                    event_id=event_id,
                    attempts=existing.attempts,
                )
                return

        # Add new entry
        if len(self._queue) >= self.max_size:
            dropped = self._queue.pop(0)
            self._stats["dropped"] += 1
            logger.error("dlq_overflow", dropped_event_id=dropped.event_id)

        self._queue.append(
            DLQEvent(
                event_id=event_id,
                original_payload=payload,
                error_type=type(error).__name__,
                error_message=str(error),
                feed=feed,
                provider=provider,
            )
        )
        self._stats["added"] += 1

        logger.warning(
            "dlq_event_added",
            event_id=event_id,
            error_type=type(error).__name__,
            feed=feed,
            provider=provider,
        )

    def pop(self) -> DLQEvent | None:
        """Remove and return the oldest event from the queue."""
        if self._queue:
            event = self._queue.pop(0)
            self._stats["reprocessed"] += 1
            return event
        return None

    def peek(self, n: int = 10) -> list[DLQEvent]:
        """View the next n events without removing them."""
        return self._queue[:n]

    def __len__(self) -> int:
        return len(self._queue)

    @property
    def stats(self) -> dict:
        return {**self._stats, "current_size": len(self._queue)}


def retry_with_backoff(
    fn: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.5,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> Any:
    """Execute function with exponential backoff and jitter (PRD §12.4).

    Args:
        fn: Function to execute
        max_retries: Maximum retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        jitter: Random jitter factor (0-1)
        on_retry: Callback on each retry with (attempt, exception)

    Returns:
        Result of fn()

    Raises:
        Last exception if all retries exhausted
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last_exception = e

            if attempt == max_retries:
                logger.error(
                    "retry_exhausted",
                    attempts=attempt + 1,
                    error=str(e),
                )
                raise

            # Calculate backoff with jitter
            delay = min(base_delay * (2**attempt), max_delay)
            jitter_amount = delay * jitter * random.random()
            actual_delay = delay + jitter_amount

            logger.warning(
                "retry_attempt",
                attempt=attempt + 1,
                max_retries=max_retries,
                delay_seconds=actual_delay,
                error=str(e),
            )

            if on_retry:
                on_retry(attempt + 1, e)

            time.sleep(actual_delay)

    raise last_exception


async def retry_with_backoff_async(
    fn: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.5,
) -> Any:
    """Async version of retry_with_backoff."""
    import asyncio

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except Exception as e:
            last_exception = e

            if attempt == max_retries:
                raise

            delay = min(base_delay * (2**attempt), max_delay)
            jitter_amount = delay * jitter * random.random()

            await asyncio.sleep(delay + jitter_amount)

    raise last_exception
