"""Reliability module for Heber per PRD §12.1-12.4.

Provides:
- Idempotency via event_id deduplication (§12.2)
- Dead-letter queue handling (§12.4)
- Retry with exponential backoff and jitter (§12.4)
"""

import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
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

    def check(self, event_id: str) -> DeduplicationResult:
        """Check if event is duplicate without registering a new success."""
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

        return DeduplicationResult(is_duplicate=False, event_id=event_id)

    def register(self, event_id: str) -> None:
        """Register an event_id after successful processing."""
        self._rotate_if_needed()
        self.bloom.add(event_id)
        if self.backing_store:
            self._backing_add(event_id)

    def check_and_register(self, event_id: str) -> DeduplicationResult:
        """Check if event is duplicate, register if new."""
        result = self.check(event_id)
        if not result.is_duplicate:
            self.register(event_id)
        return result

    def _backing_contains(self, _event_id: str) -> bool:
        """Check backing store for event_id."""
        # Override in subclass or use Redis client
        return False

    def _backing_add(self, _event_id: str) -> None:
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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DLQEvent":
        return cls(
            event_id=data["event_id"],
            original_payload=data["original_payload"],
            error_type=data["error_type"],
            error_message=data["error_message"],
            feed=data["feed"],
            provider=data["provider"],
            attempts=int(data.get("attempts", 1)),
            first_failed_at=datetime.fromisoformat(data["first_failed_at"]),
            last_failed_at=datetime.fromisoformat(data["last_failed_at"]),
        )


class DeadLetterQueue:
    """Dead Letter Queue for failed events (PRD §12.4).

    Events that fail processing are sent here for:
    - Manual inspection
    - Delayed retry
    - Alert generation
    """

    def __init__(self, max_size: int = 10000, persist_path: str | Path | None = None):
        self._queue: list[DLQEvent] = []
        self.max_size = max_size
        self.persist_path = Path(persist_path) if persist_path else None
        self._stats = {"added": 0, "reprocessed": 0, "dropped": 0}
        self._load()

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
                self._persist()
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
        self._persist()

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
            self._persist()
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

    def _persist(self) -> None:
        if not self.persist_path:
            return
        self.persist_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "events": [event.to_dict() for event in self._queue],
            "stats": self._stats,
        }
        temp_path = self.persist_path.with_suffix(self.persist_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        temp_path.replace(self.persist_path)

    def _load(self) -> None:
        if not self.persist_path or not self.persist_path.exists():
            return
        try:
            data = json.loads(self.persist_path.read_text(encoding="utf-8"))
            events = data.get("events", [])
            self._queue = [DLQEvent.from_dict(event) for event in events]
            loaded_stats = data.get("stats", {})
            self._stats["added"] = int(loaded_stats.get("added", self._stats["added"]))
            self._stats["reprocessed"] = int(loaded_stats.get("reprocessed", self._stats["reprocessed"]))
            self._stats["dropped"] = int(loaded_stats.get("dropped", self._stats["dropped"]))
        except Exception as exc:
            logger.error(
                "dlq_persist_load_failed",
                path=str(self.persist_path),
                error=str(exc),
                exc_info=True,
            )
            self._queue = []
