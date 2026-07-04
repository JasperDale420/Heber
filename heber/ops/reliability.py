"""Reliability module for Heber per PRD §12.1-12.4.

Provides:
- Idempotency via event_id deduplication (§12.2)
- Dead-letter queue handling (§12.4)
- Retry with exponential backoff and jitter (§12.4)
"""

import hashlib
import math
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import structlog

from heber.ops.metrics import record_dedupe_saturated_pass, record_dedupe_store_error

logger = structlog.get_logger(__name__)


# Default Bloom filter constants (per PRD §12.2)
# 256M bits (32MB) holds ~25M event_ids per rotation window under 1% FPR —
# sized for the observed ~105M events/day stream with market-hour peaks.
BLOOM_FILTER_SIZE = 256_000_000
BLOOM_FILTER_HASHES = 7
BLOOM_ROTATION_SECONDS = 3600.0
# Above this estimated false-positive rate a bloom match is statistically
# meaningless, so dedupe fails open rather than dropping fresh events.
BLOOM_MAX_FP_RATE = 0.01


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

    @property
    def estimated_fp_rate(self) -> float:
        """Estimated false-positive rate given current insert count."""
        if self.count <= 0:
            return 0.0
        return (1.0 - math.exp(-self.num_hashes * self.count / self.size)) ** self.num_hashes


@dataclass
class DeduplicationResult:
    """Result of deduplication check."""

    is_duplicate: bool
    event_id: str
    reason: str | None = None


DEDUPE_KEY_PREFIX = "heber:dedupe:"


class RedisDedupeStore:
    """Exact-match dedupe backing store on Redis.

    The Bloom filter alone treats false positives as duplicates — at
    ingest volume that silently drops legitimate events (they are ACKed
    with status "dropped" and never reach Bronze). With this store, a
    Bloom hit becomes ``EXISTS`` on an exact key; Bloom-negative events
    never touch Redis, so the prefilter still absorbs the common case.

    Fails OPEN on Redis errors: an event is then treated as new and
    processed. A duplicate write is recoverable downstream; a dropped
    event is not.
    """

    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int,
        key_prefix: str = DEDUPE_KEY_PREFIX,
        client: Any = None,
    ) -> None:
        self._redis_url = redis_url
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix
        self._client = client
        self.error_count = 0

    def _get_client(self) -> Any:
        if self._client is None:
            import redis as redis_sync

            self._client = redis_sync.Redis.from_url(self._redis_url)
        return self._client

    def contains(self, event_id: str) -> bool:
        try:
            return bool(self._get_client().exists(self.key_prefix + event_id))
        except Exception as exc:  # noqa: BLE001 — fail open: a duplicate write beats a dropped event
            self.error_count += 1
            record_dedupe_store_error(operation="contains")
            logger.warning(
                "dedupe_store_check_failed_failing_open",
                event_id=event_id,
                error=str(exc),
                error_count=self.error_count,
            )
            return False

    def add(self, event_id: str) -> None:
        try:
            self._get_client().set(self.key_prefix + event_id, 1, ex=self.ttl_seconds, nx=True)
        except Exception as exc:  # noqa: BLE001 — registration is best-effort; the Bloom filter still has the id
            self.error_count += 1
            record_dedupe_store_error(operation="add")
            logger.warning(
                "dedupe_store_register_failed",
                event_id=event_id,
                error=str(exc),
                error_count=self.error_count,
            )


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
        max_fp_rate: float = BLOOM_MAX_FP_RATE,
    ):
        self.bloom = BloomFilter(size=bloom_size)
        self._previous_bloom: BloomFilter | None = None
        self.backing_store = backing_store  # Redis client or similar
        self.bloom_rotation_seconds = bloom_rotation_seconds
        self.max_fp_rate = max_fp_rate
        self._now_fn = now_fn or time.time
        self._last_rotation_epoch = self._now_fn()
        self._stats = {"checked": 0, "duplicates": 0, "rotations": 0, "saturated_passes": 0}

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
        matched_filter: BloomFilter | None = None
        if self.bloom.contains(event_id):
            matched_filter = self.bloom
        elif self._previous_bloom is not None and self._previous_bloom.contains(event_id):
            matched_filter = self._previous_bloom

        if matched_filter is not None:
            # Possible duplicate - verify with backing store if available
            if self.backing_store:
                if self._backing_contains(event_id):
                    self._stats["duplicates"] += 1
                    return DeduplicationResult(
                        is_duplicate=True,
                        event_id=event_id,
                        reason="backing_store_match",
                    )
            elif matched_filter.estimated_fp_rate > self.max_fp_rate:
                # The filter is saturated beyond its false-positive budget, so
                # a match is statistically meaningless. Trusting it drops fresh
                # events permanently (Bronze is the only durable copy) — fail
                # open and let the compactor's exact dedupe handle any real
                # duplicates that slip through.
                self._record_saturated_pass(matched_filter)
                return DeduplicationResult(
                    is_duplicate=False,
                    event_id=event_id,
                    reason="bloom_saturated_fail_open",
                )
            else:
                self._stats["duplicates"] += 1
                return DeduplicationResult(
                    is_duplicate=True,
                    event_id=event_id,
                    reason="bloom_filter_match",
                )

        return DeduplicationResult(is_duplicate=False, event_id=event_id)

    def _record_saturated_pass(self, matched_filter: BloomFilter) -> None:
        """Count and loudly (but boundedly) log a saturated fail-open pass."""
        self._stats["saturated_passes"] += 1
        record_dedupe_saturated_pass()
        occurrence = self._stats["saturated_passes"]
        if occurrence in {1, 10, 100, 1000} or occurrence % 100_000 == 0:
            logger.warning(
                "dedupe_bloom_saturated_fail_open",
                occurrence=occurrence,
                estimated_fp_rate=round(matched_filter.estimated_fp_rate, 4),
                max_fp_rate=self.max_fp_rate,
                bloom_inserts=matched_filter.count,
                bloom_size_bits=matched_filter.size,
                hint="bloom filter is undersized for stream volume; dedupe is degraded until rotation",
            )

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

    def _backing_contains(self, event_id: str) -> bool:
        """Check backing store for event_id."""
        if self.backing_store is None:
            return False
        return bool(self.backing_store.contains(event_id))

    def _backing_add(self, event_id: str) -> None:
        """Add event_id to backing store."""
        if self.backing_store is not None:
            self.backing_store.add(event_id)

    @property
    def stats(self) -> dict:
        return {
            **self._stats,
            "bloom_count": self.bloom.count,
            "previous_bloom_count": self._previous_bloom.count if self._previous_bloom else 0,
            "bloom_rotation_seconds": self.bloom_rotation_seconds,
            "last_rotation_epoch": self._last_rotation_epoch,
        }
