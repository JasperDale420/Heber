"""Deduplication strategy for Heber per PRD §12.11.

Provides three-layer deduplication:
1. Consumer: In-memory bloom filter (fast approximate)
2. Writer: Append-only per batch
3. Compactor: Exact dedupe on merge

This module builds on the BloomFilter from ops/reliability.py with
additional features for rotating filters and compaction dedupe.
"""

import math
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

import structlog
from prometheus_client import Counter, Gauge

logger = structlog.get_logger(__name__)


# Prometheus metrics
dedupe_bloom_hits = Counter(
    "heber_dedupe_bloom_hits_total",
    "Events dropped by bloom filter (probably duplicate)",
    ["stream"],
)

dedupe_bloom_misses = Counter(
    "heber_dedupe_bloom_misses_total",
    "Events passed by bloom filter (definitely not duplicate)",
    ["stream"],
)

dedupe_bloom_fp_estimate = Gauge(
    "heber_dedupe_bloom_fp_rate",
    "Estimated false positive rate of current bloom filter",
    ["stream"],
)

dedupe_compaction_removed = Counter(
    "heber_dedupe_compaction_removed_total",
    "Duplicate events removed during compaction",
    ["partition"],
)

bloom_filter_rotations = Counter(
    "heber_bloom_filter_rotations_total",
    "Number of bloom filter rotations",
    ["stream"],
)

bloom_filter_size_bytes = Gauge(
    "heber_bloom_filter_size_bytes",
    "Current bloom filter size in bytes",
    ["stream"],
)


@dataclass
class BloomFilterConfig:
    """Bloom filter configuration per PRD §12.11.2."""
    expected_items: int = 10_000_000   # 10M per filter
    false_positive_rate: float = 0.01  # 1%
    rotation_interval_seconds: int = 3600  # Rotate every hour
    retention_hours: int = 1  # Keep old filter for 1 additional hour


class BloomFilter:
    """High-performance bloom filter for event deduplication.
    
    Per PRD §12.11.2:
    - Expected items: 10M
    - False positive rate: 1%
    - Memory: ~12MB per filter
    """
    
    def __init__(
        self,
        expected_items: int = 10_000_000,
        false_positive_rate: float = 0.01,
    ):
        self.expected_items = expected_items
        self.target_fp_rate = false_positive_rate
        
        # Calculate optimal size and hash count
        # m = -n * ln(p) / (ln(2)^2)
        # k = m/n * ln(2)
        self.size_bits = self._optimal_size(expected_items, false_positive_rate)
        self.num_hashes = self._optimal_hash_count(self.size_bits, expected_items)
        
        # Initialize bit array
        self._bit_array = bytearray(self.size_bits // 8 + 1)
        self._item_count = 0
        self._created_at = time.time()
    
    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal filter size in bits."""
        m = -n * math.log(p) / (math.log(2) ** 2)
        return int(m)
    
    @staticmethod
    def _optimal_hash_count(m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * math.log(2)
        return max(1, int(k))
    
    def _hash_positions(self, item: str) -> list[int]:
        """Calculate hash positions for an item using double hashing."""
        # Use Python's built-in hash and a simple rehash
        h1 = hash(item) % self.size_bits
        h2 = hash(item + "_salt") % self.size_bits
        
        positions = []
        for i in range(self.num_hashes):
            pos = (h1 + i * h2) % self.size_bits
            positions.append(pos)
        return positions
    
    def add(self, item: str) -> None:
        """Add an item to the filter."""
        for pos in self._hash_positions(item):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self._bit_array[byte_idx] |= (1 << bit_idx)
        self._item_count += 1
    
    def probably_contains(self, item: str) -> bool:
        """Check if item is probably in the filter.
        
        Returns:
            True if item is probably in the filter (may have false positives)
            False if item is definitely NOT in the filter
        """
        for pos in self._hash_positions(item):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self._bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True
    
    @property
    def item_count(self) -> int:
        """Number of items added to the filter."""
        return self._item_count
    
    @property
    def size_bytes(self) -> int:
        """Memory usage in bytes."""
        return len(self._bit_array)
    
    @property
    def estimated_fp_rate(self) -> float:
        """Estimate current false positive rate based on fill level."""
        if self._item_count == 0:
            return 0.0
        # FP rate = (1 - e^(-kn/m))^k
        exponent = -self.num_hashes * self._item_count / self.size_bits
        return (1 - math.exp(exponent)) ** self.num_hashes
    
    @property
    def age_seconds(self) -> float:
        """Age of the filter in seconds."""
        return time.time() - self._created_at


class RotatingBloomFilter:
    """Bloom filter with automatic hourly rotation per PRD §12.11.2.
    
    - New filter every hour
    - Old filter retained for 1 additional hour
    - Checks both current and previous filter
    """
    
    def __init__(
        self,
        stream_name: str,
        config: BloomFilterConfig | None = None,
    ):
        self.stream_name = stream_name
        self.config = config or BloomFilterConfig()
        
        # Current and previous filters
        self._current_filter = self._create_filter()
        self._previous_filter: BloomFilter | None = None
        self._last_rotation = time.time()
        
        self._update_metrics()
    
    def _create_filter(self) -> BloomFilter:
        """Create a new bloom filter with configured settings."""
        return BloomFilter(
            expected_items=self.config.expected_items,
            false_positive_rate=self.config.false_positive_rate,
        )
    
    def _should_rotate(self) -> bool:
        """Check if it's time to rotate filters."""
        elapsed = time.time() - self._last_rotation
        return elapsed >= self.config.rotation_interval_seconds
    
    def _rotate(self) -> None:
        """Rotate to a new filter, keeping the old one."""
        self._previous_filter = self._current_filter
        self._current_filter = self._create_filter()
        self._last_rotation = time.time()
        
        bloom_filter_rotations.labels(stream=self.stream_name).inc()
        logger.info(
            "bloom_filter_rotated",
            stream=self.stream_name,
            previous_items=self._previous_filter.item_count if self._previous_filter else 0,
        )
        
        self._update_metrics()
    
    def _update_metrics(self) -> None:
        """Update Prometheus metrics."""
        bloom_filter_size_bytes.labels(stream=self.stream_name).set(
            self._current_filter.size_bytes
        )
        dedupe_bloom_fp_estimate.labels(stream=self.stream_name).set(
            self._current_filter.estimated_fp_rate
        )
    
    def check_and_add(self, event_id: str) -> bool:
        """Check if event_id is probably a duplicate, and add if not.
        
        Per PRD §12.11.2:
        - If event_id is probably in the filter → return True (drop)
        - If event_id is definitely not in the filter → add and return False (process)
        
        Returns:
            True if event should be DROPPED (probably duplicate)
            False if event should be PROCESSED (definitely new)
        """
        # Check if we need to rotate
        if self._should_rotate():
            self._rotate()
        
        # Check current filter
        if self._current_filter.probably_contains(event_id):
            dedupe_bloom_hits.labels(stream=self.stream_name).inc()
            return True  # Probably duplicate, drop
        
        # Check previous filter (if exists)
        if self._previous_filter and self._previous_filter.probably_contains(event_id):
            dedupe_bloom_hits.labels(stream=self.stream_name).inc()
            return True  # Probably duplicate, drop
        
        # Definitely new, add to current filter
        self._current_filter.add(event_id)
        dedupe_bloom_misses.labels(stream=self.stream_name).inc()
        self._update_metrics()
        
        return False  # Process this event
    
    def force_rotate(self) -> None:
        """Force an immediate rotation."""
        self._rotate()
    
    @property
    def stats(self) -> dict[str, Any]:
        """Get filter statistics."""
        return {
            "stream": self.stream_name,
            "current_items": self._current_filter.item_count,
            "current_size_bytes": self._current_filter.size_bytes,
            "current_fp_rate": self._current_filter.estimated_fp_rate,
            "current_age_seconds": self._current_filter.age_seconds,
            "previous_items": self._previous_filter.item_count if self._previous_filter else 0,
            "time_to_rotation": max(
                0,
                self.config.rotation_interval_seconds - (time.time() - self._last_rotation)
            ),
        }


# Global filter registry
_bloom_filters: dict[str, RotatingBloomFilter] = {}


def get_bloom_filter(stream_name: str, config: BloomFilterConfig | None = None) -> RotatingBloomFilter:
    """Get or create a bloom filter for a stream."""
    if stream_name not in _bloom_filters:
        _bloom_filters[stream_name] = RotatingBloomFilter(stream_name, config)
    return _bloom_filters[stream_name]


def dedupe_at_consumer(stream_name: str, event_id: str) -> bool:
    """Consumer-layer deduplication using bloom filter.
    
    Args:
        stream_name: Name of the stream
        event_id: Unique event identifier
        
    Returns:
        True if event should be DROPPED (probably duplicate)
        False if event should be PROCESSED
    """
    bf = get_bloom_filter(stream_name)
    return bf.check_and_add(event_id)


def dedupe_batch_at_writer(events: list[dict[str, Any]], event_id_key: str = "event_id") -> list[dict[str, Any]]:
    """Writer-layer deduplication within a single batch.
    
    Per PRD §12.11.1: Ensures no duplicates in a single batch (append-only).
    
    Args:
        events: List of event dictionaries
        event_id_key: Key to use for event ID
        
    Returns:
        Deduplicated list of events
    """
    seen_ids: set[str] = set()
    unique_events = []
    
    for event in events:
        event_id = event.get(event_id_key)
        if event_id and event_id not in seen_ids:
            seen_ids.add(event_id)
            unique_events.append(event)
    
    dropped = len(events) - len(unique_events)
    if dropped > 0:
        logger.debug(
            "writer_dedupe",
            dropped=dropped,
            kept=len(unique_events),
        )
    
    return unique_events


def dedupe_at_compaction(
    records: list[dict[str, Any]],
    partition: str,
    event_id_key: str = "event_id",
    ts_ingest_key: str = "ts_ingest",
) -> list[dict[str, Any]]:
    """Compactor-layer exact deduplication.
    
    Per PRD §12.11.3:
    1. Sort by event_id
    2. Drop duplicates, keeping the row with earliest ts_ingest
    3. Invariant: event_id is unique within a partition after compaction
    
    Args:
        records: All records in the partition
        partition: Partition identifier for metrics
        event_id_key: Key for event ID
        ts_ingest_key: Key for ingest timestamp
        
    Returns:
        Deduplicated records with earliest ts_ingest per event_id
    """
    if not records:
        return records
    
    # Group by event_id, keep earliest ts_ingest
    event_map: dict[str, dict[str, Any]] = {}
    
    for record in records:
        event_id = record.get(event_id_key)
        if not event_id:
            continue
        
        if event_id not in event_map:
            event_map[event_id] = record
        else:
            # Keep record with earlier ts_ingest
            existing_ts = event_map[event_id].get(ts_ingest_key)
            new_ts = record.get(ts_ingest_key)
            
            if new_ts and existing_ts and new_ts < existing_ts:
                event_map[event_id] = record
    
    # Sort by event_id for consistent output
    unique_records = sorted(event_map.values(), key=lambda r: r.get(event_id_key, ""))
    
    dropped = len(records) - len(unique_records)
    if dropped > 0:
        dedupe_compaction_removed.labels(partition=partition).inc(dropped)
        logger.info(
            "compaction_dedupe",
            partition=partition,
            total=len(records),
            unique=len(unique_records),
            dropped=dropped,
        )
    
    return unique_records


# Convenience decorator for consumer dedupe
def with_consumer_dedupe(stream_name: str):
    """Decorator to add consumer-layer deduplication to a message handler.
    
    Usage:
        @with_consumer_dedupe("stream:market.bars")
        async def handle_message(message):
            # Only called for non-duplicate messages
            ...
    """
    def decorator(fn):
        async def wrapper(message, *args, **kwargs):
            event_id = message.get("event_id") if isinstance(message, dict) else getattr(message, "event_id", None)
            
            if event_id and dedupe_at_consumer(stream_name, event_id):
                logger.debug("consumer_dedupe_dropped", event_id=event_id)
                return None  # Skip duplicate
            
            return await fn(message, *args, **kwargs)
        return wrapper
    return decorator
