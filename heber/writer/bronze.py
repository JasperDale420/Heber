"""Bronze layer writer - raw provider payloads.

Bronze is append-only, immutable storage of original events.
Format: JSONL + gzip
Path: bronze/provider={}/feed={}/dt={}/hour={}/
"""

import gzip
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope

logger = structlog.get_logger(__name__)


class BronzeWriter:
    """Writes events to Bronze layer as JSONL files."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.buffer_counts: dict[str, int] = defaultdict(int)
        self.last_flush: datetime = datetime.now(UTC)

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        dt = envelope.ts_event.strftime("%Y-%m-%d")
        hour = envelope.ts_event.strftime("%H")
        return f"provider={envelope.provider}/feed={envelope.feed}/dt={dt}/hour={hour}"

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.bronze_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        # Use timestamp-based filename for uniqueness
        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return base / f"events-{ts}.jsonl.gz"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)

        # Store the full envelope (including raw if present)
        event_dict = envelope.model_dump(mode="json")
        self.buffers[partition_key].append(event_dict)
        self.buffer_counts[partition_key] += 1

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.now(UTC)
        elapsed = (now - self.last_flush).total_seconds()

        for partition_key, events in list(self.buffers.items()):
            should_flush = (
                len(events) >= settings.bronze_max_batch_size or elapsed >= settings.bronze_flush_interval_seconds
            )
            if should_flush and events:
                await self._flush_partition(partition_key, events)
                self.buffers[partition_key] = []

        self.last_flush = now

    async def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, events in list(self.buffers.items()):
            if events:
                await self._flush_partition(partition_key, events)
                self.buffers[partition_key] = []

    async def _flush_partition(self, partition_key: str, events: list[dict]) -> None:
        """Write events to a partition file."""
        file_path = self._get_file_path(partition_key)

        try:
            # Write as gzipped JSONL
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps(event, default=str) + "\n")

            logger.info(
                "Flushed Bronze partition",
                partition=partition_key,
                events=len(events),
                file=str(file_path),
            )
        except Exception as e:
            logger.error(
                "Failed to flush Bronze partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise
