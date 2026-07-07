"""Bronze layer writer - raw provider payloads.

Bronze is append-only, immutable storage of original events.
Format: JSONL + gzip
Path: bronze/provider={}/feed={}/dt={}/hour={}/
"""

import gzip
import json
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.metrics import record_write, record_write_error
from heber.writer.utils import flush_partitions_concurrent, get_bronze_partition_key

logger = structlog.get_logger(__name__)


class BronzeWriter:
    """Writes events to Bronze layer as JSONL files."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.now(UTC)

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        return get_bronze_partition_key(envelope)

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.bronze_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        # Use timestamp-based filename for uniqueness
        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return base / f"events-{ts}.jsonl.gz"

    def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)

        # Store the full envelope (including raw if present)
        event_dict = envelope.model_dump(mode="json")
        self.buffers[partition_key].append(event_dict)

    def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met.

        Due partitions are flushed concurrently (``writer_flush_max_workers``);
        a backfill batch scatters events across hundreds of date partitions and
        writing them serially to the bind mount starves the consumer. Each flush
        drops its key only on success — partition keys embed dt=/hour=, so
        retaining emptied entries would leak one dead key per
        (provider,feed,dt,hour) forever, and a failed flush keeps its events
        buffered for redelivery.
        """
        now = datetime.now(UTC)
        elapsed = (now - self.last_flush).total_seconds()

        due = [
            partition_key
            for partition_key in list(self.buffers)
            if self.buffers[partition_key]
            and (
                len(self.buffers[partition_key]) >= settings.bronze_max_batch_size
                or elapsed >= settings.bronze_flush_interval_seconds
            )
        ]
        if flush_partitions_concurrent(self.buffers, self._flush_partition, due, settings.writer_flush_max_workers):
            self.last_flush = now

    def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key in list(self.buffers):
            events = self.buffers[partition_key]
            if events:
                self._flush_partition(partition_key, events)
            del self.buffers[partition_key]

    def _flush_partition(self, partition_key: str, events: list[dict]) -> None:
        """Write events to a partition file."""
        file_path = self._get_file_path(partition_key)
        started = time.perf_counter()
        dataset = self._dataset_from_partition(partition_key)

        try:
            # Write as gzipped JSONL (atomic: write to tmp, then rename)
            tmp_path = file_path.with_suffix(".tmp")
            with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps(event, default=str) + "\n")
            tmp_path.rename(file_path)

            duration_seconds = max(0.0, time.perf_counter() - started)
            bytes_written = file_path.stat().st_size if file_path.exists() else 0
            record_write(
                layer="bronze",
                dataset=dataset,
                rows=len(events),
                bytes_written=bytes_written,
                duration_seconds=duration_seconds,
            )

            logger.info(
                "Flushed Bronze partition",
                partition=partition_key,
                events=len(events),
                file=str(file_path),
            )
        except OSError as e:
            record_write_error(layer="bronze", error_type=type(e).__name__)
            logger.error(
                "Failed to flush Bronze partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise

    @staticmethod
    def _dataset_from_partition(partition_key: str) -> str:
        """Extract dataset/feed label from bronze partition key."""
        for token in partition_key.split("/"):
            if token.startswith("feed="):
                return token.split("=", 1)[1]
        return "unknown"
