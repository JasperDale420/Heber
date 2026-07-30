"""Bronze layer writer - raw provider payloads.

Bronze is append-only, immutable storage of original events.
Format: JSONL + gzip
Path: bronze/provider={}/feed={}/dt={}/hour={}/
"""

import gzip
import json
import time
import uuid
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
        # Per-writer id keeps filenames unique when multiple sharded consumer
        # containers write the same partition concurrently (their .tmp staging
        # names would otherwise collide and clobber each other's partial write).
        self._writer_id = uuid.uuid4().hex[:8]

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        return get_bronze_partition_key(envelope)

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.bronze_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        # Timestamp + per-writer id for cross-process uniqueness.
        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return base / f"events-{ts}-{self._writer_id}.jsonl.gz"

    def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)

        # Store the full envelope (including raw if present)
        event_dict = envelope.model_dump(mode="json")
        self.buffers[partition_key].append(event_dict)

    def has_buffered(self) -> bool:
        """True while any event is held in memory and not yet written.

        Emptied partition keys can linger as empty lists, so test the values
        rather than the key count.
        """
        return any(self.buffers.values())

    def flush_if_needed(self) -> bool:
        """Flush buffers if conditions are met.

        Due partitions are flushed concurrently (``writer_flush_max_workers``);
        a backfill batch scatters events across hundreds of date partitions and
        writing them serially to the bind mount starves the consumer. Each flush
        drops its key only on success — partition keys embed dt=/hour=, so
        retaining emptied entries would leak one dead key per
        (provider,feed,dt,hour) forever, and a failed flush keeps its events
        buffered for redelivery.

        Returns whether any partition was actually written. That is a reporting
        signal only — it says "something was flushed", not "everything is
        durable", so callers deciding whether to acknowledge must use
        ``has_buffered()`` instead.
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
        wrote = flush_partitions_concurrent(self.buffers, self._flush_partition, due, settings.writer_flush_max_workers)
        if wrote:
            self.last_flush = now
        return wrote

    def flush(self) -> None:
        """Flush every buffered partition, regardless of size or elapsed time.

        This is the shutdown and durability-backstop path, so every partition
        must be attempted even when one fails: a serial loop that propagated the
        first error left every partition behind it unwritten, and at shutdown
        those buffers are simply discarded. The shared helper attempts all of
        them, drops a partition only once its write succeeds, keeps a failed
        partition's events buffered, and re-raises the first error so the caller
        does not acknowledge the batch.
        """
        now = datetime.now(UTC)
        if flush_partitions_concurrent(
            self.buffers,
            self._flush_partition,
            list(self.buffers),
            settings.writer_flush_max_workers,
        ):
            self.last_flush = now

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
