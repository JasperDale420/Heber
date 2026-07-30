"""Silver layer writer — typed-flat Parquet datasets.

Architecture decision (A+C model):
  - Silver = rename + type coerce ONLY (no computed/derived fields)
  - Computed fields (moneyness, DTE, volume_oi_ratio) live in Gold/Feature views
  - BronzeToSilverTransformer handles replay/backfill from raw Bronze

Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.metrics import record_write, record_write_error
from heber.schemas.silver import get_silver_schema
from heber.writer.durability import create_durable_directory
from heber.writer.ingest_contracts import resolve_feed_alias, resolve_silver_feed
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import enforce_required_non_null_fields, envelope_to_silver_row
from heber.writer.utils import flush_partitions_concurrent, get_partition_key, write_silver_parquet

logger = structlog.get_logger(__name__)


class SilverWriter:
    """Writes typed-flat events to Silver layer as Parquet.

    Responsibilities:
      1. Field renames — map provider payload names to Silver canonical names
      2. Type coercion — cast strings to float/int/date/timestamp per Arrow schema
      3. Schema enforcement — write typed Parquet with strict Arrow schemas

    NOT responsible for:
      - Computed/derived fields (Gold/Feature layer)
      - Cross-event joins or lookups
      - Deduplication (handled at consumer level via event_id)
    """

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.now(UTC)
        # Per-writer id keeps filenames unique when multiple sharded consumer
        # containers write the same partition concurrently (their .tmp staging
        # names would otherwise collide and clobber each other's partial write).
        self._writer_id = uuid.uuid4().hex[:8]

    def _get_file_path(self, partition_key: str) -> Path:
        """Build the output Parquet path for a partition (unique per writer)."""
        partition_path = settings.silver_path / partition_key
        create_durable_directory(partition_path, root=settings.data_root)
        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return partition_path / f"part-{ts}-{self._writer_id}.parquet"

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        return get_partition_key(
            feed=envelope.feed,
            instrument_type=envelope.instrument_type,
            ts_event=envelope.ts_event,
        )

    def _get_schema(self, feed: str) -> pa.Schema:
        """Get schema for a feed."""
        return get_silver_schema(feed)

    def _envelope_to_row(self, envelope: EventEnvelope) -> tuple[dict[str, Any], EventEnvelope]:
        """Normalize an envelope and return `(row, normalized_envelope)`."""
        normalized = normalize_envelope_for_silver(envelope)
        silver_feed = resolve_silver_feed(normalized.feed)

        if silver_feed is None:
            row = {
                "event_id": normalized.event_id,
                "provider": normalized.provider,
                "feed": resolve_feed_alias(normalized.feed),
                "instrument_type": normalized.instrument_type,
                "instrument_key": normalized.instrument_key,
                "symbol": normalized.symbol,
                "ts_event": normalized.ts_event,
                "ts_ingest": normalized.ts_ingest,
                "ts_available": normalized.ts_available,
                "source": normalized.source,
                "schema_version": normalized.schema_version,
                "quality_flags": normalized.quality_flags,
            }
            row["payload_json"] = json.dumps(normalized.payload, default=str)
            return row, normalized

        normalized = normalized.model_copy(update={"feed": silver_feed})
        row = envelope_to_silver_row(normalized)
        enforce_required_non_null_fields(feed=silver_feed, row=row, event_id=normalized.event_id)
        return row, normalized

    def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        row, normalized = self._envelope_to_row(envelope)
        partition_key = self._get_partition_key(normalized)
        self.buffers[partition_key].append(row)

    def write_row(self, partition_key: str, row: dict[str, Any]) -> None:
        """Buffer a pre-normalized Silver row for writing.

        Use this when the caller has already performed normalization
        (e.g. ``normalize_envelope_for_silver`` + ``envelope_to_silver_row``)
        to avoid duplicating that work.
        """
        self.buffers[partition_key].append(row)

    def has_buffered(self) -> bool:
        """True while any row is held in memory and not yet written.

        Emptied partition keys can linger as empty lists, so test the values
        rather than the key count.
        """
        return any(self.buffers.values())

    def flush_if_needed(self) -> bool:
        """Flush buffers if conditions are met.

        A partition is flushed when:
        - It reaches ``silver_max_rows_per_file`` (hard cap), OR
        - It has at least ``silver_min_rows_per_flush`` rows, OR
        - The time since last flush exceeds ``silver_max_flush_time_seconds``
          (safety valve — ensures data is eventually persisted even at low volume).

        The min-rows gate prevents creating tiny parquet files during backfill,
        where a single XREADGROUP batch scatters records across hundreds of
        date partitions with only 2-4 rows each.

        Returns whether any partition was actually written. That is a reporting
        signal only — it says "something was flushed", not "everything is
        durable", so callers deciding whether to acknowledge must use
        ``has_buffered()`` instead.
        """
        now = datetime.now(UTC)
        elapsed = (now - self.last_flush).total_seconds()
        time_triggered = elapsed >= settings.silver_max_flush_time_seconds

        # Due partitions are flushed concurrently (``writer_flush_max_workers``).
        # Each drops its key only on success — partition keys embed dt=/hour=, so
        # retaining emptied entries would leak one dead key per
        # (feed,instrument_type,dt,hour) forever, and a failed flush keeps its
        # rows buffered for redelivery.
        due = [
            partition_key
            for partition_key in list(self.buffers)
            if self.buffers[partition_key]
            and (
                len(self.buffers[partition_key]) >= settings.silver_max_rows_per_file
                or len(self.buffers[partition_key]) >= settings.silver_min_rows_per_flush
                or time_triggered
            )
        ]
        wrote = flush_partitions_concurrent(self.buffers, self._flush_partition, due, settings.writer_flush_max_workers)
        if wrote:
            self.last_flush = now
        return wrote

    def flush(self) -> None:
        """Flush every buffered partition, regardless of row count or elapsed time.

        This is the shutdown and durability-backstop path, so every partition
        must be attempted even when one fails: a serial loop that propagated the
        first error left every partition behind it unwritten, and at shutdown
        those buffers are simply discarded. The shared helper attempts all of
        them, drops a partition only once its write succeeds, keeps a failed
        partition's rows buffered, and re-raises the first error so the caller
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

    def _flush_partition(self, partition_key: str, rows: list[dict]) -> None:
        """Write rows to a Parquet file."""
        if not rows:
            return

        # Determine feed from partition key
        feed = partition_key.split("/")[0].split("=")[1]
        schema = self._get_schema(feed)

        file_path = self._get_file_path(partition_key)

        try:
            t0 = datetime.now(UTC)
            write_silver_parquet(
                rows=rows,
                schema=schema,
                file_path=file_path,
                partition_key=partition_key,
                dataset=feed,
            )
            duration = (datetime.now(UTC) - t0).total_seconds()
            bytes_written = file_path.stat().st_size if file_path.exists() else 0
            record_write(
                layer="silver", dataset=feed, rows=len(rows), bytes_written=bytes_written, duration_seconds=duration
            )
        except (OSError, pa.ArrowTypeError, pa.ArrowInvalid):
            record_write_error(layer="silver", error_type="flush_failed")
            raise
