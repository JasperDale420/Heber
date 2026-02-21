"""Silver layer writer — typed-flat Parquet datasets.

Architecture decision (A+C model):
  - Silver = rename + type coerce ONLY (no computed/derived fields)
  - Computed fields (moneyness, DTE, volume_oi_ratio) live in Gold/Feature views
  - BronzeToSilverTransformer handles replay/backfill from raw Bronze

Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

import json
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.metrics import record_write, record_write_error
from heber.schemas.silver import get_silver_schema
from heber.writer.ingest_contracts import resolve_feed_alias, resolve_silver_feed
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import enforce_required_non_null_fields, envelope_to_silver_row
from heber.writer.utils import get_partition_key, write_silver_parquet

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

    def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.now(UTC)
        elapsed = (now - self.last_flush).total_seconds()

        flushed = False
        for partition_key, rows in self.buffers.items():
            should_flush = (
                len(rows) >= settings.silver_max_rows_per_file or elapsed >= settings.silver_max_flush_time_seconds
            )
            if should_flush and rows:
                self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []
                flushed = True

        if flushed:
            self.last_flush = now

    def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, rows in self.buffers.items():
            if rows:
                self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

    def _flush_partition(self, partition_key: str, rows: list[dict]) -> None:
        """Write rows to a Parquet file."""
        if not rows:
            return

        # Determine feed from partition key
        feed = partition_key.split("/")[0].split("=")[1]
        schema = self._get_schema(feed)

        # Path management
        partition_path = settings.silver_path / partition_key
        partition_path.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        file_path = partition_path / f"part-{ts}.parquet"

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
        except Exception:
            record_write_error(layer="silver", error_type="flush_failed")
            raise
