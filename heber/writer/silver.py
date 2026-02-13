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
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.ops.metrics import record_write, record_write_error
from heber.schemas.silver import get_silver_schema
from heber.writer.ingest_contracts import resolve_feed_alias, resolve_silver_feed
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import enforce_required_non_null_fields, envelope_to_silver_row

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
        feed = resolve_feed_alias(envelope.feed)
        dt = envelope.ts_event.strftime("%Y-%m-%d")

        # High-volume feeds use hour partitioning
        if feed in ("quotes", "trades"):
            hour = envelope.ts_event.strftime("%H")
            return f"feed={feed}/instrument_type={envelope.instrument_type}/dt={dt}/hour={hour}"

        return f"feed={feed}/instrument_type={envelope.instrument_type}/dt={dt}"

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

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.silver_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return base / f"part-{ts}.parquet"

    def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        row, normalized = self._envelope_to_row(envelope)
        partition_key = self._get_partition_key(normalized)
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

        started_at = datetime.now(UTC)
        # Determine feed from partition key
        feed = partition_key.split("/")[0].split("=")[1]
        schema = self._get_schema(feed)
        file_path = self._get_file_path(partition_key)

        try:
            # Create Arrow table
            table = pa.Table.from_pylist(rows, schema=schema)

            # Write Parquet with compression
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                row_group_size=100_000,
            )
            elapsed = (datetime.now(UTC) - started_at).total_seconds()
            bytes_written = file_path.stat().st_size if file_path.exists() else 0
            record_write(
                layer="silver",
                dataset=feed,
                rows=len(rows),
                bytes_written=bytes_written,
                duration_seconds=elapsed,
            )

            logger.info(
                "Flushed Silver partition",
                partition=partition_key,
                rows=len(rows),
                file=str(file_path),
            )
        except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
            # Type coercion failure - try writing rows one-by-one to salvage valid data
            logger.warning(
                "Silver batch type error, attempting row-by-row salvage",
                partition=partition_key,
                error=str(e),
                total_rows=len(rows),
            )
            valid_rows = []
            for i, row in enumerate(rows):
                try:
                    pa.Table.from_pylist([row], schema=schema)
                    valid_rows.append(row)
                except Exception:
                    logger.debug("Skipping bad Silver row", index=i, feed=feed)

            if valid_rows:
                table = pa.Table.from_pylist(valid_rows, schema=schema)
                pq.write_table(table, file_path, compression="snappy", row_group_size=100_000)
                elapsed = (datetime.now(UTC) - started_at).total_seconds()
                bytes_written = file_path.stat().st_size if file_path.exists() else 0
                record_write(
                    layer="silver",
                    dataset=feed,
                    rows=len(valid_rows),
                    bytes_written=bytes_written,
                    duration_seconds=elapsed,
                )
                logger.info(
                    "Flushed Silver partition (salvaged)",
                    partition=partition_key,
                    valid=len(valid_rows),
                    skipped=len(rows) - len(valid_rows),
                    file=str(file_path),
                )
            else:
                record_write_error(layer="silver", error_type=type(e).__name__)
                logger.error("All Silver rows invalid, partition skipped", partition=partition_key)
        except Exception as e:
            record_write_error(layer="silver", error_type=type(e).__name__)
            logger.error(
                "Failed to flush Silver partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise
