"""Silver layer writer - normalized Parquet datasets.

Silver is the canonical, normalized event layer optimized for querying.
Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.schemas.silver import DEFAULT_SCHEMA, SILVER_SCHEMAS

logger = structlog.get_logger(__name__)


class SilverWriter:
    """Writes normalized events to Silver layer as Parquet."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.now(UTC)

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        dt = envelope.ts_event.strftime("%Y-%m-%d")

        # High-volume feeds use hour partitioning
        if envelope.feed in ("quotes", "trades"):
            hour = envelope.ts_event.strftime("%H")
            return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}/hour={hour}"

        return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}"

    def _get_schema(self, feed: str) -> pa.Schema:
        """Get schema for a feed."""
        return SILVER_SCHEMAS.get(feed, DEFAULT_SCHEMA)

    def _envelope_to_row(self, envelope: EventEnvelope) -> dict[str, Any]:
        """Convert envelope to Silver row format."""
        # Base columns from envelope
        row = {
            "event_id": envelope.event_id,
            "provider": envelope.provider,
            "feed": envelope.feed,
            "instrument_type": envelope.instrument_type,
            "instrument_key": envelope.instrument_key,
            "symbol": envelope.symbol,
            "ts_event": envelope.ts_event,
            "ts_ingest": envelope.ts_ingest,
            "ts_available": envelope.ts_available,
            "source": envelope.source,
            "schema_version": envelope.schema_version,
            "quality_flags": envelope.quality_flags,
        }

        # Add payload fields
        payload = envelope.payload
        if envelope.feed in SILVER_SCHEMAS:
            # Field name mappings for UW feeds (payload field -> Silver schema field)
            field_mappings = {
                "flow_alerts": {
                    "price": "contract_px",
                    "underlying_price": "spot_px",
                    "option_chain": "occ_symbol",
                    "symbol": "underlying",  # symbol in payload is the underlying
                    "alert_rule": "alert_type",
                },
                "darkpool": {
                    "symbol": "underlying",  # symbol in payload is the underlying
                    "exchange": "venue",
                    "tracking_id": "print_id",
                },
                "market_tide": {
                    "net_call_premium": "total_call_premium",
                    "net_put_premium": "total_put_premium",
                },
                "sector_tide": {
                    # sector_tide uses same field names, but we include for consistency
                },
            }
            mappings = field_mappings.get(envelope.feed, {})

            # Map payload fields to schema columns with type coercion
            schema = SILVER_SCHEMAS[envelope.feed]
            for field in schema:
                if field.name in row:
                    continue  # Already set from envelope

                # Try mapped name first, then direct name
                source_name = next((k for k, v in mappings.items() if v == field.name), field.name)
                value = payload.get(source_name)

                # Type coercion based on Arrow type
                if value is not None:
                    value = self._coerce_value(value, field.type)

                row[field.name] = value
        else:
            # Store payload as JSON for unknown feeds
            import json

            row["payload_json"] = json.dumps(payload, default=str)

        return row

    def _coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a value to match the expected Arrow type."""
        if value is None:
            return None

        try:
            if pa.types.is_floating(arrow_type):
                return float(value) if value != "" else None
            elif pa.types.is_integer(arrow_type):
                return int(float(value)) if value != "" else None
            elif pa.types.is_date(arrow_type):
                from datetime import date, datetime

                if isinstance(value, date):
                    return value
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    return datetime.strptime(value[:10], "%Y-%m-%d").date()
            elif pa.types.is_timestamp(arrow_type):
                from datetime import datetime

                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    # Handle ISO format with timezone
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
            # For strings and other types, return as-is
            return value
        except (ValueError, TypeError) as e:
            logger.warning(
                "Type coercion failed",
                value=str(value)[:50],
                target_type=str(arrow_type),
                error=str(e),
            )
            return None

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.silver_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        return base / f"part-{ts}.parquet"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)
        row = self._envelope_to_row(envelope)
        self.buffers[partition_key].append(row)

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.now(UTC)
        elapsed = (now - self.last_flush).total_seconds()

        flushed = False
        for partition_key, rows in list(self.buffers.items()):
            should_flush = (
                len(rows) >= settings.silver_max_rows_per_file or elapsed >= settings.silver_max_flush_time_seconds
            )
            if should_flush and rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []
                flushed = True

        if flushed:
            self.last_flush = now

    async def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, rows in list(self.buffers.items()):
            if rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

    async def _flush_partition(self, partition_key: str, rows: list[dict]) -> None:
        """Write rows to a Parquet file."""
        if not rows:
            return

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
                logger.info(
                    "Flushed Silver partition (salvaged)",
                    partition=partition_key,
                    valid=len(valid_rows),
                    skipped=len(rows) - len(valid_rows),
                    file=str(file_path),
                )
            else:
                logger.error("All Silver rows invalid, partition skipped", partition=partition_key)
        except Exception as e:
            logger.error(
                "Failed to flush Silver partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise
