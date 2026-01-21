"""Silver layer writer - normalized Parquet datasets.

Silver is the canonical, normalized event layer optimized for querying.
Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope

logger = structlog.get_logger(__name__)

# Dataset-specific schemas (per PRD Section 8.7)
SILVER_SCHEMAS = {
    "bars": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Bars-specific
            ("timeframe", pa.string()),
            ("bar_start_ts", pa.timestamp("us", tz="UTC")),
            ("open", pa.float64()),
            ("high", pa.float64()),
            ("low", pa.float64()),
            ("close", pa.float64()),
            ("volume", pa.float64()),
            ("trade_count", pa.int64()),
            ("vwap", pa.float64()),
        ]
    ),
    "quotes": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Quotes-specific
            ("bid_px", pa.float64()),
            ("bid_sz", pa.float64()),
            ("ask_px", pa.float64()),
            ("ask_sz", pa.float64()),
            ("bid_exchange", pa.string()),
            ("ask_exchange", pa.string()),
        ]
    ),
    "trades": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Trades-specific
            ("trade_id", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("exchange", pa.string()),
            ("tape", pa.string()),
        ]
    ),
    "flow_alerts": pa.schema(
        [
            ("event_id", pa.string()),
            ("provider", pa.string()),
            ("feed", pa.string()),
            ("instrument_type", pa.string()),
            ("instrument_key", pa.string()),
            ("symbol", pa.string()),
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_ingest", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("source", pa.string()),
            ("schema_version", pa.string()),
            ("quality_flags", pa.list_(pa.string())),
            # Flow-specific
            ("underlying", pa.string()),
            ("occ_symbol", pa.string()),
            ("expiry", pa.date32()),
            ("strike", pa.float64()),
            ("put_call", pa.string()),
            ("premium", pa.float64()),
            ("volume", pa.float64()),
            ("open_interest", pa.float64()),
            ("spot_px", pa.float64()),
            ("contract_px", pa.float64()),
            ("alert_type", pa.string()),
            ("side", pa.string()),
            ("aggressor", pa.string()),
        ]
    ),
}

# Default schema for unknown feeds
DEFAULT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        ("payload_json", pa.string()),  # Store payload as JSON string
    ]
)


class SilverWriter:
    """Writes normalized events to Silver layer as Parquet."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.utcnow()

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
            # Map payload fields to schema columns
            for field in SILVER_SCHEMAS[envelope.feed]:
                if field.name not in row:
                    row[field.name] = payload.get(field.name)
        else:
            # Store payload as JSON for unknown feeds
            import json

            row["payload_json"] = json.dumps(payload, default=str)

        return row

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.silver_path / partition_key
        base.mkdir(parents=True, exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return base / f"part-{ts}.parquet"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)
        row = self._envelope_to_row(envelope)
        self.buffers[partition_key].append(row)

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.utcnow()
        elapsed = (now - self.last_flush).total_seconds()

        for partition_key, rows in list(self.buffers.items()):
            should_flush = (
                len(rows) >= settings.silver_max_rows_per_file or elapsed >= settings.bronze_flush_interval_seconds
            )
            if should_flush and rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

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
        except Exception as e:
            logger.error(
                "Failed to flush Silver partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise
