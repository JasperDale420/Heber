"""Shared utilities for writer module."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.models.envelope import EventEnvelope
from heber.ops.metrics import record_write, record_write_error
from heber.writer.ingest_contracts import resolve_feed_alias, resolve_silver_feed
from heber.writer.normalizer import explode_aggregate_payload

logger = structlog.get_logger(__name__)


def get_bronze_partition_key(envelope: EventEnvelope) -> str:
    """Generate partition key for Bronze layer storage.

    Bronze uses provider/feed/date/hour partitioning for raw append-only storage.

    Args:
        envelope: Event envelope to partition.

    Returns:
        Partition key string (e.g., "provider=X/feed=Y/dt=Z/hour=H")
    """
    dt = envelope.ts_event.strftime("%Y-%m-%d")
    hour = envelope.ts_event.strftime("%H")
    return f"provider={envelope.provider}/feed={envelope.feed}/dt={dt}/hour={hour}"


def get_partition_key(
    feed: str,
    instrument_type: str,
    ts_event: datetime,
) -> str:
    """Generate partition key for an event.

    Args:
        feed: Feed name (e.g., "quotes", "bars_1m")
        instrument_type: Instrument type (e.g., "equity")
        ts_event: Event timestamp

    Returns:
        Partition key string (e.g., "feed=X/instrument_type=Y/dt=Z")
    """
    feed = resolve_feed_alias(feed)
    dt = ts_event.strftime("%Y-%m-%d")

    # High-volume feeds use hour partitioning
    if feed in ("quotes", "trades"):
        hour = ts_event.strftime("%H")
        return f"feed={feed}/instrument_type={instrument_type}/dt={dt}/hour={hour}"

    return f"feed={feed}/instrument_type={instrument_type}/dt={dt}"


def build_silver_candidates(envelope: EventEnvelope, feed_override: str | None = None) -> list[EventEnvelope]:
    """Build candidate envelopes for Silver writes.

    For aggregate REST payloads (bars/trades arrays), explode into one
    candidate envelope per item so backfill writes typed rows rather than
    null-heavy aggregate blobs.
    """
    if feed_override:
        canonical_feed = resolve_silver_feed(feed_override)
        return explode_aggregate_payload(envelope, feed_override=canonical_feed)

    return explode_aggregate_payload(envelope)


def write_silver_parquet(
    rows: list[dict[str, Any]],
    schema: pa.Schema,
    file_path: Path,
    partition_key: str,  # Used for logging context
    dataset: str,  # Used for metrics
) -> None:
    """Write rows to a Parquet file with standard compression and logging.

    Args:
        rows: List of dictionaries to write
        schema: PyArrow schema to enforce
        file_path: Destination path
        partition_key: Partition key for logging
        dataset: Dataset name for metrics
    """
    if not rows:
        return

    started_at = datetime.now(UTC)

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
            dataset=dataset,
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
                logger.debug("Skipping bad Silver row", index=i, feed=dataset)

        if valid_rows:
            table = pa.Table.from_pylist(valid_rows, schema=schema)
            # Ensure parent exists just in case (caller should handle, but write_table needs it)
            # Actually caller usually creates mkdir.
            # But let's assume caller did it.
            pq.write_table(table, file_path, compression="snappy", row_group_size=100_000)
            elapsed = (datetime.now(UTC) - started_at).total_seconds()
            bytes_written = file_path.stat().st_size if file_path.exists() else 0
            record_write(
                layer="silver",
                dataset=dataset,
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
