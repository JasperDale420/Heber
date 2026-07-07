"""Shared utilities for writer module."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
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


def flush_partitions_concurrent(
    buffers: dict[str, list[Any]],
    flush_partition: Callable[[str, list[Any]], None],
    partition_keys: list[str],
    max_workers: int,
) -> bool:
    """Flush the given partitions, concurrently when there are several.

    A backfill batch scatters records across hundreds of date partitions;
    writing them serially to the slow macOS bind mount caps consumer drain
    throughput and lets the capped stream evict un-consumed live events. Each
    partition writes a distinct file, so the writes never contend and can run
    in parallel.

    Safety is unchanged from the serial path: a partition is removed from
    ``buffers`` only after its write succeeds, so a failed write keeps its
    records buffered for redelivery, and the first error is re-raised so the
    caller does not acknowledge the batch. ``del`` happens on the calling
    thread (never inside a worker), so ``buffers`` is not mutated concurrently.

    Returns True if any non-empty partition was flushed (so the caller can
    advance its flush clock).
    """
    workers = min(len(partition_keys), max(1, max_workers))
    errors: list[Exception] = []
    attempted = False

    if workers <= 1:
        for pk in partition_keys:
            records = buffers.get(pk)
            if not records:
                buffers.pop(pk, None)
                continue
            attempted = True
            try:
                flush_partition(pk, records)
            except Exception as exc:  # noqa: BLE001 — retain buffer, surface to caller
                errors.append(exc)
            else:
                del buffers[pk]
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="heber-flush") as pool:
            futures: dict[Future[None], str] = {}
            for pk in partition_keys:
                records = buffers.get(pk)
                if not records:
                    buffers.pop(pk, None)
                    continue
                attempted = True
                futures[pool.submit(flush_partition, pk, records)] = pk
            for fut in as_completed(futures):
                pk = futures[fut]
                try:
                    fut.result()
                except Exception as exc:  # noqa: BLE001 — retain buffer, surface to caller
                    errors.append(exc)
                else:
                    del buffers[pk]

    if errors:
        raise errors[0]
    return attempted


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
        feed: Feed name (e.g., "quotes", "bars")
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
    from heber.quality.write_audit import audit_null_fields

    try:
        # Create Arrow table
        table = pa.Table.from_pylist(rows, schema=schema)

        audit_null_fields(
            table,
            layer="silver",
            dataset=dataset,
            context={"partition": partition_key, "file": str(file_path)},
        )

        # Write Parquet with compression.
        # use_dictionary=False prevents dictionary encoding of low-cardinality
        # string columns (feed, instrument_type, provider, source) so that all
        # Silver files use plain string types.  Mixed encoding (plain vs
        # dictionary<string, int32>) causes pyarrow.dataset schema merge to
        # fail with "incompatible types" when reading a directory containing
        # files from both the real-time writer and the compactor.
        tmp_path = file_path.with_suffix(".parquet.tmp")
        pq.write_table(
            table,
            tmp_path,
            compression="snappy",
            row_group_size=100_000,
            use_dictionary=False,
        )
        tmp_path.rename(file_path)
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
            except (pa.ArrowTypeError, pa.ArrowInvalid):
                logger.debug("Skipping bad Silver row", index=i, feed=dataset)

        if valid_rows:
            table = pa.Table.from_pylist(valid_rows, schema=schema)
            audit_null_fields(
                table,
                layer="silver",
                dataset=dataset,
                context={"path": str(file_path), "salvage": "true"},
            )
            salvage_tmp = file_path.with_suffix(".parquet.tmp")
            pq.write_table(table, salvage_tmp, compression="snappy", row_group_size=100_000, use_dictionary=False)
            salvage_tmp.rename(file_path)
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
    except OSError as e:
        record_write_error(layer="silver", error_type=type(e).__name__)
        logger.error(
            "Failed to flush Silver partition",
            partition=partition_key,
            error=str(e),
            exc_info=True,
        )
        raise
