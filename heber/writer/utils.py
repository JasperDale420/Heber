"""Shared utilities for writer module."""

from __future__ import annotations

import os
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


def _discard_staging(tmp_path: Path) -> None:
    """Remove a staging file, never masking the error that prompted the cleanup."""
    try:
        tmp_path.unlink(missing_ok=True)
    except OSError:
        logger.warning("Could not remove staging file", file=str(tmp_path), exc_info=True)


def _keep_staging_for_autopsy(tmp_path: Path) -> None:
    """Set a staging file aside instead of deleting it.

    Used for failures we cannot explain, where the file is the only evidence.
    The dotted name keeps it out of every reader's and the compactor's walk.
    """
    try:
        tmp_path.rename(tmp_path.with_name(f".corrupt-{os.getpid()}-{tmp_path.name}"))
    except OSError:
        logger.warning("Could not set aside staging file", file=str(tmp_path), exc_info=True)
        _discard_staging(tmp_path)


def fsync_directory(path: Path) -> None:
    """Flush a directory's own entries, making renames and deletes inside it durable."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish_file_atomically(tmp_path: Path, file_path: Path) -> None:
    """Expose the staging file ``tmp_path`` at ``file_path`` only once it is durable.

    A published path is a promise to every reader that the file behind it is
    complete. Renaming alone does not keep that promise: the rename makes the
    name visible immediately while the data may still be sitting in the page
    cache, so an unclean shutdown — a killed VM, a disconnected volume, and this
    lakehouse lives on a non-journaling exFAT disk — can leave the name on disk
    with nothing behind it. The bytes are flushed to disk first and only then
    renamed into place; a failure deletes the staging file instead of publishing
    it.

    This narrows the window rather than closing it. Inside a container the fsync
    reaches the host's virtiofs layer, and whether the USB enclosure has the data
    by then is a property of that stack, not something POSIX promises.

    Callers that go on to delete other files on the strength of this publish must
    also ``fsync_directory`` the parent — see the compactor.

    Raises:
        OSError: If the staged file is empty, i.e. the write produced no bytes.
    """
    try:
        with open(tmp_path, "rb") as handle:
            if os.fstat(handle.fileno()).st_size == 0:
                raise OSError(f"write produced an empty file: {tmp_path}")
            os.fsync(handle.fileno())
        os.replace(tmp_path, file_path)
    except BaseException:
        _discard_staging(tmp_path)
        raise
    # ponytail: the parent directory is not fsynced here, so a crash can still lose
    # the rename. For the Silver and Bronze writers that leaves the partition short
    # a file rather than holding a broken one — a row-count gap, replayable from
    # Bronze — and a directory fsync costs ~5x the file fsync on this volume. It is
    # only safe to skip because nothing is deleted on the strength of the rename.


def publish_parquet_atomically(tmp_path: Path, file_path: Path, expected_rows: int) -> None:
    """Publish a written Parquet staging file after checking its footer reads back.

    Size alone does not prove a Parquet file is complete — a truncated one still
    has a size — and any fragment pyarrow cannot open fails the entire dataset
    scan rather than just itself. Parsing the footer back is cheap because the
    file is still in the page cache, which is also the limit of what it proves:
    it catches a short or unreadable write, not a torn data page, and it reads
    the pre-crash view so it can say nothing about durability. The fsync in
    ``publish_file_atomically`` is what makes the write survive.

    Raises:
        OSError: If the staged file cannot be reopened or is missing rows.
    """
    try:
        with pq.ParquetFile(tmp_path) as staged:
            actual_rows = staged.metadata.num_rows
    except Exception as exc:  # noqa: BLE001 — anything unreadable must not be published
        _discard_staging(tmp_path)
        raise OSError(f"Parquet write produced an unreadable file: {tmp_path} ({exc})") from exc
    if actual_rows != expected_rows:
        # Unexplained: the writer returned cleanly but the file disagrees. Keep it.
        _keep_staging_for_autopsy(tmp_path)
        raise OSError(f"Parquet write lost rows: {tmp_path} holds {actual_rows}, expected {expected_rows}")

    publish_file_atomically(tmp_path, file_path)


def publish_table_atomically(table: pa.Table, file_path: Path, **write_options: Any) -> None:
    """Write ``table`` to a staging file and publish it once it is complete and durable."""
    tmp_path = file_path.with_suffix(".parquet.tmp")
    try:
        pq.write_table(table, tmp_path, **write_options)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    publish_parquet_atomically(tmp_path, file_path, expected_rows=table.num_rows)


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
        publish_table_atomically(
            table,
            file_path,
            compression="snappy",
            row_group_size=100_000,
            use_dictionary=False,
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
            # An OSError raised here is not caught by the sibling `except OSError`
            # below, so the salvage path records its own write failure.
            try:
                publish_table_atomically(
                    table, file_path, compression="snappy", row_group_size=100_000, use_dictionary=False
                )
            except OSError as publish_error:
                record_write_error(layer="silver", error_type=type(publish_error).__name__)
                logger.error(
                    "Failed to publish salvaged Silver partition",
                    partition=partition_key,
                    error=str(publish_error),
                    exc_info=True,
                )
                raise
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
