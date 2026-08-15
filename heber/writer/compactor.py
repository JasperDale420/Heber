"""Parquet file compactor.

Merges small Parquet files into target-sized files.
Runs periodically to prevent "small file problem."
"""

import asyncio
import os
import signal
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.ops.metrics import record_compaction, start_metrics_server_from_env
from heber.writer.utils import fsync_directory, publish_parquet_atomically

logger = structlog.get_logger(__name__)

# Target file size in bytes (256 MB)
TARGET_FILE_SIZE = settings.silver_target_file_size_mb * 1024 * 1024

# Maximum files to compact in a single pass to prevent OOM on large partitions.
MAX_FILES_PER_BATCH = 1000

# Maximum total compressed bytes to read in a single compaction batch.
# Feeds with large embedded JSON (e.g. option_chain_snapshot with chain_json) can
# expand 5-10x in memory, so keep this budget conservative to avoid OOM in Docker.
# At ~14 MB/file compressed and ~6x expansion: 50 MB → ~300 MB uncompressed,
# ~600 MB with concat copy — safe under typical 4 GB Docker Desktop limits.
MAX_BATCH_COMPRESSED_BYTES = 50 * 1024 * 1024  # 50 MB


def _is_temporal_type(dt: pa.DataType) -> bool:
    """Check if a type is a date, timestamp, time, or duration type."""
    return pa.types.is_date(dt) or pa.types.is_timestamp(dt) or pa.types.is_time(dt) or pa.types.is_duration(dt)


class Compactor:
    """Compacts small Parquet files into larger ones."""

    def __init__(self):
        self.running = False

    def _acquire_partition_lock(self, partition_path: Path) -> Path | None:
        """Acquire an exclusive per-partition lock file."""
        lock_path = partition_path / ".compaction.lock"
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                lock_file.write(f"pid={os.getpid()} ts={datetime.now(UTC).isoformat()}\n")
            return lock_path
        except FileExistsError:
            existing_pid = self._read_lock_pid(lock_path)
            if existing_pid == os.getpid():
                logger.warning(
                    "Removing stale compaction lock created by current process",
                    partition=str(partition_path),
                    lock_path=str(lock_path),
                )
                try:
                    lock_path.unlink(missing_ok=True)
                except OSError as e:
                    logger.warning(
                        "Failed to remove stale compaction lock",
                        partition=str(partition_path),
                        lock_path=str(lock_path),
                        error=str(e),
                    )
                    return None
                return self._acquire_partition_lock(partition_path)
            logger.info("Skipping partition compaction; lock already present", partition=str(partition_path))
            return None

    def _release_partition_lock(self, lock_path: Path | None) -> None:
        """Release a previously acquired partition lock file."""
        if not lock_path:
            return
        try:
            lock_path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Failed to release compaction lock", lock_path=str(lock_path), error=str(e))

    def _dataset_label(self, partition_path: Path) -> str:
        """Extract a low-cardinality dataset label from a partition path."""
        try:
            rel = partition_path.relative_to(settings.silver_path)
            for part in rel.parts:
                if part.startswith("feed="):
                    return part.split("=", 1)[1]
            if rel.parts:
                return rel.parts[0]
        except ValueError:
            pass
        return "unknown"

    @staticmethod
    def _read_lock_pid(lock_path: Path) -> int | None:
        """Read lock owner PID from lock file."""
        try:
            first_line = lock_path.read_text(encoding="utf-8").splitlines()[0]
        except (OSError, IndexError, UnicodeDecodeError):
            return None
        for token in first_line.split():
            if token.startswith("pid="):
                try:
                    return int(token.split("=", 1)[1])
                except ValueError:
                    return None
        return None

    @staticmethod
    def _normalize_dict_columns(table: pa.Table) -> pa.Table:
        """Cast dictionary-encoded string columns to plain strings.

        PyArrow may auto-apply dictionary encoding to low-cardinality string
        columns.  When merging files written at different times the encoding
        can differ (plain string vs dictionary<string, int32>), causing
        ArrowTypeError.  Normalizing to plain strings avoids this.
        """
        arrays = []
        fields = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if pa.types.is_dictionary(field.type) and pa.types.is_string(field.type.value_type):
                col = col.cast(pa.string())
                field = field.with_type(pa.string())
            arrays.append(col)
            fields.append(field)
        return pa.table(arrays, schema=pa.schema(fields))

    @staticmethod
    def _is_temporal(dt: pa.DataType) -> bool:
        """Check if a type is a date or timestamp type."""
        return _is_temporal_type(dt)

    @staticmethod
    def _resolve_numeric_type(existing_type: pa.DataType, incoming_type: pa.DataType) -> pa.DataType | None:
        """Resolve compatible numeric types via widening."""
        if pa.types.is_integer(existing_type) and pa.types.is_integer(incoming_type):
            return pa.int64()
        if (pa.types.is_integer(existing_type) or pa.types.is_floating(existing_type)) and (
            pa.types.is_integer(incoming_type) or pa.types.is_floating(incoming_type)
        ):
            return pa.float64()
        return None

    @staticmethod
    def _resolve_string_or_temporal_type(existing_type: pa.DataType, incoming_type: pa.DataType) -> pa.DataType | None:
        """Resolve string-like and temporal type unification."""
        is_existing_string = pa.types.is_string(existing_type) or pa.types.is_large_string(existing_type)
        is_incoming_string = pa.types.is_string(incoming_type) or pa.types.is_large_string(incoming_type)

        if is_existing_string and is_incoming_string:
            if pa.types.is_large_string(existing_type) or pa.types.is_large_string(incoming_type):
                return pa.large_string()
            return pa.string()

        is_existing_temporal = _is_temporal_type(existing_type)
        is_incoming_temporal = _is_temporal_type(incoming_type)

        if is_existing_temporal and is_incoming_string:
            return incoming_type
        if is_incoming_temporal and is_existing_string:
            return existing_type
        if is_existing_temporal and is_incoming_temporal:
            return pa.string()

        return None

    @staticmethod
    def _resolve_column_type(existing_type: pa.DataType, incoming_type: pa.DataType) -> pa.DataType | None:
        """Resolve a compatible unified type for one column.

        Widening rules (safe casts only):
        - null + T → T
        - int + int → int64
        - int/float + float/int → float64
        - string + string → string
        - large_string ↔ string → large_string
        - temporal (date/timestamp) ↔ string → string
        - temporal + temporal (different) → string
        - completely incompatible types → string
        """
        if existing_type.equals(incoming_type):
            return existing_type
        if pa.types.is_null(existing_type):
            return incoming_type
        if pa.types.is_null(incoming_type):
            return existing_type

        numeric = Compactor._resolve_numeric_type(existing_type, incoming_type)
        if numeric is not None:
            return numeric

        resolved = Compactor._resolve_string_or_temporal_type(existing_type, incoming_type)
        if resolved is not None:
            return resolved

        # Ultimate fallback: cast everything to string if we really can't figure it out
        # This prevents schema conflicts from blocking compaction
        logger.warning(
            "Incompatible types encountered during compaction, falling back to string",
            existing=str(existing_type),
            incoming=str(incoming_type),
        )
        return pa.string()

    def _build_unified_schema(self, tables: list[pa.Table]) -> pa.Schema:
        """Build unified schema across tables."""
        ordered_columns: list[str] = []
        column_types: dict[str, pa.DataType] = {}

        for table in tables:
            for field in table.schema:
                column = field.name
                incoming_type = field.type
                existing_type = column_types.get(column)
                if existing_type is None:
                    ordered_columns.append(column)
                    column_types[column] = incoming_type
                    continue

                resolved_type = self._resolve_column_type(existing_type, incoming_type)
                column_types[column] = resolved_type

        return pa.schema([pa.field(column, column_types[column]) for column in ordered_columns])

    @staticmethod
    def _align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
        """Project/cast one table to the unified schema, filling missing cols with nulls."""
        arrays: list[pa.Array | pa.ChunkedArray] = []
        for field in schema:
            if field.name in table.schema.names:
                column = table.column(field.name)
                if not column.type.equals(field.type):
                    column = column.cast(field.type)
            else:
                column = pa.nulls(table.num_rows, type=field.type)
            arrays.append(column)
        return pa.table(arrays, schema=schema)

    @staticmethod
    def _list_compactable_parquet_files(partition_path: Path) -> list[Path]:
        """List parquet data files, excluding hidden sidecars."""
        compactable_files: list[Path] = []
        for candidate in sorted(partition_path.glob("*.parquet")):
            if candidate.name.startswith("."):
                logger.debug(
                    "Skipping hidden parquet sidecar file",
                    partition=str(partition_path),
                    file=str(candidate),
                )
                continue
            compactable_files.append(candidate)
        return compactable_files

    @staticmethod
    def _safe_stat_size(path: Path, partition_path: Path) -> int | None:
        """Return file size, skipping unreadable files."""
        try:
            return path.stat().st_size
        except OSError as e:
            logger.warning(
                "Skipping unreadable parquet file",
                partition=str(partition_path),
                file=str(path),
                error=str(e),
            )
            return None

    def _collect_small_files(self, partition_path: Path) -> tuple[list[Path], dict[Path, int]] | None:
        """Collect compactable parquet files below the target size threshold.

        Returns (small_file_paths, size_cache) tuple, or None if there aren't enough files to compact.
        The size_cache maps each file to its byte size, avoiding repeated stat() calls.
        """
        parquet_files = self._list_compactable_parquet_files(partition_path)
        if len(parquet_files) <= 1:
            return None

        # Stat only as far as the batch needs, stopping at the first limit hit.
        #
        # This used to stat every file in the partition before selecting any.
        # exFAT directories carry no index, so a stat inside a 35,955-entry
        # directory measured 53.7ms — about 32 minutes to choose one batch, paid
        # again on every cycle because only MAX_FILES_PER_BATCH of them get
        # merged per visit. Selection now costs the batch, not the partition.
        #
        # Two limits bound a batch. The count cap guards against pathological
        # fan-out; the byte cap is the real memory bound, since feeds with large
        # embedded data (option_chain_snapshot's chain_json) expand ~6x from
        # compressed size. The count cap alone throttled the common case badly —
        # a trading day lands ~274 files averaging 12 KB, two orders of
        # magnitude inside the budget.
        size_cache: dict[Path, int] = {}
        small_files: list[Path] = []
        running_bytes = 0
        oversized = 0
        stopped_at: str | None = None

        for file_path in parquet_files:
            if len(small_files) >= MAX_FILES_PER_BATCH:
                stopped_at = "file_count"
                break

            file_size = self._safe_stat_size(file_path, partition_path)
            if file_size is None:
                continue
            if file_size == 0:
                logger.warning("compactor_skip_empty_file", path=str(file_path))
                continue
            if file_size >= TARGET_FILE_SIZE:
                continue

            # A file larger than the whole batch budget can never share a batch,
            # and the budget check below always admits its first candidate — so
            # one of these used to fill the batch alone, get discarded by the
            # `<= 1` gate, and leave the partition unchanged. The next scan then
            # repeated it forever: 297 massive_taq_* partitions were capped this
            # way every day for a week without a single merge.
            if file_size > MAX_BATCH_COMPRESSED_BYTES:
                oversized += 1
                continue

            if running_bytes + file_size > MAX_BATCH_COMPRESSED_BYTES and small_files:
                stopped_at = "byte_budget"
                break

            size_cache[file_path] = file_size
            small_files.append(file_path)
            running_bytes += file_size

        if oversized:
            logger.info(
                "compactor_skip_oversized_files",
                partition=str(partition_path),
                skipped_files=oversized,
                budget_bytes=MAX_BATCH_COMPRESSED_BYTES,
            )

        if len(small_files) <= 1:
            return None

        if stopped_at:
            logger.info(
                "compactor_batch_capped",
                partition=str(partition_path),
                partition_files=len(parquet_files),
                included_files=len(small_files),
                included_bytes=running_bytes,
                limit=stopped_at,
            )

        logger.info(
            "Compacting partition",
            partition=str(partition_path),
            files=len(small_files),
            total_bytes=running_bytes,
        )

        return small_files, size_cache

    def _merge_tables_to_parquet(
        self,
        source_tables: list[pa.Table],
        unified_schema: pa.Schema,
        temp_path: Path,
        dataset: str,
    ) -> int:
        """Write source tables to a temporary parquet file, returning total row count.

        Performs exact deduplication on event_id, keeping the earliest ts_ingest.
        """
        if not source_tables:
            return 0

        # 1. Align schemas of all tables
        aligned_tables = [self._align_table_to_schema(t, unified_schema) for t in source_tables]

        # 2. Combine into a single table
        combined_table = pa.concat_tables(aligned_tables)

        # 2b. Cast string columns to large_string to avoid 32-bit offset overflow
        # during sort_by/take on partitions with large string data.
        for i, field in enumerate(combined_table.schema):
            if pa.types.is_string(field.type):
                combined_table = combined_table.set_column(
                    i, field.name, combined_table.column(i).cast(pa.large_string())
                )

        # 3. Fast path: If empty or no event_id, just write it
        if combined_table.num_rows == 0:
            return 0

        has_event_id = "event_id" in combined_table.schema.names
        has_ts_ingest = "ts_ingest" in combined_table.schema.names

        # 4. Deduplication — pure PyArrow, no Pandas roundtrip
        if has_event_id:
            initial_rows = combined_table.num_rows

            # Sort by ts_ingest ascending so take(keep_indices) preserves earliest duplicate
            if has_ts_ingest:
                combined_table = combined_table.sort_by([("ts_ingest", "ascending")])

            event_ids = combined_table.column("event_id").to_pylist()
            seen: set[str] = set()
            keep_indices: list[int] = []
            for i, eid in enumerate(event_ids):
                if eid not in seen:
                    seen.add(eid)
                    keep_indices.append(i)

            final_table = combined_table.take(keep_indices)
            deduped_rows = final_table.num_rows
            duplicates_removed = initial_rows - deduped_rows

            if duplicates_removed > 0:
                logger.info(
                    "Compaction deduplication removed records",
                    removed=duplicates_removed,
                    initial=initial_rows,
                    final=deduped_rows,
                )
                from heber.ops.metrics import compactor_dedupe_drops_total

                compactor_dedupe_drops_total.labels(dataset=dataset).inc(duplicates_removed)
        else:
            final_table = combined_table

        # 5. Write out.
        # use_dictionary=False ensures compacted files use plain string types
        # for low-cardinality columns (feed, instrument_type, provider, source).
        # This prevents schema merge failures when pyarrow.dataset reads a
        # directory containing files from both the real-time writer and the
        # compactor — mixed string vs dictionary<string, int32> encoding is
        # otherwise incompatible.
        with pq.ParquetWriter(temp_path, final_table.schema, compression="snappy", use_dictionary=False) as writer:
            writer.write_table(final_table, row_group_size=250_000)

        return final_table.num_rows

    def compact_partition(self, partition_path: Path) -> int:
        """Compact all small files in a partition.

        Returns number of files merged.
        """
        lock_path = self._acquire_partition_lock(partition_path)
        if lock_path is None:
            return 0

        started_at = datetime.now(UTC)
        dataset = self._dataset_label(partition_path)

        try:
            result = self._collect_small_files(partition_path)
            if result is None:
                return 0
            small_files, size_cache = result

            source_tables = [self._normalize_dict_columns(pq.ParquetFile(f).read()) for f in small_files]

            unified_schema = self._build_unified_schema(source_tables)

            ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            merged_path = partition_path / f"compacted-{ts}-{os.getpid()}.parquet"
            temp_path = partition_path / f".compacted-{ts}-{os.getpid()}.tmp"

            merged_rows = self._merge_tables_to_parquet(source_tables, unified_schema, temp_path, dataset)

            if merged_rows == 0:
                logger.info(
                    "compaction_skipped_zero_rows",
                    partition=str(partition_path),
                    source_files=len(small_files),
                )
                return 0

            # Sources are deleted below, so publishing a merge that did not land
            # whole would destroy the only good copies along with it.
            publish_parquet_atomically(temp_path, merged_path, expected_rows=merged_rows)
            merged_size = merged_path.stat().st_size if merged_path.exists() else 0
            source_bytes = sum(size_cache.get(f, 0) for f in small_files)
            reclaimed = max(source_bytes - merged_size, 0)

            # Flush the merged file's *name* before removing what it replaced.
            # Nothing orders the rename against the unlinks on this filesystem, so
            # a crash between them could keep the deletions and lose the merged
            # file, leaving the batch in no Silver file at all — a silent bulk loss
            # that only ever looks like a smaller partition. Compaction is hourly,
            # so the extra flush costs nothing that matters.
            fsync_directory(partition_path)

            for f in small_files:
                f.unlink()

            record_compaction(
                dataset=dataset,
                status="success",
                files_merged=len(small_files),
                bytes_reclaimed=reclaimed,
                duration=(datetime.now(UTC) - started_at).total_seconds(),
            )
            logger.info(
                "Compaction complete",
                partition=str(partition_path),
                merged_files=len(small_files),
                output_file=str(merged_path),
                rows=merged_rows,
            )
            return len(small_files)

        except Exception as e:  # noqa: BLE001 — multi-step compaction must preserve sources on any failure
            record_compaction(
                dataset=dataset,
                status="error",
                files_merged=0,
                bytes_reclaimed=0,
                duration=(datetime.now(UTC) - started_at).total_seconds(),
            )
            for stale_temp in partition_path.glob(".compacted-*.tmp"):
                stale_temp.unlink(missing_ok=True)
            logger.error(
                "Compaction failed",
                partition=str(partition_path),
                error=str(e),
                exc_info=True,
            )
            return 0
        finally:
            self._release_partition_lock(lock_path)

    @staticmethod
    def _iter_partition_dirs(layer_path: Path) -> Iterator[Path]:
        """Yield every directory *below* ``layer_path``, never the root itself.

        ``rglob("*")`` yielded files as well as directories and stat'd each one
        through ``is_dir()`` to tell them apart. That is a syscall per file:
        measured at 8 entries/s against ``feed=quotes``, whose 825,080 files
        would take roughly 28 hours to walk past — the reason its partitions
        were never compacted, and why no cycle ever ran to completion.
        ``os.scandir`` answers the same question from the directory entry, so
        the walk costs one read per directory and nothing per file.

        Siblings are visited round-robin at *every* level, so no branch can be
        starved by the shape of another. Order matters more than it looks here:
        `massive_bars` alone holds 11,010 per-ticker directories, and
        `feed=quotes` keeps its partitions four levels down. A breadth-first
        walk puts all 11,010 and their children ahead of the first
        `feed=quotes` partition — 26 minutes into a live cycle it had still not
        arrived. A depth-first walk drains whichever branch it enters first and
        spent 420s the same way. Being fair only between top-level datasets
        just moves the problem one level down, where
        `instrument_type=option`'s thousands of directories delay
        `instrument_type=equity` by every compaction in between. Interleaving
        at each level reaches real partitions everywhere within a few rounds:
        the first `feed=quotes` partition now arrives 8.6s into a cycle.

        The layer root is deliberately excluded. ``rglob`` never yields the
        directory it starts from, and compaction deletes the files it merges —
        so yielding the root would let two stray ``silver/*.parquet`` files be
        treated as a partition, merged into one and unlinked.

        Names starting with "." are skipped: macOS AppleDouble sidecars on
        exFAT raise PermissionError from a stat, and their contents are not
        lake data.
        """

        def subdirectories(directory: Path) -> list[Path]:
            try:
                with os.scandir(directory) as entries:
                    return [
                        Path(entry.path)
                        for entry in entries
                        if not entry.name.startswith(".") and entry.is_dir(follow_symlinks=False)
                    ]
            except OSError as exc:
                logger.warning("compactor_scan_unreadable_dir", path=str(directory), error=str(exc))
                return []

        def interleave(directories: list[Path]) -> Iterator[Path]:
            """Yield each directory, then its subtrees a level at a time."""
            walkers = [walk(directory) for directory in directories]
            while walkers:
                for walker in list(walkers):
                    try:
                        yield next(walker)
                    except StopIteration:
                        walkers.remove(walker)

        def walk(root: Path) -> Iterator[Path]:
            yield root
            yield from interleave(subdirectories(root))

        yield from interleave(subdirectories(layer_path))

    def scan_and_compact(self, layer: str = "silver") -> dict:
        """Scan layer for partitions that need compaction."""
        layer_path = settings.data_root / layer

        # A broken/unmounted volume must fail loudly, not read as an empty
        # lakehouse: during the 2026-07-11 volume drop this method logged
        # "Compaction cycle complete, partitions_scanned: 0" while the mount
        # was gone. The sentinel is created once on the real volume.
        sentinel = settings.data_root / ".heber-sentinel"
        if not sentinel.exists():
            raise RuntimeError(
                f"Data volume sentinel missing ({sentinel}) — mount broken or "
                "pointing at an empty directory; refusing to scan."
            )

        if not layer_path.exists():
            return {"partitions_scanned": 0, "files_merged": 0}

        partitions_scanned = 0
        files_merged = 0

        for partition_path in self._iter_partition_dirs(layer_path):
            parquet_files = self._list_compactable_parquet_files(partition_path)
            if parquet_files:
                partitions_scanned += 1
                merged = self.compact_partition(partition_path)
                files_merged += merged

        return {
            "partitions_scanned": partitions_scanned,
            "files_merged": files_merged,
        }

    async def run(self, interval_minutes: int = 60):
        """Run compactor on a schedule."""
        self.running = True

        logger.info("Starting compactor", interval_minutes=interval_minutes)

        while self.running:
            try:
                # Compact Silver layer
                result = await asyncio.to_thread(self.scan_and_compact, "silver")
                logger.info("Compaction cycle complete", **result)

                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                logger.info("Compactor cancelled")
                raise
            except Exception as e:  # noqa: BLE001 — top-level event loop must not crash
                logger.error("Compactor error", error=str(e), exc_info=True)
                await asyncio.sleep(60)  # Back off on error

        logger.info("Compactor stopped")

    def stop(self):
        """Stop the compactor."""
        self.running = False


async def main():
    """Entry point for the compactor."""
    # Settings are imported at module level, so we can use them
    from heber.ops.logging import configure_logging

    configure_logging(service_name="heber-compactor", log_level=settings.log_level, json_output=True)
    try:
        start_metrics_server_from_env(default_port=9090)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))
    compactor = Compactor()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, compactor.stop)

    await compactor.run()


if __name__ == "__main__":
    asyncio.run(main())
