"""Parquet file compactor.

Merges small Parquet files into target-sized files.
Runs periodically to prevent "small file problem."
"""

import asyncio
import os
import signal
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.ops.metrics import record_compaction, start_metrics_server_from_env

logger = structlog.get_logger(__name__)

# Target file size in bytes (256 MB)
TARGET_FILE_SIZE = settings.silver_target_file_size_mb * 1024 * 1024


class SchemaConflictError(RuntimeError):
    """Raised when files in a partition contain incompatible column types."""

    def __init__(self, column: str, existing_type: pa.DataType, incoming_type: pa.DataType):
        super().__init__(f"Schema conflict for column '{column}': {existing_type} vs {incoming_type}")
        self.column = column
        self.existing_type = existing_type
        self.incoming_type = incoming_type


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
                except Exception as e:
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
        except Exception as e:
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
        except Exception:
            pass
        return "unknown"

    @staticmethod
    def _read_lock_pid(lock_path: Path) -> int | None:
        """Read lock owner PID from lock file."""
        try:
            first_line = lock_path.read_text(encoding="utf-8").splitlines()[0]
        except Exception:
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
    def _resolve_column_type(existing_type: pa.DataType, incoming_type: pa.DataType) -> pa.DataType | None:
        """Resolve a compatible unified type for one column."""
        if existing_type.equals(incoming_type):
            return existing_type
        if pa.types.is_null(existing_type):
            return incoming_type
        if pa.types.is_null(incoming_type):
            return existing_type
        if pa.types.is_integer(existing_type) and pa.types.is_integer(incoming_type):
            return pa.int64()
        if (
            (pa.types.is_integer(existing_type) and pa.types.is_floating(incoming_type))
            or (pa.types.is_floating(existing_type) and pa.types.is_integer(incoming_type))
            or (pa.types.is_floating(existing_type) and pa.types.is_floating(incoming_type))
        ):
            return pa.float64()
        if pa.types.is_string(existing_type) and pa.types.is_string(incoming_type):
            return pa.string()
        return None

    def _build_unified_schema(self, tables: list[pa.Table]) -> pa.Schema:
        """Build unified schema across tables, raising on true type conflicts."""
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
                if resolved_type is None:
                    raise SchemaConflictError(
                        column=column,
                        existing_type=existing_type,
                        incoming_type=incoming_type,
                    )
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
                logger.warning(
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
            parquet_files = self._list_compactable_parquet_files(partition_path)
            if len(parquet_files) <= 1:
                return 0

            sized_files: list[tuple[Path, int]] = []
            for file_path in parquet_files:
                file_size = self._safe_stat_size(file_path, partition_path)
                if file_size is not None:
                    sized_files.append((file_path, file_size))

            if len(sized_files) <= 1:
                return 0

            total_size = sum(size for _, size in sized_files)
            small_files = [path for path, size in sized_files if size < TARGET_FILE_SIZE]
            if len(small_files) <= 1:
                return 0

            logger.info(
                "Compacting partition",
                partition=str(partition_path),
                files=len(small_files),
                total_bytes=total_size,
            )
            size_by_path = {path: size for path, size in sized_files}
            source_bytes = sum(size_by_path[path] for path in small_files)

            # Stream files into a single temp parquet, then atomically promote.
            ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            merged_path = partition_path / f"compacted-{ts}-{os.getpid()}.parquet"
            temp_path = partition_path / f".compacted-{ts}-{os.getpid()}.tmp"
            source_tables: list[pa.Table] = []
            for source_file in small_files:
                source_table = self._normalize_dict_columns(pq.ParquetFile(source_file).read())
                source_tables.append(source_table)

            try:
                unified_schema = self._build_unified_schema(source_tables)
            except SchemaConflictError as conflict:
                record_compaction(
                    dataset=dataset,
                    status="error",
                    files_merged=0,
                    bytes_reclaimed=0,
                    duration=(datetime.now(UTC) - started_at).total_seconds(),
                )
                logger.error(
                    "Compaction skipped due schema conflict",
                    partition=str(partition_path),
                    column=conflict.column,
                    existing_type=str(conflict.existing_type),
                    incoming_type=str(conflict.incoming_type),
                )
                return 0

            writer = None
            merged_rows = 0
            try:
                for source_table in source_tables:
                    table = self._align_table_to_schema(source_table, unified_schema)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            temp_path,
                            unified_schema,
                            compression="snappy",
                        )
                    writer.write_table(table, row_group_size=250_000)
                    merged_rows += table.num_rows
            finally:
                if writer is not None:
                    writer.close()

            # Atomic file promotion once the merge succeeds.
            temp_path.replace(merged_path)
            merged_size = merged_path.stat().st_size if merged_path.exists() else 0
            reclaimed = max(source_bytes - merged_size, 0)

            # Delete source files only after merged output is durable.
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

        except Exception as e:
            record_compaction(
                dataset=dataset,
                status="error",
                files_merged=0,
                bytes_reclaimed=0,
                duration=(datetime.now(UTC) - started_at).total_seconds(),
            )
            # Best-effort cleanup: never delete source files on failed compaction.
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

    def scan_and_compact(self, layer: str = "silver") -> dict:
        """Scan layer for partitions that need compaction."""
        layer_path = settings.data_root / layer

        if not layer_path.exists():
            return {"partitions_scanned": 0, "files_merged": 0}

        partitions_scanned = 0
        files_merged = 0

        # Walk through all partitions (directories containing .parquet files)
        for partition_path in layer_path.rglob("*"):
            if partition_path.is_dir():
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
                result = self.scan_and_compact("silver")
                logger.info("Compaction cycle complete", **result)

                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                logger.info("Compactor cancelled")
                raise
            except Exception as e:
                logger.error("Compactor error", error=str(e), exc_info=True)
                await asyncio.sleep(60)  # Back off on error

        logger.info("Compactor stopped")

    def stop(self):
        """Stop the compactor."""
        self.running = False


async def main():
    """Entry point for the compactor."""
    start_metrics_server_from_env(default_port=9090)
    compactor = Compactor()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, compactor.stop)

    await compactor.run()


if __name__ == "__main__":
    asyncio.run(main())
