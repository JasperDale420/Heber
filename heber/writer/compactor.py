"""Parquet file compactor.

Merges small Parquet files into target-sized files.
Runs periodically to prevent "small file problem."
"""

import asyncio
import os
import signal
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)

# Target file size in bytes (256 MB)
TARGET_FILE_SIZE = settings.silver_target_file_size_mb * 1024 * 1024


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

    async def compact_partition(self, partition_path: Path) -> int:
        """Compact all small files in a partition.

        Returns number of files merged.
        """
        lock_path = self._acquire_partition_lock(partition_path)
        if lock_path is None:
            return 0

        parquet_files = sorted(partition_path.glob("*.parquet"))

        if len(parquet_files) <= 1:
            self._release_partition_lock(lock_path)
            return 0

        # Check total size
        total_size = sum(f.stat().st_size for f in parquet_files)

        # Only compact if we have multiple small files
        small_files = [f for f in parquet_files if f.stat().st_size < TARGET_FILE_SIZE]

        if len(small_files) <= 1:
            self._release_partition_lock(lock_path)
            return 0

        logger.info(
            "Compacting partition",
            partition=str(partition_path),
            files=len(small_files),
            total_bytes=total_size,
        )

        try:
            # Stream files into a single temp parquet, then atomically promote.
            ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            merged_path = partition_path / f"compacted-{ts}-{os.getpid()}.parquet"
            temp_path = partition_path / f".compacted-{ts}-{os.getpid()}.tmp"
            writer = None
            merged_rows = 0
            try:
                for source_file in small_files:
                    table = pq.read_table(source_file)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            temp_path,
                            table.schema,
                            compression="snappy",
                        )
                    writer.write_table(table, row_group_size=250_000)
                    merged_rows += table.num_rows
            finally:
                if writer is not None:
                    writer.close()

            # Atomic file promotion once the merge succeeds.
            temp_path.replace(merged_path)

            # Delete source files only after merged output is durable.
            for f in small_files:
                f.unlink()

            logger.info(
                "Compaction complete",
                partition=str(partition_path),
                merged_files=len(small_files),
                output_file=str(merged_path),
                rows=merged_rows,
            )

            return len(small_files)

        except Exception as e:
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

    async def scan_and_compact(self, layer: str = "silver") -> dict:
        """Scan layer for partitions that need compaction."""
        layer_path = settings.data_root / layer

        if not layer_path.exists():
            return {"partitions_scanned": 0, "files_merged": 0}

        partitions_scanned = 0
        files_merged = 0

        # Walk through all partitions (directories containing .parquet files)
        for partition_path in layer_path.rglob("*"):
            if partition_path.is_dir():
                parquet_files = list(partition_path.glob("*.parquet"))
                if parquet_files:
                    partitions_scanned += 1
                    merged = await self.compact_partition(partition_path)
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
                result = await self.scan_and_compact("silver")
                logger.info("Compaction cycle complete", **result)

                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Compactor error", error=str(e), exc_info=True)
                await asyncio.sleep(60)  # Back off on error

        logger.info("Compactor stopped")

    def stop(self):
        """Stop the compactor."""
        self.running = False


async def main():
    """Entry point for the compactor."""
    compactor = Compactor()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, compactor.stop)

    await compactor.run()


if __name__ == "__main__":
    asyncio.run(main())
