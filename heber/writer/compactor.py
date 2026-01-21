"""Parquet file compactor.

Merges small Parquet files into target-sized files.
Runs periodically to prevent "small file problem."
"""

import asyncio
import signal
from datetime import datetime
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

    async def compact_partition(self, partition_path: Path) -> int:
        """Compact all small files in a partition.

        Returns number of files merged.
        """
        parquet_files = sorted(partition_path.glob("*.parquet"))

        if len(parquet_files) <= 1:
            return 0

        # Check total size
        total_size = sum(f.stat().st_size for f in parquet_files)

        # Only compact if we have multiple small files
        small_files = [f for f in parquet_files if f.stat().st_size < TARGET_FILE_SIZE]

        if len(small_files) <= 1:
            return 0

        logger.info(
            "Compacting partition",
            partition=str(partition_path),
            files=len(small_files),
            total_bytes=total_size,
        )

        try:
            # Read all small files
            tables = []
            for f in small_files:
                table = pq.read_table(f)
                tables.append(table)

            # Concatenate
            import pyarrow as pa

            merged_table = pa.concat_tables(tables)

            # Write merged file
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            merged_path = partition_path / f"compacted-{ts}.parquet"

            pq.write_table(
                merged_table,
                merged_path,
                compression="snappy",
                row_group_size=250_000,
            )

            # Delete original small files
            for f in small_files:
                f.unlink()

            logger.info(
                "Compaction complete",
                partition=str(partition_path),
                merged_files=len(small_files),
                output_file=str(merged_path),
                rows=merged_table.num_rows,
            )

            return len(small_files)

        except Exception as e:
            logger.error(
                "Compaction failed",
                partition=str(partition_path),
                error=str(e),
                exc_info=True,
            )
            return 0

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
