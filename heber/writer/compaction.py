"""Compaction scheduler for Heber per PRD §12.9.

Provides:
- Hourly partition compaction after close
- event_id uniqueness preservation
- ts_available immutability
- Atomic writes (temp path then rename)
"""

import asyncio
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram

from heber.bus.dedupe import dedupe_at_compaction

logger = structlog.get_logger(__name__)


# Prometheus metrics
compaction_runs = Counter(
    "heber_compaction_runs_total",
    "Total compaction runs",
    ["dataset", "status"],
)

compaction_duration = Histogram(
    "heber_compaction_duration_seconds",
    "Time to compact a partition",
    ["dataset"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

compaction_records_before = Counter(
    "heber_compaction_records_before_total",
    "Records before compaction",
    ["dataset"],
)

compaction_records_after = Counter(
    "heber_compaction_records_after_total",
    "Records after compaction",
    ["dataset"],
)

compaction_queue_size = Gauge(
    "heber_compaction_queue_size",
    "Number of partitions waiting for compaction",
)

active_compactions = Gauge(
    "heber_active_compactions",
    "Number of currently running compactions",
)


@dataclass
class CompactionConfig:
    """Compaction configuration per PRD §12.9."""
    # Time after hour close to start compaction (10 minutes)
    delay_after_close_minutes: int = 10
    # Maximum compaction window (20 minutes)
    max_compaction_window_minutes: int = 20
    # Concurrent compaction workers
    max_concurrent: int = 2
    # Temp directory for atomic writes
    temp_dir: str | None = None
    # Storage root
    storage_root: str = "/data/heber"


@dataclass
class PartitionInfo:
    """Information about a partition to compact."""
    dataset: str
    dt: str
    hour: int
    partition_path: Path
    scheduled_at: datetime
    
    @property
    def partition_id(self) -> str:
        return f"{self.dataset}/dt={self.dt}/hour={self.hour:02d}"


class AtomicWriter:
    """Atomic file writer using temp path and rename.
    
    Per PRD §12.9: Must write atomically (temp path then rename/commit)
    """
    
    def __init__(self, temp_dir: str | None = None):
        self.temp_dir = temp_dir or tempfile.gettempdir()
    
    def atomic_write(
        self,
        target_path: Path,
        data: bytes,
    ) -> None:
        """Write data atomically to target path."""
        # Create temp file in same filesystem for atomic rename
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = Path(self.temp_dir) / f"heber_compact_{os.getpid()}_{target_path.name}"
        
        try:
            # Write to temp
            with open(temp_path, "wb") as f:
                f.write(data)
            
            # Atomic rename
            shutil.move(str(temp_path), str(target_path))
            
            logger.debug("atomic_write_complete", path=str(target_path))
        except Exception:
            # Cleanup temp on failure
            if temp_path.exists():
                temp_path.unlink()
            raise
    
    def atomic_replace_directory(
        self,
        target_dir: Path,
        temp_files: list[tuple[str, bytes]],
    ) -> None:
        """Atomically replace directory contents.
        
        1. Write all files to temp directory
        2. Rename temp to target.new
        3. Rename target to target.old (if exists)
        4. Rename target.new to target
        5. Delete target.old
        """
        temp_dir = Path(self.temp_dir) / f"heber_compact_{os.getpid()}_{target_dir.name}"
        target_new = target_dir.parent / f"{target_dir.name}.new"
        target_old = target_dir.parent / f"{target_dir.name}.old"
        
        try:
            # Create temp dir and write files
            temp_dir.mkdir(parents=True, exist_ok=True)
            for filename, data in temp_files:
                with open(temp_dir / filename, "wb") as f:
                    f.write(data)
            
            # Move temp to target.new
            shutil.move(str(temp_dir), str(target_new))
            
            # Swap directories
            if target_dir.exists():
                shutil.move(str(target_dir), str(target_old))
            shutil.move(str(target_new), str(target_dir))
            
            # Cleanup old
            if target_old.exists():
                shutil.rmtree(str(target_old))
            
            logger.debug("atomic_replace_complete", path=str(target_dir))
            
        except Exception:
            # Cleanup on failure
            for path in [temp_dir, target_new]:
                if path.exists():
                    shutil.rmtree(str(path))
            raise


class ParquetCompactor:
    """Compacts Parquet files in a partition.
    
    Invariants per PRD §12.9:
    - Must preserve event_id uniqueness (via dedupe)
    - Must not change ts_available
    - Must write atomically
    """
    
    def __init__(
        self,
        writer: AtomicWriter,
        storage_root: str = "/data/heber",
    ):
        self.writer = writer
        self.storage_root = Path(storage_root)
    
    def compact_partition(
        self,
        partition: PartitionInfo,
    ) -> tuple[int, int]:
        """Compact all Parquet files in a partition.
        
        Returns:
            Tuple of (records_before, records_after)
        """
        partition_path = partition.partition_path
        
        if not partition_path.exists():
            logger.warning("partition_not_found", path=str(partition_path))
            return 0, 0
        
        # Find all Parquet files
        parquet_files = list(partition_path.glob("*.parquet"))
        if not parquet_files:
            logger.debug("no_parquet_files", path=str(partition_path))
            return 0, 0
        
        # Read all records
        all_records = []
        for pq_file in parquet_files:
            records = self._read_parquet(pq_file)
            all_records.extend(records)
        
        records_before = len(all_records)
        
        if records_before == 0:
            return 0, 0
        
        # Deduplicate: keep earliest ts_ingest per event_id
        unique_records = dedupe_at_compaction(
            records=all_records,
            partition=partition.partition_id,
            event_id_key="event_id",
            ts_ingest_key="ts_ingest",
        )
        
        records_after = len(unique_records)
        
        # Write compacted file atomically
        compacted_data = self._write_parquet_bytes(unique_records)
        compacted_filename = f"compacted_{partition.dt}_{partition.hour:02d}.parquet"
        
        # Prepare new partition contents
        temp_files = [(compacted_filename, compacted_data)]
        
        # Atomic replace
        self.writer.atomic_replace_directory(partition_path, temp_files)
        
        logger.info(
            "partition_compacted",
            partition=partition.partition_id,
            before=records_before,
            after=records_after,
            removed=records_before - records_after,
        )
        
        return records_before, records_after
    
    def _read_parquet(self, path: Path) -> list[dict[str, Any]]:
        """Read records from a Parquet file."""
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(str(path))
            return table.to_pylist()
        except ImportError:
            # Fallback for dev without pyarrow
            logger.warning("pyarrow_not_available", path=str(path))
            return []
        except Exception as e:
            logger.error("parquet_read_error", path=str(path), error=str(e))
            return []
    
    def _write_parquet_bytes(self, records: list[dict[str, Any]]) -> bytes:
        """Write records to Parquet format and return bytes."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            import io
            
            if not records:
                return b""
            
            table = pa.Table.from_pylist(records)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            return buffer.getvalue()
        except ImportError:
            # Fallback for dev without pyarrow
            logger.warning("pyarrow_not_available")
            return b""


class CompactionScheduler:
    """Schedules and runs compaction tasks per PRD §12.9.
    
    Default policy:
    - Compact hourly partitions after they close
    - Example: compact dt=YYYY-MM-DD/hour=18 at 18:10-18:30
    """
    
    def __init__(
        self,
        config: CompactionConfig | None = None,
    ):
        self.config = config or CompactionConfig()
        self.writer = AtomicWriter(self.config.temp_dir)
        self.compactor = ParquetCompactor(self.writer, self.config.storage_root)
        
        self._queue: asyncio.Queue[PartitionInfo] = asyncio.Queue()
        self._running = False
        self._workers: list[asyncio.Task] = []
        self._active_count = 0
    
    def schedule_partition(self, partition: PartitionInfo) -> None:
        """Add a partition to the compaction queue."""
        self._queue.put_nowait(partition)
        compaction_queue_size.set(self._queue.qsize())
        
        logger.info(
            "partition_scheduled",
            partition=partition.partition_id,
            scheduled_at=partition.scheduled_at.isoformat(),
        )
    
    def get_partitions_to_compact(
        self,
        datasets: list[str],
        storage_root: Path | None = None,
    ) -> list[PartitionInfo]:
        """Find partitions ready for compaction.
        
        Returns partitions where:
        - Hour has closed (current time > hour + delay_after_close_minutes)
        - Not yet compacted
        """
        root = storage_root or Path(self.config.storage_root)
        now = datetime.now(UTC)
        partitions = []
        
        for dataset in datasets:
            dataset_path = root / "silver" / dataset
            if not dataset_path.exists():
                continue
            
            # Find dt partitions
            for dt_dir in dataset_path.glob("dt=*"):
                dt_str = dt_dir.name.replace("dt=", "")
                
                # Find hour partitions
                for hour_dir in dt_dir.glob("hour=*"):
                    try:
                        hour = int(hour_dir.name.replace("hour=", ""))
                    except ValueError:
                        continue
                    
                    # Check if hour has closed + delay passed
                    partition_close = datetime.fromisoformat(f"{dt_str}T{hour+1:02d}:00:00+00:00")
                    compact_start = partition_close + timedelta(
                        minutes=self.config.delay_after_close_minutes
                    )
                    
                    if now >= compact_start:
                        # Check if already compacted
                        if not self._is_compacted(hour_dir):
                            partitions.append(PartitionInfo(
                                dataset=dataset,
                                dt=dt_str,
                                hour=hour,
                                partition_path=hour_dir,
                                scheduled_at=now,
                            ))
        
        return partitions
    
    def _is_compacted(self, partition_path: Path) -> bool:
        """Check if partition is already compacted."""
        compacted_files = list(partition_path.glob("compacted_*.parquet"))
        return len(compacted_files) > 0
    
    async def _worker(self, worker_id: int) -> None:
        """Compaction worker coroutine."""
        while self._running:
            try:
                # Wait for work with timeout
                try:
                    partition = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    continue
                
                self._active_count += 1
                active_compactions.set(self._active_count)
                compaction_queue_size.set(self._queue.qsize())
                
                try:
                    # Run compaction with timing
                    start_time = asyncio.get_event_loop().time()
                    
                    before, after = self.compactor.compact_partition(partition)
                    
                    duration = asyncio.get_event_loop().time() - start_time
                    
                    # Update metrics
                    compaction_duration.labels(dataset=partition.dataset).observe(duration)
                    compaction_records_before.labels(dataset=partition.dataset).inc(before)
                    compaction_records_after.labels(dataset=partition.dataset).inc(after)
                    compaction_runs.labels(dataset=partition.dataset, status="success").inc()
                    
                except Exception as e:
                    logger.error(
                        "compaction_failed",
                        partition=partition.partition_id,
                        error=str(e),
                        exc_info=True,
                    )
                    compaction_runs.labels(dataset=partition.dataset, status="error").inc()
                
                finally:
                    self._active_count -= 1
                    active_compactions.set(self._active_count)
                    self._queue.task_done()
                
            except asyncio.CancelledError:
                break
    
    async def start(self) -> None:
        """Start the compaction scheduler."""
        if self._running:
            return
        
        self._running = True
        
        # Start workers
        for i in range(self.config.max_concurrent):
            task = asyncio.create_task(self._worker(i))
            self._workers.append(task)
        
        logger.info(
            "compaction_scheduler_started",
            workers=self.config.max_concurrent,
        )
    
    async def stop(self) -> None:
        """Stop the compaction scheduler gracefully."""
        self._running = False
        
        # Wait for queue to drain
        await self._queue.join()
        
        # Cancel workers
        for task in self._workers:
            task.cancel()
        
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        
        logger.info("compaction_scheduler_stopped")
    
    async def run_once(self, datasets: list[str]) -> int:
        """Run a single compaction cycle.
        
        Finds and compacts all ready partitions.
        
        Returns:
            Number of partitions compacted
        """
        partitions = self.get_partitions_to_compact(datasets)
        
        for partition in partitions:
            self.schedule_partition(partition)
        
        # Wait for all to complete
        await self._queue.join()
        
        return len(partitions)


async def run_scheduled_compaction(
    datasets: list[str],
    config: CompactionConfig | None = None,
    check_interval_seconds: int = 60,
) -> None:
    """Run compaction on a schedule.
    
    Args:
        datasets: List of dataset names to compact
        config: Compaction configuration
        check_interval_seconds: How often to check for ready partitions
    """
    scheduler = CompactionScheduler(config)
    await scheduler.start()
    
    try:
        while True:
            compacted = await scheduler.run_once(datasets)
            if compacted > 0:
                logger.info("compaction_cycle_complete", partitions=compacted)
            
            await asyncio.sleep(check_interval_seconds)
    except asyncio.CancelledError:
        await scheduler.stop()
