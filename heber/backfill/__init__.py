"""Backfill pipeline for Heber per PRD §13.

Provides:
- Backfill job definition and management
- Backfill coordinator with chunking and rate limiting
- Backfill writer with quality_flags tagging
- ts_available = ts_commit rule for historical data
- Gap detection and recovery
"""

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, UTC
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger(__name__)


# Prometheus metrics
backfill_jobs_total = Counter(
    "heber_backfill_jobs_total",
    "Total backfill jobs",
    ["provider", "feed", "status"],
)

backfill_rows_written = Counter(
    "heber_backfill_rows_written_total",
    "Rows written by backfill",
    ["provider", "feed"],
)

backfill_files_written = Counter(
    "heber_backfill_files_written_total",
    "Files written by backfill",
    ["provider", "feed"],
)

backfill_active_jobs = Gauge(
    "heber_backfill_active_jobs",
    "Currently running backfill jobs",
)

backfill_progress_percent = Gauge(
    "heber_backfill_progress_percent",
    "Progress of active backfill job",
    ["backfill_id"],
)

backfill_duration_seconds = Histogram(
    "heber_backfill_duration_seconds",
    "Duration of backfill jobs",
    ["provider", "feed"],
    buckets=[60, 300, 600, 1800, 3600, 7200, 14400, 28800],
)


class BackfillStatus(str, Enum):
    """Backfill job status."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TsAvailablePolicy(str, Enum):
    """Policy for setting ts_available on backfill data per PRD §13.3."""
    COMMIT = "commit"    # ts_available = ts_commit (default, recommended)
    EVENT = "event"      # ts_available = ts_event (opt-in, documented)
    CUSTOM = "custom"    # ts_available = ts_event + delay (opt-in)


@dataclass
class BackfillJobDefinition:
    """Backfill job definition per PRD §13.4.1."""
    provider: str
    feed: str
    date_range_start: date
    date_range_end: date
    symbols: list[str] | None = None  # None = all symbols
    ts_available_policy: TsAvailablePolicy = TsAvailablePolicy.COMMIT
    custom_delay_seconds: int | None = None  # For CUSTOM policy
    rate_limit_per_second: float = 10.0
    batch_size: int = 1000
    priority: int = 10  # Lower = higher priority


@dataclass
class BackfillJob:
    """Backfill job state per PRD §13.5."""
    backfill_id: str
    provider: str
    feed: str
    date_range_start: date
    date_range_end: date
    ts_available_policy: TsAvailablePolicy
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_written: int = 0
    files_written: int = 0
    status: BackfillStatus = BackfillStatus.PENDING
    error_message: str | None = None
    progress_dates_completed: list[str] = field(default_factory=list)
    symbols: list[str] | None = None
    
    @classmethod
    def from_definition(cls, definition: BackfillJobDefinition) -> "BackfillJob":
        return cls(
            backfill_id=str(uuid.uuid4()),
            provider=definition.provider,
            feed=definition.feed,
            date_range_start=definition.date_range_start,
            date_range_end=definition.date_range_end,
            ts_available_policy=definition.ts_available_policy,
            symbols=definition.symbols,
        )
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "backfill_id": self.backfill_id,
            "provider": self.provider,
            "feed": self.feed,
            "date_range_start": self.date_range_start.isoformat(),
            "date_range_end": self.date_range_end.isoformat(),
            "ts_available_policy": self.ts_available_policy.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "rows_written": self.rows_written,
            "files_written": self.files_written,
            "status": self.status.value,
            "error_message": self.error_message,
            "symbols": self.symbols,
        }


@dataclass
class BackfillChunk:
    """A chunk of work for backfill."""
    backfill_id: str
    chunk_date: date
    symbols: list[str] | None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    rows_written: int = 0


class GapDetector:
    """Detects gaps in data coverage for backfill targeting."""
    
    def __init__(self, storage_root: str = "/data/heber"):
        self.storage_root = Path(storage_root)
    
    def detect_gaps(
        self,
        provider: str,
        feed: str,
        start_date: date,
        end_date: date,
    ) -> list[tuple[date, date]]:
        """Detect gaps in data coverage.
        
        Returns list of (gap_start, gap_end) date ranges.
        """
        silver_path = self.storage_root / "silver" / f"{provider}_{feed}"
        
        if not silver_path.exists():
            # No data at all - entire range is a gap
            return [(start_date, end_date)]
        
        # Find existing dates
        existing_dates = set()
        for dt_dir in silver_path.glob("dt=*"):
            try:
                dt_str = dt_dir.name.replace("dt=", "")
                existing_dates.add(date.fromisoformat(dt_str))
            except ValueError:
                continue
        
        # Find gaps
        gaps = []
        current_gap_start = None
        current = start_date
        
        while current <= end_date:
            if current not in existing_dates:
                if current_gap_start is None:
                    current_gap_start = current
            else:
                if current_gap_start is not None:
                    gaps.append((current_gap_start, current - timedelta(days=1)))
                    current_gap_start = None
            current += timedelta(days=1)
        
        # Close final gap if open
        if current_gap_start is not None:
            gaps.append((current_gap_start, end_date))
        
        return gaps
    
    def get_coverage_summary(
        self,
        provider: str,
        feed: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        """Get coverage summary for a date range."""
        gaps = self.detect_gaps(provider, feed, start_date, end_date)
        
        total_days = (end_date - start_date).days + 1
        gap_days = sum((g[1] - g[0]).days + 1 for g in gaps)
        covered_days = total_days - gap_days
        
        return {
            "provider": provider,
            "feed": feed,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_days": total_days,
            "covered_days": covered_days,
            "gap_days": gap_days,
            "coverage_percent": (covered_days / total_days * 100) if total_days > 0 else 0,
            "gaps": [(g[0].isoformat(), g[1].isoformat()) for g in gaps],
        }


class BackfillWriter:
    """Writes backfill data per PRD §13.4.3.
    
    - Writes directly to Bronze/Silver (bypasses event bus)
    - Tags records: quality_flags += ["backfill"]
    - Updates Catalog coverage
    """
    
    def __init__(
        self,
        storage_root: str = "/data/heber",
        ts_available_policy: TsAvailablePolicy = TsAvailablePolicy.COMMIT,
        custom_delay_seconds: int | None = None,
    ):
        self.storage_root = Path(storage_root)
        self.ts_available_policy = ts_available_policy
        self.custom_delay_seconds = custom_delay_seconds
    
    def set_ts_available(
        self,
        record: dict[str, Any],
        ts_commit: datetime,
    ) -> dict[str, Any]:
        """Set ts_available based on policy per PRD §13.3."""
        if self.ts_available_policy == TsAvailablePolicy.COMMIT:
            # Default: ts_available = ts_commit
            record["ts_available"] = ts_commit
        elif self.ts_available_policy == TsAvailablePolicy.EVENT:
            # Opt-in: ts_available = ts_event
            record["ts_available"] = record.get("ts_event", ts_commit)
        elif self.ts_available_policy == TsAvailablePolicy.CUSTOM:
            # Opt-in: ts_available = ts_event + delay
            ts_event = record.get("ts_event", ts_commit)
            delay = timedelta(seconds=self.custom_delay_seconds or 0)
            record["ts_available"] = ts_event + delay
        
        return record
    
    def tag_as_backfill(self, record: dict[str, Any]) -> dict[str, Any]:
        """Tag record with backfill quality flag per PRD §13.4.3."""
        flags = record.get("quality_flags", [])
        if "backfill" not in flags:
            flags.append("backfill")
        record["quality_flags"] = flags
        return record
    
    async def write_batch(
        self,
        job: BackfillJob,
        records: list[dict[str, Any]],
        chunk_date: date,
    ) -> int:
        """Write a batch of backfill records.
        
        Writes to temp partition per PRD §13.6, then merges.
        """
        if not records:
            return 0
        
        ts_commit = datetime.now(UTC)
        
        # Process records
        processed = []
        for record in records:
            record = self.tag_as_backfill(record)
            record = self.set_ts_available(record, ts_commit)
            record["ts_ingest"] = ts_commit
            record["backfill_id"] = job.backfill_id
            processed.append(record)
        
        # Write to temp partition per PRD §13.6
        temp_path = self._get_temp_path(job, chunk_date)
        await self._write_parquet(processed, temp_path)
        
        # Atomic merge will be handled by compactor
        logger.info(
            "backfill_batch_written",
            backfill_id=job.backfill_id,
            date=chunk_date.isoformat(),
            rows=len(processed),
        )
        
        return len(processed)
    
    def _get_temp_path(self, job: BackfillJob, chunk_date: date) -> Path:
        """Get temp partition path for backfill isolation."""
        return (
            self.storage_root / "silver" / f"{job.provider}_{job.feed}" /
            f"dt={chunk_date.isoformat()}" / f"_backfill_{job.backfill_id}"
        )
    
    async def _write_parquet(
        self,
        records: list[dict[str, Any]],
        path: Path,
    ) -> None:
        """Write records to Parquet file."""
        path.mkdir(parents=True, exist_ok=True)
        output_file = path / f"{uuid.uuid4()}.parquet"
        
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            table = pa.Table.from_pylist(records)
            pq.write_table(table, str(output_file))
        except ImportError:
            logger.warning("pyarrow_not_available")


class BackfillCoordinator:
    """Coordinates backfill jobs per PRD §13.4.2.
    
    - Chunks work by date/symbol
    - Rate limits API calls
    - Tracks progress (resumable)
    """
    
    def __init__(
        self,
        storage_root: str = "/data/heber",
        data_fetcher: Callable | None = None,
    ):
        self.storage_root = storage_root
        self.data_fetcher = data_fetcher
        self._jobs: dict[str, BackfillJob] = {}
        self._active_job: str | None = None
    
    def create_job(self, definition: BackfillJobDefinition) -> BackfillJob:
        """Create a new backfill job."""
        job = BackfillJob.from_definition(definition)
        self._jobs[job.backfill_id] = job
        
        backfill_jobs_total.labels(
            provider=job.provider,
            feed=job.feed,
            status="created",
        ).inc()
        
        logger.info(
            "backfill_job_created",
            backfill_id=job.backfill_id,
            provider=job.provider,
            feed=job.feed,
            date_range=f"{job.date_range_start} to {job.date_range_end}",
        )
        
        return job
    
    def get_job(self, backfill_id: str) -> BackfillJob | None:
        """Get a backfill job by ID."""
        return self._jobs.get(backfill_id)
    
    def list_jobs(
        self,
        status: BackfillStatus | None = None,
    ) -> list[BackfillJob]:
        """List backfill jobs, optionally filtered by status."""
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        return sorted(jobs, key=lambda j: j.started_at or datetime.min, reverse=True)
    
    def _generate_chunks(
        self,
        job: BackfillJob,
        definition: BackfillJobDefinition,
    ) -> list[BackfillChunk]:
        """Generate work chunks for a backfill job."""
        chunks = []
        current = job.date_range_start
        
        while current <= job.date_range_end:
            # Skip already completed dates (for resume)
            if current.isoformat() not in job.progress_dates_completed:
                chunks.append(BackfillChunk(
                    backfill_id=job.backfill_id,
                    chunk_date=current,
                    symbols=job.symbols,
                ))
            current += timedelta(days=1)
        
        return chunks
    
    async def run_job(
        self,
        backfill_id: str,
        definition: BackfillJobDefinition,
    ) -> BackfillJob:
        """Run a backfill job to completion."""
        job = self._jobs.get(backfill_id)
        if not job:
            raise ValueError(f"Job not found: {backfill_id}")
        
        if job.status == BackfillStatus.RUNNING:
            raise ValueError(f"Job already running: {backfill_id}")
        
        # Start job
        job.status = BackfillStatus.RUNNING
        job.started_at = datetime.now(UTC)
        self._active_job = backfill_id
        backfill_active_jobs.inc()
        
        writer = BackfillWriter(
            storage_root=self.storage_root,
            ts_available_policy=job.ts_available_policy,
            custom_delay_seconds=definition.custom_delay_seconds,
        )
        
        chunks = self._generate_chunks(job, definition)
        total_chunks = len(chunks)
        
        try:
            for i, chunk in enumerate(chunks):
                # Fetch data
                if self.data_fetcher:
                    records = await self.data_fetcher(
                        provider=job.provider,
                        feed=job.feed,
                        date=chunk.chunk_date,
                        symbols=chunk.symbols,
                    )
                else:
                    records = []
                
                # Write with rate limiting
                await asyncio.sleep(1.0 / definition.rate_limit_per_second)
                
                rows = await writer.write_batch(job, records, chunk.chunk_date)
                
                # Update progress
                job.rows_written += rows
                job.files_written += 1
                job.progress_dates_completed.append(chunk.chunk_date.isoformat())
                
                progress = (i + 1) / total_chunks * 100
                backfill_progress_percent.labels(backfill_id=backfill_id).set(progress)
                
                # Update metrics
                backfill_rows_written.labels(
                    provider=job.provider,
                    feed=job.feed,
                ).inc(rows)
                backfill_files_written.labels(
                    provider=job.provider,
                    feed=job.feed,
                ).inc()
            
            # Complete job
            job.status = BackfillStatus.COMPLETED
            job.completed_at = datetime.now(UTC)
            
            duration = (job.completed_at - job.started_at).total_seconds()
            backfill_duration_seconds.labels(
                provider=job.provider,
                feed=job.feed,
            ).observe(duration)
            
            backfill_jobs_total.labels(
                provider=job.provider,
                feed=job.feed,
                status="completed",
            ).inc()
            
            logger.info(
                "backfill_job_completed",
                backfill_id=backfill_id,
                rows=job.rows_written,
                files=job.files_written,
                duration_seconds=duration,
            )
            
        except Exception as e:
            job.status = BackfillStatus.FAILED
            job.error_message = str(e)
            
            backfill_jobs_total.labels(
                provider=job.provider,
                feed=job.feed,
                status="failed",
            ).inc()
            
            logger.error(
                "backfill_job_failed",
                backfill_id=backfill_id,
                error=str(e),
                exc_info=True,
            )
            raise
        
        finally:
            self._active_job = None
            backfill_active_jobs.dec()
        
        return job
    
    def cancel_job(self, backfill_id: str) -> BackfillJob:
        """Cancel a running backfill job."""
        job = self._jobs.get(backfill_id)
        if not job:
            raise ValueError(f"Job not found: {backfill_id}")
        
        if job.status == BackfillStatus.RUNNING:
            job.status = BackfillStatus.CANCELLED
            job.completed_at = datetime.now(UTC)
        
        return job


# FastAPI router for backfill API per PRD §11.7

def create_backfill_router():
    """Create FastAPI router for backfill endpoints."""
    from fastapi import APIRouter, HTTPException
    from pydantic import BaseModel
    
    router = APIRouter(prefix="/backfill", tags=["backfill"])
    coordinator = BackfillCoordinator()
    gap_detector = GapDetector()
    
    class BackfillRequest(BaseModel):
        """Request body for creating a backfill job."""
        provider: str
        feed: str
        date_range_start: date
        date_range_end: date
        symbols: list[str] | None = None
        ts_available_policy: str = "commit"
        rate_limit_per_second: float = 10.0
    
    class GapDetectionRequest(BaseModel):
        """Request body for gap detection."""
        provider: str
        feed: str
        start_date: date
        end_date: date
    
    @router.post("")
    async def create_backfill(request: BackfillRequest) -> dict:
        """Create a new backfill job (POST /backfill)."""
        try:
            policy = TsAvailablePolicy(request.ts_available_policy)
        except ValueError:
            policy = TsAvailablePolicy.COMMIT
        
        definition = BackfillJobDefinition(
            provider=request.provider,
            feed=request.feed,
            date_range_start=request.date_range_start,
            date_range_end=request.date_range_end,
            symbols=request.symbols,
            ts_available_policy=policy,
            rate_limit_per_second=request.rate_limit_per_second,
        )
        
        job = coordinator.create_job(definition)
        
        # Start job in background
        asyncio.create_task(coordinator.run_job(job.backfill_id, definition))
        
        return job.to_dict()
    
    @router.get("/{backfill_id}")
    async def get_backfill(backfill_id: str) -> dict:
        """Get backfill job status (GET /backfill/{id})."""
        job = coordinator.get_job(backfill_id)
        if not job:
            raise HTTPException(status_code=404, detail="Backfill job not found")
        return job.to_dict()
    
    @router.get("")
    async def list_backfills(status: str | None = None) -> list[dict]:
        """List backfill jobs (GET /backfill)."""
        filter_status = BackfillStatus(status) if status else None
        jobs = coordinator.list_jobs(status=filter_status)
        return [j.to_dict() for j in jobs]
    
    @router.delete("/{backfill_id}")
    async def cancel_backfill(backfill_id: str) -> dict:
        """Cancel a backfill job."""
        try:
            job = coordinator.cancel_job(backfill_id)
            return job.to_dict()
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
    
    @router.post("/gaps")
    async def detect_gaps(request: GapDetectionRequest) -> dict:
        """Detect gaps in data coverage."""
        return gap_detector.get_coverage_summary(
            provider=request.provider,
            feed=request.feed,
            start_date=request.start_date,
            end_date=request.end_date,
        )
    
    return router
