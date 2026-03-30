"""Catalog REST API - FastAPI routes.

See PRD Section 11.7 for API contract.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from heber.catalog.db import Base
from heber.catalog.seeds import (
    discover_datasets_from_disk,
    seed_coverage_from_disk,
    seed_datasets,
    seed_feed_mappings,
    seed_schema_versions,
)
from heber.catalog.service import CatalogService
from heber.config import settings
from heber.ops.logging import configure_logging
from heber.ops.metrics import start_metrics_server_from_env

logger = structlog.get_logger(__name__)

# Database setup — echo=True blocks the async event loop with synchronous logging.
# Use pool_pre_ping to detect stale connections after container restarts.
engine = create_async_engine(
    settings.postgres_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=10,
    pool_recycle=300,
)
async_session = async_sessionmaker(engine, expire_on_commit=False)


def _should_auto_create_catalog_tables() -> bool:
    """Keep SQLAlchemy create_all for local dev only.

    Non-dev environments should use Alembic migrations instead.
    """
    return settings.environment == "dev"


async def _periodic_discovery_loop() -> None:
    """Background loop that periodically re-scans Silver for new datasets and coverage."""
    interval = settings.catalog_discover_interval_seconds

    # Run an initial scan immediately (non-blocking to startup)
    try:
        async with async_session() as session:
            await seed_coverage_from_disk(session)
            logger.info("catalog_initial_coverage_scan_done")
    except Exception:
        logger.exception("catalog_initial_coverage_scan_error")

    while True:
        await asyncio.sleep(interval)
        try:
            async with async_session() as session:
                discovered = await discover_datasets_from_disk(session)
                if discovered:
                    logger.info("catalog_periodic_discovery", new_datasets=discovered)
                await seed_coverage_from_disk(session)
        except Exception:
            logger.exception("catalog_periodic_scan_error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Initialize logging first
    configure_logging(service_name="heber-catalog", log_level=settings.log_level, json_output=True)

    if settings.metrics_port:
        start_metrics_server_from_env(default_port=9090)

    if _should_auto_create_catalog_tables():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("catalog_schema_bootstrap_applied", mode="sqlalchemy_create_all", environment=settings.environment)

        async with async_session() as session:
            await seed_datasets(session)
            await seed_schema_versions(session)
            await seed_feed_mappings(session)
        logger.info("catalog_data_seeded")
    else:
        logger.info("catalog_schema_bootstrap_skipped", reason="non_dev_environment", environment=settings.environment)

    # Auto-discovery runs in all environments (idempotent, fast)
    discovery_task = None
    if settings.catalog_auto_discover:
        async with async_session() as session:
            discovered = await discover_datasets_from_disk(session)
            logger.info("catalog_auto_discovery_done", new_datasets=discovered)

        # Background task handles periodic discovery + coverage scanning
        if settings.catalog_discover_interval_seconds > 0:
            discovery_task = asyncio.create_task(_periodic_discovery_loop())
            logger.info(
                "catalog_periodic_scan_started",
                interval_seconds=settings.catalog_discover_interval_seconds,
            )

    yield

    if discovery_task is not None:
        discovery_task.cancel()
        try:
            await discovery_task
        except asyncio.CancelledError:
            logger.info("catalog_periodic_scan_stopped")


app = FastAPI(
    title="Heber Catalog API",
    description="Dataset and instrument registry for Heber Data Lakehouse",
    version="0.1.0",
    lifespan=lifespan,
)


# Dependency for database session
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session


def get_service(session: AsyncSession = Depends(get_session)) -> CatalogService:
    return CatalogService(session)


# Response models
class MetaResponse(BaseModel):
    request_id: str | None = None
    ts: datetime


class DatasetResponse(BaseModel):
    dataset_name: str
    layer: str
    owner: str
    description: str | None
    storage_root: str
    path_template: str | None
    partition_cols: list[str] | None
    is_active: bool


class DatasetListResponse(BaseModel):
    data: list[DatasetResponse]
    meta: MetaResponse


class DatasetDetailResponse(BaseModel):
    data: DatasetResponse
    meta: MetaResponse


class InstrumentResponse(BaseModel):
    instrument_key: str
    instrument_type: str
    canonical_symbol: str
    underlying_key: str | None
    occ_symbol: str | None
    expiry: datetime | None
    strike: float | None
    put_call: str | None


class InstrumentLookupRequest(BaseModel):
    symbols: list[str]


class FeedMappingResponse(BaseModel):
    provider: str
    gateway_feed: str
    silver_dataset_name: str


# Health check
@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy", "service": "heber-catalog"}


# Dataset endpoints
@app.get("/datasets", response_model=DatasetListResponse, include_in_schema=False)
@app.get("/api/v1/datasets", response_model=DatasetListResponse)
async def list_datasets(
    layer: str | None = Query(None, description="Filter by layer (bronze|silver|gold)"),
    service: CatalogService = Depends(get_service),
) -> DatasetListResponse:
    datasets = await service.list_datasets(layer=layer)
    return DatasetListResponse(
        data=[
            DatasetResponse(
                dataset_name=d.dataset_name,
                layer=d.layer,
                owner=d.owner,
                description=d.description,
                storage_root=d.storage_root,
                path_template=d.path_template,
                partition_cols=d.partition_cols,
                is_active=d.is_active,
            )
            for d in datasets
        ],
        meta=MetaResponse(ts=datetime.now(UTC)),
    )


@app.get("/datasets/{name}", response_model=DatasetDetailResponse, include_in_schema=False)
@app.get("/api/v1/datasets/{name}", response_model=DatasetDetailResponse)
async def get_dataset(name: str, service: CatalogService = Depends(get_service)) -> DatasetDetailResponse:
    dataset = await service.get_dataset(name)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    return DatasetDetailResponse(
        data=DatasetResponse(
            dataset_name=dataset.dataset_name,
            layer=dataset.layer,
            owner=dataset.owner,
            description=dataset.description,
            storage_root=dataset.storage_root,
            path_template=dataset.path_template,
            partition_cols=dataset.partition_cols,
            is_active=dataset.is_active,
        ),
        meta=MetaResponse(ts=datetime.now(UTC)),
    )


@app.get("/datasets/{name}/versions", include_in_schema=False)
@app.get("/api/v1/datasets/{name}/versions")
async def get_dataset_versions(name: str, service: CatalogService = Depends(get_service)) -> dict[str, Any]:
    versions = await service.get_dataset_versions(name)
    return {
        "data": [
            {
                "schema_version": v.schema_version,
                "schema_json": v.schema_json,
                "is_current": v.is_current,
                "created_at": v.created_at,
            }
            for v in versions
        ],
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/datasets/{name}/coverage", include_in_schema=False)
@app.get("/api/v1/datasets/{name}/coverage")
async def get_dataset_coverage(name: str, service: CatalogService = Depends(get_service)) -> dict[str, Any]:
    coverage = await service.get_coverage(name)
    data = []
    for c in coverage:
        record: dict[str, Any] = {
            "instrument_key": c.instrument_key,
            "dt_min": c.dt_min,
            "dt_max": c.dt_max,
            "approx_row_count": c.approx_row_count,
        }
        # Per-date records (instrument_key="dt:YYYY-MM-DD") also
        # expose a flat "dt" field for downstream day-counting.
        if c.instrument_key and c.instrument_key.startswith("dt:"):
            record["dt"] = str(c.dt_min)
        data.append(record)
    return {
        "data": data,
        "meta": {"ts": datetime.now(UTC)},
    }


# Instrument endpoints
def _instrument_response(inst: Any) -> InstrumentResponse:
    """Build an InstrumentResponse from a DB model instance."""
    return InstrumentResponse(
        instrument_key=inst.instrument_key,
        instrument_type=inst.instrument_type,
        canonical_symbol=inst.canonical_symbol,
        underlying_key=inst.underlying_key,
        occ_symbol=inst.occ_symbol,
        expiry=inst.expiry,
        strike=inst.strike,
        put_call=inst.put_call,
    )


@app.get("/api/v1/instruments/search")
async def search_instruments(
    instrument_type: str | None = Query(None),
    symbol_prefix: str | None = Query(None),
    limit: int = Query(100, le=1000),
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    instruments = await service.search_instruments(
        instrument_type=instrument_type,
        symbol_prefix=symbol_prefix,
        limit=limit,
    )
    return {
        "data": [_instrument_response(i) for i in instruments],
        "meta": {"ts": datetime.now(UTC)},
    }


@app.post("/api/v1/instruments/lookup")
async def lookup_instruments(
    request: InstrumentLookupRequest,
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    instruments = await service.lookup_instruments(request.symbols)
    return {
        "data": [_instrument_response(i) for i in instruments],
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/api/v1/instruments/{key}")
async def get_instrument(key: str, service: CatalogService = Depends(get_service)) -> dict[str, Any]:
    instrument = await service.get_instrument(key)
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{key}' not found")
    return {
        "data": _instrument_response(instrument),
        "meta": {"ts": datetime.now(UTC)},
    }


# Feed mapping endpoints
@app.get("/feeds", include_in_schema=False)
@app.get("/api/v1/feeds")
async def list_feeds(service: CatalogService = Depends(get_service)) -> dict[str, Any]:
    mappings = await service.list_feed_mappings()
    return {
        "data": [
            FeedMappingResponse(
                provider=m.provider,
                gateway_feed=m.gateway_feed,
                silver_dataset_name=m.silver_dataset_name,
            )
            for m in mappings
        ],
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/feeds/resolve", include_in_schema=False)
@app.get("/api/v1/feeds/resolve")
async def resolve_feed(
    provider: str = Query(...),
    feed: str = Query(...),
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    silver_dataset = await service.resolve_feed(provider, feed)
    if not silver_dataset:
        raise HTTPException(
            status_code=404,
            detail=f"No mapping found for provider='{provider}', feed='{feed}'",
        )
    return {
        "data": {"silver_dataset_name": silver_dataset},
        "meta": {"ts": datetime.now(UTC)},
    }


# Additional Dataset endpoints (PRD §11.7.3)
@app.get("/datasets/{name}/versions/{version}", include_in_schema=False)
@app.get("/api/v1/datasets/{name}/versions/{version}")
async def get_dataset_version(
    name: str,
    version: str,
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    """Get specific schema version for a dataset."""
    versions = await service.get_dataset_versions(name)
    target = next((v for v in versions if v.schema_version == version), None)
    if not target:
        raise HTTPException(
            status_code=404,
            detail=f"Version '{version}' not found for dataset '{name}'",
        )
    return {
        "data": {
            "schema_version": target.schema_version,
            "schema_json": target.schema_json,
            "is_current": target.is_current,
            "created_at": target.created_at,
        },
        "meta": {"ts": datetime.now(UTC)},
    }


class DatasetCreateRequest(BaseModel):
    """Request to create a new dataset."""

    dataset_name: str
    layer: str
    owner: str = "shared"
    description: str | None = None
    storage_root: str
    path_template: str | None = None
    partition_cols: list[str] | None = None
    primary_keys: list[str] | None = None


@app.post("/datasets", status_code=201, include_in_schema=False)
@app.post("/api/v1/datasets", status_code=201)
async def create_dataset(
    request: DatasetCreateRequest,
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    """Create a new dataset in the catalog."""
    try:
        dataset = await service.create_dataset(
            name=request.dataset_name,
            layer=request.layer,
            owner=request.owner,
            description=request.description,
            storage_root=request.storage_root,
            path_template=request.path_template,
            partition_cols=request.partition_cols,
            primary_keys=request.primary_keys,
        )
    except Exception as exc:
        if "IntegrityError" in type(exc).__name__ or "UniqueViolation" in str(type(exc).__mro__):
            raise HTTPException(status_code=409, detail=f"Dataset '{request.dataset_name}' already exists")
        raise
    return {
        "data": {"dataset_name": dataset.dataset_name},
        "meta": {"ts": datetime.now(UTC)},
    }


class InstrumentUpsertRequest(BaseModel):
    """Request to upsert an instrument."""

    instrument_key: str
    instrument_type: str
    canonical_symbol: str
    underlying_key: str | None = None
    occ_symbol: str | None = None
    expiry: datetime | None = None
    strike: float | None = None
    put_call: str | None = None


@app.put("/api/v1/instruments/{key}")
async def upsert_instrument(
    key: str,
    request: InstrumentUpsertRequest,
    service: CatalogService = Depends(get_service),
) -> dict[str, Any]:
    """Upsert an instrument in the registry."""
    instrument = await service.upsert_instrument(
        instrument_key=key,
        instrument_type=request.instrument_type,
        canonical_symbol=request.canonical_symbol,
        underlying_key=request.underlying_key,
        occ_symbol=request.occ_symbol,
        expiry=request.expiry,
        strike=request.strike,
        put_call=request.put_call,
    )
    return {
        "data": {"instrument_key": instrument.instrument_key},
        "meta": {"ts": datetime.now(UTC)},
    }


# Backfill endpoints (PRD §11.7.3)
class BackfillRequest(BaseModel):
    """Request to create a backfill job."""

    provider: str
    feed: str
    instrument_keys: list[str]
    start_date: str
    end_date: str
    project: str | None = None


# In-memory backfill jobs (use Redis/DB in production)
_backfill_jobs: dict[str, dict[str, Any]] = {}


@app.post("/api/v1/backfill", status_code=201)
async def create_backfill(request: BackfillRequest) -> dict[str, Any]:
    """Create a new backfill job."""
    from uuid import uuid4

    job_id = str(uuid4())
    _backfill_jobs[job_id] = {
        "id": job_id,
        "provider": request.provider,
        "feed": request.feed,
        "instrument_keys": request.instrument_keys,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "project": request.project,
        "status": "pending",
        "created_at": datetime.now(UTC).isoformat(),
    }
    return {
        "data": {"backfill_id": job_id, "status": "pending"},
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/api/v1/backfill/{id}")
async def get_backfill(id: str) -> dict[str, Any]:
    """Get backfill job status."""
    job = _backfill_jobs.get(id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Backfill job '{id}' not found")
    return {
        "data": job,
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/api/v1/backfill")
async def list_backfills(
    status: str | None = Query(None),
    limit: int = Query(50, le=100),
) -> dict[str, Any]:
    """List backfill jobs."""
    jobs = list(_backfill_jobs.values())
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    return {
        "data": jobs[:limit],
        "meta": {"ts": datetime.now(UTC), "count": len(jobs)},
    }


# Health Monitor summary endpoint
@app.get("/api/v1/health/summary")
async def health_summary(days: int = Query(1, ge=1, le=30)) -> dict[str, Any]:
    """Return latest health check results and trend data."""
    from datetime import date, timedelta

    import pandas as pd

    from heber.health_monitor.store import HealthStore

    store = HealthStore()
    today = date.today()
    start = today - timedelta(days=days - 1)

    frames: list[pd.DataFrame] = []
    current = start
    while current <= today:
        df = store.read_results(current)
        if len(df) > 0:
            frames.append(df)
        current += timedelta(days=1)

    if not frames:
        return {"checks": [], "summary": {"total": 0, "pass": 0, "warn": 0, "fail": 0, "error": 0}}

    all_results = pd.concat(frames, ignore_index=True)
    latest = all_results.sort_values("ts_checked").groupby(["check_name", "feed"]).last().reset_index()
    summary = latest["status"].value_counts().to_dict()

    return {
        "checks": latest.to_dict(orient="records"),
        "summary": {
            "total": len(latest),
            "pass": summary.get("pass", 0),
            "warn": summary.get("warn", 0),
            "fail": summary.get("fail", 0),
            "error": summary.get("error", 0),
        },
    }


# Error codes (PRD §11.7.5)
ERROR_CODES: dict[int, str] = {
    400: "INVALID_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONFLICT",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
}


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Any, exc: HTTPException) -> Any:
    """Convert HTTP exceptions to PRD-compliant error format."""
    from fastapi.responses import JSONResponse

    code = ERROR_CODES.get(exc.status_code, "UNKNOWN_ERROR")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": str(exc.detail),
            },
            "meta": {"ts": datetime.now(UTC).isoformat()},
        },
    )
