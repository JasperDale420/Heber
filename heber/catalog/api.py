"""Catalog REST API - FastAPI routes.

See PRD Section 11.7 for API contract.
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from heber.catalog.db import Base
from heber.catalog.service import CatalogService
from heber.config import settings
from heber.ops.metrics import start_metrics_server_from_env

logger = structlog.get_logger(__name__)

# Database setup
engine = create_async_engine(settings.postgres_url, echo=settings.environment == "dev")
async_session = async_sessionmaker(engine, expire_on_commit=False)


def _should_auto_create_catalog_tables() -> bool:
    """Keep SQLAlchemy create_all for local dev only.

    Non-dev environments should use Alembic migrations instead.
    """
    return settings.environment == "dev"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    if settings.metrics_port:
        start_metrics_server_from_env(default_port=9090)

    if _should_auto_create_catalog_tables():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("catalog_schema_bootstrap_applied", mode="sqlalchemy_create_all", environment=settings.environment)
    else:
        logger.info("catalog_schema_bootstrap_skipped", reason="non_dev_environment", environment=settings.environment)
    yield


app = FastAPI(
    title="Heber Catalog API",
    description="Dataset and instrument registry for Heber Data Lakehouse",
    version="0.1.0",
    lifespan=lifespan,
)


from collections.abc import AsyncGenerator


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
    partition_cols: list | None
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


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None


class ErrorEnvelope(BaseModel):
    error: ErrorResponse
    meta: MetaResponse


# Health check
@app.get("/health")
async def health():
    return {"status": "healthy", "service": "heber-catalog"}


# Dataset endpoints
@app.get("/api/v1/datasets", response_model=DatasetListResponse)
async def list_datasets(
    layer: str | None = Query(None, description="Filter by layer (bronze|silver|gold)"),
    service: CatalogService = Depends(get_service),
):
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


@app.get("/api/v1/datasets/{name}", response_model=DatasetDetailResponse)
async def get_dataset(name: str, service: CatalogService = Depends(get_service)):
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


@app.get("/api/v1/datasets/{name}/versions")
async def get_dataset_versions(name: str, service: CatalogService = Depends(get_service)):
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


@app.get("/api/v1/datasets/{name}/coverage")
async def get_dataset_coverage(name: str, service: CatalogService = Depends(get_service)):
    coverage = await service.get_coverage(name)
    return {
        "data": [
            {
                "instrument_key": c.instrument_key,
                "dt_min": c.dt_min,
                "dt_max": c.dt_max,
                "approx_row_count": c.approx_row_count,
            }
            for c in coverage
        ],
        "meta": {"ts": datetime.now(UTC)},
    }


# Instrument endpoints
@app.get("/api/v1/instruments/{key}")
async def get_instrument(key: str, service: CatalogService = Depends(get_service)):
    instrument = await service.get_instrument(key)
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{key}' not found")
    return {
        "data": InstrumentResponse(
            instrument_key=instrument.instrument_key,
            instrument_type=instrument.instrument_type,
            canonical_symbol=instrument.canonical_symbol,
            underlying_key=instrument.underlying_key,
            occ_symbol=instrument.occ_symbol,
            expiry=instrument.expiry,
            strike=instrument.strike,
            put_call=instrument.put_call,
        ),
        "meta": {"ts": datetime.now(UTC)},
    }


@app.post("/api/v1/instruments/lookup")
async def lookup_instruments(
    request: InstrumentLookupRequest,
    service: CatalogService = Depends(get_service),
):
    instruments = await service.lookup_instruments(request.symbols)
    return {
        "data": [
            InstrumentResponse(
                instrument_key=i.instrument_key,
                instrument_type=i.instrument_type,
                canonical_symbol=i.canonical_symbol,
                underlying_key=i.underlying_key,
                occ_symbol=i.occ_symbol,
                expiry=i.expiry,
                strike=i.strike,
                put_call=i.put_call,
            )
            for i in instruments
        ],
        "meta": {"ts": datetime.now(UTC)},
    }


@app.get("/api/v1/instruments/search")
async def search_instruments(
    instrument_type: str | None = Query(None),
    symbol_prefix: str | None = Query(None),
    limit: int = Query(100, le=1000),
    service: CatalogService = Depends(get_service),
):
    instruments = await service.search_instruments(
        instrument_type=instrument_type,
        symbol_prefix=symbol_prefix,
        limit=limit,
    )
    return {
        "data": [
            InstrumentResponse(
                instrument_key=i.instrument_key,
                instrument_type=i.instrument_type,
                canonical_symbol=i.canonical_symbol,
                underlying_key=i.underlying_key,
                occ_symbol=i.occ_symbol,
                expiry=i.expiry,
                strike=i.strike,
                put_call=i.put_call,
            )
            for i in instruments
        ],
        "meta": {"ts": datetime.now(UTC)},
    }


# Feed mapping endpoints
@app.get("/api/v1/feeds")
async def list_feeds(service: CatalogService = Depends(get_service)):
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


@app.get("/api/v1/feeds/resolve")
async def resolve_feed(
    provider: str = Query(...),
    feed: str = Query(...),
    service: CatalogService = Depends(get_service),
):
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
@app.get("/api/v1/datasets/{name}/versions/{version}")
async def get_dataset_version(
    name: str,
    version: str,
    service: CatalogService = Depends(get_service),
):
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
    partition_cols: list | None = None
    primary_keys: list | None = None


@app.post("/api/v1/datasets", status_code=201)
async def create_dataset(
    request: DatasetCreateRequest,
    service: CatalogService = Depends(get_service),
):
    """Create a new dataset in the catalog."""
    dataset = await service.create_dataset(
        dataset_name=request.dataset_name,
        layer=request.layer,
        owner=request.owner,
        description=request.description,
        storage_root=request.storage_root,
        path_template=request.path_template,
        partition_cols=request.partition_cols,
        primary_keys=request.primary_keys,
    )
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
):
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
_backfill_jobs: dict = {}


@app.post("/api/v1/backfill", status_code=201)
async def create_backfill(request: BackfillRequest):
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
async def get_backfill(id: str):
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
):
    """List backfill jobs."""
    jobs = list(_backfill_jobs.values())
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    return {
        "data": jobs[:limit],
        "meta": {"ts": datetime.now(UTC), "count": len(jobs)},
    }


# Error codes (PRD §11.7.5)
ERROR_CODES = {
    400: "INVALID_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONFLICT",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
}


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
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


# Rate limiting (PRD §11.7.6)
# Note: Production should use Redis-backed rate limiter
import time
from collections import defaultdict

_rate_limit_store: dict = defaultdict(list)
RATE_LIMITS = {
    "read": 1000,  # 1000 req/min
    "write": 100,  # 100 req/min
}


def check_rate_limit(api_key: str, endpoint_type: str = "read"):
    """Simple in-memory rate limiter (use Redis in production)."""
    now = time.time()
    window = 60  # 1 minute

    key = f"{api_key}:{endpoint_type}"
    _rate_limit_store[key] = [t for t in _rate_limit_store[key] if now - t < window]

    limit = RATE_LIMITS.get(endpoint_type, 1000)
    if len(_rate_limit_store[key]) >= limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    _rate_limit_store[key].append(now)


# Authentication middleware (PRD §11.7.2)
from fastapi import Header


def verify_api_key(authorization: str | None = Header(None)):
    """Simple API key verification (MVP)."""
    if settings.environment == "dev":
        return "dev-user"

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    token = authorization[7:]
    # In production, validate against a key store
    if not token or len(token) < 10:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return token
