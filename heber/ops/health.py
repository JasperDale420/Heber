"""Health check endpoints for Heber per PRD §12.12.

Provides:
- /health (liveness) - process alive, no dependency checks
- /ready (readiness) - ready to handle requests, checks dependencies
- /startup - initialization complete
- Dependency health check framework
"""

import os
import time
from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum
from typing import Any, Callable

import structlog

logger = structlog.get_logger(__name__)


# Service start time for uptime calculation
_start_time = time.time()

# Global startup state
_startup_complete = False


class HealthStatus(str, Enum):
    """Health check status."""
    OK = "ok"
    DEGRADED = "degraded"
    ERROR = "error"


class ReadinessStatus(str, Enum):
    """Readiness status."""
    READY = "ready"
    NOT_READY = "not_ready"


@dataclass
class HealthCheck:
    """Individual health check result."""
    name: str
    status: HealthStatus
    message: str | None = None
    latency_ms: float | None = None
    
    def to_dict(self) -> dict[str, Any]:
        result = {"status": self.status.value}
        if self.message:
            result["message"] = self.message
        if self.latency_ms is not None:
            result["latency_ms"] = round(self.latency_ms, 2)
        return result


@dataclass
class LivenessResponse:
    """Response for /health endpoint (PRD §12.12.2)."""
    status: str = "ok"
    service: str = ""
    instance_id: str = ""
    uptime_seconds: int = 0
    version: str = ""
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "service": self.service,
            "instance_id": self.instance_id,
            "uptime_seconds": self.uptime_seconds,
            "version": self.version,
        }


@dataclass
class ReadinessResponse:
    """Response for /ready endpoint (PRD §12.12.3)."""
    status: ReadinessStatus
    checks: dict[str, HealthCheck] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "checks": {name: check.to_dict() for name, check in self.checks.items()},
        }
    
    @property
    def is_ready(self) -> bool:
        return self.status == ReadinessStatus.READY


# Dependency check registry
_dependency_checks: dict[str, Callable[[], HealthCheck]] = {}


def register_dependency_check(name: str, check_fn: Callable[[], HealthCheck]) -> None:
    """Register a dependency health check.
    
    Args:
        name: Dependency name (e.g., "event_bus", "object_storage")
        check_fn: Function that returns HealthCheck
    """
    _dependency_checks[name] = check_fn
    logger.debug("dependency_check_registered", name=name)


def unregister_dependency_check(name: str) -> None:
    """Unregister a dependency check."""
    _dependency_checks.pop(name, None)


def get_service_info() -> tuple[str, str, str]:
    """Get service name, instance ID, and version from environment."""
    service = os.environ.get("SERVICE_NAME", "heber")
    instance_id = os.environ.get("INSTANCE_ID", os.environ.get("HOSTNAME", "unknown"))
    version = os.environ.get("SERVICE_VERSION", "0.1.0")
    return service, instance_id, version


def get_uptime_seconds() -> int:
    """Get process uptime in seconds."""
    return int(time.time() - _start_time)


# Health check endpoint handlers

def check_liveness() -> LivenessResponse:
    """Liveness check (/health) per PRD §12.12.2.
    
    Returns 200 if process is alive. Does NOT check dependencies.
    """
    service, instance_id, version = get_service_info()
    
    return LivenessResponse(
        status="ok",
        service=service,
        instance_id=instance_id,
        uptime_seconds=get_uptime_seconds(),
        version=version,
    )


def check_readiness() -> ReadinessResponse:
    """Readiness check (/ready) per PRD §12.12.3.
    
    Returns 200 only if all dependencies are healthy.
    """
    checks: dict[str, HealthCheck] = {}
    all_ok = True
    
    for name, check_fn in _dependency_checks.items():
        try:
            start = time.time()
            check = check_fn()
            check.latency_ms = (time.time() - start) * 1000
            checks[name] = check
            
            if check.status != HealthStatus.OK:
                all_ok = False
                
        except Exception as e:
            checks[name] = HealthCheck(
                name=name,
                status=HealthStatus.ERROR,
                message=str(e),
            )
            all_ok = False
            logger.warning("dependency_check_failed", name=name, error=str(e))
    
    status = ReadinessStatus.READY if all_ok else ReadinessStatus.NOT_READY
    
    return ReadinessResponse(status=status, checks=checks)


def check_startup() -> dict[str, Any]:
    """Startup check (/startup) per PRD §12.12.4.
    
    Returns 200 when initialization is complete.
    """
    if _startup_complete:
        return {"status": "started", "ready": True}
    return {"status": "starting", "ready": False}


def mark_startup_complete() -> None:
    """Mark service initialization as complete."""
    global _startup_complete
    _startup_complete = True
    logger.info("startup_complete")


def mark_startup_incomplete() -> None:
    """Mark service as not yet initialized (for testing or graceful shutdown)."""
    global _startup_complete
    _startup_complete = False


def is_startup_complete() -> bool:
    """Check if startup is complete."""
    return _startup_complete


# Pre-built dependency checks

def create_redis_check(redis_client: Any) -> Callable[[], HealthCheck]:
    """Create a Redis dependency check."""
    def check() -> HealthCheck:
        try:
            redis_client.ping()
            return HealthCheck(name="redis", status=HealthStatus.OK)
        except Exception as e:
            return HealthCheck(name="redis", status=HealthStatus.ERROR, message=str(e))
    return check


def create_postgres_check(engine: Any) -> Callable[[], HealthCheck]:
    """Create a PostgreSQL dependency check."""
    def check() -> HealthCheck:
        try:
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return HealthCheck(name="postgres", status=HealthStatus.OK)
        except Exception as e:
            return HealthCheck(name="postgres", status=HealthStatus.ERROR, message=str(e))
    return check


def create_s3_check(s3_client: Any, bucket: str) -> Callable[[], HealthCheck]:
    """Create an S3/object storage dependency check."""
    def check() -> HealthCheck:
        try:
            s3_client.head_bucket(Bucket=bucket)
            return HealthCheck(name="object_storage", status=HealthStatus.OK)
        except Exception as e:
            return HealthCheck(name="object_storage", status=HealthStatus.ERROR, message=str(e))
    return check


def create_http_check(url: str, timeout: float = 5.0) -> Callable[[], HealthCheck]:
    """Create an HTTP endpoint dependency check."""
    import httpx
    
    def check() -> HealthCheck:
        try:
            response = httpx.get(url, timeout=timeout)
            if response.status_code == 200:
                return HealthCheck(name=url, status=HealthStatus.OK)
            return HealthCheck(
                name=url,
                status=HealthStatus.ERROR,
                message=f"HTTP {response.status_code}",
            )
        except Exception as e:
            return HealthCheck(name=url, status=HealthStatus.ERROR, message=str(e))
    return check


# FastAPI integration helpers

def create_health_router():
    """Create FastAPI router with health endpoints."""
    from fastapi import APIRouter, Response
    
    router = APIRouter(tags=["Health"])
    
    @router.get("/health")
    async def liveness():
        """Liveness probe - is the process alive?"""
        return check_liveness().to_dict()
    
    @router.get("/livez")
    async def liveness_k8s():
        """Kubernetes-style liveness probe."""
        return check_liveness().to_dict()
    
    @router.get("/ready")
    async def readiness(response: Response):
        """Readiness probe - is the service ready to accept traffic?"""
        result = check_readiness()
        if not result.is_ready:
            response.status_code = 503
        return result.to_dict()
    
    @router.get("/readyz")
    async def readiness_k8s(response: Response):
        """Kubernetes-style readiness probe."""
        result = check_readiness()
        if not result.is_ready:
            response.status_code = 503
        return result.to_dict()
    
    @router.get("/startup")
    async def startup(response: Response):
        """Startup probe - is initialization complete?"""
        result = check_startup()
        if not result["ready"]:
            response.status_code = 503
        return result
    
    return router
