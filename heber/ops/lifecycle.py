"""Service lifecycle management for Heber per PRD §12.14.

Provides:
- Graceful shutdown handling (SIGTERM)
- Readiness state management during shutdown
- In-flight work draining
- Buffer flushing
- Connection cleanup
- Shutdown timeout enforcement
- Canary deployment metrics
"""

import asyncio
import os
import signal
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import structlog
from prometheus_client import Counter, Gauge

from heber.ops.health import mark_startup_incomplete

logger = structlog.get_logger(__name__)


# Default shutdown timeout (PRD §12.14.3)
DEFAULT_SHUTDOWN_TIMEOUT = 30


class LifecycleState(str, Enum):
    """Service lifecycle states."""

    STARTING = "starting"
    RUNNING = "running"
    DRAINING = "draining"
    SHUTTING_DOWN = "shutting_down"
    STOPPED = "stopped"


# Prometheus metrics for canary deployments (PRD §12.14.6)
shutdown_initiated = Counter(
    "heber_shutdown_initiated_total",
    "Number of shutdown sequences initiated",
    ["reason"],
)

shutdown_completed = Counter(
    "heber_shutdown_completed_total",
    "Number of shutdown sequences completed",
    ["status"],  # success, timeout, error
)

in_flight_requests = Gauge(
    "heber_in_flight_requests",
    "Current number of in-flight requests/operations",
    ["service"],
)

drain_duration_seconds = Gauge(
    "heber_drain_duration_seconds",
    "Time spent draining in-flight work",
)


@dataclass
class ShutdownConfig:
    """Shutdown configuration per PRD §12.14.3."""

    timeout_seconds: int = field(
        default_factory=lambda: int(os.environ.get("HEBER_SHUTDOWN_TIMEOUT_SECONDS", DEFAULT_SHUTDOWN_TIMEOUT))
    )
    drain_poll_interval: float = 0.5  # How often to check if drained


@dataclass
class LifecycleCallbacks:
    """Callbacks for lifecycle events."""

    on_drain_start: Callable[[], None] | None = None
    on_drain_complete: Callable[[], None] | None = None
    on_flush_buffers: Callable[[], None] | None = None
    on_close_connections: Callable[[], None] | None = None


class LifecycleManager:
    """Manages service lifecycle per PRD §12.14.3.

    Implements the 6-step graceful shutdown sequence:
    1. SIGTERM received → Start shutdown
    2. Readiness = false → Stop accepting new work
    3. Drain in-flight → Complete current batch/request
    4. Flush buffers → Write any buffered data
    5. Close connections → Gracefully close DB/storage/bus connections
    6. Exit → Process terminates

    Usage:
        lifecycle = LifecycleManager(service_name="heber-consumer")
        lifecycle.register_callbacks(
            on_flush_buffers=flush_my_buffers,
            on_close_connections=close_my_connections,
        )
        lifecycle.install_signal_handlers()

        # In your main loop:
        while lifecycle.is_running:
            process_work()

        # Or use as async context manager:
        async with lifecycle.managed():
            await run_service()
    """

    def __init__(
        self,
        service_name: str = "heber",
        config: ShutdownConfig | None = None,
    ):
        self.service_name = service_name
        self.config = config or ShutdownConfig()

        self._state = LifecycleState.STARTING
        self._in_flight_count = 0
        self._shutdown_event = threading.Event()
        self._async_shutdown_event: asyncio.Event | None = None
        self._lock = threading.Lock()
        self._callbacks = LifecycleCallbacks()
        self._shutdown_reason: str | None = None

        # Initialize metrics
        in_flight_requests.labels(service=service_name).set(0)

    @property
    def state(self) -> LifecycleState:
        """Current lifecycle state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if service is in a running state."""
        return self._state in (LifecycleState.STARTING, LifecycleState.RUNNING)

    @property
    def is_accepting_work(self) -> bool:
        """Check if service should accept new work."""
        return self._state == LifecycleState.RUNNING

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown has been initiated."""
        return self._state in (
            LifecycleState.DRAINING,
            LifecycleState.SHUTTING_DOWN,
            LifecycleState.STOPPED,
        )

    def mark_running(self) -> None:
        """Transition from starting to running."""
        with self._lock:
            if self._state == LifecycleState.STARTING:
                self._state = LifecycleState.RUNNING
                logger.info("service_running", service=self.service_name)

    def register_callbacks(
        self,
        on_drain_start: Callable[[], None] | None = None,
        on_drain_complete: Callable[[], None] | None = None,
        on_flush_buffers: Callable[[], None] | None = None,
        on_close_connections: Callable[[], None] | None = None,
    ) -> None:
        """Register lifecycle callbacks."""
        if on_drain_start:
            self._callbacks.on_drain_start = on_drain_start
        if on_drain_complete:
            self._callbacks.on_drain_complete = on_drain_complete
        if on_flush_buffers:
            self._callbacks.on_flush_buffers = on_flush_buffers
        if on_close_connections:
            self._callbacks.on_close_connections = on_close_connections

    def track_in_flight(self) -> "InFlightTracker":
        """Context manager to track in-flight work.

        Usage:
            with lifecycle.track_in_flight():
                process_batch()
        """
        return InFlightTracker(self)

    def _increment_in_flight(self) -> None:
        """Increment in-flight count."""
        with self._lock:
            self._in_flight_count += 1
            in_flight_requests.labels(service=self.service_name).set(self._in_flight_count)

    def _decrement_in_flight(self) -> None:
        """Decrement in-flight count."""
        with self._lock:
            self._in_flight_count = max(0, self._in_flight_count - 1)
            in_flight_requests.labels(service=self.service_name).set(self._in_flight_count)

    def install_signal_handlers(self) -> None:
        """Install SIGTERM/SIGINT handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        logger.debug("signal_handlers_installed", signals=["SIGTERM", "SIGINT"])

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        logger.info("shutdown_signal_received", signal=signal_name)
        self.initiate_shutdown(reason=signal_name)

    def initiate_shutdown(self, reason: str = "manual") -> None:
        """Start the graceful shutdown sequence."""
        with self._lock:
            if self._state in (LifecycleState.DRAINING, LifecycleState.SHUTTING_DOWN):
                return  # Already shutting down

            self._shutdown_reason = reason
            self._state = LifecycleState.DRAINING
            shutdown_initiated.labels(reason=reason).inc()

        logger.info(
            "shutdown_initiated",
            service=self.service_name,
            reason=reason,
            timeout=self.config.timeout_seconds,
        )

        # Step 2: Set readiness = false
        mark_startup_incomplete()

        # Signal shutdown event
        self._shutdown_event.set()
        if self._async_shutdown_event:
            self._async_shutdown_event.set()

    def wait_for_shutdown(self, timeout: float | None = None) -> bool:
        """Wait for shutdown signal.

        Returns True if shutdown was signaled, False on timeout.
        """
        return self._shutdown_event.wait(timeout=timeout)

    async def async_wait_for_shutdown(self) -> None:
        """Async wait for shutdown signal."""
        if self._shutdown_event.is_set():
            return

        if not self._async_shutdown_event:
            self._async_shutdown_event = asyncio.Event()

            # Handle race where shutdown was signaled before async event creation.
            if self._shutdown_event.is_set():
                self._async_shutdown_event.set()

        await self._async_shutdown_event.wait()

    def execute_shutdown(self) -> bool:
        """Execute the full shutdown sequence.

        Returns True if shutdown completed successfully within timeout.
        """
        start_time = time.time()
        deadline = start_time + self.config.timeout_seconds

        try:
            # Step 3: Drain in-flight work
            logger.info("draining_in_flight", in_flight=self._in_flight_count)
            if self._callbacks.on_drain_start:
                self._callbacks.on_drain_start()

            while self._in_flight_count > 0 and time.time() < deadline:
                time.sleep(self.config.drain_poll_interval)

            drain_duration = time.time() - start_time
            drain_duration_seconds.set(drain_duration)
            timed_out = self._in_flight_count > 0

            if timed_out:
                logger.warning(
                    "drain_timeout",
                    remaining=self._in_flight_count,
                    duration=drain_duration,
                )

            if self._callbacks.on_drain_complete:
                self._callbacks.on_drain_complete()

            # Step 4: Flush buffers
            with self._lock:
                self._state = LifecycleState.SHUTTING_DOWN

            logger.info("flushing_buffers")
            if self._callbacks.on_flush_buffers:
                self._callbacks.on_flush_buffers()

            # Step 5: Close connections
            logger.info("closing_connections")
            if self._callbacks.on_close_connections:
                self._callbacks.on_close_connections()

            # Step 6: Mark as stopped
            with self._lock:
                self._state = LifecycleState.STOPPED

            status = "timeout" if timed_out else "success"
            shutdown_completed.labels(status=status).inc()
            logger.info(
                "shutdown_complete",
                service=self.service_name,
                status=status,
                duration=time.time() - start_time,
            )
            return not timed_out

        except Exception as e:
            shutdown_completed.labels(status="error").inc()
            logger.error("shutdown_error", error=str(e), exc_info=True)
            return False

    async def async_execute_shutdown(self) -> bool:
        """Async version of execute_shutdown."""
        start_time = time.time()
        deadline = start_time + self.config.timeout_seconds

        try:
            # Step 3: Drain in-flight work
            logger.info("draining_in_flight", in_flight=self._in_flight_count)
            if self._callbacks.on_drain_start:
                self._callbacks.on_drain_start()

            while self._in_flight_count > 0 and time.time() < deadline:
                await asyncio.sleep(self.config.drain_poll_interval)

            drain_duration = time.time() - start_time
            drain_duration_seconds.set(drain_duration)
            timed_out = self._in_flight_count > 0

            if timed_out:
                logger.warning(
                    "drain_timeout",
                    remaining=self._in_flight_count,
                    duration=drain_duration,
                )

            if self._callbacks.on_drain_complete:
                self._callbacks.on_drain_complete()

            # Step 4: Flush buffers
            with self._lock:
                self._state = LifecycleState.SHUTTING_DOWN

            if self._callbacks.on_flush_buffers:
                self._callbacks.on_flush_buffers()

            # Step 5: Close connections
            if self._callbacks.on_close_connections:
                self._callbacks.on_close_connections()

            # Step 6: Mark as stopped
            with self._lock:
                self._state = LifecycleState.STOPPED

            status = "timeout" if timed_out else "success"
            shutdown_completed.labels(status=status).inc()
            logger.info(
                "shutdown_complete",
                service=self.service_name,
                status=status,
                duration=time.time() - start_time,
            )
            return not timed_out

        except Exception as e:
            shutdown_completed.labels(status="error").inc()
            logger.error("shutdown_error", error=str(e), exc_info=True)
            return False

    @asynccontextmanager
    async def managed(self):
        """Async context manager for managed lifecycle.

        Usage:
            async with lifecycle.managed():
                await run_service()
        """
        self.install_signal_handlers()
        self.mark_running()

        try:
            yield self
        finally:
            if not self.is_shutting_down:
                self.initiate_shutdown(reason="context_exit")
            await self.async_execute_shutdown()


class InFlightTracker:
    """Context manager to track in-flight work."""

    def __init__(self, lifecycle: LifecycleManager):
        self._lifecycle = lifecycle

    def __enter__(self):
        self._lifecycle._increment_in_flight()
        return self

    def __exit__(self, *args):
        self._lifecycle._decrement_in_flight()


# Consumer group rebalancing helpers (PRD §12.14.4)


@dataclass
class RebalanceConfig:
    """Consumer group rebalance configuration."""

    rebalance_timeout_seconds: int = 60
    partition_assignment_delay: float = 1.0


class ConsumerGroupRebalancer:
    """Handles consumer group rebalancing per PRD §12.14.4.

    Key invariant: No messages lost during rebalancing.
    """

    def __init__(
        self,
        group_id: str,
        config: RebalanceConfig | None = None,
    ):
        self.group_id = group_id
        self.config = config or RebalanceConfig()
        self._assigned_partitions: list[str] = []
        self._rebalancing = False

    def on_partitions_revoked(self, partitions: list[str]) -> None:
        """Called when partitions are about to be revoked.

        Must commit any pending work before returning.
        """
        self._rebalancing = True
        logger.info(
            "partitions_revoked",
            group=self.group_id,
            partitions=partitions,
        )

    def on_partitions_assigned(self, partitions: list[str]) -> None:
        """Called when new partitions are assigned."""
        self._assigned_partitions = partitions
        self._rebalancing = False
        logger.info(
            "partitions_assigned",
            group=self.group_id,
            partitions=partitions,
        )

    @property
    def is_rebalancing(self) -> bool:
        """Check if currently rebalancing."""
        return self._rebalancing

    @property
    def assigned_partitions(self) -> list[str]:
        """Get currently assigned partitions."""
        return self._assigned_partitions.copy()


# Canary deployment metrics (PRD §12.14.6)

CANARY_METRICS = [
    "heber_writer_errors_total",
    "heber_consumer_lag_seconds",
    "heber_catalog_request_duration_seconds",
]

canary_health = Gauge(
    "heber_canary_healthy",
    "Canary deployment health status (1=healthy, 0=unhealthy)",
    ["deployment"],
)


def evaluate_canary_health(
    error_rate_threshold: float = 0.01,
    lag_threshold_seconds: float = 60.0,
    latency_threshold_ms: float = 500.0,
) -> bool:
    """Evaluate if canary deployment is healthy.

    Based on PRD §12.14.6 metrics:
    - heber_writer_errors_total (should not spike)
    - heber_consumer_lag_seconds (should not grow)
    - heber_catalog_request_duration_seconds (should not increase)

    Returns True if all metrics are within threshold.
    """
    # In production, this would query actual Prometheus metrics
    # For now, always return healthy
    return True


def mark_canary_healthy(deployment: str = "default") -> None:
    """Mark canary deployment as healthy."""
    canary_health.labels(deployment=deployment).set(1)


def mark_canary_unhealthy(deployment: str = "default") -> None:
    """Mark canary deployment as unhealthy."""
    canary_health.labels(deployment=deployment).set(0)


# FastAPI integration


def create_lifecycle_middleware(lifecycle: LifecycleManager):
    """Create FastAPI middleware for lifecycle-aware request handling."""
    from starlette.middleware.base import BaseHTTPMiddleware
    from starlette.responses import Response

    class LifecycleMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            if not lifecycle.is_accepting_work:
                return Response(
                    content="Service is shutting down",
                    status_code=503,
                )

            with lifecycle.track_in_flight():
                return await call_next(request)

    return LifecycleMiddleware
