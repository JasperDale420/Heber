"""Circuit breaker pattern for Heber per PRD §12.13.

Provides:
- Dependency classification (hard vs soft)
- Circuit breaker with configurable settings
- Degradation matrix for graceful degradation
- Degraded mode metrics and indicators
"""

import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import structlog
from prometheus_client import Gauge

logger = structlog.get_logger(__name__)


# Degraded mode metric (PRD §12.13.4)
degraded_mode = Gauge(
    "heber_degraded_mode",
    "Service operating in degraded mode (1=degraded, 0=normal)",
    ["dependency"],
)


class DependencyType(str, Enum):
    """Dependency classification per PRD §12.13.1."""
    HARD = "hard"   # Service cannot function without it
    SOFT = "soft"   # Service can continue with reduced functionality


class CircuitState(str, Enum):
    """Circuit breaker states per PRD §12.13.3."""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Bypass dependency, use degraded path
    HALF_OPEN = "half_open" # Testing if dependency recovered


@dataclass
class CircuitBreakerSettings:
    """Circuit breaker configuration per PRD §12.13.3."""
    failure_threshold: int = 5      # Consecutive failures to open
    open_duration_seconds: int = 30 # How long circuit stays open
    half_open_probes: int = 3       # Probes allowed in half-open
    success_threshold: int = 2      # Successes to close from half-open


@dataclass
class DependencyInfo:
    """Metadata about a dependency."""
    name: str
    dep_type: DependencyType
    degraded_behavior: str


# Default circuit breaker settings
DEFAULT_SETTINGS = CircuitBreakerSettings()


class CircuitBreaker:
    """Circuit breaker implementation per PRD §12.13.3.
    
    Usage:
        breaker = CircuitBreaker("catalog")
        
        if breaker.allow_request():
            try:
                result = call_catalog()
                breaker.record_success()
                return result
            except Exception as e:
                breaker.record_failure()
                raise
        else:
            # Degraded path
            return cached_value
    """
    
    def __init__(
        self,
        name: str,
        settings: CircuitBreakerSettings | None = None,
    ):
        self.name = name
        self.settings = settings or DEFAULT_SETTINGS
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()
        
        # Initialize metric
        degraded_mode.labels(dependency=name).set(0)
    
    @property
    def state(self) -> CircuitState:
        """Current circuit state (may transition on access)."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if we should transition to half-open
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.settings.open_duration_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state
    
    @property
    def is_open(self) -> bool:
        """Check if circuit is open (requests should be bypassed)."""
        return self.state == CircuitState.OPEN
    
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == CircuitState.CLOSED
    
    def allow_request(self) -> bool:
        """Check if a request should be allowed through.
        
        Returns True if the request should proceed to the dependency.
        Returns False if the circuit is open (use degraded path).
        """
        state = self.state
        
        if state == CircuitState.CLOSED:
            return True
        
        if state == CircuitState.OPEN:
            return False
        
        # Half-open: allow limited probes
        with self._lock:
            if self._half_open_calls < self.settings.half_open_probes:
                self._half_open_calls += 1
                return True
            return False
    
    def record_success(self) -> None:
        """Record a successful call to the dependency."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.settings.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    def record_failure(self) -> None:
        """Record a failed call to the dependency."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open reopens circuit
                self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.settings.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state (must hold lock)."""
        old_state = self._state
        self._state = new_state
        
        # Reset counters on transition
        if new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
            self._half_open_calls = 0
            degraded_mode.labels(dependency=self.name).set(0)
            logger.info(
                "circuit_closed",
                dependency=self.name,
                message=f"Circuit breaker closed for {self.name}",
            )
        elif new_state == CircuitState.OPEN:
            self._success_count = 0
            self._half_open_calls = 0
            degraded_mode.labels(dependency=self.name).set(1)
            logger.warning(
                "circuit_opened",
                dependency=self.name,
                failure_count=self._failure_count,
                message=f"Operating in degraded mode: {self.name} unreachable",
            )
        elif new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
            self._half_open_calls = 0
            logger.info(
                "circuit_half_open",
                dependency=self.name,
                message=f"Testing if {self.name} recovered",
            )
    
    def force_open(self) -> None:
        """Manually force the circuit open."""
        with self._lock:
            self._last_failure_time = time.time()
            self._transition_to(CircuitState.OPEN)
    
    def force_close(self) -> None:
        """Manually force the circuit closed."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
    
    def reset(self) -> None:
        """Reset the circuit to initial state."""
        self.force_close()


# Degradation matrix per PRD §12.13.2
DEGRADATION_MATRIX: dict[str, dict[str, DependencyInfo]] = {
    "heber-consumer": {
        "event_bus": DependencyInfo(
            name="event_bus",
            dep_type=DependencyType.HARD,
            degraded_behavior="Crash/restart (bus buffers data)",
        ),
        "object_storage": DependencyInfo(
            name="object_storage",
            dep_type=DependencyType.HARD,
            degraded_behavior="Retry + DLQ after max attempts",
        ),
        "catalog": DependencyInfo(
            name="catalog",
            dep_type=DependencyType.SOFT,
            degraded_behavior="Cache-only; skip coverage updates",
        ),
        "bloom_filter": DependencyInfo(
            name="bloom_filter",
            dep_type=DependencyType.SOFT,
            degraded_behavior="Rebuild from scratch (memory only)",
        ),
    },
    "heber-writer": {
        "object_storage": DependencyInfo(
            name="object_storage",
            dep_type=DependencyType.HARD,
            degraded_behavior="Retry + DLQ",
        ),
        "catalog": DependencyInfo(
            name="catalog",
            dep_type=DependencyType.SOFT,
            degraded_behavior="Continue writes; queue metadata updates",
        ),
    },
    "heber-compactor": {
        "object_storage": DependencyInfo(
            name="object_storage",
            dep_type=DependencyType.HARD,
            degraded_behavior="Wait and retry",
        ),
        "catalog": DependencyInfo(
            name="catalog",
            dep_type=DependencyType.SOFT,
            degraded_behavior="Skip catalog updates",
        ),
    },
    "heber-catalog": {
        "postgres": DependencyInfo(
            name="postgres",
            dep_type=DependencyType.HARD,
            degraded_behavior="Return 503 on all requests",
        ),
    },
    "heber-hotloader": {
        "hot_store": DependencyInfo(
            name="hot_store",
            dep_type=DependencyType.HARD,
            degraded_behavior="Skip hot writes; emit alert",
        ),
        "event_bus": DependencyInfo(
            name="event_bus",
            dep_type=DependencyType.HARD,
            degraded_behavior="Crash/restart",
        ),
    },
    "sdk": {
        "catalog_api": DependencyInfo(
            name="catalog_api",
            dep_type=DependencyType.SOFT,
            degraded_behavior="Use local cache if available",
        ),
        "object_storage": DependencyInfo(
            name="object_storage",
            dep_type=DependencyType.HARD,
            degraded_behavior="Fail read/write operations",
        ),
    },
}


# Global circuit breaker registry
_circuit_breakers: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_circuit_breaker(
    name: str,
    settings: CircuitBreakerSettings | None = None,
) -> CircuitBreaker:
    """Get or create a circuit breaker for a dependency.
    
    Args:
        name: Dependency name (e.g., "catalog", "object_storage")
        settings: Optional custom settings
        
    Returns:
        CircuitBreaker instance
    """
    with _registry_lock:
        if name not in _circuit_breakers:
            _circuit_breakers[name] = CircuitBreaker(name, settings)
        return _circuit_breakers[name]


def get_all_circuit_breakers() -> dict[str, CircuitBreaker]:
    """Get all registered circuit breakers."""
    with _registry_lock:
        return dict(_circuit_breakers)


def get_dependency_info(service: str, dependency: str) -> DependencyInfo | None:
    """Get dependency info from degradation matrix."""
    service_deps = DEGRADATION_MATRIX.get(service, {})
    return service_deps.get(dependency)


def is_hard_dependency(service: str, dependency: str) -> bool:
    """Check if a dependency is hard (required)."""
    info = get_dependency_info(service, dependency)
    return info is not None and info.dep_type == DependencyType.HARD


def is_soft_dependency(service: str, dependency: str) -> bool:
    """Check if a dependency is soft (degradable)."""
    info = get_dependency_info(service, dependency)
    return info is not None and info.dep_type == DependencyType.SOFT


def get_degraded_header() -> dict[str, str] | None:
    """Get X-Heber-Degraded header if any circuits are open.
    
    Returns header dict for FastAPI response, or None if all normal.
    """
    degraded_deps = [
        name for name, cb in get_all_circuit_breakers().items()
        if cb.is_open
    ]
    
    if degraded_deps:
        return {"X-Heber-Degraded": ",".join(degraded_deps)}
    return None


# Decorator for circuit breaker pattern
def with_circuit_breaker(
    dependency_name: str,
    fallback: Callable[[], Any] | None = None,
    settings: CircuitBreakerSettings | None = None,
):
    """Decorator to wrap a function with circuit breaker protection.
    
    Args:
        dependency_name: Name of the dependency being called
        fallback: Optional fallback function when circuit is open
        settings: Optional custom circuit breaker settings
    """
    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            cb = get_circuit_breaker(dependency_name, settings)
            
            if not cb.allow_request():
                if fallback:
                    return fallback()
                raise CircuitOpenError(
                    f"Circuit open for {dependency_name}: service degraded"
                )
            
            try:
                result = fn(*args, **kwargs)
                cb.record_success()
                return result
            except Exception as e:
                cb.record_failure()
                raise
        
        return wrapper
    return decorator


class CircuitOpenError(Exception):
    """Raised when circuit is open and no fallback provided."""
    pass
