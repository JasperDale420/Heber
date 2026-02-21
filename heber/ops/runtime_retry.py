"""Runtime retry helpers for long-running service loops."""

from __future__ import annotations

import random

_REDIS_LOADING_MARKERS = (
    "loading the dataset in memory",
    "busy loading",
)

_TRANSIENT_MARKERS = (
    "connection closed by server",
    "network is unreachable",
    "connection refused",
    "connection reset",
    "timed out",
    "timeout",
    "try again",
    "temporary failure",
)


def classify_runtime_error(error: Exception) -> tuple[bool, str]:
    """Classify whether a runtime error is transient and return its category."""
    message = str(error).lower()

    if any(marker in message for marker in _REDIS_LOADING_MARKERS):
        return True, "redis_loading"

    if _is_transient_redis_exception(error) or any(marker in message for marker in _TRANSIENT_MARKERS):
        return True, "redis_transport"

    return False, "unknown"


def calculate_retry_delay(
    *,
    attempt: int,
    base_seconds: float,
    max_seconds: float = 30.0,
    jitter_ratio: float = 0.2,
) -> float:
    """Calculate bounded exponential backoff with optional positive jitter."""
    normalized_attempt = max(1, attempt)
    normalized_base = max(0.05, base_seconds)
    normalized_max = max(normalized_base, max_seconds)

    delay = min(normalized_max, normalized_base * (2 ** (normalized_attempt - 1)))
    if jitter_ratio > 0:
        delay += random.uniform(0.0, delay * jitter_ratio)
    return delay


def _is_transient_redis_exception(error: Exception) -> bool:
    try:
        from redis.exceptions import BusyLoadingError, ConnectionError, TimeoutError
    except Exception:  # pragma: no cover - redis dependency absent
        return False
    return isinstance(error, ConnectionError | TimeoutError | BusyLoadingError)
