"""Structured logging configuration for Heber.

Delegates base configuration to empire_core.logger (daily file rotation,
error log, service name injection). Provides Heber-specific structured
log helpers for event processing, batch writes, DLQ, and retries.
"""

from __future__ import annotations

from datetime import datetime

import structlog
from empire_core.logger import (
    bind_context,
    clear_context,
    get_logger,
    log_error,
    log_retry,
    setup_logging,
    unbind_context,
)

# Re-export empire_core helpers so existing `from heber.ops.logging import ...` keeps working
__all__ = [
    "bind_context",
    "clear_context",
    "configure_logging",
    "get_logger",
    "log_batch_written",
    "log_dlq_event",
    "log_error",
    "log_event_received",
    "log_retry",
    "unbind_context",
]


_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def configure_logging(
    service_name: str | None = None,
    log_level: str = "INFO",
    json_output: bool = True,
    *,
    force: bool = True,
) -> None:
    """Configure Heber logging via empire_core.

    Args:
        service_name: Override service name (default: "heber").
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        json_output: Ignored; empire_core uses EMPIRE_LOG_FORMAT env var.
        force: Re-run ``empire_core.setup_logging`` even if the global
            ``_configured`` flag is already set (default ``True``). Without
            this, a second service starting in the same Python process
            (e.g. a CLI shelling into multiple service modules, an embedded
            test harness, or any code path that imports two services) would
            silently inherit the *first* service's log filename and
            ``service=`` field, leaking the wrong service name into the
            wrong daily log file. Set ``force=False`` only if you are sure
            the caller has already done the right setup.

    Raises:
        ValueError: If ``log_level`` is not a recognised stdlib log level name.
    """
    normalised = log_level.upper()
    if normalised not in _VALID_LOG_LEVELS:
        raise ValueError(f"Invalid log level: {log_level!r}. Must be one of: {', '.join(sorted(_VALID_LOG_LEVELS))}")
    setup_logging(
        service_name or "heber",
        level=normalised,
        force=force,
    )


# ---------------------------------------------------------------------------
# Domain-specific log helpers
# ---------------------------------------------------------------------------


def log_event_received(
    logger: structlog.stdlib.BoundLogger,
    event_id: str,
    provider: str,
    feed: str,
    instrument_key: str,
    ts_event: datetime,
    ts_ingest: datetime,
    ts_available: datetime,
    schema_version: str = "v1",
    quality_flags: list[str] | None = None,
) -> None:
    """Log gateway event receipt."""
    logger.info(
        "event_received",
        event_id=event_id,
        provider=provider,
        feed=feed,
        instrument_key=instrument_key,
        ts_event=ts_event.isoformat() if ts_event else None,
        ts_ingest=ts_ingest.isoformat() if ts_ingest else None,
        ts_available=ts_available.isoformat() if ts_available else None,
        schema_version=schema_version,
        quality_flags=quality_flags or [],
    )


def log_batch_written(
    logger: structlog.stdlib.BoundLogger,
    feed: str,
    dt: str,
    file_count: int,
    rows_written: int,
    duration_ms: float,
    ingest_lag_ms: float | None = None,
) -> None:
    """Log batch write completion."""
    logger.info(
        "batch_written",
        feed=feed,
        dt=dt,
        file_count=file_count,
        rows_written=rows_written,
        duration_ms=duration_ms,
        ingest_lag_ms=ingest_lag_ms,
    )


def log_dlq_event(
    logger: structlog.stdlib.BoundLogger,
    event_id: str,
    feed: str,
    provider: str,
    error_type: str,
    attempts: int,
) -> None:
    """Log dead-letter queue event."""
    logger.warning(
        "dlq_event",
        event_id=event_id,
        feed=feed,
        provider=provider,
        error_type=error_type,
        attempts=attempts,
    )
