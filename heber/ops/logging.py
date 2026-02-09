"""Structured logging configuration per PRD §12.3 and §12.5.5.

Provides:
- JSON structured logs with required fields
- Log levels: DEBUG, INFO, WARNING, ERROR
- Service/instance identification
- Trace context propagation
"""

import logging
import os
from datetime import UTC, datetime
from typing import Any

import structlog


def get_instance_id() -> str:
    """Get unique instance identifier."""
    from heber.config import get_settings

    return get_settings().instance_id


def get_service_name() -> str:
    """Get service name."""
    from heber.config import get_settings

    return get_settings().service_name


def add_timestamp(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add ISO timestamp to log event."""
    event_dict["ts"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return event_dict


def add_service_context(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add service context per PRD §12.5.5."""
    event_dict["service"] = get_service_name()
    event_dict["instance_id"] = get_instance_id()
    return event_dict


def configure_logging(
    service_name: str | None = None,
    log_level: str = "INFO",
    json_output: bool = True,
) -> None:
    """Configure structured logging per PRD §12.3 and §12.5.5.

    Args:
        service_name: Override service name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_output: If True, output JSON; otherwise, dev-friendly format
    """
    if service_name:
        os.environ["SERVICE_NAME"] = service_name  # Keep env sync for child processes

    normalized_level = log_level.strip().upper()
    level_value = logging.getLevelName(normalized_level)
    if not isinstance(level_value, int):
        valid_levels = "DEBUG, INFO, WARNING, ERROR, CRITICAL"
        raise ValueError(f"Invalid log level '{log_level}'. Expected one of: {valid_levels}.")

    # Keep stdlib and structlog filtering aligned for consistent behavior.
    logging.getLogger().setLevel(level_value)

    # Shared processors
    shared_processors = [
        structlog.stdlib.add_log_level,
        add_timestamp,
        add_service_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_output:
        # Production: JSON output per PRD §12.5.5
        processors = shared_processors + [structlog.processors.JSONRenderer()]
    else:
        # Development: Console-friendly output
        processors = shared_processors + [structlog.dev.ConsoleRenderer()]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level_value),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance."""
    return structlog.get_logger(name)


# Log event helpers for common operations (PRD §12.3)


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
    """Log gateway event receipt per PRD §12.3."""
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
    """Log batch write per PRD §12.3."""
    logger.info(
        "batch_written",
        feed=feed,
        dt=dt,
        file_count=file_count,
        rows_written=rows_written,
        duration_ms=duration_ms,
        ingest_lag_ms=ingest_lag_ms,
    )


def log_error(
    logger: structlog.stdlib.BoundLogger,
    error: Exception,
    operation: str,
    **context: Any,
) -> None:
    """Log error with context."""
    logger.error(
        "error",
        operation=operation,
        error_type=type(error).__name__,
        error_message=str(error),
        **context,
        exc_info=True,
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


def log_retry(
    logger: structlog.stdlib.BoundLogger,
    operation: str,
    attempt: int,
    max_retries: int,
    delay_seconds: float,
    error: str,
) -> None:
    """Log retry attempt."""
    logger.warning(
        "retry",
        operation=operation,
        attempt=attempt,
        max_retries=max_retries,
        delay_seconds=delay_seconds,
        error=error,
    )
