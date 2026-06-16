"""
Shared HTTP client utilities with structured logging and retry support.

Provides factory functions for creating httpx clients with consistent
defaults (timeouts, logging hooks, follow_redirects) and a pre-configured
tenacity retry decorator for transient HTTP failures.

Usage:
    from heber.core.http_client import create_async_http_client, http_retry

    client = create_async_http_client(base_url="http://localhost:8080")

    @http_retry
    async def fetch_data():
        resp = await client.get("/api/data")
        raise_for_status(resp)
        return resp.json()
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import structlog
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = structlog.get_logger(__name__)

# Default timeout for all HTTP clients (seconds)
DEFAULT_TIMEOUT = 30.0


def _log_request(request: httpx.Request) -> None:
    """Event hook: log outgoing HTTP requests at DEBUG level."""
    logger.debug(
        "http_request",
        method=str(request.method),
        url=str(request.url),
    )


def _log_response(response: httpx.Response) -> None:
    """Event hook: log incoming HTTP responses at DEBUG level."""
    request = response.request
    try:
        elapsed = response.elapsed.total_seconds() if response.elapsed else 0.0
    except RuntimeError:
        elapsed = 0.0
    logger.debug(
        "http_response",
        method=str(request.method),
        url=str(request.url),
        status_code=response.status_code,
        elapsed_s=round(elapsed, 3),
    )


async def _async_log_request(request: httpx.Request) -> None:
    """Async event hook: log outgoing HTTP requests at DEBUG level."""
    _log_request(request)


async def _async_log_response(response: httpx.Response) -> None:
    """Async event hook: log incoming HTTP responses at DEBUG level."""
    _log_response(response)


def create_http_client(
    base_url: str = "",
    timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Client:
    """Create a configured synchronous httpx.Client with standard defaults.

    Args:
        base_url: Base URL for all requests.
        timeout: Request timeout in seconds, or httpx.Timeout for fine-grained control.
        headers: Default headers to include on every request.
        **kwargs: Additional arguments passed to httpx.Client.

    Returns:
        Configured httpx.Client with logging hooks.
    """
    return httpx.Client(
        base_url=base_url,
        timeout=timeout,
        headers=headers or {},
        follow_redirects=True,
        event_hooks={
            "request": [_log_request],
            "response": [_log_response],
        },
        **kwargs,
    )


def create_async_http_client(
    base_url: str = "",
    timeout: float | httpx.Timeout = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Create a configured asynchronous httpx.AsyncClient with standard defaults.

    Args:
        base_url: Base URL for all requests.
        timeout: Request timeout in seconds, or httpx.Timeout for fine-grained control.
        headers: Default headers to include on every request.
        **kwargs: Additional arguments passed to httpx.AsyncClient.

    Returns:
        Configured httpx.AsyncClient with logging hooks.
    """
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        headers=headers or {},
        follow_redirects=True,
        event_hooks={
            "request": [_async_log_request],
            "response": [_async_log_response],
        },
        **kwargs,
    )


def raise_for_status(response: httpx.Response) -> None:
    """Validate HTTP response status, raising with structured context on failure.

    Args:
        response: The httpx response to validate.

    Raises:
        httpx.HTTPStatusError: If the response status indicates an error,
            with additional structured logging context.
    """
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        logger.error(
            "http_error",
            method=str(response.request.method),
            url=str(response.request.url),
            status_code=response.status_code,
            response_text=response.text[:500],
        )
        raise


# Pre-configured retry decorator for transient HTTP failures.
# Retries up to 3 times with exponential backoff (1s, 2s, 4s) capped at 10s.
# Only retries on transport errors (connection failures, DNS) and timeouts.
http_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
