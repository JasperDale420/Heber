"""Shared Data Gateway URL helpers for watch-service clients."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

DEFAULT_GATEWAY_API_PREFIX = "/api/v1"


def gateway_url_candidates(
    gateway_url: str,
    route: str,
    api_prefix: str = DEFAULT_GATEWAY_API_PREFIX,
) -> list[str]:
    """Return gateway endpoint candidates with API-prefix-first ordering.

    The watch stack historically used both `/api/v1/...` and legacy `/<provider>/...`
    paths. This helper standardizes construction while preserving compatibility
    through fallback ordering.
    """
    raw_base = gateway_url.strip()
    parsed_base = urlsplit(raw_base)
    if parsed_base.scheme and parsed_base.netloc:
        # Base URLs should not carry query/fragment parts when building API routes.
        base = urlunsplit((parsed_base.scheme, parsed_base.netloc, parsed_base.path, "", "")).rstrip("/")
    else:
        base = raw_base.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    normalized_route = route if route.startswith("/") else f"/{route}"
    stripped_prefix = api_prefix.strip().strip("/")
    normalized_prefix = f"/{stripped_prefix}" if stripped_prefix else ""
    route_has_prefix = bool(normalized_prefix) and (
        normalized_route == normalized_prefix or normalized_route.startswith(f"{normalized_prefix}/")
    )
    base_has_prefix = bool(normalized_prefix) and (base == normalized_prefix or base.endswith(normalized_prefix))

    base_without_prefix = base
    if base_has_prefix:
        base_without_prefix = base[: -len(normalized_prefix)].rstrip("/")

    if normalized_prefix:
        if route_has_prefix and base_has_prefix:
            suffix = normalized_route[len(normalized_prefix) :]
            prefixed_route = suffix if suffix.startswith("/") else f"/{suffix}" if suffix else ""
        elif route_has_prefix or base_has_prefix:
            prefixed_route = normalized_route
        else:
            prefixed_route = f"{normalized_prefix}{normalized_route}"
    else:
        prefixed_route = normalized_route

    prefixed = f"{base}{prefixed_route}"
    legacy = f"{base_without_prefix}{normalized_route}" if base_has_prefix else f"{base}{normalized_route}"

    candidates = [prefixed]
    if legacy != prefixed:
        candidates.append(legacy)
    return candidates


def classify_gateway_http_error(error: Exception) -> str:
    """Classify gateway request exceptions into stable failure buckets."""
    if isinstance(error, httpx.TimeoutException):
        return "timeout"
    if isinstance(error, httpx.TransportError):
        return "transport_error"
    return "request_error"


def route_failure_for_exception(route: str, error: Exception, failure: str | None = None) -> dict[str, Any]:
    """Build a standardized route-failure payload for exceptions."""
    failure_code = failure or classify_gateway_http_error(error)
    return {
        "route": route,
        "failure": failure_code,
        "error_type": type(error).__name__,
        "error": str(error),
    }


def route_failure_for_http_status(route: str, status_code: int) -> dict[str, Any]:
    """Build a standardized route-failure payload for non-200 responses."""
    return {
        "route": route,
        "failure": "http_status",
        "status": status_code,
    }


def route_failure_for_payload_shape(route: str, failure: str, payload: Any) -> dict[str, Any]:
    """Build a standardized route-failure payload for decoded payload-shape mismatches."""
    return {
        "route": route,
        "failure": failure,
        "expected_type": "dict",
        "payload_type": type(payload).__name__,
    }


def route_failure_for_symbol_missing(route: str, symbol: str) -> dict[str, Any]:
    """Build a standardized route-failure payload for missing quote symbols."""
    return {
        "route": route,
        "failure": "quote_symbol_missing",
        "symbol": symbol,
    }


def route_failure_for_symbol_shape(route: str, symbol: str, payload: Any) -> dict[str, Any]:
    """Build a standardized route-failure payload for invalid per-symbol quote items."""
    return {
        "route": route,
        "failure": "quote_symbol_shape",
        "symbol": symbol,
        "expected_type": "dict",
        "payload_type": type(payload).__name__,
    }


def route_failure_for_partial_quotes(
    route: str,
    requested_symbols: list[str],
    available_symbols: list[str],
    invalid_symbols: list[str],
) -> dict[str, Any]:
    """Build standardized payload for partial quote coverage across a batch."""
    available_set = set(available_symbols)
    invalid_set = set(invalid_symbols)
    missing_symbols = [
        symbol for symbol in requested_symbols if symbol not in available_set and symbol not in invalid_set
    ]
    return {
        "route": route,
        "failure": "quote_coverage_partial",
        "requested_count": len(requested_symbols),
        "available_count": len(available_symbols),
        "invalid_count": len(invalid_symbols),
        "missing_count": len(missing_symbols),
        "missing_symbols": missing_symbols[:5],
        "invalid_symbols": invalid_symbols[:5],
    }
