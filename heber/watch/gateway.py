"""Shared Data Gateway URL helpers for watch-service clients."""

from __future__ import annotations

import math
import os
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx

DEFAULT_GATEWAY_API_PREFIX = "/api/v1"
DEFAULT_ROUTE_QUOTE_MAX_AGE_SECONDS = 300

# Env vars that supply the gateway API key, in the same precedence order used
# by ``empire_gateway_client.auth`` and ``heber.config.Settings.watch_gateway_api_key``.
# Listed here so 401 diagnostics can report which vars were checked without
# importing the optional empire_gateway_client package.
GATEWAY_API_KEY_ENV_VARS: tuple[str, ...] = (
    "HEBER_WATCH_GATEWAY_API_KEY",
    "DATA_GATEWAY_API_KEY",
    "GATEWAY_API_KEY",
    "EMPIRE_GATEWAY_KEY",
    "X_GATEWAY_KEY",
)

GATEWAY_AUTH_HEADER_NAME = "X-Gateway-Key"


def is_retryable_http_status(status_code: int) -> bool:
    """Return True if the HTTP status code indicates a transient error."""
    return status_code in {429, 500, 502, 503, 504}


def parse_retry_after(headers: Any) -> float | None:
    """Parse Retry-After header from response headers.

    Handles both integer seconds and HTTP-date formats (though currently
    only float/int parsing is implemented as that's what we need).
    Returns None if header is missing or invalid.
    """
    if not hasattr(headers, "get"):
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        val = float(raw)
        return val if val >= 0 else None
    except (TypeError, ValueError):
        return None


def gateway_auth_headers(gateway_api_key: str | None) -> dict[str, str]:
    """Build HTTP headers for Data Gateway auth when a key is configured."""
    if gateway_api_key is None:
        return {}
    normalized_key = gateway_api_key.strip()
    if not normalized_key:
        return {}
    return {GATEWAY_AUTH_HEADER_NAME: normalized_key}


def describe_gateway_auth_env() -> dict[str, Any]:
    """Return a credential-safe summary of the gateway auth env state.

    Used for diagnostic logging on 401s — never includes credential values.
    Reports header name(s), each env var consulted, whether it was set, and
    the redacted length of the first non-empty value found.
    """
    env_status: list[dict[str, Any]] = []
    first_set: str | None = None
    first_set_len: int | None = None
    for var in GATEWAY_API_KEY_ENV_VARS:
        raw = os.environ.get(var)
        is_set = raw is not None and raw.strip() != ""
        env_status.append({"name": var, "set": is_set})
        if is_set and first_set is None:
            first_set = var
            first_set_len = len(raw.strip()) if raw is not None else 0
    return {
        "auth_header_names": [GATEWAY_AUTH_HEADER_NAME],
        "env_vars_checked": env_status,
        "env_var_used": first_set,
        "key_length": first_set_len,
        "any_env_var_set": first_set is not None,
    }


def _normalize_gateway_base(raw_base: str) -> str:
    """Normalize base URL by stripping query/fragment parts."""
    parsed = urlsplit(raw_base)
    if parsed.scheme and parsed.netloc:
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")).rstrip("/")
    return raw_base.split("?", 1)[0].split("#", 1)[0].rstrip("/")


def _compute_prefixed_route(
    normalized_route: str,
    normalized_prefix: str,
    route_has_prefix: bool,
    base_has_prefix: bool,
) -> str:
    """Compute the API-prefixed route string."""
    if not normalized_prefix:
        return normalized_route
    if route_has_prefix and base_has_prefix:
        suffix = normalized_route[len(normalized_prefix) :]
        if suffix.startswith("/"):
            return suffix
        return f"/{suffix}" if suffix else ""
    if route_has_prefix or base_has_prefix:
        return normalized_route
    return f"{normalized_prefix}{normalized_route}"


def gateway_url_candidates(
    gateway_url: str,
    route: str,
    api_prefix: str = DEFAULT_GATEWAY_API_PREFIX,
    include_legacy_fallback: bool = True,
) -> list[str]:
    """Return gateway endpoint candidates with API-prefix-first ordering.

    The watch stack historically used both `/api/v1/...` and legacy `/<provider>/...`
    paths. This helper standardizes construction while preserving compatibility
    through fallback ordering.
    """
    base = _normalize_gateway_base(gateway_url.strip())
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

    prefixed_route = _compute_prefixed_route(
        normalized_route,
        normalized_prefix,
        route_has_prefix,
        base_has_prefix,
    )

    prefixed = f"{base}{prefixed_route}"
    legacy = f"{base_without_prefix}{normalized_route}" if base_has_prefix else f"{base}{normalized_route}"

    candidates = [prefixed]
    if not include_legacy_fallback:
        return candidates
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


def should_try_legacy_fallback_for_status(status_code: int) -> bool:
    """Return True when trying legacy route fallback can plausibly help.

    Legacy fallback is for route-shape compatibility, so it should only
    run when the prefixed route is not found.
    """
    return status_code == 404


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
    stale_symbols: list[str] | None = None,
) -> dict[str, Any]:
    """Build standardized payload for partial quote coverage across a batch."""
    stale_symbols = stale_symbols or []
    available_set = set(available_symbols)
    invalid_set = set(invalid_symbols)
    stale_set = set(stale_symbols)
    missing_symbols = [
        symbol
        for symbol in requested_symbols
        if symbol not in available_set and symbol not in invalid_set and symbol not in stale_set
    ]
    return {
        "route": route,
        "failure": "quote_coverage_partial",
        "requested_count": len(requested_symbols),
        "available_count": len(available_symbols),
        "invalid_count": len(invalid_symbols),
        "stale_count": len(stale_symbols),
        "missing_count": len(missing_symbols),
        "missing_symbols": missing_symbols[:5],
        "invalid_symbols": invalid_symbols[:5],
        "stale_symbols": stale_symbols[:5],
    }


def coerce_optional_float(value: Any) -> float | None:
    """Convert payload values to float when possible, rejecting non-finite results.

    Rejects None, booleans, NaN, and Inf. Used by consumer, poller, and features
    modules for safe numeric extraction from untrusted payloads.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return numeric
    except (TypeError, ValueError):
        return None


def should_continue_route_fallback(route_failures: list[dict[str, Any]]) -> bool:
    """Return True when it is useful to try legacy route fallback URLs.

    Only continues when the latest failure is a 404 (route-shape mismatch);
    other status codes indicate the route was found but the request itself failed.
    """
    if not route_failures:
        return True
    latest = route_failures[-1]
    if latest.get("failure") != "http_status":
        return True
    status = latest.get("status")
    if not isinstance(status, int):
        return True
    return should_try_legacy_fallback_for_status(status)


def _coerce_numeric_to_utc(value: int | float) -> datetime | None:
    """Convert a numeric epoch (seconds or milliseconds) to a UTC datetime."""
    epoch = value / 1000.0 if abs(value) >= 100_000_000_000 else value
    try:
        return datetime.fromtimestamp(epoch, tz=UTC)
    except (OSError, OverflowError, ValueError):
        return None


def _coerce_string_to_utc(value: str) -> datetime | None:
    """Parse an ISO-8601 string or numeric string to a UTC datetime."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return _coerce_numeric_string_to_utc(value)
    return _ensure_utc(parsed)


def _coerce_numeric_string_to_utc(value: str) -> datetime | None:
    """Attempt to parse a string as a numeric epoch value."""
    try:
        numeric_value = float(value)
        epoch = numeric_value / 1000.0 if abs(numeric_value) >= 100_000_000_000 else numeric_value
        return datetime.fromtimestamp(epoch, tz=UTC)
    except (OSError, OverflowError, ValueError):
        return None


def _ensure_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def coerce_utc_timestamp(value: Any) -> datetime | None:
    """Convert timestamp payload values into UTC-aware datetimes."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, int | float):
        return _coerce_numeric_to_utc(value)
    if isinstance(value, str):
        return _coerce_string_to_utc(value)
    return None


def extract_quote_timestamp(quote: dict[str, Any]) -> datetime | None:
    """Extract quote timestamp from known payload keys."""
    for key in ("timestamp", "ts_event", "t"):
        parsed = coerce_utc_timestamp(quote.get(key))
        if parsed is not None:
            return parsed
    return None


def quote_age_seconds(quote: dict[str, Any], now: datetime) -> float | None:
    """Compute quote age in seconds from payload timestamp to now."""
    ts = extract_quote_timestamp(quote)
    if ts is None:
        return None
    now_utc = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    return max(0.0, (now_utc - ts).total_seconds())
