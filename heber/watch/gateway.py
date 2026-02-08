"""Shared Data Gateway URL helpers for watch-service clients."""

from __future__ import annotations

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
    base = gateway_url.rstrip("/")
    normalized_route = route if route.startswith("/") else f"/{route}"
    stripped_prefix = api_prefix.strip().strip("/")
    normalized_prefix = f"/{stripped_prefix}" if stripped_prefix else ""

    prefixed_route = normalized_route
    if normalized_prefix and not (
        normalized_route == normalized_prefix or normalized_route.startswith(f"{normalized_prefix}/")
    ):
        prefixed_route = f"{normalized_prefix}{normalized_route}"

    prefixed = f"{base}{prefixed_route}"
    legacy = f"{base}{normalized_route}"

    if prefixed == legacy:
        return [prefixed]
    return [prefixed, legacy]
