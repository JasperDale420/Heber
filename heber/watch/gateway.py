"""Shared Data Gateway URL helpers for watch-service clients."""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

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
