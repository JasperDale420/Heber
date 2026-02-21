"""Route alias regression tests for catalog API compatibility paths."""

from __future__ import annotations

from heber.catalog.api import app


def test_catalog_exposes_unprefixed_dataset_and_feed_routes() -> None:
    route_index: set[tuple[str, str]] = set()
    for route in app.routes:
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if not methods or not path:
            continue
        for method in methods:
            route_index.add((method, path))

    expected = {
        ("GET", "/datasets"),
        ("GET", "/datasets/{name}"),
        ("GET", "/datasets/{name}/versions"),
        ("GET", "/datasets/{name}/coverage"),
        ("GET", "/datasets/{name}/versions/{version}"),
        ("GET", "/feeds"),
        ("GET", "/feeds/resolve"),
    }
    for route_key in expected:
        assert route_key in route_index
