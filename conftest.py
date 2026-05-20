"""Root conftest — isolate test log output and reset cross-test logger state."""

from __future__ import annotations

import os

import pytest


def pytest_configure(config):
    """Redirect EMPIRE_LOG_DIR to a temp directory so tests never pollute production logs/."""
    os.environ["EMPIRE_LOG_DIR"] = "/tmp/heber-test-logs"


@pytest.fixture(autouse=True)
def _reset_empire_logger_globals():
    """Reset ``empire_core.logger``'s module-globals between tests.

    Without this, a test that calls ``configure_logging(service_name="heber-catalog")``
    (typically via importing ``heber.catalog.api`` or running its lifespan)
    leaves the global ``_service_name`` pinned to ``"heber-catalog"``. Every
    subsequent test's structlog emit then gets stamped with
    ``service="heber-catalog"`` and routed into the catalog's daily log file
    — which is how unrelated writer/consumer/backpressure emits ended up in
    ``logs/heber-catalog_errors_*.log`` historically. Resetting between tests
    is the deterministic fix; the production protection is
    ``configure_logging(force=True)`` which re-applies the requested
    service name even if a previous call has already configured.
    """
    try:
        from empire_core import logger as _ec_logger
    except ImportError:
        yield
        return

    # Start each test from a clean slate so a previous test's
    # configure_logging() call can't leak into this one.
    _ec_logger._service_name = "unknown"
    try:
        yield
    finally:
        # And leave a clean slate for the next test too.
        _ec_logger._service_name = "unknown"
