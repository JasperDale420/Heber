"""Root conftest — isolate test log output from production log files."""

from __future__ import annotations

import os


def pytest_configure(config):
    """Redirect EMPIRE_LOG_DIR to a temp directory so tests never pollute production logs/."""
    os.environ["EMPIRE_LOG_DIR"] = "/tmp/heber-test-logs"
