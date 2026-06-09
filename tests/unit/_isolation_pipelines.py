"""Importable stand-in pipelines for gold-poller isolation tests.

These live in a real (non-test_) module so the ``spawn`` multiprocessing child
can import them by module path — the same way the poller imports real pipelines
via ``importlib.import_module``.
"""

from __future__ import annotations

import os
import time
from typing import Any


class HealthyPipeline:
    """Returns a normal stats dict, like a well-behaved pipeline."""

    def __init__(self, project: str = "test", version: str = "v1") -> None:
        self.project = project
        self.version = version

    def run(self, **kwargs: Any) -> dict[str, Any]:
        return {"status": "success", "rows": 3}


class CrashPipeline:
    """Hard-crashes the process (``os._exit``) — simulates an OOM SIGKILL.

    Bypasses Python exception handling entirely, so the child dies without
    reporting a result. This is the failure mode that used to kill the poller.
    """

    def __init__(self, project: str = "test", version: str = "v1") -> None:
        self.project = project
        self.version = version

    def run(self, **kwargs: Any) -> dict[str, Any]:
        os._exit(137)


class HangPipeline:
    """Never returns — simulates a wedged pipeline that must be timed out."""

    def __init__(self, project: str = "test", version: str = "v1") -> None:
        self.project = project
        self.version = version

    def run(self, **kwargs: Any) -> dict[str, Any]:
        while True:
            time.sleep(1)
