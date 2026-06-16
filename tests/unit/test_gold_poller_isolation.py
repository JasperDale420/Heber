"""Tests for gold-poller pipeline crash isolation.

Regression for the fail-silent OOM: pipelines run in one sequential in-process
loop, so a hard crash (OOM SIGKILL) in one pipeline used to kill the whole
poller and silently starve every pipeline scheduled after it. Each pipeline now
runs in an isolated subprocess; a crash or hang must surface as a normal
exception in the parent (which the retry/continue loop already handles), never
take the poller down.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from heber.gold_poller.service import GoldFeaturePoller

_MODULE = "tests.unit._isolation_pipelines"
_START = date(2026, 6, 4)
_END = date(2026, 6, 5)


def _entry(cls: str, name: str = "isolated_test") -> dict:
    return {
        "name": name,
        "module": _MODULE,
        "class": cls,
        "datasets": None,
        "gold_datasets": [name],
    }


def _poller(timeout: int = 60) -> GoldFeaturePoller:
    poller = GoldFeaturePoller()
    # Only the timeout is read parent-side; the child reconstructs settings itself.
    poller._settings = SimpleNamespace(gold_poller_pipeline_timeout_seconds=timeout)
    return poller


@pytest.mark.slow
def test_isolated_healthy_pipeline_returns_stats() -> None:
    stats = _poller()._execute_pipeline_isolated(_entry("HealthyPipeline"), _START, _END)
    assert stats["status"] == "success"
    assert stats["rows"] == 3


@pytest.mark.slow
def test_isolated_hard_crash_raises_instead_of_killing_parent() -> None:
    """A pipeline that os._exit()s (OOM-like) must raise in the parent, not kill it."""
    with pytest.raises(RuntimeError):
        _poller()._execute_pipeline_isolated(_entry("CrashPipeline"), _START, _END)
    # Reaching here proves the parent survived the child's hard death.


@pytest.mark.slow
def test_isolated_hang_times_out() -> None:
    with pytest.raises(TimeoutError):
        _poller(timeout=2)._execute_pipeline_isolated(_entry("HangPipeline"), _START, _END)
