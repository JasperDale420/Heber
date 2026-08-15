"""Tests for gold-poller pipeline crash isolation.

Regression for the fail-silent OOM: pipelines run in one sequential in-process
loop, so a hard crash (OOM SIGKILL) in one pipeline used to kill the whole
poller and silently starve every pipeline scheduled after it. Each pipeline now
runs in an isolated subprocess; a crash or hang must surface as a normal
exception in the parent (which the retry/continue loop already handles), never
take the poller down.
"""

from __future__ import annotations

import subprocess
import sys
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


def test_child_entrypoint_imports_nothing_heavy() -> None:
    """Pin the leaf property the two fixes above both depend on.

    ``spawn`` imports this chain before any of our code can run, so a heavy
    import here is memory the pipeline cannot use, and a *failing* import here
    kills the child mute — it never reaches the try/except that reports over the
    result queue. Both regress silently on a one-line import added to
    ``child.py`` or to either package ``__init__``, which is why this asserts the
    property rather than trusting a comment.
    """
    probe = (
        "import sys, heber.gold_poller.child; "
        "print(','.join(m for m in "
        "('pandas','pyarrow','numpy','heber.config','heber.reader','heber.gold_poller.service') "
        "if m in sys.modules))"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True)

    assert result.returncode == 0, f"the spawn bootstrap chain no longer imports cleanly:\n{result.stderr}"
    assert result.stdout.strip() == "", f"the spawn bootstrap chain now imports: {result.stdout.strip()}"


def test_isolated_child_settings_failure_names_the_cause(monkeypatch) -> None:
    """A child that cannot build its settings must say so, not die mute.

    The child entrypoint imports config inside the function rather than at module
    scope, so a settings failure lands in the reporting path. Imported at module
    scope it killed the child during ``spawn`` bootstrap instead — before any
    code could report — and the parent could only say "died without a result",
    which is indistinguishable from the OOM kill this isolation exists for.
    """
    # Build the poller first: it reads settings in __init__, so poisoning the
    # environment beforehand would fail the parent instead of the child.
    poller = _poller()
    monkeypatch.setenv("HEBER_GOLD_POLLER_PIPELINE_TIMEOUT_SECONDS", "1")  # below the ge=60 bound

    with pytest.raises(RuntimeError, match="ValidationError"):
        poller._execute_pipeline_isolated(_entry("HealthyPipeline"), _START, _END)
