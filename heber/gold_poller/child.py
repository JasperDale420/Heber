"""Child-process entrypoint for an isolated Gold pipeline run.

Deliberately a leaf module: it imports stdlib plus ``heber.config`` and nothing
else. ``spawn`` re-imports whichever module holds the process target before it
can run anything, so hanging this off ``heber.gold_poller.service`` made every
child import ``HeberReader`` and its pandas/pyarrow chain — 150 MB and ~1.1 s
of bootstrap — before reaching the pipeline it was spawned to run. That is
memory the pipeline itself cannot then use, on the one code path whose whole
purpose is surviving memory exhaustion.
"""

from __future__ import annotations

import importlib
from datetime import UTC, date, datetime
from typing import Any

from heber.config import Settings, get_settings


def _instantiate_pipeline(entry: dict[str, Any], settings: Settings) -> Any:
    """Lazy-import and instantiate a pipeline class."""
    mod = importlib.import_module(entry["module"])
    cls = getattr(mod, entry["class"])
    return cls(
        project=settings.gold_poller_project,
        version=settings.gold_poller_version,
    )


def _compute_pipeline(entry: dict[str, Any], start_date: date, end_date: date) -> dict[str, Any]:
    """Instantiate and run a single pipeline synchronously.

    Reconstructs settings from the environment because the ``spawn`` child
    inherits no parent state.
    """
    settings = get_settings()
    pipeline = _instantiate_pipeline(entry, settings)

    kwargs: dict[str, Any] = {
        "start_date": datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC),
        "end_date": datetime(end_date.year, end_date.month, end_date.day, tzinfo=UTC),
    }
    if entry["datasets"] is not None:
        kwargs["datasets"] = entry["datasets"]

    return pipeline.run(**kwargs)


def pipeline_subprocess_entry(
    entry: dict[str, Any],
    start_date: date,
    end_date: date,
    result_q: Any,
) -> None:
    """Run a pipeline and report the outcome back to the parent.

    A hard crash here (OOM SIGKILL, segfault) is contained to this process and
    leaves no result on the queue — the parent detects the missing payload. Any
    Python-level error is reported back so the parent can log and continue.
    """
    try:
        result_q.put({"ok": True, "stats": _compute_pipeline(entry, start_date, end_date)})
    except BaseException as exc:  # noqa: BLE001 - report every failure to the parent
        result_q.put({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
