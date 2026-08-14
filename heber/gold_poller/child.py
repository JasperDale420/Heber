"""Child-process entrypoint for an isolated Gold pipeline run.

Deliberately a leaf module: nothing but stdlib at module scope. ``spawn``
re-imports whichever module holds the process target before it can run
anything, and two things follow from that.

First, weight. Hanging this off ``heber.gold_poller.service`` made every child
import ``HeberReader`` and its pandas/pyarrow chain — 143 MB and ~0.6 s of
bootstrap, against 37 MB and ~0.06 s now — before reaching the pipeline it was
spawned for. That is memory the pipeline itself cannot then use, on the one
code path whose whole purpose is surviving memory exhaustion.

Second, reporting. Anything imported at module scope here is imported *during*
that bootstrap, where a failure kills the child before any of our code runs and
the reason reaches nobody — the parent can only report a death without a
result, which is indistinguishable from the OOM kill this isolation exists for.
So config is imported inside the functions instead, where the failure is caught
and reported back over the queue.
"""

from __future__ import annotations

import importlib
import traceback
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from heber.config import Settings


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
    from heber.config import get_settings

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
    Python-level error is reported back, with its traceback, so the parent can
    say what went wrong instead of guessing from an exit code.
    """
    try:
        result_q.put({"ok": True, "stats": _compute_pipeline(entry, start_date, end_date)})
    except BaseException as exc:  # noqa: BLE001 - report every failure to the parent
        result_q.put(
            {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
            }
        )
