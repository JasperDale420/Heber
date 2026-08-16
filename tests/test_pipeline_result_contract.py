"""Contract test: every registered Gold pipeline returns the single nested shape.

The Gold poller (`heber.gold_poller.service`) requires that every pipeline
``run()`` return the nested result shape::

    {gold_dataset_name: {"status": ..., "rows": N, "path": ...}}

A flat top-level ``{"status": ..., "rows": N}`` is a contract violation — the
poller now raises on it instead of silently adapting it. This test drives every
pipeline in ``PIPELINE_REGISTRY`` through an empty-data run and asserts the
nested shape, so a regression to the flat shape fails here rather than at 16:35
ET in production.
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.gold_poller.child import _instantiate_pipeline
from heber.gold_poller.service import PIPELINE_REGISTRY

_START = datetime(2026, 1, 5)
_END = datetime(2026, 1, 10)


def _registry_entry(name: str) -> dict:
    for entry in PIPELINE_REGISTRY:
        if entry["name"] == name:
            return entry
    raise AssertionError(f"pipeline '{name}' not in PIPELINE_REGISTRY")


@pytest.fixture
def empty_reader() -> MagicMock:
    """A reader whose every read returns an empty frame (drives the no_data path)."""
    reader = MagicMock()
    reader.read_silver.return_value = pd.DataFrame()
    reader.read_gold.return_value = pd.DataFrame()
    return reader


def _instantiate_with_reader(entry: dict, reader: MagicMock, monorepo_root) -> object:
    """Build a pipeline with the empty reader injected.

    ``excursion_analytics`` reads trading-system ledgers from disk rather than
    Silver, so it also takes a ``monorepo_root`` that we point at an empty tmp
    dir to keep the run hermetic.
    """
    import importlib

    mod = importlib.import_module(entry["module"])
    cls = getattr(mod, entry["class"])
    kwargs = {"reader": reader, "project": "test", "version": "v1"}
    if entry["name"] == "excursion_analytics":
        kwargs["monorepo_root"] = monorepo_root
    return cls(**kwargs)


@pytest.mark.parametrize("entry", PIPELINE_REGISTRY, ids=[e["name"] for e in PIPELINE_REGISTRY])
def test_registered_pipeline_returns_nested_shape(entry: dict, empty_reader: MagicMock, tmp_path) -> None:
    """Every registered pipeline returns {dataset: {status, rows, ...}}, never flat."""
    pipeline = _instantiate_with_reader(entry, empty_reader, tmp_path)

    run_kwargs: dict = {"start_date": _START, "end_date": _END, "dry_run": True}
    if entry["datasets"] is not None:
        run_kwargs["datasets"] = entry["datasets"]

    result = pipeline.run(**run_kwargs)

    # Nested shape: top-level dict keyed by Gold dataset name.
    assert isinstance(result, dict), f"{entry['name']} did not return a dict"
    assert "status" not in result, (
        f"{entry['name']} returned a FLAT result {{'status': ...}}; pipelines must "
        f"return the nested shape {{gold_dataset_name: {{'status', 'rows', 'path'}}}}"
    )
    assert result, f"{entry['name']} returned an empty result dict"
    assert set(result) == set(entry["gold_datasets"]), (
        f"{entry['name']} returned top-level keys {sorted(result)}; expected the "
        f"registered Gold dataset names {sorted(entry['gold_datasets'])}"
    )

    for dataset_name, info in result.items():
        assert isinstance(info, dict), f"{entry['name']}[{dataset_name}] is not a dict"
        assert "status" in info, f"{entry['name']}[{dataset_name}] missing 'status'"
        assert "rows" in info, f"{entry['name']}[{dataset_name}] missing 'rows'"


@pytest.mark.parametrize("entry", PIPELINE_REGISTRY, ids=[e["name"] for e in PIPELINE_REGISTRY])
def test_registered_pipeline_survives_poller_aggregation(entry: dict, empty_reader: MagicMock, tmp_path) -> None:
    """The poller's row aggregation runs cleanly over each result.

    The poller computes ``total_rows = sum(s.get("rows", 0) for s in
    result.values() if isinstance(s, dict))``. Every nested value must be a dict
    carrying an integer ``rows`` so that aggregation never silently yields 0
    from a mis-shaped result.
    """
    pipeline = _instantiate_with_reader(entry, empty_reader, tmp_path)

    run_kwargs: dict = {"start_date": _START, "end_date": _END, "dry_run": True}
    if entry["datasets"] is not None:
        run_kwargs["datasets"] = entry["datasets"]

    result = pipeline.run(**run_kwargs)

    total_rows = sum(s.get("rows", 0) for s in result.values() if isinstance(s, dict))
    assert total_rows == 0, f"{entry['name']} reported rows on an empty-data run"
    assert all(isinstance(s.get("rows"), int) for s in result.values() if isinstance(s, dict)), (
        f"{entry['name']} has a non-integer 'rows' in its nested result"
    )


def test_instantiate_pipeline_helper_round_trips() -> None:
    """The poller's lazy import/instantiate path resolves every registry class."""
    from types import SimpleNamespace

    settings = SimpleNamespace(
        gold_poller_project="test",
        gold_poller_version="v1",
        gold_poller_version_for=lambda _name: "v1",
    )
    for entry in PIPELINE_REGISTRY:
        pipeline = _instantiate_pipeline(entry, settings)
        assert hasattr(pipeline, "run"), f"{entry['name']} has no run()"
