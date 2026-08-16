"""Quarantined rows must not come back through the reader.

Gold writers move rows they refuse to serve into a sibling `_quarantine` tree
and the callers assume readers never walk it. They did: `_collect_parquet_files`
recursed the whole dataset root and filtered only `._*` sidecars and `.tmp`
partial writes, so every quarantined row was returned by `read_gold` — 10,392 of
the 10,512 files under `meta_label_features/version=v1` were quarantine files.

Underscore-prefixed directories are the conventional "not data" marker in Hive
layouts, and no real partition uses one: partitions are `dt=`, `hour=`,
`project=`, `version=`, `feed=`, `instrument_type=`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from heber.reader.core import _collect_parquet_files

pytestmark = pytest.mark.unit


def _parquet(path: Path, alert_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"alert_id": [alert_id]}).to_parquet(path, index=False)


def test_quarantine_tree_is_not_collected(tmp_path: Path) -> None:
    _parquet(tmp_path / "dt=2026-03-11" / "part-a.parquet", "kept")
    _parquet(tmp_path / "_quarantine" / "all_greeks_null" / "dt=2026-03-11" / "q.parquet", "held")

    collected = _collect_parquet_files(tmp_path)

    assert [Path(f).name for f in collected] == ["part-a.parquet"]


def test_lock_tree_is_not_collected(tmp_path: Path) -> None:
    _parquet(tmp_path / "dt=2026-03-11" / "part-a.parquet", "kept")
    (tmp_path / "_locks").mkdir()
    (tmp_path / "_locks" / "stray.parquet").write_bytes(b"")

    assert len(_collect_parquet_files(tmp_path)) == 1


def test_real_partition_components_still_collected(tmp_path: Path) -> None:
    _parquet(tmp_path / "project=watch" / "version=v1" / "dt=2026-03-11" / "hour=14" / "p.parquet", "kept")

    assert len(_collect_parquet_files(tmp_path)) == 1


def test_an_underscore_ancestor_of_the_root_does_not_hide_the_data(tmp_path: Path) -> None:
    """Only components below the root are inspected, not the caller's own path."""
    root = tmp_path / "_scratch" / "version=v1"
    _parquet(root / "dt=2026-03-11" / "part-a.parquet", "kept")

    assert len(_collect_parquet_files(root)) == 1


def test_appledouble_and_tmp_filters_still_apply(tmp_path: Path) -> None:
    _parquet(tmp_path / "dt=2026-03-11" / "part-a.parquet", "kept")
    (tmp_path / "dt=2026-03-11" / "._part-a.parquet").write_bytes(b"junk")
    (tmp_path / "dt=2026-03-11" / "part-b.parquet.tmp").write_bytes(b"partial")

    assert [Path(f).name for f in _collect_parquet_files(tmp_path)] == ["part-a.parquet"]


def test_a_quarantined_file_is_excluded_even_when_addressed_directly(tmp_path: Path) -> None:
    """No read path may reach quarantine, not even a caller naming the exact file."""
    held = tmp_path / "_quarantine" / "reason" / "dt=2026-03-11" / "q.parquet"
    _parquet(held, "held")

    assert _collect_parquet_files(held) == []
