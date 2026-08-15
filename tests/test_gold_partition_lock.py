"""Gold partition rewriters must not interleave.

The compactor merges a partition's fragments and deletes the originals; the
retro-flag migration replaces individual fragments in place. Run concurrently
they duplicate or drop label rows, so both take the same lock.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest
from filelock import Timeout

from heber.gold.partition_lock import LOCK_DIRNAME, partition_lock

pytestmark = pytest.mark.unit


def test_second_holder_is_refused_rather_than_racing(tmp_path: Path) -> None:
    partition = tmp_path / "dt=2026-03-11"
    partition.mkdir()

    with partition_lock(partition), pytest.raises(Timeout):
        with partition_lock(partition, timeout=0.05):
            pass


def test_different_partitions_do_not_block_each_other(tmp_path: Path) -> None:
    a = tmp_path / "dt=2026-03-11"
    b = tmp_path / "dt=2026-03-12"
    a.mkdir()
    b.mkdir()

    with partition_lock(a), partition_lock(b, timeout=0.05):
        pass


def test_lock_file_is_not_left_inside_the_partition(tmp_path: Path) -> None:
    """A zero-byte file beside the data breaks pyarrow's dataset auto-walk."""
    partition = tmp_path / "dt=2026-03-11"
    partition.mkdir()

    with partition_lock(partition):
        assert list(partition.iterdir()) == []

    assert (tmp_path / LOCK_DIRNAME).is_dir()


def _load(script_name: str):
    path = Path(__file__).parent.parent / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(script_name.removesuffix(".py"), path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_compactor_refuses_a_partition_the_migration_holds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    compact = _load("compact_gold.py")
    partition = tmp_path / "dt=2026-03-11"
    partition.mkdir()
    for name in ("part-a.parquet", "part-b.parquet"):
        pd.DataFrame({"alert_id": [name]}).to_parquet(partition / name, index=False)
    monkeypatch.setattr(compact, "LOCK_TIMEOUT_SECONDS", 0.05)

    with partition_lock(partition):
        with pytest.raises(Timeout):
            compact.compact_partition(partition)

    # Nothing was merged or deleted while the lock was held elsewhere.
    assert len(compact.find_parquet_files(partition)) == 2


def test_migration_refuses_a_partition_the_compactor_holds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    flagger = _load("flag_truncated_label_windows.py")
    monkeypatch.setattr(flagger, "GOLD_LABELS", tmp_path)
    monkeypatch.setattr(flagger, "LOCK_TIMEOUT_SECONDS", 0.05)
    partition = tmp_path / "dt=2026-03-11"
    partition.mkdir()
    pd.DataFrame(
        [{"alert_id": "x", "horizon": "swing", "window_duration_hours": 3.0}],
    ).to_parquet(partition / "part-a.parquet", index=False)

    with partition_lock(partition):
        _rows, _flagged, errors = flagger.flag_partition("2026-03-11", write=True)

    assert len(errors) == 1
    assert "lock" in errors[0].lower()
