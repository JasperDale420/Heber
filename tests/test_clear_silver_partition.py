"""Unit tests for scripts/clear_silver_partition — the delete-then-write recovery tool.

No real lakehouse data is touched; every case builds a throwaway Silver tree
under ``tmp_path``.
"""

import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# The tool lives in scripts/ (not an importable package), so load it by path.
_TOOL = Path(__file__).resolve().parents[1] / "scripts" / "clear_silver_partition.py"
_spec = importlib.util.spec_from_file_location("clear_silver_partition", _TOOL)
assert _spec and _spec.loader
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
clear_partitions = _mod.clear_partitions
partition_path = _mod.partition_path

pytestmark = pytest.mark.unit


def _make_partition(silver_root: Path, feed: str, itype: str, day: str, rows: int) -> Path:
    p = silver_root / f"feed={feed}" / f"instrument_type={itype}" / f"dt={day}"
    p.mkdir(parents=True, exist_ok=True)
    tbl = pa.table({"event_id": [f"e{i}" for i in range(rows)], "symbol": ["AAPL"] * rows})
    pq.write_table(tbl, p / "part-000.parquet")
    return p


def test_partition_path_resolves_expected(tmp_path):
    root = tmp_path / "silver"
    root.mkdir()
    p = partition_path(root, "oi_change", "equity", "2026-07-01")
    assert p == (root / "feed=oi_change" / "instrument_type=equity" / "dt=2026-07-01").resolve()


@pytest.mark.parametrize(
    "feed,itype,day",
    [
        ("../etc", "equity", "2026-07-01"),  # feed traversal
        ("oi_change", "eq/uity", "2026-07-01"),  # instrument_type separator
        ("oi_change", "equity", "2026-7-1"),  # malformed date
        ("oi_change", "equity", "not-a-date"),
    ],
)
def test_partition_path_rejects_unsafe(tmp_path, feed, itype, day):
    with pytest.raises(ValueError):
        partition_path(tmp_path, feed, itype, day)


def test_dry_run_reports_but_does_not_delete(tmp_path):
    root = tmp_path / "silver"
    part = _make_partition(root, "oi_change", "equity", "2026-07-01", rows=50)
    report = clear_partitions(root, "oi_change", "equity", ["2026-07-01"], apply=False)
    assert part.exists()  # untouched
    assert report[0]["action"] == "would-delete"
    assert report[0]["files"] == 1
    assert report[0]["rows"] == 50


def test_apply_deletes_only_target_day(tmp_path):
    root = tmp_path / "silver"
    keep = _make_partition(root, "oi_change", "equity", "2026-07-02", rows=10)
    drop = _make_partition(root, "oi_change", "equity", "2026-07-01", rows=50)
    report = clear_partitions(root, "oi_change", "equity", ["2026-07-01"], apply=True)
    assert not drop.exists()  # deleted
    assert keep.exists()  # sibling untouched
    assert report[0]["action"] == "deleted"


def test_missing_partition_is_reported_not_error(tmp_path):
    root = tmp_path / "silver"
    root.mkdir()
    report = clear_partitions(root, "oi_change", "equity", ["2026-01-01"], apply=True)
    assert report[0]["action"] == "missing"
    assert report[0]["files"] == 0
