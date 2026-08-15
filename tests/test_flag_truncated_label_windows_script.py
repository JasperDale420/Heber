"""The retro-flag migration must add flags without disturbing anything else."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

from heber.ml.datasets import QUALITY_FLAG_LABEL_WINDOW_TRUNCATED, quality_flag_series

pytestmark = pytest.mark.integration

_SCRIPT = Path(__file__).parent.parent / "scripts" / "flag_truncated_label_windows.py"


@pytest.fixture
def script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    spec = importlib.util.spec_from_file_location("flag_truncated_label_windows", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "GOLD_LABELS", tmp_path)
    return module


def _write_partition(root: Path, dt_str: str, frames: dict[str, pd.DataFrame]) -> Path:
    partition = root / f"dt={dt_str}"
    partition.mkdir(parents=True)
    for name, df in frames.items():
        df.to_parquet(partition / name, index=False)
    return partition


def _labels(*specs: tuple[str, str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        [{"alert_id": a, "horizon": h, "window_duration_hours": w, "outcome": "expired"} for a, h, w in specs]
    )


def test_dry_run_reports_without_writing(script, tmp_path: Path) -> None:
    partition = _write_partition(
        tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "swing", 3.0), ("y", "swing", 448.0))}
    )
    before = (partition / "part-a.parquet").read_bytes()

    rows, flagged, errors = script.flag_partition("2026-03-11", write=False)

    assert (rows, flagged, errors) == (2, 1, [])
    assert (partition / "part-a.parquet").read_bytes() == before


def test_write_flags_only_the_truncated_rows(script, tmp_path: Path) -> None:
    partition = _write_partition(
        tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "leap", 5.0), ("y", "leap", 2688.0))}
    )

    rows, flagged, errors = script.flag_partition("2026-03-11", write=True)
    assert (rows, flagged, errors) == (2, 1, [])

    out = pd.read_parquet(partition / "part-a.parquet")
    flags = list(quality_flag_series(out))
    assert flags[0] == [QUALITY_FLAG_LABEL_WINDOW_TRUNCATED]
    assert flags[1] == []
    # Every other column survives the rewrite untouched.
    assert list(out["alert_id"]) == ["x", "y"]
    assert list(out["window_duration_hours"]) == [5.0, 2688.0]
    assert list(out["outcome"]) == ["expired", "expired"]


def test_rerun_is_a_noop(script, tmp_path: Path) -> None:
    partition = _write_partition(tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "swing", 3.0))})

    script.flag_partition("2026-03-11", write=True)
    after_first = (partition / "part-a.parquet").read_bytes()

    rows, flagged, errors = script.flag_partition("2026-03-11", write=True)

    assert (rows, flagged, errors) == (1, 0, [])
    assert (partition / "part-a.parquet").read_bytes() == after_first


def test_every_fragment_in_a_partition_is_visited(script, tmp_path: Path) -> None:
    _write_partition(
        tmp_path,
        "2026-03-11",
        {
            "part-a.parquet": _labels(("x", "swing", 3.0)),
            "part-b.parquet": _labels(("y", "leap", 5.0)),
            "part-c.parquet": _labels(("z", "intraday", 21.4)),
        },
    )

    rows, flagged, errors = script.flag_partition("2026-03-11", write=True)

    assert (rows, flagged, errors) == (3, 2, [])


def test_sidecars_and_partial_writes_are_skipped(script, tmp_path: Path) -> None:
    partition = _write_partition(tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "swing", 3.0))})
    (partition / "._part-a.parquet").write_bytes(b"appledouble junk")
    (partition / "part-b.parquet.tmp").write_bytes(b"half a write")

    rows, flagged, errors = script.flag_partition("2026-03-11", write=True)

    assert (rows, flagged, errors) == (1, 1, [])


def test_unreadable_fragment_is_reported_not_swallowed(script, tmp_path: Path) -> None:
    partition = _write_partition(tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "swing", 3.0))})
    (partition / "part-corrupt.parquet").write_bytes(b"not parquet")

    rows, flagged, errors = script.flag_partition("2026-03-11", write=True)

    assert (rows, flagged) == (1, 1)
    assert len(errors) == 1
    assert "part-corrupt.parquet" in errors[0]


def test_partition_dates_ignores_non_partition_directories(script, tmp_path: Path) -> None:
    _write_partition(tmp_path, "2026-03-11", {"part-a.parquet": _labels(("x", "swing", 3.0))})
    (tmp_path / "_quarantine").mkdir()

    assert script.partition_dates() == ["2026-03-11"]
