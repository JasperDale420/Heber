"""Label rows whose outcome predates their alert must leave the canonical tree.

`ts_available` on a label row is the outcome time, and for an expiry that is
`window_end`. The timezone relabel in `add_trading_hours` put some window_ends
before their own alert, so those rows carry `ts_available < ts_event` — an
as-of read pushes `ts_available <= asof_time` into the scan and surfaces the
label before the alert that produced it. A flag cannot stop that; only moving
the rows out of the `dt=` partitions can.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.integration

_SCRIPT = Path(__file__).parent.parent / "scripts" / "quarantine_inverted_label_windows.py"

_ALERT = datetime(2026, 3, 11, 15, 0, tzinfo=UTC)


@pytest.fixture
def script(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    spec = importlib.util.spec_from_file_location("quarantine_inverted_label_windows", _SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "GOLD_LABELS", tmp_path)
    return module


def _labels(*offsets_hours: float) -> pd.DataFrame:
    """One row per offset; ts_available = alert + offset (negative = inverted)."""
    return pd.DataFrame(
        [
            {
                "alert_id": f"a{i}",
                "ts_event": _ALERT,
                "ts_available": _ALERT + timedelta(hours=off),
                "outcome": "expired",
                "window_duration_hours": off,
            }
            for i, off in enumerate(offsets_hours)
        ]
    )


def _write(root: Path, dt_str: str, frames: dict[str, pd.DataFrame]) -> Path:
    partition = root / f"dt={dt_str}"
    partition.mkdir(parents=True)
    for name, df in frames.items():
        df.to_parquet(partition / name, index=False)
    return partition


def test_dry_run_moves_nothing(script, tmp_path: Path) -> None:
    partition = _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, 3.0)})
    before = (partition / "part-a.parquet").read_bytes()

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=False)

    assert (rows, moved, errors) == (2, 1, [])
    assert (partition / "part-a.parquet").read_bytes() == before
    assert not (tmp_path / "_quarantine").exists()


def test_inverted_rows_are_moved_out_of_the_partition(script, tmp_path: Path) -> None:
    partition = _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, 3.0, -1.5)})

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)
    assert (rows, moved, errors) == (3, 2, [])

    kept = pd.read_parquet(partition / "part-a.parquet")
    assert list(kept["alert_id"]) == ["a1"]

    held = pd.read_parquet(tmp_path / "_quarantine" / script.QUARANTINE_REASON / "dt=2026-03-11")
    assert sorted(held["alert_id"]) == ["a0", "a2"]
    # The rows are preserved intact, not summarised.
    assert sorted(held.columns) == sorted(kept.columns)


def test_no_rows_are_lost_across_the_move(script, tmp_path: Path) -> None:
    partition = _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, 3.0, -1.5, 8.0)})
    before = set(pd.read_parquet(partition / "part-a.parquet")["alert_id"])

    script.quarantine_partition("2026-03-11", write=True)

    after = set(pd.read_parquet(partition / "part-a.parquet")["alert_id"]) | set(
        pd.read_parquet(tmp_path / "_quarantine" / script.QUARANTINE_REASON / "dt=2026-03-11")["alert_id"]
    )
    assert after == before


def test_a_fragment_with_nothing_inverted_is_untouched(script, tmp_path: Path) -> None:
    partition = _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(3.0, 8.0)})
    before = (partition / "part-a.parquet").read_bytes()

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)

    assert (rows, moved, errors) == (2, 0, [])
    assert (partition / "part-a.parquet").read_bytes() == before


def test_a_fragment_that_is_entirely_inverted_is_emptied_not_left_behind(script, tmp_path: Path) -> None:
    partition = _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, -1.5)})

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)

    assert (rows, moved, errors) == (2, 2, [])
    assert pd.read_parquet(partition / "part-a.parquet").empty
    assert len(pd.read_parquet(tmp_path / "_quarantine" / script.QUARANTINE_REASON / "dt=2026-03-11")) == 2


def test_rerun_is_a_noop_and_does_not_duplicate_quarantined_rows(script, tmp_path: Path) -> None:
    _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, 3.0)})

    script.quarantine_partition("2026-03-11", write=True)
    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)

    assert (rows, moved, errors) == (1, 0, [])
    held = pd.read_parquet(tmp_path / "_quarantine" / script.QUARANTINE_REASON / "dt=2026-03-11")
    assert len(held) == 1


def test_naive_timestamps_are_compared_as_utc(script, tmp_path: Path) -> None:
    """Some partitions store ts columns without a timezone."""
    df = _labels(-2.5, 3.0)
    df["ts_event"] = df["ts_event"].dt.tz_localize(None)
    df["ts_available"] = df["ts_available"].dt.tz_localize(None)
    _write(tmp_path, "2026-03-11", {"part-a.parquet": df})

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)

    assert (rows, moved, errors) == (2, 1, [])


def test_missing_partition_is_an_error_not_silence(script) -> None:
    rows, moved, errors = script.quarantine_partition("2026-01-01", write=False)

    assert (rows, moved) == (0, 0)
    assert len(errors) == 1
    assert "missing" in errors[0]


def test_nested_fragments_with_the_same_stem_do_not_overwrite_each_other(script, tmp_path: Path) -> None:
    """rglob reaches nested layouts; a stem-only quarantine name would collide."""
    partition = tmp_path / "dt=2026-03-11"
    for hour in ("hour=14", "hour=15"):
        (partition / hour).mkdir(parents=True)
        _labels(-2.5, 3.0).to_parquet(partition / hour / "part-a.parquet", index=False)

    rows, moved, errors = script.quarantine_partition("2026-03-11", write=True)

    assert (rows, moved, errors) == (4, 2, [])
    held = pd.read_parquet(tmp_path / "_quarantine" / script.QUARANTINE_REASON / "dt=2026-03-11")
    assert len(held) == 2


def test_a_recovery_copy_of_every_rewritten_fragment_is_kept(script, tmp_path: Path) -> None:
    """The retained rows exist only in the fragment being replaced, on a
    filesystem with no journal — keep a durable copy before touching it."""
    _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5, 3.0)})

    script.quarantine_partition("2026-03-11", write=True)

    backups = list((tmp_path / "_quarantine" / script.QUARANTINE_REASON / "_source_backup").rglob("*.parquet"))
    assert len(backups) == 1
    assert sorted(pd.read_parquet(backups[0])["alert_id"]) == ["a0", "a1"]


def test_the_quarantine_tree_is_not_itself_scanned(script, tmp_path: Path) -> None:
    _write(tmp_path, "2026-03-11", {"part-a.parquet": _labels(-2.5)})
    script.quarantine_partition("2026-03-11", write=True)

    assert script.partition_dates() == ["2026-03-11"]
