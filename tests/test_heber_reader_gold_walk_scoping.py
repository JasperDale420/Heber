"""`read_gold` must be able to prune its walk the way `read_silver` can.

`breadth_proxy` reads Gold `momentum_features` — ~7,000 files across 88 dates —
and `read_gold` walked all of them on every call because it had no
`prune_by_dt` at all. That read was measured at 25m27s on one cold run, the
single largest component of the market_regime pipeline once the Silver reads
were scoped. `ticker_base_rates`, which sits 1.7s under its 1800s timeout,
reads Gold labels the same way.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.reader import core as reader_core
from heber.reader.core import HeberReader

DTS = ["2026-01-05", "2026-02-10", "2026-03-15", "2026-04-20"]


def _write_gold(base: Path, dt: str, *, time_col: str = "ts_event") -> None:
    part = base / "gold" / "dataset=momentum_features" / "project=watch" / "version=v1" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    ts = datetime.fromisoformat(dt).replace(hour=20, tzinfo=UTC)
    row = {
        time_col: ts,
        "ts_available": ts,
        "instrument_key": "equity:AAPL",
        "momentum_20d": 1.0,
    }
    pq.write_table(pa.Table.from_pandas(pd.DataFrame([row])), str(part / "part.parquet"))


@pytest.fixture
def gold_tree(tmp_path: Path) -> Path:
    for dt in DTS:
        _write_gold(tmp_path, dt)
    return tmp_path


def _spy_walk(monkeypatch: pytest.MonkeyPatch) -> list:
    seen: list = []
    original = reader_core._collect_parquet_files

    def _spy(root, dt_range=None):
        seen.append(dt_range)
        return original(root, dt_range=dt_range)

    monkeypatch.setattr(reader_core, "_collect_parquet_files", _spy)
    return seen


def test_read_gold_scopes_the_walk_when_pruning(gold_tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen = _spy_walk(monkeypatch)
    reader = HeberReader(gold_tree)

    reader.read_gold(
        "momentum_features",
        project="watch",
        time_range=("2026-02-01", "2026-03-31"),
        prune_by_dt=True,
    )

    assert ("2026-02-01", "2026-03-31") in seen, f"walk not scoped: {seen}"


def test_read_gold_walk_unscoped_by_default(gold_tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Opt-in, matching read_silver — existing callers keep their behaviour."""
    seen = _spy_walk(monkeypatch)
    reader = HeberReader(gold_tree)

    reader.read_gold("momentum_features", project="watch", time_range=("2026-02-01", "2026-03-31"))

    assert all(r is None for r in seen), f"walk scoped without opt-in: {seen}"


def test_read_gold_returns_the_same_rows_scoped_or_not(gold_tree: Path) -> None:
    reader = HeberReader(gold_tree)

    unscoped = reader.read_gold("momentum_features", project="watch", time_range=("2026-02-01", "2026-03-31"))
    scoped = reader.read_gold(
        "momentum_features",
        project="watch",
        time_range=("2026-02-01", "2026-03-31"),
        prune_by_dt=True,
    )

    assert len(scoped) == 2
    pd.testing.assert_frame_equal(
        unscoped.sort_values("ts_event").reset_index(drop=True),
        scoped.sort_values("ts_event").reset_index(drop=True),
    )


def test_pruning_a_label_timed_dataset_fails_loud(tmp_path: Path) -> None:
    """dt is derived from ts_event, so pruning a ts_label filter is unsound.

    write_gold partitions on `date(ts_event)`. A dataset whose only time column
    is `ts_label` is filtered on a different axis, so dropping partitions by the
    label range can discard rows that belong in the result. Silently under-
    reading a label set is worse than refusing.
    """
    for dt in DTS:
        _write_gold(tmp_path, dt, time_col="ts_label")
    reader = HeberReader(tmp_path)

    with pytest.raises(ValueError, match="ts_event"):
        reader.read_gold(
            "momentum_features",
            project="watch",
            time_range=("2026-02-01", "2026-03-31"),
            prune_by_dt=True,
        )
