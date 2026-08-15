"""The file walk must honour the dt range the caller already declared.

`_collect_parquet_files` rglobs the entire feed tree on every read, regardless
of `time_range`. On the production bars feed that is 2,580 partition
directories walked to read one month of 22, and on a cold page cache it was
measured at 15m48s — per read, five reads per market_regime run, against an
1800s pipeline timeout. `prune_by_dt=True` already tells the reader the range;
the walk simply ignored it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.reader.core import HeberReader, _collect_parquet_files


def _write(base: Path, dt: str, *, instrument_type: str = "equity", name: str = "part.parquet") -> Path:
    part = base / "silver" / "feed=bars" / f"instrument_type={instrument_type}" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    ts = datetime.fromisoformat(dt).replace(hour=20, tzinfo=UTC)
    table = pa.Table.from_pandas(
        pd.DataFrame(
            [
                {
                    "ts_event": ts,
                    "ts_available": ts,
                    "instrument_key": "equity:AAPL",
                    "timeframe": "1Day",
                    "close": 100.0,
                }
            ]
        )
    )
    pq.write_table(table, str(part / name))
    return part


def _tree(base: Path, dts: list[str]) -> Path:
    for dt in dts:
        _write(base, dt)
    return base / "silver" / "feed=bars" / "instrument_type=equity"


DTS = ["2026-01-05", "2026-02-10", "2026-03-15", "2026-04-20", "2026-05-25"]


def test_scoped_walk_visits_only_the_requested_partitions(tmp_path: Path) -> None:
    root = _tree(tmp_path, DTS)

    scoped = _collect_parquet_files(str(root), dt_range=("2026-02-10", "2026-03-15"))

    assert len(scoped) == 2
    assert all("dt=2026-02-10" in f or "dt=2026-03-15" in f for f in scoped)


def test_scoped_walk_matches_full_walk_when_the_range_covers_everything(tmp_path: Path) -> None:
    """Scoping must not change which files are found, only how many dirs are visited."""
    root = _tree(tmp_path, DTS)

    full = sorted(_collect_parquet_files(str(root)))
    scoped = sorted(_collect_parquet_files(str(root), dt_range=("2026-01-01", "2026-12-31")))

    assert scoped == full


def test_dt_directories_are_found_below_instrument_type(tmp_path: Path) -> None:
    """Reads without instrument_type start a level higher, so dt is a grandchild."""
    _write(tmp_path, "2026-02-10", instrument_type="equity")
    _write(tmp_path, "2026-05-25", instrument_type="crypto")
    feed_root = tmp_path / "silver" / "feed=bars"

    scoped = _collect_parquet_files(str(feed_root), dt_range=("2026-02-01", "2026-02-28"))

    assert len(scoped) == 1
    assert "dt=2026-02-10" in scoped[0]


def test_falls_back_to_a_full_walk_when_no_dt_partitions_exist(tmp_path: Path) -> None:
    """An unpartitioned layout must still be read, not silently return nothing."""
    flat = tmp_path / "silver" / "feed=bars" / "instrument_type=equity"
    flat.mkdir(parents=True)
    table = pa.Table.from_pandas(pd.DataFrame([{"a": 1}]))
    pq.write_table(table, str(flat / "part.parquet"))

    scoped = _collect_parquet_files(str(flat), dt_range=("2026-01-01", "2026-12-31"))

    assert len(scoped) == 1


def test_sidecars_and_partials_stay_filtered_when_scoped(tmp_path: Path) -> None:
    """The scoped path must keep the AppleDouble and .tmp filtering."""
    part = _write(tmp_path, "2026-02-10")
    (part / "._part.parquet").write_bytes(b"sidecar")
    (part / "part.parquet.tmp").write_bytes(b"partial")
    root = tmp_path / "silver" / "feed=bars" / "instrument_type=equity"

    scoped = _collect_parquet_files(str(root), dt_range=("2026-02-01", "2026-02-28"))

    assert len(scoped) == 1
    assert scoped[0].endswith("part.parquet")


def test_read_silver_scopes_the_walk_when_pruning_by_dt(tmp_path: Path, monkeypatch) -> None:
    """prune_by_dt already declares the range; the walk must receive it."""
    _tree(tmp_path, DTS)
    seen: list[tuple[str, str] | None] = []

    from heber.reader import core as reader_core

    original = reader_core._collect_parquet_files

    def _spy(root, dt_range=None):
        seen.append(dt_range)
        return original(root, dt_range=dt_range)

    monkeypatch.setattr(reader_core, "_collect_parquet_files", _spy)

    reader = HeberReader(tmp_path)
    reader.read_silver(
        "bars",
        instrument_type="equity",
        time_range=("2026-02-01", "2026-03-31"),
        prune_by_dt=True,
    )

    assert seen and seen[0] == ("2026-02-01", "2026-03-31"), f"walk not scoped: {seen}"


def test_read_silver_returns_the_same_rows_scoped_or_not(tmp_path: Path) -> None:
    """Scoping is a cost change, not a result change."""
    _tree(tmp_path, DTS)
    reader = HeberReader(tmp_path)

    unscoped = reader.read_silver("bars", instrument_type="equity", time_range=("2026-02-01", "2026-03-31"))
    scoped = reader.read_silver(
        "bars", instrument_type="equity", time_range=("2026-02-01", "2026-03-31"), prune_by_dt=True
    )

    pd.testing.assert_frame_equal(
        unscoped.sort_values("ts_event").reset_index(drop=True),
        scoped.sort_values("ts_event").reset_index(drop=True),
    )
