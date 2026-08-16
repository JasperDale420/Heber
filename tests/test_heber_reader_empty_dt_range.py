"""A date range matching no partition must read nothing, not everything.

`_collect_parquet_files` falls back to the unscoped walk when `_scoped_dt_dirs`
returns no directories. That fallback exists for layouts with no `dt=`
partitioning at all, but it also fires when a feed *is* partitioned and simply
has no data in the requested range — so the read collects every file in the
feed instead of none.

Measured against production `feed=bars/instrument_type=equity`:

    dt_range 2015-09-01..2015-09-30 (no data) -> 5,967 files, 165.7s
    dt_range 2026-07-01..2026-07-31 (data)    -> 1,972 files,   6.2s

The rows are still filtered out by the scan predicate, so the answer stays
correct — this is purely wasted I/O, and it is paid on every empty month of a
chunked historical read.

The two cases are distinguished by whether the dataset has any `dt=` directory
at all, not by whether any fall inside the range.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from heber.reader.core import _collect_parquet_files, _failed_write_present, _scoped_dt_dirs


def _write(base: Path, dt: str, rows: int = 3) -> None:
    part = base / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp(f"{dt}T15:00:00", tz="UTC")
    pq.write_table(
        pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "ts_event": [ts] * rows,
                    "ts_available": [ts] * rows,
                    "instrument_key": ["equity:AAPL"] * rows,
                    "close": [1.0] * rows,
                }
            )
        ),
        str(part / "part-a.parquet"),
    )


def test_range_matching_no_partition_collects_nothing(tmp_path: Path) -> None:
    """The production case: a chunked read asking for a month before the data starts."""
    base = tmp_path / "feed=bars" / "instrument_type=equity"
    for dt in ("2026-07-01", "2026-07-02", "2026-07-03"):
        _write(base, dt)

    collected = _collect_parquet_files(str(base), dt_range=("2015-09-01", "2015-09-30"))

    assert collected == [], f"an empty range collected {len(collected)} files from other partitions"


def test_range_matching_some_partitions_collects_only_those(tmp_path: Path) -> None:
    base = tmp_path / "feed=bars" / "instrument_type=equity"
    for dt in ("2026-07-01", "2026-07-02", "2026-08-01"):
        _write(base, dt)

    collected = _collect_parquet_files(str(base), dt_range=("2026-07-01", "2026-07-31"))

    assert len(collected) == 2
    assert all("dt=2026-07" in c for c in collected)


def test_unpartitioned_layout_still_falls_back_to_the_full_walk(tmp_path: Path) -> None:
    """The fallback's real purpose: a dataset with no `dt=` directories at all.

    Removing it would make these layouts read as empty whenever a caller passes
    a range, so the distinction is between "no dt= partitioning exists" and
    "none of the partitions match".
    """
    base = tmp_path / "feed=flat"
    base.mkdir(parents=True)
    ts = pd.Timestamp("2026-07-01T15:00:00", tz="UTC")
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({"ts_event": [ts], "instrument_key": ["equity:AAPL"]})),
        str(base / "part-a.parquet"),
    )

    collected = _collect_parquet_files(str(base), dt_range=("2015-09-01", "2015-09-30"))

    assert len(collected) == 1, "an unpartitioned dataset stopped reading"


def test_partitions_at_the_deeper_level_are_still_found(tmp_path: Path) -> None:
    """A shallow out-of-range `dt=` must not stop the deeper probe.

    `dt=` sits at depth 1 when a caller scopes by `instrument_type` and depth 2
    when it does not. Returning as soon as the shallow glob saw *any* `dt=`
    directory would hide in-range data one level down, in a layout holding
    partitions at both depths.
    """
    feed = tmp_path / "feed=bars"
    _write(feed, "2020-01-01")  # depth 1, out of range
    _write(feed / "instrument_type=equity", "2026-07-01")  # depth 2, in range

    collected = _collect_parquet_files(str(feed), dt_range=("2026-07-01", "2026-07-31"))

    assert len(collected) == 1, "in-range data one level deeper was not found"
    assert "dt=2026-07-01" in collected[0]


class TestScopedDtDirsContract:
    """The three-way return is load-bearing in two callers, so pin it directly.

    An integration-level test only catches this once the fallback has already
    stopped happening.
    """

    def test_matching_partitions_are_returned(self, tmp_path: Path) -> None:
        base = tmp_path / "feed=bars" / "instrument_type=equity"
        _write(base, "2026-07-01")
        assert _scoped_dt_dirs(base, ("2026-07-01", "2026-07-31")) != []

    def test_partitioned_but_no_match_returns_empty_list(self, tmp_path: Path) -> None:
        base = tmp_path / "feed=bars" / "instrument_type=equity"
        _write(base, "2026-07-01")
        assert _scoped_dt_dirs(base, ("2015-09-01", "2015-09-30")) == []

    def test_unpartitioned_returns_none(self, tmp_path: Path) -> None:
        base = tmp_path / "feed=flat"
        base.mkdir(parents=True)
        assert _scoped_dt_dirs(base, ("2015-09-01", "2015-09-30")) is None


def test_failed_write_search_is_scoped_to_the_requested_range(tmp_path: Path) -> None:
    """A zero-byte file outside the range must not fail an unrelated read.

    `_failed_write_present` decides whether an unreadable result raises or reads
    as empty. Searching the whole feed would let a truncated file in an
    unrelated partition raise on every query.
    """
    base = tmp_path / "feed=bars" / "instrument_type=equity"
    _write(base, "2026-07-01")
    broken = base / "dt=2020-01-01"
    broken.mkdir(parents=True)
    (broken / "part-truncated.parquet").write_bytes(b"")

    assert _failed_write_present(str(base), dt_range=("2020-01-01", "2020-01-31")) is True
    assert _failed_write_present(str(base), dt_range=("2026-07-01", "2026-07-31")) is False
    # The range that matches NO partition is the one that discriminates: falling
    # back to the whole feed here would find the unrelated truncated file and
    # raise on a query that never asked about it.
    assert _failed_write_present(str(base), dt_range=("2015-09-01", "2015-09-30")) is False
