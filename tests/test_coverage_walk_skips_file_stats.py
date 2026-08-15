"""The partition walk must not stat files it is never going to open.

The zero-byte check was paid on every Parquet file on every pass, including
the ~95% a reuse pass skips. Measured on the production mount: `scandir` alone
runs at 6,669 files/sec, `scandir` plus a per-file `stat()` at 228 — 29x
slower, 4.24 ms of pure stat per file. Parallelism is per directory, so one
leaf holding ~800 files serialises 800 stats while occupying a worker. For
`feed=quotes` at ~825k files that is why a pass has never once finished.

The check belongs immediately before the footer read, where it is paid only
for files being opened anyway and 4 ms sits inside a 106 ms read.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.catalog import seeds

pytestmark = pytest.mark.unit


def _parquet(path: Path, rows: int = 3) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame({"x": range(rows)})), str(path))


def test_the_walk_stats_no_files(tmp_path, monkeypatch):
    """The whole point: no per-file stat during the listing."""
    part = tmp_path / "feed=q" / "dt=2026-08-15"
    for i in range(4):
        _parquet(part / f"part-{i}.parquet")

    statted: list[str] = []
    real_scandir = seeds.os.scandir

    class _Entry:
        """Wraps os.DirEntry (a C type, so it cannot be patched directly)."""

        def __init__(self, inner):
            self._inner = inner
            self.name = inner.name
            self.path = inner.path

        def is_dir(self, **kw):
            return self._inner.is_dir(**kw)

        def stat(self, **kw):
            statted.append(self.name)
            return self._inner.stat(**kw)

    class _Scandir:
        def __init__(self, path):
            self._it = real_scandir(path)

        def __enter__(self):
            return [_Entry(e) for e in self._it]

        def __exit__(self, *a):
            self._it.close()
            return False

    monkeypatch.setattr(seeds.os, "scandir", _Scandir)
    listing = seeds._list_one_directory(part, "2026-08-15")

    statted_files = [n for n in statted if n.endswith(".parquet")]

    assert len(listing.files) == 4, "files must still be collected"
    assert statted_files == [], f"the walk stat()ed files it may never open: {statted_files}"


def test_a_zero_byte_file_is_still_collected_by_the_walk(tmp_path):
    """It is filtered at read time now, not during the listing."""
    part = tmp_path / "feed=q" / "dt=2026-08-15"
    _parquet(part / "good.parquet")
    (part / "empty.parquet").touch()

    listing = seeds._list_one_directory(part, "2026-08-15")

    assert len(listing.files) == 2


def test_a_zero_byte_file_reads_as_empty_not_corrupt(tmp_path):
    """Distinguishable from real corruption: same row count, different meaning."""
    empty = tmp_path / "empty.parquet"
    empty.touch()

    assert seeds._read_row_count(empty) == 0


def test_a_truncated_file_still_reads_as_unreadable(tmp_path):
    """A partial write is corruption and must not be silently counted as zero."""
    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"PAR1garbage")

    assert seeds._read_row_count(corrupt) is None


def test_a_healthy_file_still_reports_its_rows(tmp_path):
    good = tmp_path / "good.parquet"
    _parquet(good, rows=7)

    assert seeds._read_row_count(good) == 7


def test_empty_files_are_counted_separately_from_unreadable_ones(tmp_path, monkeypatch):
    """`skipped_empty` must keep meaning benign debris, not corruption."""
    part = tmp_path / "feed=q" / "dt=2026-08-15"
    _parquet(part / "good.parquet", rows=5)
    (part / "empty.parquet").touch()
    monkeypatch.setattr(seeds.settings, "data_root", tmp_path)

    results = seeds._scan_partition_dates(tmp_path / "feed=q", None)

    assert results == [("2026-08-15", 5)], "the empty file must contribute no rows"


def test_pyarrow_is_still_never_handed_a_zero_byte_file(tmp_path, monkeypatch):
    """The original contract, now enforced at read time instead of during the walk.

    175 zero-byte part files exist in the lake, 173 under feed=quotes. Each one
    reaching pyarrow costs an open, a raised ArrowInvalid and a full traceback
    rendered into a JSON log line.
    """
    empty = tmp_path / "empty.parquet"
    empty.touch()
    opened: list[str] = []
    monkeypatch.setattr(seeds.pq, "read_metadata", lambda p, *a, **k: opened.append(str(p)))

    assert seeds._read_row_count(empty) == 0
    assert opened == [], "a zero-byte file was handed to pyarrow"
