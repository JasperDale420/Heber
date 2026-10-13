"""A fragment the reader cannot use must not silently shrink the answer (#46).

`_parquet_files_in` used to drop zero-byte and unstatable files with only a
warning. When a readable sibling existed the dataset opened normally and the
caller got a partial frame: the rows the lost file should hold were simply
missing, which is the "unreadable is not empty" failure one level down.

The contract now:

* An unstatable data file always fails the read. ``stat()`` failing on a file
  the walk just listed is a degraded mount or a file vanishing mid-read, not a
  known writer artifact, and there is no way to tell what it held.
* A zero-byte data file fails a read whose walk the caller scoped to specific
  ``dt=`` partitions (``prune_by_dt`` + ``time_range``). That is the read most
  likely to mistake a hole for absence, e.g. a Gold rebuild of one date.
* An unscoped walk still skips zero-byte files with a warning. Known historical
  writer artifacts of this kind sit in old partitions of high-volume feeds, and
  failing every unscoped read of those feeds on their account would turn a
  bounded hole into an outage. ``test_heber_reader_skips_empty_files`` pins it.
* AppleDouble sidecars (``._*``) and ``.tmp`` partial writes stay ignorable.
"""

from __future__ import annotations

import errno
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.reader.core import HeberReader, HeberReadError, _collect_parquet_files

pytestmark = pytest.mark.unit

_SCOPED = {
    "time_range": ("2026-08-07T00:00:00Z", "2026-08-07T23:59:59Z"),
    "prune_by_dt": True,
}


def _partition(root: Path, dt: str) -> Path:
    part = root / "silver" / "feed=darkpool" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    return part


def _write_rows(part: Path, name: str, dt: str, rows: int = 3) -> None:
    ts = pd.Timestamp(f"{dt}T15:00:00", tz="UTC").to_pydatetime()
    pq.write_table(
        pa.table(
            {
                "ts_event": pa.array([ts] * rows, pa.timestamp("us", tz="UTC")),
                "ts_available": pa.array([ts] * rows, pa.timestamp("us", tz="UTC")),
                "instrument_key": ["equity:AAPL"] * rows,
                "notional": [1.0] * rows,
            }
        ),
        str(part / name),
    )


def _unstatable(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Make ``stat()`` fail with EIO for one file name, the way a degraded mount does."""
    real_stat = Path.stat

    def flaky_stat(self: Path, *args: object, **kwargs: object) -> object:
        if self.name == name:
            raise OSError(errno.EIO, "Input/output error", str(self))
        return real_stat(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "stat", flaky_stat)


class TestScopedRead:
    def test_zero_byte_sibling_fails_the_read(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "part-truncated.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError, match="part-truncated.parquet"):
            HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity", **_SCOPED)

    def test_unstatable_sibling_fails_the_read(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        _write_rows(part, "part-lost.parquet", "2026-08-07")
        _unstatable(monkeypatch, "part-lost.parquet")

        with pytest.raises(HeberReadError, match="part-lost.parquet"):
            HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity", **_SCOPED)

    def test_appledouble_sibling_is_still_ignored(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "._part-good.parquet").write_bytes(b"")
        (part / "._sidecar.parquet").write_bytes(b"resource fork")
        (part / "part-partial.parquet.tmp").write_bytes(b"")

        rows = HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity", **_SCOPED)

        assert len(rows) == 3

    def test_all_good_read_is_unchanged(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-a.parquet", "2026-08-07")
        _write_rows(part, "part-b.parquet", "2026-08-07", rows=2)

        rows = HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity", **_SCOPED)

        assert len(rows) == 5

    def test_zero_byte_file_outside_the_requested_range_does_not_fail_it(self, tmp_path: Path) -> None:
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        (_partition(tmp_path, "2026-08-01") / "part-truncated.parquet").write_bytes(b"")

        rows = HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity", **_SCOPED)

        assert len(rows) == 3

    def test_gold_scoped_read_fails_on_a_zero_byte_sibling(self, tmp_path: Path) -> None:
        part = tmp_path / "gold" / "dataset=feat" / "project=kairos" / "version=v1" / "dt=2026-08-07"
        part.mkdir(parents=True)
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "part-truncated.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError, match="part-truncated.parquet"):
            HeberReader(tmp_path).read_gold("feat", project="kairos", **_SCOPED)

    def test_collector_raises_for_a_scoped_walk(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "part-truncated.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError):
            _collect_parquet_files(
                str(tmp_path / "silver" / "feed=darkpool" / "instrument_type=equity"),
                dt_range=("2026-08-07", "2026-08-07"),
            )


class TestUnscopedRead:
    def test_unstatable_sibling_fails_the_read(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _write_rows(_partition(tmp_path, "2026-08-01"), "part-good.parquet", "2026-08-01")
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-lost.parquet", "2026-08-07")
        _unstatable(monkeypatch, "part-lost.parquet")

        with pytest.raises(HeberReadError, match="part-lost.parquet"):
            HeberReader(tmp_path).read_silver("darkpool")

    def test_every_file_unstatable_fails_rather_than_reading_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The degraded-mount case: nothing statable used to read as "no data"."""
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-lost.parquet", "2026-08-07")
        _unstatable(monkeypatch, "part-lost.parquet")

        with pytest.raises(HeberReadError):
            HeberReader(tmp_path).read_silver("darkpool")

    def test_zero_byte_sibling_is_still_skipped(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "part-truncated.parquet").write_bytes(b"")

        rows = HeberReader(tmp_path).read_silver("darkpool")

        assert len(rows) == 3


def test_daily_health_reports_an_unstatable_file_instead_of_crashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The invariant scan collects files outside its per-partition guard; an
    unstatable file must become a named read failure, not abort the report."""
    from datetime import date

    from heber.ops.daily_health import _check_silver_invariants

    part = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-08-07"
    part.mkdir(parents=True)
    _write_rows(part, "part-lost.parquet", "2026-08-07")
    _unstatable(monkeypatch, "part-lost.parquet")

    class _Settings:
        silver_path = tmp_path / "silver"

    result = _check_silver_invariants(date(2026, 8, 7), _Settings())  # type: ignore[arg-type]

    assert result["status"] == "fail"
    assert result["observed"]["read_failures"]
