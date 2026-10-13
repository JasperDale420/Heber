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
from typing import Any

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


def test_daily_health_reports_a_zero_byte_file_in_the_report_partition(tmp_path: Path) -> None:
    """The scan reads one specific dt= partition, so a lost write in it is a
    read failure, not a healthy partition with fewer rows."""
    from datetime import date

    from heber.ops.daily_health import _check_silver_invariants

    part = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-08-07"
    part.mkdir(parents=True)
    _write_rows(part, "part-good.parquet", "2026-08-07")
    (part / "part-truncated.parquet").write_bytes(b"")

    class _Settings:
        silver_path = tmp_path / "silver"

    result = _check_silver_invariants(date(2026, 8, 7), _Settings())  # type: ignore[arg-type]

    assert result["status"] == "fail"
    assert result["observed"]["read_failures"] == ["bars/instrument_type=equity"]


class TestReadParquetDataset:
    """The arbitrary-path read is for callers that name the exact data they want."""

    def test_zero_byte_sibling_in_a_named_partition_fails_the_read(self, tmp_path: Path) -> None:
        part = tmp_path / "gold" / "dataset=feat" / "dt=2026-08-07"
        part.mkdir(parents=True)
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "part-truncated.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError, match="part-truncated.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(part)

    def test_zero_byte_file_anywhere_under_the_named_path_fails_the_read(self, tmp_path: Path) -> None:
        root = tmp_path / "gold" / "dataset=feat"
        _write_rows(_mkdir(root / "dt=2026-08-01"), "part-good.parquet", "2026-08-01")
        (_mkdir(root / "dt=2026-08-07") / "part-truncated.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError, match="part-truncated.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(root)

    def test_unstatable_sibling_fails_the_read(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        part = _mkdir(tmp_path / "gold" / "dataset=feat" / "dt=2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        _write_rows(part, "part-lost.parquet", "2026-08-07")
        _unstatable(monkeypatch, "part-lost.parquet")

        with pytest.raises(HeberReadError, match="part-lost.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(part)

    def test_a_zero_byte_file_named_directly_fails_the_read(self, tmp_path: Path) -> None:
        part = _mkdir(tmp_path / "gold" / "dataset=labels" / "version=v1")
        (part / "data.parquet").write_bytes(b"")

        with pytest.raises(HeberReadError, match=r"data\.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(part / "data.parquet")

    def test_all_good_partition_reads_unchanged(self, tmp_path: Path) -> None:
        part = _mkdir(tmp_path / "gold" / "dataset=feat" / "dt=2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        (part / "._part-good.parquet").write_bytes(b"")

        assert len(HeberReader(tmp_path).read_parquet_dataset(part)) == 3


def _mkdir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


class TestUnstatableRequestedPath:
    """A path the caller names that cannot be statted is unknown, not absent.

    `Path.exists()` reports only ENOENT-style errors as False; EIO or EACCES
    escape it as a raw OSError, outside the reader's HeberReadError contract.
    """

    def test_read_parquet_dataset_on_an_unstatable_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        part = _mkdir(tmp_path / "gold" / "dataset=labels" / "version=v1")
        _write_rows(part, "data.parquet", "2026-08-07")
        _unstatable(monkeypatch, "data.parquet")

        with pytest.raises(HeberReadError, match=r"data\.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(part / "data.parquet")

    def test_read_silver_on_an_unstatable_feed_directory(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        _unstatable(monkeypatch, "feed=darkpool")

        with pytest.raises(HeberReadError, match="feed=darkpool"):
            HeberReader(tmp_path).read_silver("darkpool")

    def test_a_symlink_loop_is_not_read_as_absent(self, tmp_path: Path) -> None:
        """`Path.exists()` answers False for ELOOP; a looped path is broken, not missing."""
        silver = _mkdir(tmp_path / "silver")
        (silver / "feed=looped").symlink_to(silver / "feed=looped")
        loop_file = tmp_path / "looped.parquet"
        loop_file.symlink_to(loop_file)

        with pytest.raises(HeberReadError, match="feed=looped"):
            HeberReader(tmp_path).read_silver("looped")
        with pytest.raises(HeberReadError, match="looped.parquet"):
            HeberReader(tmp_path).read_parquet_dataset(loop_file)

    def test_a_looped_explicit_gold_version_is_not_read_as_absent(self, tmp_path: Path) -> None:
        project = _mkdir(tmp_path / "gold" / "dataset=feat" / "project=kairos")
        _write_rows(_mkdir(project / "version=v0" / "dt=2026-08-07"), "part-good.parquet", "2026-08-07")
        (project / "version=v1").symlink_to(project / "version=v1")

        with pytest.raises(HeberReadError, match="version=v1"):
            HeberReader(tmp_path).read_gold("feat", project="kairos", version="v1")
        assert HeberReader(tmp_path).read_gold("feat", project="kairos", version="v9").empty

    def test_a_looped_newer_version_fails_a_latest_read_instead_of_serving_an_older_one(self, tmp_path: Path) -> None:
        project = _mkdir(tmp_path / "gold" / "dataset=feat" / "project=kairos")
        _write_rows(_mkdir(project / "version=v0" / "dt=2026-08-07"), "part-good.parquet", "2026-08-07")
        (project / "version=v1").symlink_to(project / "version=v1")

        with pytest.raises(HeberReadError, match="version=v1"):
            HeberReader(tmp_path).read_gold("feat", project="kairos")

    def test_a_looped_requested_dt_partition_fails_a_scoped_read(self, tmp_path: Path) -> None:
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        crypto = _mkdir(tmp_path / "silver" / "feed=darkpool" / "instrument_type=crypto")
        (crypto / "dt=2026-08-07").symlink_to(crypto / "dt=2026-08-07")

        with pytest.raises(HeberReadError, match="dt=2026-08-07"):
            HeberReader(tmp_path).read_silver("darkpool", **_SCOPED)

    def test_a_looped_dt_partition_outside_the_range_does_not_fail_the_read(self, tmp_path: Path) -> None:
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        crypto = _mkdir(tmp_path / "silver" / "feed=darkpool" / "instrument_type=crypto")
        (crypto / "dt=2026-08-01").symlink_to(crypto / "dt=2026-08-01")

        assert len(HeberReader(tmp_path).read_silver("darkpool", **_SCOPED)) == 3

    def test_appledouble_sidecar_directories_are_skipped_before_any_stat(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On Docker bind mounts stat() on a `._` sidecar raises EPERM; discovery
        must filter them by name first, as the file walk already does."""
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        _mkdir(tmp_path / "silver" / "feed=darkpool" / "._instrument_type=equity" / "dt=2026-08-07")
        project = _mkdir(tmp_path / "gold" / "dataset=feat" / "project=kairos")
        _write_rows(_mkdir(project / "version=v0" / "dt=2026-08-07"), "part-good.parquet", "2026-08-07")
        _mkdir(project / "._sidecar" / "version=v9")
        real_stat = Path.stat

        def eperm_on_sidecars(self: Path, *args: object, **kwargs: object) -> object:
            if any(part.startswith("._") for part in self.parts):
                raise OSError(errno.EPERM, "Operation not permitted", str(self))
            return real_stat(self, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(Path, "stat", eperm_on_sidecars)

        assert len(HeberReader(tmp_path).read_silver("darkpool", **_SCOPED)) == 3
        assert len(HeberReader(tmp_path).read_gold("feat", project="kairos")) == 3

    def test_a_missing_path_still_reads_empty(self, tmp_path: Path) -> None:
        assert HeberReader(tmp_path).read_parquet_dataset(tmp_path / "nope" / "data.parquet").empty
        assert HeberReader(tmp_path).read_silver("never_written").empty


def _unlistable(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Make listing one directory fail with EACCES, the way a permissions or
    mount fault does. pathlib's glob/rglob swallow this and simply omit the
    directory's contents."""
    import os

    real_scandir = os.scandir

    def flaky_scandir(path: Any = ".") -> Any:
        if Path(path).name == name:
            raise PermissionError(errno.EACCES, "Permission denied", str(path))
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", flaky_scandir)


class TestUnlistableDirectory:
    def test_whole_feed_read_fails_instead_of_dropping_a_partition(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_rows(_partition(tmp_path, "2026-08-01"), "part-good.parquet", "2026-08-01")
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-hidden.parquet", "2026-08-07")
        _unlistable(monkeypatch, "dt=2026-08-07")

        with pytest.raises(HeberReadError, match="dt=2026-08-07"):
            HeberReader(tmp_path).read_silver("darkpool")

    def test_scoped_read_fails_when_an_instrument_type_cannot_be_listed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-good.parquet", "2026-08-07")
        crypto = tmp_path / "silver" / "feed=darkpool" / "instrument_type=crypto" / "dt=2026-08-07"
        _write_rows(_mkdir(crypto), "part-hidden.parquet", "2026-08-07")
        _unlistable(monkeypatch, "instrument_type=crypto")

        with pytest.raises(HeberReadError, match="instrument_type=crypto"):
            HeberReader(tmp_path).read_silver("darkpool", **_SCOPED)

    def test_latest_gold_read_fails_when_versions_cannot_be_listed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        project = _mkdir(tmp_path / "gold" / "dataset=feat" / "project=kairos")
        _write_rows(_mkdir(project / "version=v0" / "dt=2026-08-07"), "part-good.parquet", "2026-08-07")
        _unlistable(monkeypatch, "project=kairos")

        with pytest.raises(HeberReadError, match="project=kairos"):
            HeberReader(tmp_path).read_gold("feat", project="kairos")

    def test_daily_health_reports_an_unlistable_feed_instead_of_skipping_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from datetime import date

        from heber.ops.daily_health import _check_silver_invariants

        part = _mkdir(tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        _unlistable(monkeypatch, "feed=bars")

        class _Settings:
            silver_path = tmp_path / "silver"

        result = _check_silver_invariants(date(2026, 8, 7), _Settings())  # type: ignore[arg-type]

        assert result["status"] == "fail"
        assert result["observed"]["read_failures"] == ["bars"]

    def test_quarantine_and_sidecar_trees_are_still_never_listed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        _write_rows(_mkdir(part / "_quarantine"), "part-bad.parquet", "2026-08-07")
        _mkdir(part / "._sidecar")
        _unlistable(monkeypatch, "_quarantine")
        import os

        real_scandir = os.scandir

        def no_sidecars(path: Any = ".") -> Any:
            if Path(path).name.startswith("._"):
                raise PermissionError(errno.EPERM, "Operation not permitted", str(path))
            return real_scandir(path)

        monkeypatch.setattr(os, "scandir", no_sidecars)

        assert len(HeberReader(tmp_path).read_silver("darkpool")) == 3
        assert len(HeberReader(tmp_path).read_silver("darkpool", **_SCOPED)) == 3


def test_list_gold_versions_does_not_report_a_looped_root_as_empty(tmp_path: Path) -> None:
    gold = _mkdir(tmp_path / "gold")
    (gold / "dataset=looped").symlink_to(gold / "dataset=looped")

    with pytest.raises(HeberReadError, match="dataset=looped"):
        HeberReader(tmp_path).list_gold_versions("looped")
    assert HeberReader(tmp_path).list_gold_versions("never_written") == []


def _unclassifiable(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    """Make `DirEntry.is_dir()` fail with EIO for one entry. `os.walk` treats
    that as "not a directory", so the directory's contents silently vanish."""
    import os

    real_scandir = os.scandir

    class _Entry:
        def __init__(self, real: Any) -> None:
            self._real = real
            self.name = real.name
            self.path = real.path

        def is_dir(self, *, follow_symlinks: bool = True) -> bool:
            if self.name == name:
                raise OSError(errno.EIO, "Input/output error", self.path)
            return bool(self._real.is_dir(follow_symlinks=follow_symlinks))

        def is_file(self, *, follow_symlinks: bool = True) -> bool:
            return bool(self._real.is_file(follow_symlinks=follow_symlinks))

        def is_symlink(self) -> bool:
            return bool(self._real.is_symlink())

    class _Scandir:
        def __init__(self, path: Any) -> None:
            self._it = real_scandir(path)

        def __enter__(self) -> _Scandir:
            return self

        def __exit__(self, *exc: object) -> None:
            self._it.close()

        def __iter__(self) -> _Scandir:
            return self

        def __next__(self) -> _Entry:
            return _Entry(next(self._it))

        def close(self) -> None:
            self._it.close()

    monkeypatch.setattr(os, "scandir", _Scandir)


class TestWalkClassificationAndSkips:
    def test_an_unclassifiable_partition_fails_the_read(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _write_rows(_partition(tmp_path, "2026-08-01"), "part-good.parquet", "2026-08-01")
        _write_rows(_partition(tmp_path, "2026-08-07"), "part-hidden.parquet", "2026-08-07")
        _unclassifiable(monkeypatch, "dt=2026-08-07")

        with pytest.raises(HeberReadError, match="dt=2026-08-07"):
            HeberReader(tmp_path).read_silver("darkpool")

    def test_underscore_prefixed_files_are_never_read(self, tmp_path: Path) -> None:
        part = _partition(tmp_path, "2026-08-07")
        _write_rows(part, "part-good.parquet", "2026-08-07")
        _write_rows(part, "_staging.parquet", "2026-08-07")

        assert len(HeberReader(tmp_path).read_silver("darkpool")) == 3

    def test_a_quarantined_newer_version_is_never_served_as_latest(self, tmp_path: Path) -> None:
        project = _mkdir(tmp_path / "gold" / "dataset=feat" / "project=kairos")
        _write_rows(_mkdir(project / "version=v0" / "dt=2026-08-07"), "part-good.parquet", "2026-08-07")
        _write_rows(
            _mkdir(project / "_quarantine" / "version=v9" / "dt=2026-08-07"), "part-bad.parquet", "2026-08-07", 7
        )

        assert HeberReader(tmp_path).list_gold_versions("feat", project="kairos") == ["v0"]
        assert len(HeberReader(tmp_path).read_gold("feat", project="kairos")) == 3
