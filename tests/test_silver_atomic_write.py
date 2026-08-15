"""Regression tests for the durability of the Silver parquet publish step.

A Silver part file becomes visible to every reader the instant it carries the
``.parquet`` name.  A zero-byte file at that name is not a harmless artifact:
pyarrow refuses to read it, and one of them under a feed is enough to make the
whole feed scan return nothing.  These tests pin the publish contract — a
``.parquet`` path only ever appears once the bytes behind it are complete and
on disk.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.writer.utils import write_silver_parquet

pytestmark = pytest.mark.unit

SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("instrument_key", pa.string()),
        pa.field("size", pa.int64()),
    ]
)
ROWS = [{"event_id": "abc123", "instrument_key": "equity:AAPL", "size": 100}]
PARTITION = "feed=darkpool/instrument_type=equity/dt=2026-08-07"


def _write(dest: Path) -> None:
    write_silver_parquet(
        rows=ROWS,
        schema=SCHEMA,
        file_path=dest,
        partition_key=PARTITION,
        dataset="darkpool",
    )


def test_zero_byte_parquet_is_never_published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A staged file that ended up empty must fail the flush, not become a part file.

    Reproduces the darkpool artifact: a zero-byte ``part-*.parquet`` that reads
    as a broken fragment and silently empties the feed.
    """

    def empty_write_table(_table: pa.Table, where: str | Path, **_kwargs: object) -> None:
        Path(where).write_bytes(b"")

    monkeypatch.setattr(pq, "write_table", empty_write_table)

    dest = tmp_path / "part-20260807231354544957-99dae4bf.parquet"
    with pytest.raises(OSError):
        _write(dest)

    assert not dest.exists(), "an empty staged file was published as a part file"
    assert list(tmp_path.iterdir()) == [], "staging file left behind"


def test_failed_write_leaves_no_staging_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A write that dies part-way must clean up after itself.

    Without this the volume accumulates orphaned ``.tmp`` files that no process
    ever revisits.
    """

    def failing_write_table(_table: pa.Table, where: str | Path, **_kwargs: object) -> None:
        Path(where).write_bytes(b"PAR1truncated")
        raise OSError("no space left on device")

    monkeypatch.setattr(pq, "write_table", failing_write_table)

    dest = tmp_path / "part-1.parquet"
    with pytest.raises(OSError):
        _write(dest)

    assert not dest.exists()
    assert list(tmp_path.iterdir()) == [], "staging file left behind"


def test_truncated_parquet_is_never_published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A staged file that is non-empty but unreadable must not be published either.

    Size alone does not prove a Parquet file is complete, and any fragment
    pyarrow cannot open fails the whole dataset scan — not just that one file.
    """

    def truncating_write_table(table: pa.Table, where: str | Path, **kwargs: object) -> None:
        real_write_table(table, where, **kwargs)  # type: ignore[operator]
        complete = Path(where).read_bytes()
        Path(where).write_bytes(complete[: len(complete) // 2])

    real_write_table = pq.write_table
    monkeypatch.setattr(pq, "write_table", truncating_write_table)

    dest = tmp_path / "part-1.parquet"
    with pytest.raises(OSError):
        _write(dest)

    assert not dest.exists(), "a truncated staged file was published as a part file"
    assert list(tmp_path.iterdir()) == [], "staging file left behind"


def test_bytes_are_flushed_to_disk_before_the_part_file_appears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The staged bytes must be fsynced before the rename publishes them.

    Renaming a file whose data is still in the page cache is what turns an
    unclean shutdown into a visible zero-byte part file.
    """
    calls: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def tracked_fsync(fd: int) -> None:
        calls.append("fsync")
        real_fsync(fd)

    def tracked_replace(src: str | Path, dst: str | Path) -> None:
        calls.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", tracked_fsync)
    monkeypatch.setattr(os, "replace", tracked_replace)

    dest = tmp_path / "part-1.parquet"
    _write(dest)

    assert calls[:2] == ["fsync", "replace"], f"expected fsync before publish, saw {calls}"
    assert dest.stat().st_size > 0
    assert pq.read_table(dest).num_rows == 1


def test_compactor_keeps_sources_when_the_merge_cannot_be_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A merge that lands unreadable must not replace the files it was merging.

    The compactor deletes its sources once the merged file is in place, so
    publishing a broken merge destroys the only good copies with it. A zero-byte
    ``compacted-*.parquet`` from exactly this path is on the volume today.
    """
    from heber.writer.compactor import Compactor

    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-05"
    partition.mkdir(parents=True)
    sources = [partition / "part-1.parquet", partition / "part-2.parquet"]
    for i, source in enumerate(sources, start=1):
        pq.write_table(pa.Table.from_pylist([{"event_id": f"evt-{i}"}]), source, compression="snappy")

    real_writer = pq.ParquetWriter

    class TruncatingWriter(real_writer):  # type: ignore[misc, valid-type]
        def __exit__(self, *exc: object) -> None:
            super().__exit__(*exc)
            Path(self.where).write_bytes(b"")

    monkeypatch.setattr(pq, "ParquetWriter", TruncatingWriter)

    merged = Compactor().compact_partition(partition)

    assert merged == 0, "compaction reported success on an unpublishable merge"
    assert all(source.exists() for source in sources), "sources deleted despite a failed merge"
    assert list(partition.glob("compacted-*.parquet")) == [], "broken merge was published"
    assert list(partition.glob(".compacted-*.tmp")) == [], "staging file left behind"


def test_bronze_does_not_publish_an_empty_partition_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bronze is the replay source of truth, so an empty Bronze file is unrecoverable loss."""
    import gzip

    from heber.writer.bronze import BronzeWriter

    monkeypatch.setattr("heber.config.settings.data_root", tmp_path)

    real_gzip_open = gzip.open

    def empty_gzip_open(path: str | Path, *args: object, **kwargs: object) -> object:
        Path(path).write_bytes(b"")
        return real_gzip_open(os.devnull, *args, **kwargs)  # type: ignore[arg-type]

    writer = BronzeWriter()
    partition = "provider=unusual_whales/feed=darkpool/dt=2026-08-07/hour=23"  # pragma: allowlist secret
    writer.buffers[partition].append({"event_id": "evt-1"})
    monkeypatch.setattr(gzip, "open", empty_gzip_open)

    with pytest.raises(OSError):
        writer.flush()

    assert list(tmp_path.rglob("*.jsonl.gz")) == [], "an empty Bronze file was published"
    assert list(tmp_path.rglob("*.tmp")) == [], "staging file left behind"


def test_compactor_flushes_the_directory_before_deleting_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The merged file's *name* must be durable before its sources are removed.

    Flushing the file only guarantees its contents. On a filesystem with no
    ordering guarantee a crash can lose the rename while keeping the unlinks,
    and then the merged batch exists nowhere in Silver — a silent bulk loss that
    nothing downstream flags, because the next pass just sees a smaller
    partition.
    """
    from heber.writer.compactor import Compactor

    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-05"
    partition.mkdir(parents=True)
    sources = [partition / "part-1.parquet", partition / "part-2.parquet"]
    for i, source in enumerate(sources, start=1):
        pq.write_table(pa.Table.from_pylist([{"event_id": f"evt-{i}"}]), source, compression="snappy")

    events: list[str] = []
    real_fsync = os.fsync
    real_unlink = Path.unlink

    def tracked_fsync(fd: int) -> None:
        events.append("fsync_dir" if os.path.isdir(f"/dev/fd/{fd}") else "fsync_file")
        real_fsync(fd)

    def tracked_unlink(self: Path, **kwargs: object) -> None:
        if self.suffix == ".parquet":
            events.append("unlink_source")
        real_unlink(self, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "fsync", tracked_fsync)
    monkeypatch.setattr(Path, "unlink", tracked_unlink)

    assert Compactor().compact_partition(partition) == 2

    assert "fsync_dir" in events, f"partition directory was never flushed, saw {events}"
    assert events.index("fsync_dir") < events.index("unlink_source"), (
        f"sources deleted before the merged file's name was durable: {events}"
    )


def test_a_short_parquet_is_kept_for_autopsy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A write that silently lost rows is unexplained, so keep the evidence.

    Deleting it destroys the only artifact of a failure we have never seen. The
    staged file is set aside under a dotted name, which every reader skips.
    """

    def short_write_table(table: pa.Table, where: str | Path, **kwargs: object) -> None:
        real_write_table(table.slice(0, 0), where, **kwargs)  # type: ignore[operator]

    real_write_table = pq.write_table
    monkeypatch.setattr(pq, "write_table", short_write_table)

    dest = tmp_path / "part-1.parquet"
    with pytest.raises(OSError):
        _write(dest)

    assert not dest.exists()
    kept = list(tmp_path.glob(".corrupt-*"))
    assert len(kept) == 1, f"evidence discarded, found {list(tmp_path.iterdir())}"
    # The kept file stays invisible to readers: dotted name *and* .tmp suffix,
    # both of which every walk in the codebase already filters.
    assert kept[0].name.startswith(".") and kept[0].name.endswith(".tmp")
    live = [p for p in tmp_path.iterdir() if not p.name.startswith(".")]
    assert live == [], f"a short file was published or left staged: {live}"
