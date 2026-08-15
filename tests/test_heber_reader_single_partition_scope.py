"""A dt-scoped read that matches exactly one partition must still return rows.

Found while reconciling the darkpool Gold regeneration. Against production
Silver:

    08-07 only,   prune_by_dt=True   ->      0 rows   (heber_reader_open_failed)
    08-07 only,   prune_by_dt=False  -> 178,153 rows
    08-06..08-08, prune_by_dt=True   -> 354,850 rows

`_open_dataset_safe`'s manual schema unification derives the dataset root with
`os.path.commonpath(file_list)`. When the scoped walk narrows to a single `dt=`
directory, every file shares that directory, so the common path *is* the
partition directory and no `key=value` segment remains relative to it — the
unified schema is then built without `dt`, contradicting the hive partitioning
passed alongside it. With two or more partitions the common root sits one level
higher and `dt` survives, which is why this only ever bites the narrowest read.

The failure was silent: the raised `ArrowInvalid` left `_open_dataset_safe`
returning None, and the caller logged `heber_reader_open_failed` and returned an
empty frame — indistinguishable from a partition that was never written.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from heber.reader.core import HeberReader, HeberReadError, _open_dataset_safe


def _write(base: Path, dt: str, rows: int = 5) -> Path:
    """One partition holding the `feed` string/dictionary conflict.

    The conflict matters: `_open_dataset_safe` only reaches its manual schema
    unification — where this bug lives — after pyarrow's own cross-fragment
    merge fails. Production darkpool fragments mix these encodings because the
    real-time writer and the compactor emit them differently, which is why
    `heber_reader_schema_conflict_detected` fires on every read of that feed.
    A fixture without the conflict takes the fast path and proves nothing.
    """
    part = base / "silver" / "feed=darkpool" / "instrument_type=equity" / f"dt={dt}"
    part.mkdir(parents=True, exist_ok=True)
    ts = pd.Timestamp(f"{dt}T15:00:00", tz="UTC")
    half = max(rows // 2, 1)
    for name, feed_type in (
        ("part-a.parquet", pa.string()),
        ("part-b.parquet", pa.dictionary(pa.int32(), pa.string())),
    ):
        n = half if name == "part-a.parquet" else rows - half
        table = pa.table(
            {
                "ts_event": pa.array([ts.to_pydatetime()] * n, pa.timestamp("us", tz="UTC")),
                "ts_available": pa.array([ts.to_pydatetime()] * n, pa.timestamp("us", tz="UTC")),
                "instrument_key": pa.array(["equity:AAPL"] * n, pa.string()),
                "underlying": pa.array(["AAPL"] * n, pa.string()),
                "notional": pa.array([1.0] * n, pa.float64()),
                "feed": pa.array(["darkpool"] * n).cast(feed_type),
            }
        )
        pq.write_table(table, str(part / name))
    return part


def test_single_day_pruned_read_returns_its_rows(tmp_path: Path) -> None:
    """The exact production shape: many partitions on disk, one in range."""
    for dt in ("2026-08-05", "2026-08-06", "2026-08-07"):
        _write(tmp_path, dt)

    rows = HeberReader(tmp_path).read_silver(
        "darkpool",
        instrument_type="equity",
        time_range=("2026-08-07T00:00:00Z", "2026-08-07T23:59:59Z"),
        prune_by_dt=True,
    )

    assert len(rows) == 5, "a range matching one partition read as empty"


def test_single_day_pruned_matches_unpruned(tmp_path: Path) -> None:
    """Pruning is an optimisation; it must not change the answer."""
    for dt in ("2026-08-05", "2026-08-06", "2026-08-07"):
        _write(tmp_path, dt)

    reader = HeberReader(tmp_path)
    window = ("2026-08-07T00:00:00Z", "2026-08-07T23:59:59Z")
    pruned = reader.read_silver("darkpool", instrument_type="equity", time_range=window, prune_by_dt=True)
    plain = reader.read_silver("darkpool", instrument_type="equity", time_range=window, prune_by_dt=False)

    assert len(pruned) == len(plain) == 5


def test_partition_column_survives_a_single_partition_file_list(tmp_path: Path) -> None:
    """The mechanism itself: `dt` must stay in the schema when scoped to one dir."""
    part = _write(tmp_path, "2026-08-07")

    dataset = _open_dataset_safe(
        str(part.parent),
        partitioning=ds.partitioning(flavor="hive"),  # what read_silver passes
        dt_range=("2026-08-07", "2026-08-07"),
    )

    assert dataset is not None, "open returned None for a healthy single partition"
    assert "dt" in dataset.schema.names, "hive partition column dropped for a single-partition file list"


def test_an_unopenable_dataset_raises_rather_than_reading_empty(tmp_path: Path) -> None:
    """This bug hid behind a silent empty frame; that path must fail loudly."""
    part = _write(tmp_path, "2026-08-07")
    (part / "part-a.parquet").write_bytes(b"PAR1notactuallyparquet")  # nonzero, so not the empty-file filter

    with pytest.raises(HeberReadError):
        HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity")


def test_unreadable_files_raise_but_no_files_reads_empty(tmp_path: Path) -> None:
    """The distinction the raise depends on.

    `_open_dataset_safe` returns None both when every candidate was filtered
    out (nothing to read) and when the files it found could not be opened.
    Only the second is a failure — collapsing the first into a raise would make
    an unwritten partition throw.

    AppleDouble sidecars and `.tmp` partial writes are ignorable filesystem
    noise and stay on the absence side. A zero-byte `part-*.parquet` does not:
    that is a lost write, and it is covered by
    `test_a_partition_holding_only_a_zero_byte_file_raises`.
    """
    part = _write(tmp_path, "2026-08-07")
    for f in part.glob("*.parquet"):
        f.unlink()
    (part / "._sidecar.parquet").write_bytes(b"junk")
    (part / "part-partial.parquet.tmp").write_bytes(b"half a write")

    assert HeberReader(tmp_path).read_silver("darkpool", instrument_type="equity").empty


def test_a_partition_holding_only_a_zero_byte_file_raises(tmp_path: Path) -> None:
    """A truncated write is not an empty partition.

    The absence check asks the collector what it found, and the collector drops
    zero-byte files — so a partition whose only artifact is a writer-killed
    `part-*.parquet` looked exactly like one nobody had written yet, and a
    scoped read of it returned empty. That is the original bug wearing a
    different hat: a Gold regeneration would skip the date and exit 0.
    """
    part = _write(tmp_path, "2026-08-07")
    for f in part.glob("*.parquet"):
        f.unlink()
    (part / "part-truncated.parquet").write_bytes(b"")

    with pytest.raises(HeberReadError):
        HeberReader(tmp_path).read_silver(
            "darkpool",
            instrument_type="equity",
            time_range=("2026-08-07T00:00:00Z", "2026-08-07T23:59:59Z"),
            prune_by_dt=True,
        )


def test_gold_open_failure_raises_but_absence_reads_empty(tmp_path: Path) -> None:
    """read_gold had the same None-to-empty mapping read_silver just lost."""
    part = tmp_path / "gold" / "dataset=feat" / "project=kairos" / "version=v1" / "dt=2026-08-07"
    part.mkdir(parents=True)
    (part / "corrupt.parquet").write_bytes(b"PAR1nope")

    with pytest.raises(HeberReadError):
        HeberReader(tmp_path).read_gold("feat", project="kairos")

    for f in part.glob("*.parquet"):
        f.unlink()
    (part / "._sidecar.parquet").write_bytes(b"junk")
    assert HeberReader(tmp_path).read_gold("feat", project="kairos").empty
