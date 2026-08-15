"""Which files the reader collects, and what it refuses to open.

Two classes of file break a whole-dataset read if handed to pyarrow, and both
were doing so in production:

- Rows parked under ``_quarantine/``. ``persist_features_to_gold`` diverts rows
  whose Greek enrichment is entirely null there, on the stated assumption that
  downstream readers only load ``dt=`` partitions. pyarrow's walk is recursive,
  so it loaded all 10,396 of them anyway — undoing the quarantine and dragging
  the schemas they had when parked into the unified read.
- Zero-byte parquet files. The lakehouse volume is exFAT, where a rename
  without an explicit flush can publish an empty file. A single one of them
  failed the scan for an entire dataset: one such file in
  ``labels_alert_barriers`` made all 194,630 outcome rows unreadable.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from heber.reader.core import _collect_parquet_files, _open_dataset_safe


def _write(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _live_table() -> pa.Table:
    return pa.table(
        {
            "alert_id": pa.array(["a-1"], type=pa.string()),
            "alert_time": pa.array([datetime(2026, 4, 14, 13, 30, tzinfo=UTC)], type=pa.timestamp("ns", tz="UTC")),
            "expiry": pa.array([20557], type=pa.int32()).cast(pa.date32()),
        }
    )


@pytest.fixture
def dataset_root(tmp_path: Path) -> Path:
    """A normal partition plus a quarantined one with a conflicting schema."""
    _write(tmp_path / "dt=2026-04-14" / "data.parquet", _live_table())
    _write(
        tmp_path / "_quarantine" / "all_greeks_null" / "dt=2026-04-14" / "quarantine-1.parquet",
        pa.table(
            {
                "alert_id": pa.array(["q-1"], type=pa.string()),
                "alert_time": pa.array([datetime(2026, 4, 14, 13, 30, tzinfo=UTC)], type=pa.timestamp("us", tz="UTC")),
                # The parked schema — an int64 expiry cannot merge with date32.
                "expiry": pa.array([20260819], type=pa.int64()),
            }
        ),
    )
    return tmp_path


@pytest.mark.unit
class TestQuarantineExclusion:
    def test_quarantined_files_are_not_collected(self, dataset_root: Path) -> None:
        collected = _collect_parquet_files(dataset_root)

        assert len(collected) == 1
        relative = [Path(path).relative_to(dataset_root).parts for path in collected]
        assert not any("_quarantine" in parts for parts in relative)

    def test_dataset_opens_despite_a_conflicting_quarantined_schema(self, dataset_root: Path) -> None:
        dataset = _open_dataset_safe(str(dataset_root), partitioning=ds.partitioning(flavor="hive"))

        assert dataset is not None
        table = dataset.to_table()
        assert table.column("alert_id").to_pylist() == ["a-1"], "quarantined rows must not reach the reader"

    def test_a_root_that_is_itself_under_a_quarantine_dir_still_reads(self, tmp_path: Path) -> None:
        """Only directories *below* the root are filtered, not the root's own path.

        This is what lets someone deliberately point a reader at quarantined
        data to inspect it.
        """
        root = tmp_path / "_quarantine" / "all_greeks_null"
        _write(root / "dt=2026-04-14" / "data.parquet", _live_table())

        collected = _collect_parquet_files(root)

        assert len(collected) == 1

    def test_ordinary_partitions_are_still_collected(self, tmp_path: Path) -> None:
        for day in ("2026-04-14", "2026-04-15"):
            _write(tmp_path / f"dt={day}" / "data.parquet", _live_table())

        assert len(_collect_parquet_files(tmp_path)) == 2

    def test_other_underscore_directories_are_not_hidden(self, tmp_path: Path) -> None:
        """Only `_quarantine` is excluded — a future `_staging` layout that holds
        real partitions must not silently vanish from the dataset."""
        _write(tmp_path / "dt=2026-04-14" / "data.parquet", _live_table())
        _write(tmp_path / "_staging" / "dt=2026-04-15" / "data.parquet", _live_table())

        assert len(_collect_parquet_files(tmp_path)) == 2


@pytest.mark.unit
class TestZeroByteFiles:
    def test_zero_byte_parquet_is_not_collected(self, tmp_path: Path) -> None:
        _write(tmp_path / "dt=2026-04-14" / "data.parquet", _live_table())
        empty_file = tmp_path / "dt=2026-04-15" / "part-empty.parquet"
        empty_file.parent.mkdir(parents=True)
        empty_file.write_bytes(b"")

        collected = _collect_parquet_files(tmp_path)

        assert len(collected) == 1
        assert "part-empty.parquet" not in collected[0]

    def test_one_zero_byte_file_does_not_sink_the_whole_dataset(self, tmp_path: Path) -> None:
        """The production failure: a single empty file made every row unreadable."""
        for day in ("2026-04-14", "2026-04-15"):
            _write(tmp_path / f"dt={day}" / "data.parquet", _live_table())
        empty_file = tmp_path / "dt=2026-04-16" / "part-empty.parquet"
        empty_file.parent.mkdir(parents=True)
        empty_file.write_bytes(b"")

        dataset = _open_dataset_safe(str(tmp_path), partitioning=ds.partitioning(flavor="hive"))

        assert dataset is not None
        assert dataset.to_table().num_rows == 2

    def test_a_non_empty_file_is_still_collected(self, tmp_path: Path) -> None:
        _write(tmp_path / "dt=2026-04-14" / "data.parquet", _live_table())

        assert len(_collect_parquet_files(tmp_path)) == 1


@pytest.mark.integration
class TestStrictRead:
    def test_strict_read_raises_on_an_unreadable_dataset(self, tmp_path: Path) -> None:
        """A corrupt dataset must not reach training as "no rows"."""
        from heber.reader import HeberReader

        partition = tmp_path / "dt=2026-04-14"
        partition.mkdir(parents=True)
        (partition / "data.parquet").write_bytes(b"this is not a parquet file")

        reader = HeberReader()
        assert reader.read_parquet_dataset(path=tmp_path).empty, "lenient mode keeps returning empty"
        with pytest.raises(ValueError):
            reader.read_parquet_dataset(path=tmp_path, strict=True)

    def test_strict_read_still_returns_empty_for_a_missing_path(self, tmp_path: Path) -> None:
        """An absent dataset is a genuine absence, not a failure."""
        from heber.reader import HeberReader

        assert HeberReader().read_parquet_dataset(path=tmp_path / "nope", strict=True).empty

    def test_dataset_builder_surfaces_an_unreadable_feature_dataset(self, tmp_path: Path) -> None:
        from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder

        partition = tmp_path / "dt=2026-04-14"
        partition.mkdir(parents=True)
        (partition / "data.parquet").write_bytes(b"not parquet")

        builder = MetaLabelDatasetBuilder(config=DatasetConfig(features_path=tmp_path))
        with pytest.raises(ValueError):
            builder._load_features(date(2026, 1, 1), date(2026, 12, 31))

    def test_dataset_builder_surfaces_an_unreadable_outcomes_dataset(self, tmp_path: Path) -> None:
        """Outcomes swallowed read failures the same way features did."""
        from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder

        outcomes = tmp_path / "outcomes"
        partition = outcomes / "dt=2026-04-14"
        partition.mkdir(parents=True)
        (partition / "data.parquet").write_bytes(b"not parquet")

        builder = MetaLabelDatasetBuilder(
            config=DatasetConfig(outcomes_path=outcomes, features_path=tmp_path / "features")
        )
        with pytest.raises(ValueError):
            builder._load_outcomes(date(2026, 1, 1), date(2026, 12, 31))

    def test_strict_refuses_a_dataset_with_a_lost_fragment(self, tmp_path: Path) -> None:
        """Skipping a zero-byte file keeps the dataset readable, but a strict
        caller asked for the complete dataset and must not get a partial one."""
        from heber.reader import HeberReader

        _write(tmp_path / "dt=2026-04-14" / "data.parquet", _live_table())
        lost = tmp_path / "dt=2026-04-15" / "part-lost.parquet"
        lost.parent.mkdir(parents=True)
        lost.write_bytes(b"")

        reader = HeberReader()
        assert len(reader.read_parquet_dataset(path=tmp_path)) == 1, "lenient mode still reads what survives"
        with pytest.raises(ValueError, match="zero-byte"):
            reader.read_parquet_dataset(path=tmp_path, strict=True)

    def test_strict_raises_when_corruption_appears_during_the_scan(self, tmp_path: Path) -> None:
        """Corruption inside a row group fails at to_table, not at open."""
        from heber.reader import HeberReader

        path = tmp_path / "dt=2026-04-14" / "data.parquet"
        _write(path, _live_table())
        # Keep a valid footer so the dataset opens, then truncate the body.
        raw = path.read_bytes()
        path.write_bytes(raw[:20] + b"\x00" * 40 + raw[60:])

        reader = HeberReader()
        with pytest.raises((ValueError, OSError)):
            reader.read_parquet_dataset(path=tmp_path, strict=True)

    def test_a_missing_outcomes_path_is_still_an_empty_frame(self, tmp_path: Path) -> None:
        """An absent dataset is a genuine absence, not a failure."""
        from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder

        builder = MetaLabelDatasetBuilder(
            config=DatasetConfig(outcomes_path=tmp_path / "nope", features_path=tmp_path / "features")
        )
        assert builder._load_outcomes(date(2026, 1, 1), date(2026, 12, 31)).empty
