"""The reader must not load data parked under `_quarantine/`.

``persist_features_to_gold`` diverts rows whose Greek enrichment is entirely
null into ``_quarantine/all_greeks_null/dt=<dt>/``, on the stated assumption
that downstream readers only load ``dt=`` partitions. pyarrow's dataset walk is
recursive, so it loaded them anyway: 10,396 quarantined files were being read
back into the meta_label_features dataset, both undoing the quarantine and
breaking the dataset-wide read, since parked files keep whatever schema they
had when they were set aside.
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
