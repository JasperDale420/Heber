"""Compactor schema-union regression tests for mixed historical partitions."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from heber.writer import compactor as compactor_module
from heber.writer.compactor import Compactor


def _write_table(path: Path, table: pa.Table) -> None:
    pq.write_table(table, path, compression="snappy")


def test_compactor_unifies_missing_optional_columns(tmp_path: Path) -> None:
    partition = tmp_path / "silver" / "feed=flow_alerts" / "instrument_type=option" / "dt=2026-02-11"
    partition.mkdir(parents=True, exist_ok=True)

    _write_table(
        partition / "old.parquet",
        pa.table(
            {
                "event_id": ["e1"],
                "underlying": ["AAPL"],
            }
        ),
    )
    _write_table(
        partition / "new.parquet",
        pa.table(
            {
                "event_id": ["e2"],
                "underlying": ["AAPL"],
                "total_size": [10],
                "expiry_count": [2],
            }
        ),
    )

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 2
    compacted_files = list(partition.glob("compacted-*.parquet"))
    assert len(compacted_files) == 1

    merged_table = pq.read_table(compacted_files[0])
    assert "total_size" in merged_table.column_names
    assert "expiry_count" in merged_table.column_names

    data = merged_table.to_pylist()
    row_by_event = {row["event_id"]: row for row in data}
    assert row_by_event["e1"]["total_size"] is None
    assert row_by_event["e1"]["expiry_count"] is None
    assert row_by_event["e2"]["total_size"] == 10
    assert row_by_event["e2"]["expiry_count"] == 2


def test_compactor_preserves_values_across_multiple_optional_columns(tmp_path: Path) -> None:
    partition = tmp_path / "silver" / "feed=flow_alerts" / "instrument_type=option" / "dt=2026-02-10"
    partition.mkdir(parents=True, exist_ok=True)

    _write_table(
        partition / "part-1.parquet",
        pa.table(
            {
                "event_id": ["e1"],
                "underlying": ["MSFT"],
                "total_size": [7],
            }
        ),
    )
    _write_table(
        partition / "part-2.parquet",
        pa.table(
            {
                "event_id": ["e2"],
                "underlying": ["MSFT"],
                "expiry_count": [4],
            }
        ),
    )
    _write_table(
        partition / "part-3.parquet",
        pa.table(
            {
                "event_id": ["e3"],
                "underlying": ["MSFT"],
                "total_size": [9],
                "expiry_count": [1],
            }
        ),
    )

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 3
    compacted_files = list(partition.glob("compacted-*.parquet"))
    assert len(compacted_files) == 1

    merged_table = pq.read_table(compacted_files[0])
    data = {row["event_id"]: row for row in merged_table.to_pylist()}
    assert data["e1"]["total_size"] == 7
    assert data["e1"]["expiry_count"] is None
    assert data["e2"]["total_size"] is None
    assert data["e2"]["expiry_count"] == 4
    assert data["e3"]["total_size"] == 9
    assert data["e3"]["expiry_count"] == 1


def test_compactor_skips_partition_when_types_conflict(tmp_path: Path, monkeypatch) -> None:
    partition = tmp_path / "silver" / "feed=flow_alerts" / "instrument_type=option" / "dt=2026-02-09"
    partition.mkdir(parents=True, exist_ok=True)

    source_a = partition / "a.parquet"
    source_b = partition / "b.parquet"

    _write_table(
        source_a,
        pa.table(
            {
                "event_id": ["e1"],
                "total_size": [11],
            }
        ),
    )
    _write_table(
        source_b,
        pa.table(
            {
                "event_id": ["e2"],
                "total_size": ["bad-type"],
            }
        ),
    )

    error_calls: list[tuple[str, dict]] = []

    def _capture_error(message: str, **kwargs) -> None:  # noqa: ANN003
        error_calls.append((message, kwargs))

    monkeypatch.setattr(compactor_module.logger, "error", _capture_error)

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 0
    assert source_a.exists()
    assert source_b.exists()
    assert list(partition.glob("compacted-*.parquet")) == []
    assert any(call[0] == "Compaction skipped due schema conflict" for call in error_calls)
