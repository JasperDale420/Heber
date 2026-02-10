"""Regression tests for compactor safety and atomicity."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from heber.writer import compactor as compactor_module
from heber.writer.compactor import Compactor


def _write_parquet(path: Path, rows: list[dict]) -> None:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="snappy")


def test_compactor_streams_merge_and_deletes_sources(tmp_path: Path) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-05"
    partition.mkdir(parents=True)

    source_files = [
        partition / "part-1.parquet",
        partition / "part-2.parquet",
        partition / "part-3.parquet",
    ]
    for i, source in enumerate(source_files, start=1):
        _write_parquet(source, [{"event_id": f"evt-{i}"}])

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 3
    assert not any(path.exists() for path in source_files)
    assert list(partition.glob(".compacted-*.tmp")) == []
    assert not (partition / ".compaction.lock").exists()

    compacted_files = list(partition.glob("compacted-*.parquet"))
    assert len(compacted_files) == 1
    merged_table = pq.read_table(compacted_files[0])
    assert merged_table.num_rows == 3


def test_compactor_failure_keeps_source_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-05"
    partition.mkdir(parents=True)

    source_files = [
        partition / "part-1.parquet",
        partition / "part-2.parquet",
    ]
    for i, source in enumerate(source_files, start=1):
        _write_parquet(source, [{"event_id": f"evt-{i}"}])

    original_read_table = compactor_module.pq.read_table
    call_count = {"value": 0}

    def failing_read_table(path: Path):  # type: ignore[override]
        call_count["value"] += 1
        if call_count["value"] == 2:
            raise RuntimeError("synthetic read failure")
        return original_read_table(path)

    monkeypatch.setattr(compactor_module.pq, "read_table", failing_read_table)

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 0
    assert all(path.exists() for path in source_files)
    assert list(partition.glob("compacted-*.parquet")) == []
    assert list(partition.glob(".compacted-*.tmp")) == []
    assert not (partition / ".compaction.lock").exists()
