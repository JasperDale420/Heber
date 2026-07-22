"""Regression tests for compactor safety and atomicity."""

from __future__ import annotations

import os
from datetime import UTC, datetime
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
    merged_table = pq.ParquetFile(compacted_files[0]).read()
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

    original_parquet_file = compactor_module.pq.ParquetFile
    call_count = {"value": 0}

    def failing_parquet_file(path):  # type: ignore[override]
        call_count["value"] += 1
        if call_count["value"] == 2:
            raise RuntimeError("synthetic read failure")
        return original_parquet_file(path)

    monkeypatch.setattr(compactor_module.pq, "ParquetFile", failing_parquet_file)

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 0
    assert all(path.exists() for path in source_files)
    assert list(partition.glob("compacted-*.parquet")) == []
    assert list(partition.glob(".compacted-*.tmp")) == []
    assert not (partition / ".compaction.lock").exists()


def test_compactor_ignores_hidden_sidecar_parquet_files(tmp_path: Path) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-06"
    partition.mkdir(parents=True)

    source_files = [
        partition / "part-1.parquet",
        partition / "part-2.parquet",
    ]
    for i, source in enumerate(source_files, start=1):
        _write_parquet(source, [{"event_id": f"evt-{i}"}])

    hidden_sidecar = partition / "._part-2.parquet"
    hidden_sidecar.write_text("not a parquet payload", encoding="utf-8")

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 2
    assert not any(path.exists() for path in source_files)
    assert hidden_sidecar.exists()
    assert list(partition.glob(".compacted-*.tmp")) == []
    assert not (partition / ".compaction.lock").exists()


def test_compactor_logs_hidden_sidecar_skip_as_debug(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-06"
    partition.mkdir(parents=True)
    (partition / "._part-2.parquet").write_text("not a parquet payload", encoding="utf-8")

    warning_calls: list[str] = []
    debug_calls: list[str] = []

    def _capture_warning(message: str, **kwargs) -> None:  # noqa: ANN003
        warning_calls.append(message)

    def _capture_debug(message: str, **kwargs) -> None:  # noqa: ANN003
        debug_calls.append(message)

    monkeypatch.setattr(compactor_module.logger, "warning", _capture_warning)
    monkeypatch.setattr(compactor_module.logger, "debug", _capture_debug)

    _ = Compactor._list_compactable_parquet_files(partition)

    assert "Skipping hidden parquet sidecar file" in debug_calls
    assert "Skipping hidden parquet sidecar file" not in warning_calls


def test_compactor_recovers_from_stale_self_lock(tmp_path: Path) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-07"
    partition.mkdir(parents=True)

    source_files = [
        partition / "part-1.parquet",
        partition / "part-2.parquet",
    ]
    for i, source in enumerate(source_files, start=1):
        _write_parquet(source, [{"event_id": f"evt-{i}"}])

    (partition / ".compaction.lock").write_text(
        f"pid={os.getpid()} ts={datetime.now(UTC).isoformat()}\n",
        encoding="utf-8",
    )

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 2
    assert not (partition / ".compaction.lock").exists()


def test_scan_and_compact_skips_macos_resource_fork_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """rglob walk must skip names starting with `.` so PermissionError on
    macOS AppleDouble files (`._foo`) cannot abort the cycle. Same class
    of bug as commit 8af04ef fixed for the catalog.
    """
    (tmp_path / ".heber-sentinel").touch()
    layer_path = tmp_path / "silver"
    real_partition = layer_path / "feed=bars" / "instrument_type=equity" / "dt=2026-04-28"
    real_partition.mkdir(parents=True)
    _write_parquet(real_partition / "part-1.parquet", [{"event_id": "evt-1"}])
    _write_parquet(real_partition / "part-2.parquet", [{"event_id": "evt-2"}])

    # Simulate macOS AppleDouble resource-fork files at multiple levels.
    (layer_path / "._feed=bars").touch()
    (real_partition / "._part-1.parquet").touch()

    original_is_dir = Path.is_dir

    def strict_is_dir(self: Path, *args, **kwargs):  # noqa: ANN002,ANN003
        if self.name.startswith("._"):
            raise PermissionError(f"Operation not permitted: {self}")
        return original_is_dir(self, *args, **kwargs)

    monkeypatch.setattr(compactor_module.Path, "is_dir", strict_is_dir)
    monkeypatch.setattr(compactor_module.settings, "data_root", tmp_path)

    compactor = Compactor()
    result = compactor.scan_and_compact("silver")

    assert result["partitions_scanned"] == 1
    assert result["files_merged"] == 2


def test_compactor_skips_unstatable_files_without_crashing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    partition = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-02-08"
    partition.mkdir(parents=True)

    healthy = partition / "part-1.parquet"
    unhealthy = partition / "part-2.parquet"
    _write_parquet(healthy, [{"event_id": "evt-1"}])
    _write_parquet(unhealthy, [{"event_id": "evt-2"}])

    original_stat = Path.stat

    def flaky_stat(path: Path, *args, **kwargs):  # noqa: ANN002,ANN003
        if path == unhealthy:
            raise PermissionError("Operation not permitted")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(compactor_module.Path, "stat", flaky_stat)

    compactor = Compactor()
    merged = compactor.compact_partition(partition)

    assert merged == 0
    assert healthy.exists()
    assert unhealthy.name in {path.name for path in partition.glob("*.parquet")}
    assert not (partition / ".compaction.lock").exists()
