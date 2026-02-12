import os
from datetime import UTC, date, datetime
from pathlib import Path

from heber.retention import (
    DataLayer,
    DatasetRetentionConfig,
    LifecycleAction,
    PartitionInfo,
    ReaperScheduler,
    ReaperWorker,
    RetentionPolicy,
    create_reaper,
)


def test_scan_gold_partitions_reads_project_version_layout(tmp_path: Path) -> None:
    dt_dir = tmp_path / "gold" / "dataset=flow_features" / "project=core" / "version=v1.10.0" / "dt=2026-01-05"
    dt_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = dt_dir / "part-0001.parquet"
    parquet_file.write_bytes(b"abcd")

    worker = ReaperWorker(storage_root=str(tmp_path))
    partitions = worker.scan_partitions("flow_features", DataLayer.GOLD)

    assert len(partitions) == 1
    partition = partitions[0]
    assert partition.path == dt_dir
    assert partition.layer == DataLayer.GOLD
    assert partition.dataset == "flow_features"
    assert partition.version == "v1.10.0"
    assert partition.partition_date == date(2026, 1, 5)
    assert partition.file_count == 1
    assert partition.total_bytes == 4


def test_scan_gold_partitions_reads_type_version_layout_without_dt(tmp_path: Path) -> None:
    version_dir = tmp_path / "gold" / "dataset=returns_5d" / "type=label" / "version=v1.2.0"
    version_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = version_dir / "data.parquet"
    parquet_file.write_bytes(b"labels")

    expected_date = date(2024, 2, 10)
    mtime = datetime(2024, 2, 10, 12, 30, tzinfo=UTC).timestamp()
    os.utime(parquet_file, (mtime, mtime))

    worker = ReaperWorker(storage_root=str(tmp_path))
    partitions = worker.scan_partitions("returns_5d", DataLayer.GOLD)

    assert len(partitions) == 1
    partition = partitions[0]
    assert partition.path == version_dir
    assert partition.version == "v1.2.0"
    assert partition.partition_date == expected_date
    assert partition.file_count == 1
    assert partition.total_bytes == 6


def test_find_expired_versions_sorts_semver_not_lexicographic(tmp_path: Path) -> None:
    worker = ReaperWorker(storage_root=str(tmp_path))
    policy = RetentionPolicy(retention_versions=1)

    partitions = [
        PartitionInfo(
            path=Path("/tmp/gold/v1.2.0"),
            dataset="returns_5d",
            layer=DataLayer.GOLD,
            partition_date=date(2025, 1, 1),
            version="v1.2.0",
        ),
        PartitionInfo(
            path=Path("/tmp/gold/v1.10.0"),
            dataset="returns_5d",
            layer=DataLayer.GOLD,
            partition_date=date(2025, 1, 2),
            version="v1.10.0",
        ),
    ]

    expired = worker.find_expired_versions(partitions, policy, pinned_versions=[])
    assert {p.version for p in expired} == {"v1.2.0"}

    expired_with_pin = worker.find_expired_versions(partitions, policy, pinned_versions=["v1.2.0"])
    assert expired_with_pin == []


def test_scheduler_processes_hot_store_and_dlq_layers(tmp_path: Path) -> None:
    dataset = "flow_alerts"
    for layer in [DataLayer.BRONZE, DataLayer.SILVER, DataLayer.HOT_STORE, DataLayer.DLQ]:
        dt_dir = tmp_path / layer.value / f"dataset={dataset}" / "dt=2020-01-01"
        dt_dir.mkdir(parents=True, exist_ok=True)
        (dt_dir / "part-0001.parquet").write_bytes(b"x")

    gold_dt_dir = tmp_path / "gold" / f"dataset={dataset}" / "project=core" / "version=v1.0.0" / "dt=2020-01-01"
    gold_dt_dir.mkdir(parents=True, exist_ok=True)
    (gold_dt_dir / "part-0001.parquet").write_bytes(b"y")

    config = DatasetRetentionConfig(
        dataset=dataset,
        bronze=RetentionPolicy(retention_days=1, action=LifecycleAction.DELETE),
        silver=RetentionPolicy(retention_days=1, action=LifecycleAction.DELETE),
        gold=RetentionPolicy(retention_days=1, action=LifecycleAction.DELETE),
        hot_store=RetentionPolicy(retention_days=1, action=LifecycleAction.DELETE),
        dlq=RetentionPolicy(retention_days=1, action=LifecycleAction.DELETE),
    )
    worker = ReaperWorker(storage_root=tmp_path, dry_run=True)
    scheduler = ReaperScheduler(worker=worker, retention_configs={dataset: config})

    result = scheduler.run_once()
    assert result.partitions_scanned == 5
    assert result.partitions_deleted == 5


def test_create_reaper_uses_configured_data_root(monkeypatch, tmp_path: Path) -> None:
    configured_root = tmp_path / "configured-root"
    monkeypatch.setenv("HEBER_DATA_ROOT", str(configured_root))

    from heber.config import get_settings

    get_settings.cache_clear()

    scheduler = create_reaper(dry_run=True)
    assert scheduler.worker.storage_root == configured_root
    assert scheduler.worker.archiver.archive_root == configured_root / "archive"

    # Restore settings cache for subsequent tests
    get_settings.cache_clear()
