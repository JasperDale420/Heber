from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from heber.config import settings
from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder, persist_features_to_gold


def test_dataset_config_defaults_use_configured_gold_root() -> None:
    config = DatasetConfig()

    assert config.features_path == settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"
    assert config.outcomes_path == settings.gold_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"


def test_load_features_loads_from_configured_path(tmp_path: Path) -> None:
    configured_path = tmp_path / "dataset=meta_label_features" / "project=watch" / "version=v1"
    dt_date = date(2026, 2, 7)
    dt_dir = configured_path / f"dt={dt_date}"
    dt_dir.mkdir(parents=True, exist_ok=True)

    pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "feature_x": [1.0],
        }
    ).write_parquet(dt_dir / "data.parquet")

    builder = MetaLabelDatasetBuilder(
        config=DatasetConfig(
            features_path=configured_path,
            outcomes_path=tmp_path / "unused",
        )
    )

    loaded = builder._load_features(dt_date, dt_date)

    assert len(loaded) == 1
    assert loaded.get_column("alert_id").to_list() == ["a1"]


def test_persist_features_to_gold_appends_partitions(tmp_path: Path) -> None:
    row_a = pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "feature_x": [1.0],
        }
    )
    row_b = pl.DataFrame(
        {
            "alert_id": ["a2"],
            "alert_time": [datetime(2026, 2, 7, 16, 0, tzinfo=UTC)],
            "feature_x": [2.0],
        }
    )

    persist_features_to_gold(row_a, output_path=tmp_path, partition_col="alert_time")
    persist_features_to_gold(row_b, output_path=tmp_path, partition_col="alert_time")

    output_file = tmp_path / "dt=2026-02-07" / "data.parquet"
    assert output_file.exists()

    persisted = pl.read_parquet(output_file)
    assert set(persisted.get_column("alert_id").to_list()) == {"a1", "a2"}


def test_persist_features_to_gold_handles_column_order_mismatch(tmp_path: Path) -> None:
    existing = pl.DataFrame(
        {
            "alert_id": ["a1"],
            "symbol": ["AAPL"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "feature_x": [1.0],
        }
    ).select(["alert_id", "symbol", "alert_time", "feature_x"])
    update = pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "symbol": ["AAPL"],
            "feature_x": [2.0],
        }
    ).select(["alert_id", "alert_time", "symbol", "feature_x"])

    persist_features_to_gold(existing, output_path=tmp_path, partition_col="alert_time")
    persist_features_to_gold(update, output_path=tmp_path, partition_col="alert_time")

    output_file = tmp_path / "dt=2026-02-07" / "data.parquet"
    persisted = pl.read_parquet(output_file)

    assert len(persisted) == 1
    assert persisted.get_column("alert_id").to_list() == ["a1"]
    assert persisted.get_column("feature_x").to_list() == [2.0]


def test_persist_features_to_gold_quarantines_unreadable_existing_partition(tmp_path: Path) -> None:
    dt_dir = tmp_path / "dt=2026-02-07"
    dt_dir.mkdir(parents=True, exist_ok=True)
    unreadable_file = dt_dir / "data.parquet"
    unreadable_file.write_text("not a parquet file", encoding="utf-8")

    incoming = pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "feature_x": [1.0],
        }
    )

    persist_features_to_gold(incoming, output_path=tmp_path, partition_col="alert_time")

    assert unreadable_file.exists()
    persisted = pl.read_parquet(unreadable_file)
    assert persisted.get_column("alert_id").to_list() == ["a1"]

    quarantined = list(dt_dir.glob("data.parquet.corrupt-*"))
    assert len(quarantined) == 1
