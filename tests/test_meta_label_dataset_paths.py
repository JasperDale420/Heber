from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from heber.config import settings
from heber.ml import datasets as datasets_module
from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder, persist_features_to_gold


def test_dataset_config_defaults_use_configured_gold_root() -> None:
    config = DatasetConfig()

    assert config.features_path == settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"
    assert config.outcomes_path == settings.gold_path / "dataset=labels_alert_barriers" / "project=watch" / "version=v1"


def test_load_features_uses_legacy_fallback_path(tmp_path: Path, monkeypatch) -> None:
    configured_path = tmp_path / "dataset=meta_label_features" / "project=watch" / "version=v1"
    legacy_path = tmp_path / "meta_labels" / "features"
    dt_dir = legacy_path / "dt=2026-02-07"
    dt_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "alert_id": ["a1"],
            "alert_time": [datetime(2026, 2, 7, 15, 0, tzinfo=UTC)],
            "feature_x": [1.0],
        }
    ).write_parquet(dt_dir / "data.parquet")

    monkeypatch.setattr(datasets_module, "LEGACY_FEATURES_PATHS", [legacy_path])
    builder = MetaLabelDatasetBuilder(
        config=DatasetConfig(
            features_path=configured_path,
            outcomes_path=tmp_path / "unused",
        )
    )

    loaded = builder._load_features(date(2026, 2, 7), date(2026, 2, 7))

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
