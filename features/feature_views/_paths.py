"""Shared path builders for Feast FileSource definitions."""

from __future__ import annotations

from heber.config import get_settings


def gold_dataset_glob(dataset: str) -> str:
    """Return Gold dataset glob aligned with Heber write_gold layout.

    Layout:
      <gold_root>/dataset=<dataset>/project=<project>/version=<version>/dt=<date>/*.parquet
    """
    settings = get_settings()
    gold_root = settings.gold_path
    project_glob = settings.gold_project
    version_glob = settings.gold_version

    return str(
        gold_root / f"dataset={dataset}" / f"project={project_glob}" / f"version={version_glob}" / "dt=*" / "*.parquet"
    )
