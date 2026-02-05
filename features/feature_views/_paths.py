"""Shared path builders for Feast FileSource definitions."""

from __future__ import annotations

import os
from pathlib import Path


def gold_dataset_glob(dataset: str) -> str:
    """Return Gold dataset glob aligned with Heber write_gold layout.

    Layout:
      <gold_root>/dataset=<dataset>/project=<project>/version=<version>/dt=<date>/*.parquet
    """
    data_root = Path(os.getenv("HEBER_DATA_ROOT", "/data"))
    gold_root = Path(os.getenv("HEBER_GOLD_ROOT", str(data_root / "gold")))
    project_glob = os.getenv("HEBER_GOLD_PROJECT", "*")
    version_glob = os.getenv("HEBER_GOLD_VERSION", "*")

    return str(
        gold_root / f"dataset={dataset}" / f"project={project_glob}" / f"version={version_glob}" / "dt=*" / "*.parquet"
    )
