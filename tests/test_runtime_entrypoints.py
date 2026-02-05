"""Regression tests for Docker/Kubernetes runtime entrypoints."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_legacy_missing_modules_are_not_referenced() -> None:
    files = [
        ROOT / "Dockerfile",
        ROOT / "k8s/base/deployments/consumer.yaml",
        ROOT / "k8s/base/deployments/writer.yaml",
        ROOT / "k8s/base/deployments/compactor.yaml",
    ]
    contents = "\n".join(path.read_text(encoding="utf-8") for path in files)

    assert "heber.bus.consumer" not in contents
    assert "heber.writer.service" not in contents
    assert "heber.writer.compaction" not in contents


def test_runtime_entrypoint_modules_exist() -> None:
    expected_modules = [
        "heber.catalog.api",
        "heber.writer.consumer",
        "heber.writer.compactor",
        "heber.writer.hotstore",
    ]

    missing = [name for name in expected_modules if importlib.util.find_spec(name) is None]
    assert not missing, f"Missing runtime modules: {missing}"
