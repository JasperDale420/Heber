"""Regression tests for Docker/Kubernetes runtime entrypoints."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_legacy_missing_modules_are_not_referenced() -> None:
    files = [
        ROOT / "Dockerfile",
        ROOT / "k8s/base/deployments/consumer.yaml",
        ROOT / "k8s/base/deployments/writer.yaml",
        ROOT / "k8s/base/deployments/compactor.yaml",
        ROOT / "k8s/base/deployments/hotloader.yaml",
        ROOT / "k8s/base/deployments/backfill.yaml",
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
        "heber.backfill.__main__",
    ]

    missing = [name for name in expected_modules if importlib.util.find_spec(name) is None]
    assert not missing, f"Missing runtime modules: {missing}"


def test_base_deployments_reference_valid_python_entrypoints() -> None:
    deployment_files = [
        ROOT / "k8s/base/deployments/catalog.yaml",
        ROOT / "k8s/base/deployments/consumer.yaml",
        ROOT / "k8s/base/deployments/writer.yaml",
        ROOT / "k8s/base/deployments/compactor.yaml",
        ROOT / "k8s/base/deployments/hotloader.yaml",
        ROOT / "k8s/base/deployments/backfill.yaml",
    ]

    missing_modules: list[str] = []
    for path in deployment_files:
        deployment = yaml.safe_load(path.read_text(encoding="utf-8"))
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        command = container["command"]

        assert command[:2] == ["python", "-m"], f"Unexpected command shape in {path}: {command!r}"

        runtime_module = command[2]
        if runtime_module == "uvicorn":
            app_module = command[3].split(":", 1)[0]
            if importlib.util.find_spec(app_module) is None:
                missing_modules.append(f"{path.name}:{app_module}")
            continue

        if importlib.util.find_spec(runtime_module) is None:
            missing_modules.append(f"{path.name}:{runtime_module}")

    assert not missing_modules, f"Deployments reference missing Python modules: {missing_modules}"
