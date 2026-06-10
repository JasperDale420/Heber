"""Regression tests for Docker runtime entrypoints.

The deployment target is docker-compose + launchd on the host (the former
Kubernetes manifests were removed 2026-06-10 — they were never deployed),
so these contracts are enforced against docker-compose.yml.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _compose_services() -> dict:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    return compose["services"]


def test_legacy_missing_modules_are_not_referenced() -> None:
    files = [
        ROOT / "Dockerfile",
        ROOT / "docker-compose.yml",
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
        "heber.backfill.__main__",
    ]

    missing = [name for name in expected_modules if importlib.util.find_spec(name) is None]
    assert not missing, f"Missing runtime modules: {missing}"


def test_compose_services_reference_valid_python_entrypoints() -> None:
    """Every `python -m <module>` compose command must resolve to a real module.

    Catches the rename-the-module-but-not-the-compose-file drift that
    previously shipped containers crash-looping on ModuleNotFoundError.
    """
    missing_modules: list[str] = []
    checked = 0

    for name, service in _compose_services().items():
        command = service.get("command")
        if not isinstance(command, list) or command[:2] != ["python", "-m"]:
            continue
        checked += 1

        runtime_module = command[2]
        if runtime_module == "uvicorn":
            app_module = command[3].split(":", 1)[0]
            if importlib.util.find_spec(app_module) is None:
                missing_modules.append(f"{name}:{app_module}")
            continue

        if importlib.util.find_spec(runtime_module) is None:
            missing_modules.append(f"{name}:{runtime_module}")

    assert checked >= 5, f"Expected at least 5 python -m services in compose, found {checked}"
    assert not missing_modules, f"Compose services reference missing Python modules: {missing_modules}"
