"""Regression tests for Kubernetes HPA metric and probe conformance."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_hpas_use_resource_metrics_only() -> None:
    hpa_paths = [
        ROOT / "k8s/base/hpa/catalog.yaml",
        ROOT / "k8s/base/hpa/consumer.yaml",
        ROOT / "k8s/base/hpa/writer.yaml",
    ]

    for path in hpa_paths:
        hpa = _load_yaml(path)
        metrics = hpa["spec"]["metrics"]
        assert metrics, f"No metrics configured in {path}"

        for metric in metrics:
            assert metric["type"] == "Resource", f"Unexpected metric type in {path}: {metric}"
            assert metric["resource"]["name"] in {"cpu", "memory"}, (
                f"Unexpected resource metric in {path}: {metric['resource']['name']}"
            )


def test_worker_deployments_use_exec_probes() -> None:
    worker_expectations = {
        ROOT / "k8s/base/deployments/consumer.yaml": "heber.writer.consumer",
        ROOT / "k8s/base/deployments/writer.yaml": "heber.writer.consumer",
        ROOT / "k8s/base/deployments/compactor.yaml": "heber.writer.compactor",
        ROOT / "k8s/base/deployments/hotloader.yaml": "heber.writer.hotstore",
        ROOT / "k8s/base/deployments/backfill.yaml": "heber.backfill",
    }

    for path, expected_entrypoint in worker_expectations.items():
        deployment = _load_yaml(path)
        container = deployment["spec"]["template"]["spec"]["containers"][0]

        for probe_name in ("livenessProbe", "readinessProbe"):
            probe = container[probe_name]
            assert "exec" in probe, f"Expected exec probe in {path}::{probe_name}"
            assert "httpGet" not in probe, f"Unexpected HTTP probe in {path}::{probe_name}"

            command = " ".join(probe["exec"]["command"])
            assert expected_entrypoint in command, (
                f"Probe command in {path}::{probe_name} does not match expected entrypoint "
                f"{expected_entrypoint!r}: {command!r}"
            )
