"""Regression tests for metrics exporter and deployment scrape alignment."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_scraped_deployments_map_to_metrics_enabled_entrypoints() -> None:
    deployments = {
        "catalog": ROOT / "k8s/base/deployments/catalog.yaml",
        "consumer": ROOT / "k8s/base/deployments/consumer.yaml",
        "writer": ROOT / "k8s/base/deployments/writer.yaml",
        "compactor": ROOT / "k8s/base/deployments/compactor.yaml",
        "hotloader": ROOT / "k8s/base/deployments/hotloader.yaml",
        "backfill": ROOT / "k8s/base/deployments/backfill.yaml",
    }

    source_files = {
        "catalog": ROOT / "heber/catalog/api.py",
        "consumer": ROOT / "heber/writer/consumer.py",
        "writer": ROOT / "heber/writer/consumer.py",
        "compactor": ROOT / "heber/writer/compactor.py",
        "hotloader": ROOT / "heber/writer/hotstore.py",
        "backfill": ROOT / "heber/backfill/__main__.py",
    }

    for name, deployment_path in deployments.items():
        deployment = _load_yaml(deployment_path)
        template = deployment["spec"]["template"]
        annotations = template["metadata"]["annotations"]
        container = template["spec"]["containers"][0]

        assert annotations.get("prometheus.io/scrape") == "true", f"{name} scrape annotation missing"
        assert annotations.get("prometheus.io/port") == "9090", f"{name} scrape port drifted"
        assert any(port["containerPort"] == 9090 for port in container.get("ports", [])), (
            f"{name} no longer exposes metrics container port"
        )

        source = source_files[name].read_text(encoding="utf-8")
        assert "start_metrics_server_from_env" in source, f"{name} entrypoint does not start metrics server"
