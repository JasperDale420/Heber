"""Regression tests for metrics exporter wiring in service entrypoints.

Previously this also checked Kubernetes scrape annotations; the K8s
manifests were removed 2026-06-10 (never deployed). The durable invariant
is that every long-running service entrypoint starts the Prometheus
metrics server.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_service_entrypoints_start_metrics_server() -> None:
    source_files = {
        "catalog": ROOT / "heber/catalog/api.py",
        "consumer": ROOT / "heber/writer/consumer.py",
        "compactor": ROOT / "heber/writer/compactor.py",
        "backfill": ROOT / "heber/backfill/__main__.py",
    }

    for name, path in source_files.items():
        source = path.read_text(encoding="utf-8")
        assert "start_metrics_server_from_env" in source, f"{name} entrypoint does not start metrics server"
