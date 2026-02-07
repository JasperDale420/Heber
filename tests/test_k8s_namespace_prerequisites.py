"""Regression tests for k8s namespace-scoped runtime prerequisites."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE_KUSTOMIZATION = ROOT / "k8s" / "base" / "kustomization.yaml"
OVERLAY_NAMESPACES = {
    "dev": "heber-dev",
    "staging": "heber-staging",
    "prod": "heber-prod",
}


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _render_overlay_docs(overlay: str) -> list[dict]:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required to render overlays for this regression test")

    overlay_dir = ROOT / "k8s" / "overlays" / overlay
    proc = subprocess.run(
        [kubectl, "kustomize", str(overlay_dir)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"kubectl kustomize failed for {overlay}: {proc.stderr or proc.stdout}"

    return [doc for doc in yaml.safe_load_all(proc.stdout) if isinstance(doc, dict)]


def _find_doc(docs: list[dict], kind: str, name: str, namespace: str | None = None) -> dict | None:
    for doc in docs:
        if doc.get("kind") != kind:
            continue
        metadata = doc.get("metadata", {})
        if metadata.get("name") != name:
            continue
        if namespace is not None and metadata.get("namespace") != namespace:
            continue
        return doc
    return None


def test_base_kustomization_includes_runtime_prerequisite_resources() -> None:
    base_cfg = _load_yaml(BASE_KUSTOMIZATION)
    resources = set(base_cfg.get("resources") or [])

    assert "serviceaccount.yaml" in resources
    assert "secrets/cluster-secret-store.yaml" in resources
    assert "secrets/external-secret.yaml" in resources


def test_rendered_overlays_include_secret_and_serviceaccount_prerequisites() -> None:
    for overlay, namespace in OVERLAY_NAMESPACES.items():
        docs = _render_overlay_docs(overlay)

        assert _find_doc(docs, "Namespace", namespace) is not None
        assert _find_doc(docs, "ServiceAccount", "heber", namespace) is not None
        assert _find_doc(docs, "ConfigMap", "heber-config", namespace) is not None
        assert _find_doc(docs, "ExternalSecret", "heber-secrets", namespace) is not None
        assert _find_doc(docs, "ClusterSecretStore", "aws-secrets-manager") is not None

        deployments = [doc for doc in docs if doc.get("kind") == "Deployment"]
        assert deployments, f"No deployments rendered for {overlay}"

        for deployment in deployments:
            pod_spec = deployment["spec"]["template"]["spec"]
            assert pod_spec.get("serviceAccountName") == "heber"

            env_from = pod_spec["containers"][0].get("envFrom") or []
            configmap_refs = [
                item["configMapRef"]["name"]
                for item in env_from
                if "configMapRef" in item and "name" in item["configMapRef"]
            ]
            secret_refs = [
                item["secretRef"]["name"] for item in env_from if "secretRef" in item and "name" in item["secretRef"]
            ]

            assert "heber-config" in configmap_refs
            assert "heber-secrets" in secret_refs
