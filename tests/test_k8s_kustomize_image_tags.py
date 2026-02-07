"""Regression tests for Kubernetes kustomize image-tag overrides."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE_KUSTOMIZATION = ROOT / "k8s" / "base" / "kustomization.yaml"
OVERLAY_EXPECTED_TAGS = {
    "dev": "latest",
    "staging": "staging",
    "prod": "v1.0.0",
}


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _overlay_kustomization_path(overlay: str) -> Path:
    return ROOT / "k8s" / "overlays" / overlay / "kustomization.yaml"


def _base_rewritten_image_name() -> str:
    base = _load_yaml(BASE_KUSTOMIZATION)
    images = base.get("images") or []
    assert images, "Base kustomization must declare image rewrites."
    return images[0]["newName"]


def test_overlay_image_rules_target_base_rewritten_name() -> None:
    rewritten_name = _base_rewritten_image_name()

    for overlay, expected_tag in OVERLAY_EXPECTED_TAGS.items():
        overlay_cfg = _load_yaml(_overlay_kustomization_path(overlay))
        images = overlay_cfg.get("images") or []
        assert images, f"{overlay} overlay missing images transformer."

        image_rule = images[0]
        assert image_rule.get("name") == rewritten_name, (
            f"{overlay} overlay image rule must target {rewritten_name!r}; found {image_rule.get('name')!r}"
        )
        assert image_rule.get("newTag") == expected_tag, (
            f"{overlay} overlay image tag mismatch: expected {expected_tag!r}, found {image_rule.get('newTag')!r}"
        )


def test_kubectl_kustomize_renders_overlay_specific_tags() -> None:
    kubectl = shutil.which("kubectl")
    if kubectl is None:
        pytest.skip("kubectl is required to render overlays for this regression test")

    rewritten_name = _base_rewritten_image_name()
    image_pattern = re.compile(r"^\s*image:\s*(\S+)\s*$", re.MULTILINE)

    for overlay, expected_tag in OVERLAY_EXPECTED_TAGS.items():
        overlay_dir = ROOT / "k8s" / "overlays" / overlay
        proc = subprocess.run(
            [kubectl, "kustomize", str(overlay_dir)],
            check=False,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, f"kubectl kustomize failed for {overlay}: {proc.stderr or proc.stdout}"

        rendered_images = image_pattern.findall(proc.stdout)
        assert rendered_images, f"No image references found in rendered {overlay} manifest."

        expected_image = f"{rewritten_name}:{expected_tag}"
        for image in rendered_images:
            assert image == expected_image, (
                f"{overlay} rendered unexpected image {image!r}; expected {expected_image!r}"
            )
