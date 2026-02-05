"""Regression checks for Terraform local module wiring."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = ROOT / "infrastructure" / "terraform"


def test_root_module_sources_exist_on_disk() -> None:
    main_tf = (TERRAFORM_ROOT / "main.tf").read_text(encoding="utf-8")
    sources = re.findall(r'source\s*=\s*"([^"]+)"', main_tf)

    local_sources = [src for src in sources if src.startswith("./")]
    assert local_sources

    missing = [(src, (TERRAFORM_ROOT / src)) for src in local_sources if not (TERRAFORM_ROOT / src).exists()]
    assert not missing, f"Missing Terraform module sources: {missing}"


def test_expected_module_entrypoints_have_main_tf() -> None:
    expected_modules = ["vpc", "s3", "rds", "elasticache", "ecr", "eks"]
    missing = []

    for name in expected_modules:
        module_main = TERRAFORM_ROOT / "modules" / name / "main.tf"
        if not module_main.exists():
            missing.append(str(module_main))

    assert not missing, f"Missing Terraform module files: {missing}"
