"""Regression checks for Terraform root-module wiring contracts."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TERRAFORM_ROOT = ROOT / "infrastructure" / "terraform"


def _module_output_names(module_name: str) -> set[str]:
    module_main = TERRAFORM_ROOT / "modules" / module_name / "main.tf"
    text = module_main.read_text(encoding="utf-8")
    return set(re.findall(r'output\s+"([^"]+)"', text))


def test_root_module_references_existing_module_outputs() -> None:
    root_main = (TERRAFORM_ROOT / "main.tf").read_text(encoding="utf-8")
    references = re.findall(r"module\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)", root_main)

    missing: list[str] = []
    for module_name, output_name in references:
        available_outputs = _module_output_names(module_name)
        if output_name not in available_outputs:
            missing.append(f"{module_name}.{output_name}")

    assert not missing, f"Root module references missing module outputs: {missing}"
