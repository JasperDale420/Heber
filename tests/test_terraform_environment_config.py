"""Regression checks for Terraform environment region/backend configurability."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_ROOT = ROOT / "infrastructure" / "terraform" / "environments"


def test_environment_main_tf_uses_variable_region_and_partial_backend() -> None:
    for env_name in ("dev", "staging", "prod"):
        main_tf = (ENV_ROOT / env_name / "main.tf").read_text(encoding="utf-8")

        assert 'backend "s3" {}' in main_tf, f"{env_name}: backend should be partial"
        assert 'variable "aws_region"' in main_tf, f"{env_name}: missing aws_region variable"
        assert "region             = var.aws_region" in main_tf, f"{env_name}: module region should use variable"


def test_environment_backend_config_files_exist_without_hardcoded_region() -> None:
    for env_name in ("dev", "staging", "prod"):
        backend_hcl = ENV_ROOT / env_name / "backend.hcl"
        assert backend_hcl.exists(), f"{env_name}: missing backend.hcl"

        contents = backend_hcl.read_text(encoding="utf-8")
        assert 'bucket         = "heber-terraform-state"' in contents
        assert f'key            = "{env_name}/terraform.tfstate"' in contents
        assert 'dynamodb_table = "heber-terraform-locks"' in contents
        assert "encrypt        = true" in contents
        assert "region" not in contents, f"{env_name}: backend config should not hardcode region"
