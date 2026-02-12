"""Static compose contract tests for metadata/search infrastructure services."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _load_compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def test_openmetadata_has_healthcheck_for_system_status() -> None:
    compose = _load_compose()
    openmetadata = compose["services"]["openmetadata"]

    healthcheck = openmetadata.get("healthcheck")
    assert healthcheck is not None
    assert healthcheck["test"] == [
        "CMD-SHELL",
        "wget -q --spider http://localhost:8585/api/v1/system/version || exit 1",
    ]


def test_metadata_services_define_explicit_stop_grace_period() -> None:
    compose = _load_compose()
    services = compose["services"]

    assert services["elasticsearch"].get("stop_grace_period") == "90s"
    assert services["openmetadata"].get("stop_grace_period") == "90s"
