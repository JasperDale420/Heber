"""Bind-address contracts: unauthenticated services must not listen on LAN interfaces.

The catalog and backfill HTTP APIs have no authentication, and the
deployment is a single macOS host (audit decision Q2, 2026-06-10). Native
processes therefore default to loopback binds, and docker-compose restricts
host-side port publishing to 127.0.0.1 (containers bind 0.0.0.0 internally —
exposure is controlled at the publish boundary).
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from heber.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def test_backfill_host_defaults_to_loopback() -> None:
    assert Settings.model_fields["backfill_host"].default == "127.0.0.1"


def test_compose_published_ports_are_loopback_only() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))

    offenders: list[str] = []
    for name, service in compose["services"].items():
        for mapping in service.get("ports", []):
            # Mappings are "host:container" or "ip:host:container"; only an
            # explicit loopback IP keeps the port off LAN interfaces.
            if not re.match(r"^127\.0\.0\.1:", str(mapping)):
                offenders.append(f"{name}: {mapping}")

    assert not offenders, f"Published ports must bind 127.0.0.1: {offenders}"
