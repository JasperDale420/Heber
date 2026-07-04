"""Static contract tests for Docker restart resilience.

Validates that all services in docker-compose.yml are configured to survive
Docker daemon restarts and have adequate startup grace periods.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]


def _load_compose() -> dict[str, Any]:
    return dict(yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8")))


def test_all_services_use_restart_always() -> None:
    """restart: always ensures containers restart after Docker daemon restarts,
    unlike unless-stopped which leaves Exited(255) containers dead."""
    compose = _load_compose()
    for name, svc in compose["services"].items():
        policy = svc.get("restart")
        assert policy == "always", (
            f"Service '{name}' uses restart: {policy!r}, must be 'always' to survive Docker daemon restarts"
        )


def test_healthchecked_services_have_start_period() -> None:
    """Services with healthchecks need start_period >= 120s to give Postgres
    and other dependencies time to initialize before marking unhealthy."""
    compose = _load_compose()
    min_start_period_seconds = 120

    # heber-consumer's healthcheck is a self-heartbeat liveness probe (added in the
    # EOD-incident hardening) that passes whenever the consumer's loop heartbeat is
    # fresh within a 180s window — that window already absorbs slow dependency
    # startup, so its shorter start_period is intentional for fast liveness
    # detection. It is exempt from the Postgres-init 120s floor but must still
    # declare a start_period.
    heartbeat_healthcheck_services = {"heber-consumer"}

    for name, svc in compose["services"].items():
        hc = svc.get("healthcheck", {})
        if hc.get("disable", False):
            continue
        if "test" not in hc:
            continue

        start_period = hc.get("start_period")
        assert start_period is not None, (
            f"Service '{name}' has a healthcheck but no start_period — "
            "it will be marked unhealthy before dependencies are ready"
        )

        if name in heartbeat_healthcheck_services:
            continue

        # Parse the duration string (e.g. "120s") to seconds
        numeric = int(start_period.rstrip("sm"))
        if start_period.endswith("m"):
            numeric *= 60
        assert numeric >= min_start_period_seconds, (
            f"Service '{name}' start_period={start_period} is below the minimum {min_start_period_seconds}s"
        )


def test_built_services_include_workspace_shared_packages() -> None:
    """Built services need the monorepo root as context so uv path dependencies
    like ../empire-core and ../empire-schemas resolve during docker build."""
    compose = _load_compose()

    built_services = {name: svc["build"] for name, svc in compose["services"].items() if "build" in svc}
    assert built_services, "Expected Heber services to define docker build settings"

    for name, build in built_services.items():
        assert build["context"] == "..", (
            f"Service '{name}' build context must be '..' so sibling workspace packages are available"
        )
        assert build["dockerfile"] == "Heber/Dockerfile", (
            f"Service '{name}' dockerfile must point at 'Heber/Dockerfile' when using workspace context"
        )

    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "WORKDIR /workspace/Heber" in dockerfile
    assert "COPY empire-core/ /workspace/empire-core/" in dockerfile
    assert "COPY empire-schemas/ /workspace/empire-schemas/" in dockerfile
