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

        # Parse the duration string (e.g. "120s") to seconds
        numeric = int(start_period.rstrip("sm"))
        if start_period.endswith("m"):
            numeric *= 60
        assert numeric >= min_start_period_seconds, (
            f"Service '{name}' start_period={start_period} is below the minimum {min_start_period_seconds}s"
        )
