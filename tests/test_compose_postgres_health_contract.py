"""Static contract tests for Postgres startup health behavior in docker compose."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]


def _load_compose() -> dict[str, Any]:
    return dict(yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8")))


def test_postgres_healthcheck_allows_slow_recovery_windows() -> None:
    compose = _load_compose()
    postgres = compose["services"]["postgres"]
    healthcheck = postgres.get("healthcheck", {})

    assert healthcheck.get("interval") == "10s"
    assert healthcheck.get("timeout") == "5s"
    assert healthcheck.get("retries") == 5
    assert healthcheck.get("start_period") == "120s"
