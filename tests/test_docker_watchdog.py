"""The supervisor must cover everything an outage can take down.

On 2026-08-12 a Docker Desktop restart stopped every container on the host.
The `heber-*` services carry `restart: always` and came back; `data-gateway`
and `data-gateway-redis` carry `restart: unless-stopped` and did not, because
Docker treats a container that received SIGTERM as user-stopped and skips it on
daemon start. heber-consumer, heber-backfill-consumer and heber-watch then
crash-looped ~105 times against a dead Redis for 1h40m.

The watchdog could not help: it only knows about seven `heber-*` services, so
Heber's own upstream is outside its view. It also never saw
`heber-backfill-consumer`, and containers have since been observed `paused`,
which is neither running nor recoverable by `compose up`.

The script is driven here through a stub `docker` on PATH, so what it decides
to act on is asserted rather than assumed.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

WATCHDOG = Path(__file__).resolve().parents[1] / "scripts" / "heber_docker_watchdog.sh"

_STUB = """#!/usr/bin/env bash
# Records every invocation, and answers inspect from STATES.
echo "$@" >> "$DOCKER_CALL_LOG"
case "$1" in
  info) exit 0 ;;
  inspect)
    name="${@: -1}"
    state="$(grep -E "^${name}=" "$DOCKER_STATES" 2>/dev/null | cut -d= -f2)"
    [[ -z "$state" ]] && { echo missing; exit 0; }
    case "$*" in
      *Health*) echo none ;;
      *) echo "$state" ;;
    esac
    ;;
  *) exit 0 ;;
esac
"""


@pytest.fixture
def harness(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "docker"
    stub.write_text(_STUB)
    stub.chmod(0o755)
    calls = tmp_path / "calls.log"
    states = tmp_path / "states"

    def run(state_map: dict[str, str]) -> str:
        states.write_text("\n".join(f"{k}={v}" for k, v in state_map.items()))
        calls.write_text("")
        env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "DOCKER_BIN": str(stub),
            "DOCKER_CALL_LOG": str(calls),
            "DOCKER_STATES": str(states),
        }
        subprocess.run(["bash", str(WATCHDOG)], env=env, capture_output=True, timeout=60, check=False)
        return calls.read_text()

    return run


ALL_HEBER = [
    "heber-consumer",
    "heber-backfill-consumer",
    "heber-watch",
    "heber-catalog",
    "heber-gold-poller",
    "heber-compactor",
    "heber-dataflow-health",
    "heber-health-monitor",
]
UPSTREAM = ["data-gateway-redis", "data-gateway"]


def _all_running() -> dict[str, str]:
    return {name: "running" for name in ALL_HEBER + UPSTREAM}


def test_quiet_when_everything_is_running(harness) -> None:
    calls = harness(_all_running())

    assert "compose up" not in calls
    assert "restart" not in calls
    assert "unpause" not in calls


def test_recovers_the_backfill_consumer(harness) -> None:
    """It ingests the UW backfill stream and was missing from the watch list."""
    states = _all_running()
    states["heber-backfill-consumer"] = "exited"

    assert "heber-backfill-consumer" in harness(states)


def test_recovers_heber_upstream(harness) -> None:
    """Heber crash-loops without these, and nothing else brings them back."""
    states = _all_running()
    states["data-gateway-redis"] = "exited"
    states["data-gateway"] = "exited"

    calls = harness(states)

    assert "data-gateway-redis" in calls, "Redis is what the consumers crash-loop against"
    assert "data-gateway" in calls


def test_unpauses_paused_containers(harness) -> None:
    """A paused container is not running and `compose up` will not revive it.

    Observed repeatedly on this host: heber-watch and heber-backfill-consumer
    sat `paused` while still reporting `Up`, processing nothing.
    """
    states = _all_running()
    states["heber-watch"] = "paused"

    calls = harness(states)

    assert "unpause heber-watch" in calls, "paused container was not unpaused"


def test_upstream_started_by_name_not_compose(harness) -> None:
    """They live in a different compose project, so `compose up` cannot reach them."""
    states = _all_running()
    states["data-gateway-redis"] = "exited"

    calls = harness(states)
    compose_lines = [ln for ln in calls.splitlines() if ln.startswith("compose up")]

    assert all("data-gateway-redis" not in ln for ln in compose_lines), (
        "upstream must be started by name; it is not in Heber's compose project"
    )
