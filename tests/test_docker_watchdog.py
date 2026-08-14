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

    # The lakehouse volume is faked here rather than read from the host: these
    # tests must not depend on /Volumes/heber being mounted (it never is on CI).
    volume_root = tmp_path / "volume"
    (volume_root / "data").mkdir(parents=True)

    def run(state_map: dict[str, str], *, volume_mounted: bool = True) -> str:
        sentinel = volume_root / "data" / ".heber-sentinel"
        if volume_mounted:
            sentinel.touch()
        elif sentinel.exists():
            sentinel.unlink()
        states.write_text("\n".join(f"{k}={v}" for k, v in state_map.items()))
        calls.write_text("")
        env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "DOCKER_BIN": str(stub),
            "DOCKER_CALL_LOG": str(calls),
            "DOCKER_STATES": str(states),
            "HEBER_VOLUME_ROOT": str(volume_root),
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


def test_missing_lakehouse_volume_does_not_recreate_heber_services(harness) -> None:
    """`compose up` cannot fix an absent bind-mount source — it just thrashes.

    After the 2026-08-12 boot the exFAT volume was under repair for 76 minutes
    (`diskarbitrationd: The volume heber was repaired successfully`, 10:14:17Z).
    Every heber service bind-mounts it, so postgres could not start and
    `depends_on: service_healthy` failed everything downstream. The watchdog
    retried all seven services every 120s for 35 ticks and fixed nothing,
    emitting 89 `mkdir /host_mnt/Volumes/heber: permission denied` errors.
    """
    states = _all_running()
    for name in ALL_HEBER:
        states[name] = "exited"

    calls = harness(states, volume_mounted=False)

    assert "compose up" not in calls, "must not thrash compose while the volume is gone"


def test_missing_lakehouse_volume_still_recovers_upstream(harness) -> None:
    """The guard must not take the upstream down with it.

    data-gateway and data-gateway-redis do not touch the lakehouse volume, and
    Redis is what buffers live events while Heber is down — heber:events is
    capped at 500K and evicts unread entries. Skipping upstream recovery during
    a volume outage would turn one outage into two, and lose data.
    """
    states = _all_running()
    for name in ALL_HEBER:
        states[name] = "exited"
    states["data-gateway-redis"] = "exited"
    states["data-gateway"] = "exited"

    calls = harness(states, volume_mounted=False)

    assert "start data-gateway-redis" in calls, "upstream Redis must still be recovered"
    assert "data-gateway" in calls


def test_missing_lakehouse_volume_still_unpauses(harness) -> None:
    """Unpausing is free and volume-independent; a paused container processes nothing."""
    states = _all_running()
    states["heber-watch"] = "paused"

    calls = harness(states, volume_mounted=False)

    assert "unpause heber-watch" in calls


def test_volume_guard_line_is_not_counted_as_an_intervention() -> None:
    """The skip line must not read as a repair to the stack alarm.

    heber/ops/stack_check.py matches watchdog *actions* by these four strings and
    pages Discord for each. A volume outage already pages once via stack_volume;
    the watchdog's skip line must not add a second alert every cycle.
    """
    script = WATCHDOG.read_text()
    actions = (
        "reconciling down service(s)",
        "restarting unhealthy service(s)",
        "unpausing service(s)",
        "starting upstream service(s)",
    )
    skip_lines = [ln for ln in script.splitlines() if "echo" in ln and "lakehouse volume" in ln.lower()]

    assert skip_lines, "expected a volume-missing skip line"
    for line in skip_lines:
        assert not any(action in line for action in actions), f"skip line reads as an action: {line}"
