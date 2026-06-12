"""Contracts for the native LaunchD pilot scaffolding."""

from __future__ import annotations

import plistlib
import stat
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_ROOT = Path("/Users/jacobmcmillan/Empire/Heber")


PILOT_SERVICES = {
    "dataflow-health": {
        "label": "com.empire.heber.dataflow-health",
        "log_name": "dataflow-health",
    },
    "health-monitor": {
        "label": "com.empire.heber.health-monitor",
        "log_name": "health-monitor",
    },
    "gold-poller": {
        "label": "com.empire.heber.gold-poller",
        "log_name": "gold-poller",
    },
    "compactor": {
        "label": "com.empire.heber.compactor",
        "log_name": "compactor",
    },
}


def _load_plist(service: str) -> dict[str, Any]:
    plist_path = ROOT / "launchd" / f"com.empire.heber.{service}.plist"
    with plist_path.open("rb") as handle:
        return cast(dict[str, Any], plistlib.load(handle))


def test_native_runner_is_executable_and_host_wired() -> None:
    script = ROOT / "scripts" / "run_native_heber_service.sh"
    mode = script.stat().st_mode
    text = script.read_text(encoding="utf-8")

    assert mode & stat.S_IXUSR, "native runner must be executable by the owner"
    assert 'HEBER_DATA_ROOT="${HEBER_DATA_ROOT:-/Volumes/heber/data}"' in text
    assert 'HEBER_REDIS_URL="${HEBER_REDIS_URL:-redis://localhost:6379}"' in text
    assert 'DATA_GATEWAY_URL="${DATA_GATEWAY_URL:-http://localhost:8080}"' in text
    assert "HEBER_HEALTH_CONSUMER_METRICS_URL=" in text
    assert "http://localhost:9090/metrics" in text
    assert "HEBER_HEALTH_WATCH_METRICS_URL=" in text
    assert "http://localhost:9091/metrics" in text
    assert "host.docker.internal" not in text


def test_native_runner_only_allows_low_risk_pilot_services() -> None:
    text = (ROOT / "scripts" / "run_native_heber_service.sh").read_text(encoding="utf-8")

    for service in PILOT_SERVICES:
        assert f'"{service}")' in text

    assert '"consumer")' not in text
    assert '"watch")' not in text
    assert '"catalog")' not in text


def test_launchd_plists_run_native_runner_with_restart_policy() -> None:
    runner = str(CANONICAL_ROOT / "scripts" / "run_native_heber_service.sh")

    for service, expected in PILOT_SERVICES.items():
        plist = _load_plist(service)

        assert plist["Label"] == expected["label"]
        assert plist["ProgramArguments"] == [runner, service]
        assert plist["WorkingDirectory"] == str(CANONICAL_ROOT)
        assert plist["RunAtLoad"] is True
        assert plist["KeepAlive"] == {"SuccessfulExit": False}
        assert plist["ThrottleInterval"] >= 30
        assert plist["StandardOutPath"].endswith(f"logs/native/{expected['log_name']}.out.log")
        assert plist["StandardErrorPath"].endswith(f"logs/native/{expected['log_name']}.err.log")

        env = plist["EnvironmentVariables"]
        assert "/Users/jacobmcmillan/.local/bin" in env["PATH"]
        assert "/opt/homebrew/bin" in env["PATH"]


def test_install_script_defaults_to_no_start_for_safe_pilot_rollout() -> None:
    text = (ROOT / "scripts" / "install_native_launchd.sh").read_text(encoding="utf-8")

    assert "--start" in text
    assert "START_SERVICES=false" in text
    assert "launchctl bootstrap" in text
    assert "launchctl bootout" in text
    assert "launchctl kickstart" in text
    assert "kickstart -k" not in text


def test_native_launchd_docs_include_checkpoint_gates() -> None:
    text = (ROOT / "docs" / "operations" / "native-launchd.md").read_text(encoding="utf-8")

    assert "June 1, 2026" in text
    assert "Do not migrate `heber-consumer`" in text
    assert "Checkpoint 1" in text
    assert "Checkpoint 2" in text
    assert "Checkpoint 3" in text
    assert "rollback" in text.lower()
