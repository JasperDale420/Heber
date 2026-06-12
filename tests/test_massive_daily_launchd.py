"""Contracts for the Massive daily LaunchAgent."""

from __future__ import annotations

import plistlib
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_ROOT = Path("/Users/jacobmcmillan/Empire/Heber")


def _load_massive_plist() -> dict[str, Any]:
    with (ROOT / "launchd" / "com.empire.heber.massive-daily.plist").open("rb") as handle:
        return cast(dict[str, Any], plistlib.load(handle))


def test_native_runner_has_massive_daily_service_and_raw_archive_default() -> None:
    text = (ROOT / "scripts" / "run_native_heber_service.sh").read_text(encoding="utf-8")

    assert '"massive-daily")' in text
    assert 'MASSIVE_ARCHIVE_ROOT="${MASSIVE_ARCHIVE_ROOT:-${HEBER_DATA_ROOT}/_vendor_raw/massive}"' in text
    assert "exec uv run heber massive-daily" in text


def test_install_script_accepts_massive_daily_service() -> None:
    text = (ROOT / "scripts" / "install_native_launchd.sh").read_text(encoding="utf-8")

    assert "massive-daily" in text
    assert "dataflow-health|health-monitor|gold-poller|compactor|alert-check|massive-daily" in text


def test_massive_daily_launchagent_runs_after_login_restart_and_retries_failures() -> None:
    plist = _load_massive_plist()

    assert plist["Label"] == "com.empire.heber.massive-daily"
    assert plist["ProgramArguments"] == [
        str(CANONICAL_ROOT / "scripts" / "run_native_heber_service.sh"),
        "massive-daily",
    ]
    assert plist["WorkingDirectory"] == str(CANONICAL_ROOT)
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] == {"SuccessfulExit": False}
    assert plist["ThrottleInterval"] >= 300
    assert plist["StartCalendarInterval"] == {"Hour": 9, "Minute": 15}
    assert plist["StandardOutPath"].endswith("logs/native/massive-daily.out.log")
    assert plist["StandardErrorPath"].endswith("logs/native/massive-daily.err.log")
