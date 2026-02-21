from __future__ import annotations

import json
import sys

from heber import cli as cli_module


def test_health_dataflow_command_outputs_json_only(monkeypatch, capsys) -> None:
    def _fake_once(**kwargs):  # noqa: ANN003
        return {
            "ts_utc": "2026-02-12T00:00:00+00:00",
            "mode": kwargs["mode"],
            "market_open": True,
            "overall_status": "ok",
            "window_seconds": kwargs["window_seconds"],
            "checks": [],
            "summary": {"ok": 1, "warn": 0, "fail": 0, "skipped": 0},
        }

    from heber.ops import dataflow_health as dataflow_health_module

    monkeypatch.setattr(dataflow_health_module, "run_dataflow_health_once", _fake_once)
    monkeypatch.setattr(
        sys,
        "argv",
        ["heber", "health-dataflow", "--mode", "manual", "--window-seconds", "900"],
    )

    exit_code = cli_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert payload["overall_status"] == "ok"
    assert captured.err == ""


def test_health_dataflow_loop_mode_emits_periodic_json_entries(monkeypatch, capsys) -> None:
    from heber.ops import dataflow_health as dataflow_health_module

    calls = {"count": 0}

    def _fake_once(**kwargs):  # noqa: ANN003
        calls["count"] += 1
        if calls["count"] > 2:
            raise KeyboardInterrupt
        return {
            "ts_utc": f"2026-02-12T00:00:0{calls['count']}+00:00",
            "mode": kwargs["mode"],
            "market_open": True,
            "overall_status": "ok",
            "window_seconds": kwargs["window_seconds"],
            "checks": [],
            "summary": {"ok": 1, "warn": 0, "fail": 0, "skipped": 0},
        }

    monkeypatch.setattr(dataflow_health_module, "run_dataflow_health_once", _fake_once)
    monkeypatch.setattr(dataflow_health_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["heber", "health-dataflow", "--mode", "scheduled", "--loop", "--interval-seconds", "0"],
    )

    exit_code = cli_module.main()
    captured = capsys.readouterr()

    assert exit_code == 0
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 2
    assert all(json.loads(line)["overall_status"] == "ok" for line in lines)
