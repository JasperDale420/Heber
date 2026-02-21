"""Regression tests for executable worker entrypoint service modes."""

from __future__ import annotations

import importlib.util

from heber.backfill import __main__ as backfill_main


def test_backfill_package_has_executable_module() -> None:
    assert importlib.util.find_spec("heber.backfill.__main__") is not None


def test_backfill_main_invokes_server_runner_with_app() -> None:
    calls: dict[str, object] = {}

    def fake_run(app, host: str, port: int, log_level: str) -> None:  # noqa: ANN001
        calls["app"] = app
        calls["host"] = host
        calls["port"] = port
        calls["log_level"] = log_level

    exit_code = backfill_main.main(
        ["--host", "127.0.0.1", "--port", "8099", "--log-level", "warning"],
        run_server=fake_run,
        metrics_server_starter=lambda **_kwargs: None,
    )

    assert exit_code == 0
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8099
    assert calls["log_level"] == "warning"
    paths = {route.path for route in calls["app"].routes}  # type: ignore[attr-defined]
    assert "/health" in paths
    assert "/ready" in paths
    assert any(path.startswith("/backfill") for path in paths)
