"""Regression tests for executable worker entrypoint service modes."""

from __future__ import annotations

import importlib.util

from heber.backfill import __main__ as backfill_main
from heber.writer import hotstore as hotstore_main


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


def test_hotloader_once_mode_runs_sync_and_exits() -> None:
    class _FakeSyncer:
        def __init__(self) -> None:
            self.ensure_tables_called = False
            self.synced: list[tuple[str, str | None]] = []
            self.flush_called = False
            self.run_sync_loop_called = False

        def ensure_tables(self) -> None:
            self.ensure_tables_called = True

        def sync_from_silver(self, dataset: str, silver_path: str | None = None) -> int:
            self.synced.append((dataset, silver_path))
            return 0

        def flush(self) -> int:
            self.flush_called = True
            return 0

        async def run_sync_loop(self, datasets, silver_base_path):  # noqa: ANN001
            self.run_sync_loop_called = True

        def stop(self) -> None:
            return None

    fake_syncer = _FakeSyncer()

    def fake_factory(*, silver_base_path: str | None = None, **_kwargs) -> _FakeSyncer:
        assert silver_base_path == "/tmp/silver"
        return fake_syncer

    exit_code = hotstore_main.main(
        [
            "--once",
            "--datasets",
            "quotes,bars",
            "--silver-base-path",
            "/tmp/silver",
        ],
        syncer_factory=fake_factory,
        metrics_server_starter=lambda **_kwargs: None,
    )

    assert exit_code == 0
    assert fake_syncer.ensure_tables_called is True
    assert fake_syncer.synced == [("quotes", "/tmp/silver"), ("bars", "/tmp/silver")]
    assert fake_syncer.flush_called is True
    assert fake_syncer.run_sync_loop_called is False


def test_hotloader_service_mode_invokes_sync_loop() -> None:
    class _FakeSyncer:
        def __init__(self) -> None:
            self.ensure_tables_called = False
            self.run_sync_loop_called = False
            self.stop_called = False

        def ensure_tables(self) -> None:
            self.ensure_tables_called = True

        def sync_from_silver(self, dataset: str, silver_path: str | None = None) -> int:
            return 0

        def flush(self) -> int:
            return 0

        async def run_sync_loop(self, datasets, silver_base_path):  # noqa: ANN001
            self.run_sync_loop_called = True

        def stop(self) -> None:
            self.stop_called = True

    fake_syncer = _FakeSyncer()

    def fake_factory(*, silver_base_path: str | None = None, **_kwargs) -> _FakeSyncer:
        return fake_syncer

    exit_code = hotstore_main.main(
        ["--datasets", "quotes"],
        syncer_factory=fake_factory,
        metrics_server_starter=lambda **_kwargs: None,
    )

    assert exit_code == 0
    assert fake_syncer.ensure_tables_called is True
    assert fake_syncer.run_sync_loop_called is True
    assert fake_syncer.stop_called is True


def test_hotloader_dedupes_dataset_list() -> None:
    class _FakeSyncer:
        def __init__(self) -> None:
            self.ensure_tables_called = False
            self.synced: list[tuple[str, str | None]] = []
            self.flush_called = False

        def ensure_tables(self) -> None:
            self.ensure_tables_called = True

        def sync_from_silver(self, dataset: str, silver_path: str | None = None) -> int:
            self.synced.append((dataset, silver_path))
            return 0

        def flush(self) -> int:
            self.flush_called = True
            return 0

        async def run_sync_loop(self, datasets, silver_base_path):  # noqa: ANN001
            raise AssertionError("run_sync_loop should not be called in --once mode")

        def stop(self) -> None:
            return None

    fake_syncer = _FakeSyncer()

    def fake_factory(*, silver_base_path: str | None = None, **_kwargs) -> _FakeSyncer:
        assert silver_base_path == "/tmp/silver"
        return fake_syncer

    exit_code = hotstore_main.main(
        [
            "--once",
            "--datasets",
            "quotes, bars, quotes, trades, trades",
            "--silver-base-path",
            "/tmp/silver",
        ],
        syncer_factory=fake_factory,
        metrics_server_starter=lambda **_kwargs: None,
    )

    assert exit_code == 0
    assert fake_syncer.ensure_tables_called is True
    assert fake_syncer.synced == [
        ("quotes", "/tmp/silver"),
        ("bars", "/tmp/silver"),
        ("trades", "/tmp/silver"),
    ]
    assert fake_syncer.flush_called is True
