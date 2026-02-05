"""Regression tests for Catalog migration strategy and startup behavior."""

from __future__ import annotations

from pathlib import Path

import pytest

from heber.catalog import api as catalog_api


class _StubConnection:
    def __init__(self) -> None:
        self.run_sync_calls: list[object] = []

    async def run_sync(self, fn) -> None:  # noqa: ANN001
        self.run_sync_calls.append(fn)


class _StubBeginContext:
    def __init__(self, connection: _StubConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _StubConnection:
        return self._connection

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False


class _StubEngine:
    def __init__(self) -> None:
        self.connection = _StubConnection()
        self.begin_calls = 0

    def begin(self) -> _StubBeginContext:
        self.begin_calls += 1
        return _StubBeginContext(self.connection)


@pytest.mark.asyncio
async def test_catalog_lifespan_auto_creates_tables_in_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_engine = _StubEngine()
    monkeypatch.setattr(catalog_api, "engine", stub_engine)
    monkeypatch.setattr(catalog_api.settings, "environment", "dev")

    assert catalog_api._should_auto_create_catalog_tables() is True

    async with catalog_api.lifespan(catalog_api.app):
        pass

    assert stub_engine.begin_calls == 1
    assert stub_engine.connection.run_sync_calls == [catalog_api.Base.metadata.create_all]


@pytest.mark.asyncio
async def test_catalog_lifespan_skips_auto_create_tables_outside_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    stub_engine = _StubEngine()
    monkeypatch.setattr(catalog_api, "engine", stub_engine)
    monkeypatch.setattr(catalog_api.settings, "environment", "prod")

    assert catalog_api._should_auto_create_catalog_tables() is False

    async with catalog_api.lifespan(catalog_api.app):
        pass

    assert stub_engine.begin_calls == 0
    assert stub_engine.connection.run_sync_calls == []


def test_alembic_catalog_baseline_assets_exist() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    assert (repo_root / "alembic.ini").exists()
    assert (repo_root / "alembic" / "env.py").exists()

    versions_dir = repo_root / "alembic" / "versions"
    revision_files = list(versions_dir.glob("*.py"))
    assert revision_files, "Expected at least one Alembic revision file"
    assert any("catalog_initial" in path.name for path in revision_files)
