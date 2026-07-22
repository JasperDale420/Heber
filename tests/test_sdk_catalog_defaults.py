"""Regression tests for SDK catalog URL defaults."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from heber.config import Settings
from heber.reader import HeberReader


def test_settings_defaults_for_api_and_catalog_url(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_API_PORT", raising=False)
    monkeypatch.delenv("HEBER_CATALOG_URL", raising=False)
    monkeypatch.delenv("HEBER_FEAST_REPO_PATH", raising=False)
    monkeypatch.delenv("FEAST_REPO_PATH", raising=False)
    monkeypatch.delenv("HEBER_POSTGRES_URL", raising=False)
    monkeypatch.delenv("HEBER_REDIS_URL", raising=False)

    settings = Settings(_env_file=None)
    assert settings.api_port == 8080
    assert settings.catalog_url == "http://localhost:8085/api/v1"
    assert str(settings.feast_repo_path) == "features"
    assert settings.postgres_url == (
        "postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog"  # pragma: allowlist secret
    )
    assert settings.redis_url == "redis://localhost:6379"


def test_settings_accept_legacy_feast_repo_path_env(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_FEAST_REPO_PATH", raising=False)
    monkeypatch.setenv("FEAST_REPO_PATH", "/tmp/custom-feast")

    settings = Settings(_env_file=None)
    assert str(settings.feast_repo_path) == "/tmp/custom-feast"


def test_heber_reader_uses_data_root(tmp_path: Path) -> None:
    reader = HeberReader(tmp_path)
    assert reader._root == tmp_path


def test_heber_reader_returns_empty_when_path_missing(tmp_path: Path) -> None:
    reader = HeberReader(tmp_path)
    df = reader.read_silver("nonexistent_feed")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_heber_reader_context_manager(tmp_path: Path) -> None:
    with HeberReader(tmp_path) as reader:
        assert reader._root == tmp_path


def test_heber_reader_list_gold_versions_empty(tmp_path: Path) -> None:
    reader = HeberReader(tmp_path)
    versions = reader.list_gold_versions("no_such_dataset")
    assert versions == []
