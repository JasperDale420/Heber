"""Regression tests for SDK catalog URL defaults."""

from __future__ import annotations

from heber.config import Settings
from heber.sdk import client as sdk_client


def test_settings_defaults_for_api_and_catalog_url(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_API_PORT", raising=False)
    monkeypatch.delenv("HEBER_CATALOG_URL", raising=False)
    monkeypatch.delenv("HEBER_POSTGRES_URL", raising=False)
    monkeypatch.delenv("HEBER_REDIS_URL", raising=False)

    settings = Settings(_env_file=None)
    assert settings.api_port == 8080
    assert settings.catalog_url == "http://localhost:8085/api/v1"
    assert settings.postgres_url == (
        "postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog"  # pragma: allowlist secret
    )
    assert settings.redis_url == "redis://localhost:6380"


def test_heber_client_uses_settings_catalog_url_by_default(monkeypatch) -> None:
    monkeypatch.setattr(sdk_client.settings, "api_port", 8080)
    monkeypatch.setattr(sdk_client.settings, "catalog_url", "http://localhost:8085/api/v1")

    client = sdk_client.HeberClient()
    assert client.catalog_url == "http://localhost:8085/api/v1"
