"""Credential guard: the dev Postgres password must not reach non-dev environments."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from heber.config import DEV_POSTGRES_PASSWORD, Settings


def _clear_postgres_env(monkeypatch) -> None:
    """Ensure neither the password nor the full URL leak in from the real env."""
    monkeypatch.delenv("HEBER_POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("HEBER_POSTGRES_URL", raising=False)


def test_dev_with_default_password_is_allowed(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "dev")

    settings = Settings(_env_file=None)

    assert settings.environment == "dev"
    assert DEV_POSTGRES_PASSWORD in settings.postgres_url


def test_prod_with_default_password_raises(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    message = str(exc_info.value)
    assert "HEBER_POSTGRES_PASSWORD" in message
    assert "prod" in message


def test_prod_with_explicit_credentials_is_allowed(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")
    monkeypatch.setenv("HEBER_POSTGRES_PASSWORD", "s3cret-prod-password")

    settings = Settings(_env_file=None)

    assert settings.environment == "prod"
    assert DEV_POSTGRES_PASSWORD not in settings.postgres_url
    assert "s3cret-prod-password" in settings.postgres_url


def test_staging_with_default_password_raises(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "staging")

    with pytest.raises(ValidationError) as exc_info:
        Settings(_env_file=None)

    message = str(exc_info.value)
    assert "HEBER_POSTGRES_PASSWORD" in message
    assert "staging" in message


def test_prod_with_explicit_url_still_carrying_dev_password_raises(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")
    monkeypatch.setenv(
        "HEBER_POSTGRES_URL",
        f"postgresql+asyncpg://heber:{DEV_POSTGRES_PASSWORD}@db:5432/heber_catalog",
    )

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_prod_with_url_encoded_dev_password_raises(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")
    monkeypatch.setenv(
        "HEBER_POSTGRES_URL",
        "postgresql+asyncpg://heber:heber%5Fdev%5Fpassword@db:5432/heber_catalog",  # pragma: allowlist secret
    )

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_prod_with_password_containing_dev_password_substring_is_allowed(monkeypatch) -> None:
    _clear_postgres_env(monkeypatch)
    monkeypatch.setenv("HEBER_ENVIRONMENT", "prod")
    monkeypatch.setenv(
        "HEBER_POSTGRES_URL",
        f"postgresql+asyncpg://heber:not_{DEV_POSTGRES_PASSWORD}_real"  # pragma: allowlist secret
        "@db:5432/heber_catalog",
    )

    settings = Settings(_env_file=None)

    assert settings.environment == "prod"
