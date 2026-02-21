"""Regression tests for SDK catalog URL defaults."""

from __future__ import annotations

from heber.config import Settings
from heber.sdk import client as sdk_client


class _StubResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _RecordingHttpClient:
    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.base_url = kwargs.get("base_url")
        self.calls: list[tuple[str, str]] = []

    def get(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append(("GET", path))
        if path == "datasets":
            return _StubResponse({"data": []})
        if path == "datasets/bars":
            return _StubResponse({"data": {"dataset_name": "bars"}})
        if path == "datasets/bars/versions":
            return _StubResponse({"data": []})
        return _StubResponse({"data": []})

    def post(self, path, **kwargs):  # noqa: ANN001, ANN003
        self.calls.append(("POST", path))
        if path == "instruments/lookup":
            return _StubResponse({"data": [{"instrument_key": "equity:AAPL"}]})
        return _StubResponse({"data": []})

    def close(self) -> None:
        return None


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
    assert settings.redis_url == "redis://localhost:6380"


def test_settings_accept_legacy_feast_repo_path_env(monkeypatch) -> None:
    monkeypatch.delenv("HEBER_FEAST_REPO_PATH", raising=False)
    monkeypatch.setenv("FEAST_REPO_PATH", "/tmp/custom-feast")

    settings = Settings(_env_file=None)
    assert str(settings.feast_repo_path) == "/tmp/custom-feast"


def test_heber_client_uses_settings_catalog_url_by_default(monkeypatch) -> None:
    monkeypatch.setattr(sdk_client.settings, "api_port", 8080)
    monkeypatch.setattr(sdk_client.settings, "catalog_url", "http://localhost:8085/api/v1")

    client = sdk_client.HeberClient()
    assert client.catalog_url == "http://localhost:8085/api/v1"


def test_heber_client_uses_relative_catalog_paths(monkeypatch) -> None:
    created_clients: list[_RecordingHttpClient] = []

    def _client_factory(*args, **kwargs):  # noqa: ANN002, ANN003
        instance = _RecordingHttpClient(*args, **kwargs)
        created_clients.append(instance)
        return instance

    monkeypatch.setattr(sdk_client.httpx, "Client", _client_factory)

    client = sdk_client.HeberClient(catalog_url="http://localhost:8085/api/v1")
    _ = client.list_datasets()
    _ = client.get_dataset("bars")
    resolved = client.resolve_instrument("AAPL")
    client.close()

    assert resolved == "equity:AAPL"
    assert len(created_clients) == 1
    assert created_clients[0].calls == [
        ("GET", "datasets"),
        ("GET", "datasets/bars"),
        ("POST", "instruments/lookup"),
    ]
