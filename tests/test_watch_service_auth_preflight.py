"""Tests for the WatchService gateway auth preflight."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from heber.watch import writer as writer_module
from heber.watch.gateway import GATEWAY_API_KEY_ENV_VARS, GATEWAY_AUTH_HEADER_NAME
from heber.watch.writer import WatchService


class _ProbeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


class _ProbeClient:
    """Captures the request and returns a configured status."""

    response_status: int = 200
    raise_exc: Exception | None = None
    captured: dict[str, Any] = {}

    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        pass

    async def __aenter__(self) -> _ProbeClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    async def get(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        **_: Any,
    ) -> _ProbeResponse:
        type(self).captured = {"url": url, "headers": headers}
        if type(self).raise_exc is not None:
            raise type(self).raise_exc
        return _ProbeResponse(type(self).response_status)


def _make_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> WatchService:
    # Stub out heavy WatchService deps so we can construct it cheaply.
    monkeypatch.setattr(
        writer_module,
        "_resolve_gateway_api_key",
        lambda *a, **kw: "test-key",  # pragma: allowlist secret
    )

    class _StubManager:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

    class _StubConsumer:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

    class _StubPoller:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

    class _StubExtractor:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

    class _StubBackfill:
        def __init__(self, *a: Any, **kw: Any) -> None:
            pass

    monkeypatch.setattr("heber.watch.manager.WatchManager", _StubManager)
    monkeypatch.setattr("heber.watch.consumer.AlertWatchConsumer", _StubConsumer)
    monkeypatch.setattr("heber.watch.poller.SnapshotPoller", _StubPoller)
    monkeypatch.setattr("heber.watch.features.AlertFeatureExtractor", _StubExtractor)
    monkeypatch.setattr("heber.watch.backfill_scanner.EnrichmentBackfillScanner", _StubBackfill)

    redis_client = SimpleNamespace()
    return WatchService(
        redis_client=redis_client,
        gateway_url="http://gateway:8000",
        gateway_api_key="test-key",  # pragma: allowlist secret
        output_path=tmp_path,
    )


@pytest.mark.asyncio
async def test_preflight_on_401_logs_critical_without_raising(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    for var in GATEWAY_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    _ProbeClient.response_status = 401
    _ProbeClient.raise_exc = None
    _ProbeClient.captured = {}
    monkeypatch.setattr("httpx.AsyncClient", _ProbeClient)

    critical_calls: list[tuple[str, dict[str, Any]]] = []
    info_calls: list[tuple[str, dict[str, Any]]] = []

    def _capture_critical(message: str, **kwargs: Any) -> None:
        critical_calls.append((message, kwargs))

    def _capture_info(message: str, **kwargs: Any) -> None:
        info_calls.append((message, kwargs))

    monkeypatch.setattr(writer_module.logger, "critical", _capture_critical)
    monkeypatch.setattr(writer_module.logger, "info", _capture_info)

    service = _make_service(monkeypatch, tmp_path)
    # Must not raise.
    await service._gateway_auth_preflight()

    assert service._auth_preflight_ok is False
    assert critical_calls, "Expected CRITICAL log on 401"
    msg, fields = critical_calls[0]
    assert "GATEWAY AUTH PREFLIGHT FAILED" in msg
    assert fields["status_code"] == 401
    assert fields["auth_header_names_sent"] == [GATEWAY_AUTH_HEADER_NAME]
    assert "auth_env" in fields
    # Sent header captured by probe client.
    assert _ProbeClient.captured["headers"] == {
        GATEWAY_AUTH_HEADER_NAME: "test-key",  # pragma: allowlist secret
    }


@pytest.mark.asyncio
async def test_preflight_on_200_logs_ok(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _ProbeClient.response_status = 200
    _ProbeClient.raise_exc = None
    _ProbeClient.captured = {}
    monkeypatch.setattr("httpx.AsyncClient", _ProbeClient)

    info_calls: list[tuple[str, dict[str, Any]]] = []
    critical_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(writer_module.logger, "info", lambda m, **k: info_calls.append((m, k)))
    monkeypatch.setattr(writer_module.logger, "critical", lambda m, **k: critical_calls.append((m, k)))

    service = _make_service(monkeypatch, tmp_path)
    await service._gateway_auth_preflight()

    assert service._auth_preflight_ok is True
    assert not critical_calls
    ok_logs = [c for c in info_calls if "preflight ok" in c[0]]
    assert ok_logs


@pytest.mark.asyncio
async def test_preflight_on_transport_error_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _ProbeClient.response_status = 500
    _ProbeClient.raise_exc = ConnectionError("simulated network drop")
    _ProbeClient.captured = {}
    monkeypatch.setattr("httpx.AsyncClient", _ProbeClient)

    warning_calls: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(writer_module.logger, "warning", lambda m, **k: warning_calls.append((m, k)))
    # Don't crash other log paths.
    monkeypatch.setattr(writer_module.logger, "critical", lambda m, **k: None)
    monkeypatch.setattr(writer_module.logger, "info", lambda m, **k: None)

    service = _make_service(monkeypatch, tmp_path)
    await service._gateway_auth_preflight()

    assert service._auth_preflight_ok is None  # unknown
    assert warning_calls, "Expected a warning on transport failure"
    assert any("transport error" in m for m, _ in warning_calls)
