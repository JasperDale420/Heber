from __future__ import annotations

from pathlib import Path

import pytest

from heber.watch import writer as writer_module


def test_run_watch_service_stops_service_on_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeService:
        last_instance: _FakeService | None = None

        def __init__(
            self,
            redis_client,
            gateway_url: str,
            gateway_api_key: str | None = None,
            output_path: Path | None = None,
        ):  # noqa: ANN001
            _FakeService.last_instance = self
            self.redis_client = redis_client
            self.gateway_url = gateway_url
            self.gateway_api_key = gateway_api_key
            self.output_path = output_path
            self.stopped = False

        async def run(self) -> None:
            raise RuntimeError("boom")

        def stop(self) -> None:
            self.stopped = True

    import redis

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)

    with pytest.raises(RuntimeError, match="boom"):
        writer_module.run_watch_service(
            redis_url="redis://example:6379",
            gateway_url="http://gateway:8000",
            output_path=None,
        )

    assert _FakeService.last_instance is not None
    assert _FakeService.last_instance.stopped is True


def test_run_watch_service_stops_service_on_normal_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeService:
        last_instance: _FakeService | None = None

        def __init__(
            self,
            redis_client,
            gateway_url: str,
            gateway_api_key: str | None = None,
            output_path: Path | None = None,
        ):  # noqa: ANN001
            _FakeService.last_instance = self
            self.redis_client = redis_client
            self.gateway_url = gateway_url
            self.gateway_api_key = gateway_api_key
            self.output_path = output_path
            self.stopped = False

        async def run(self) -> None:
            return None

        def stop(self) -> None:
            self.stopped = True

    import redis

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)

    writer_module.run_watch_service(
        redis_url="redis://example:6379",
        gateway_url="http://gateway:8000",
        output_path=None,
    )

    assert _FakeService.last_instance is not None
    assert _FakeService.last_instance.stopped is True


def test_run_watch_service_preserves_run_error_when_stop_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeService:
        last_instance: _FakeService | None = None

        def __init__(
            self,
            redis_client,
            gateway_url: str,
            gateway_api_key: str | None = None,
            output_path: Path | None = None,
        ):  # noqa: ANN001
            _FakeService.last_instance = self
            self.redis_client = redis_client
            self.gateway_url = gateway_url
            self.gateway_api_key = gateway_api_key
            self.output_path = output_path
            self.stopped = False

        async def run(self) -> None:
            raise RuntimeError("boom")

        def stop(self) -> None:
            self.stopped = True
            raise RuntimeError("stop failed")

    import redis

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)

    with pytest.raises(RuntimeError, match="boom"):
        writer_module.run_watch_service(
            redis_url="redis://example:6379",
            gateway_url="http://gateway:8000",
            output_path=None,
        )

    assert _FakeService.last_instance is not None
    assert _FakeService.last_instance.stopped is True


def test_run_watch_service_ignores_stop_error_on_normal_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeService:
        last_instance: _FakeService | None = None

        def __init__(
            self,
            redis_client,
            gateway_url: str,
            gateway_api_key: str | None = None,
            output_path: Path | None = None,
        ):  # noqa: ANN001
            _FakeService.last_instance = self
            self.redis_client = redis_client
            self.gateway_url = gateway_url
            self.gateway_api_key = gateway_api_key
            self.output_path = output_path
            self.stopped = False

        async def run(self) -> None:
            return None

        def stop(self) -> None:
            self.stopped = True
            raise RuntimeError("stop failed")

    import redis

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)

    writer_module.run_watch_service(
        redis_url="redis://example:6379",
        gateway_url="http://gateway:8000",
        output_path=None,
    )

    assert _FakeService.last_instance is not None
    assert _FakeService.last_instance.stopped is True
