from __future__ import annotations

from types import SimpleNamespace

import pytest

from heber.watch import __main__ as watch_main


def test_watch_entrypoint_stops_service_on_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeService:
        last_instance: _FakeService | None = None

        def __init__(self, _redis_client, gateway_url: str, output_path):  # noqa: ANN001
            self.gateway_url = gateway_url
            self.output_path = output_path
            self.stop_called = False
            _FakeService.last_instance = self

        async def run(self) -> None:
            raise RuntimeError("watch runtime failed")

        def stop(self) -> None:
            self.stop_called = True

    monkeypatch.setattr(
        watch_main.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(redis="redis://example:6379", gateway="http://gateway:8000", output=None),
    )
    monkeypatch.setattr(watch_main.signal, "signal", lambda *_args, **_kwargs: None)

    import redis

    from heber.watch import writer as writer_module

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)

    with pytest.raises(RuntimeError, match="watch runtime failed"):
        watch_main.run()

    assert _FakeService.last_instance is not None
    assert _FakeService.last_instance.stop_called is True
