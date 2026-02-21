from __future__ import annotations

from types import SimpleNamespace

import pytest

from heber.watch import __main__ as watch_main


def test_watch_entrypoint_starts_metrics_server_once(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeService:
        def __init__(self, _redis_client, gateway_url: str, output_path):  # noqa: ANN001
            self.gateway_url = gateway_url
            self.output_path = output_path

        async def run(self) -> None:
            return None

        def stop(self) -> None:
            return None

    metrics_ports: list[int | None] = []

    monkeypatch.setattr(
        watch_main.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(redis="redis://example:6379", gateway="http://gateway:8000", output=None),
    )
    monkeypatch.setattr(watch_main.signal, "signal", lambda *_args, **_kwargs: None)

    import redis

    from heber.ops import metrics as metrics_module
    from heber.watch import writer as writer_module

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _FakeService)
    monkeypatch.setattr(
        metrics_module,
        "start_metrics_server_from_env",
        lambda default_port=None: metrics_ports.append(default_port),
    )

    watch_main.run()

    assert metrics_ports == [9090]
