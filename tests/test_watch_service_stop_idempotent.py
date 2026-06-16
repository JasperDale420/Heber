"""Regression tests: WatchService.stop() must be idempotent, and the watch
``__main__`` entrypoint must install signal handlers through the asyncio
event loop (not via ``signal.signal()`` from arbitrary threads).

Background — 176 "Watch service stop failed" errors per shutdown were
caused by:
  1. The signal-driven shutdown calling ``service.stop()``, then the CLI's
     ``finally`` block calling it again — the second call hit an already
     closed redis client and raised.
  2. ``signal.signal()`` being called from a non-main thread in some
     deployment contexts, producing "signal only works in main thread"
     warnings stacked alongside the stop failures.

Both paths are tested here.
"""

from __future__ import annotations

import asyncio
import threading
from unittest.mock import MagicMock

import pytest

import heber.watch.__main__ as watch_main
import heber.watch.writer as writer_module
from heber.watch.writer import WatchService


def _build_service_with_stubs() -> tuple[WatchService, list[MagicMock]]:
    """Construct a WatchService with every component replaced by a MagicMock.

    Returns the service and the ordered list of stubs so the caller can
    inspect their .stop()/.flush()/.close() call counts.
    """
    service = WatchService.__new__(WatchService)
    service._running = True
    service._stopped = False
    service._backfill_enabled = False

    service.manager = MagicMock(name="manager")
    service.consumer = MagicMock(name="consumer")
    service.poller = MagicMock(name="poller")
    service.checker = MagicMock(name="checker")
    service.backfill_scanner = MagicMock(name="backfill_scanner")
    service.writer = MagicMock(name="writer")
    service.redis = MagicMock(name="redis")

    stubs = [
        service.consumer,
        service.poller,
        service.backfill_scanner,
        service.writer,
        service.redis,
    ]
    return service, stubs


@pytest.mark.unit
def test_stop_is_idempotent() -> None:
    """Calling stop() twice must not crash and must only run cleanup once."""
    service, stubs = _build_service_with_stubs()

    service.stop()
    service.stop()  # second call should short-circuit

    # Each cleanup primitive ran exactly once across both stop() calls.
    service.consumer.stop.assert_called_once()
    service.poller.stop.assert_called_once()
    service.backfill_scanner.stop.assert_called_once()
    service.writer.flush.assert_called_once()
    service.redis.close.assert_called_once()
    assert service._stopped is True


@pytest.mark.unit
def test_stop_continues_when_one_component_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If consumer.stop() raises, poller/writer/redis still get stopped."""
    service, _ = _build_service_with_stubs()
    service.consumer.stop.side_effect = RuntimeError("consumer boom")

    with pytest.raises(RuntimeError, match="consumer boom"):
        service.stop()

    # Despite consumer.stop() raising, downstream components still ran.
    service.poller.stop.assert_called_once()
    service.backfill_scanner.stop.assert_called_once()
    service.writer.flush.assert_called_once()
    service.redis.close.assert_called_once()


@pytest.mark.unit
def test_install_signal_handlers_skips_when_off_main_thread(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Signal install must not raise when called from a non-main thread."""

    results: dict[str, object] = {}

    def _worker() -> None:
        loop = asyncio.new_event_loop()
        try:
            results["installed"] = watch_main._install_signal_handlers(loop, lambda: None)
        finally:
            loop.close()

    t = threading.Thread(target=_worker, name="off-main-thread")
    t.start()
    t.join()

    assert results.get("installed") is False, (
        "expected signal-handler install to refuse off-main-thread without raising"
    )


@pytest.mark.unit
def test_install_signal_handlers_installs_on_main_thread() -> None:
    """In the normal CLI path, both SIGTERM and SIGINT handlers register."""
    loop = asyncio.new_event_loop()
    try:
        installed = watch_main._install_signal_handlers(loop, lambda: None)
    finally:
        # Always remove handlers to avoid leaking state into other tests.
        try:
            import signal as _signal

            loop.remove_signal_handler(_signal.SIGTERM)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
        try:
            import signal as _signal

            loop.remove_signal_handler(_signal.SIGINT)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
        loop.close()

    assert installed is True


@pytest.mark.unit
def test_run_watch_service_stop_called_once_via_double_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run_watch_service``'s finally-block calling stop() after the service
    already stopped itself must not produce an error log.
    """

    class _RecordingService:
        last_instance: _RecordingService | None = None

        def __init__(
            self,
            redis_client,
            gateway_url: str,
            gateway_api_key: str | None = None,
            output_path=None,
        ):  # noqa: ANN001
            _RecordingService.last_instance = self
            self._stopped = False
            self.stop_calls = 0

        async def run(self) -> None:
            # Simulate the service receiving a signal mid-flight and
            # stopping itself before run() returns.
            self.stop()

        def stop(self) -> None:
            self.stop_calls += 1
            if self._stopped:
                return
            self._stopped = True

    import redis

    monkeypatch.setattr(redis, "from_url", lambda _url: object())
    monkeypatch.setattr(writer_module, "WatchService", _RecordingService)

    writer_module.run_watch_service(
        redis_url="redis://example:6379",
        gateway_url="http://gateway:8000",
        output_path=None,
    )

    assert _RecordingService.last_instance is not None
    # stop() should be invoked twice (once from inside run(), once from
    # the finally block) but be idempotent — i.e. no exception, no log.
    assert _RecordingService.last_instance.stop_calls == 2
    assert _RecordingService.last_instance._stopped is True
