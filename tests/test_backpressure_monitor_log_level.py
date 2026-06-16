"""Regression test: BackpressureMonitor.monitor_loop must not emit ERROR
for defensive lag-check failures.

The monitor is a periodic, self-recovering check. A transient failure on one
tick is recovered on the next, so spamming ERROR every check_interval pollutes
the error log and drowns out real production alerts. The log line is now
WARNING; this test pins that behaviour so a casual edit can't reintroduce the
old noise.
"""

from __future__ import annotations

import asyncio

import pytest

from heber.bus import StreamName
from heber.bus import backpressure as backpressure_module
from heber.bus.backpressure import BackpressureMonitor


class _StubBus:
    """Minimal EventBus stand-in: get_pending_count always raises."""

    async def get_pending_count(self, stream: StreamName, group: str) -> int:
        raise RuntimeError("redis unavailable")


class _RecordingLogger:
    """structlog logger stand-in that records method calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []

    def _record(self, level: str, event: str, **kwargs: object) -> None:
        self.calls.append((level, event, dict(kwargs)))

    def debug(self, event: str, **kwargs: object) -> None:
        self._record("debug", event, **kwargs)

    def info(self, event: str, **kwargs: object) -> None:
        self._record("info", event, **kwargs)

    def warning(self, event: str, **kwargs: object) -> None:
        self._record("warning", event, **kwargs)

    def error(self, event: str, **kwargs: object) -> None:
        self._record("error", event, **kwargs)

    def exception(self, event: str, **kwargs: object) -> None:
        self._record("exception", event, **kwargs)


@pytest.mark.unit
def test_monitor_loop_logs_lag_check_failures_at_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When check_lag raises, the loop logs WARNING (not ERROR or exception)."""
    recorder = _RecordingLogger()
    monkeypatch.setattr(backpressure_module, "logger", recorder)

    monitor = BackpressureMonitor(
        bus=_StubBus(),
        lag_threshold_seconds=60.0,
        check_interval_seconds=0.01,
    )

    async def _run_one_tick() -> None:
        task = asyncio.create_task(
            monitor.monitor_loop([(StreamName.MARKET_BARS, "test-group")]),
        )
        await asyncio.sleep(0.05)
        monitor.stop()
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(_run_one_tick())

    lag_calls = [c for c in recorder.calls if c[1] == "lag_check_failed"]
    assert lag_calls, "expected at least one lag_check_failed log call"
    error_calls = [c for c in lag_calls if c[0] in ("error", "exception")]
    assert not error_calls, (
        f"lag_check_failed must not be logged at ERROR/exception level "
        f"(got {[c[0] for c in error_calls]}). Downgrading was intentional — "
        f"see the comment in heber/bus/backpressure.py:monitor_loop."
    )
    for level, _event, _kwargs in lag_calls:
        assert level == "warning", f"lag_check_failed must be WARNING, got {level!r}."
