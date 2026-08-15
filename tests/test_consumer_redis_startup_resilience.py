"""Startup must survive an upstream Redis restart, and a stall must stay visible.

On 2026-08-08 and 08-11 `data-gateway-redis` went away and every Heber service
crash-looped for 1h38m and 25m respectively, while the watchdog restarted them
into the same dead upstream every 120s. The run loop already retries Redis
forever (`EventConsumer.run`), but `connect()` runs *before* that loop, so the
process died in `xgroup_create` and never reached the retry:

    redis.exceptions.ConnectionError: Error 101 connecting to
      host.docker.internal:6379. Network is unreachable
    redis.exceptions.BusyLoadingError: Redis is loading the dataset in memory

`data-gateway-redis` takes ~77s to load its AOF, so any restart guarantees this.

Retrying startup alone would be worse than the crash-loop, though: the liveness
heartbeat is set at the *top* of every run-loop iteration, including iterations
that caught a Redis error and slept, so the container healthcheck reports
`healthy` while nothing is being consumed. The second test pins the distinction —
a separate progress gauge that only a successful XREADGROUP round-trip advances.
"""

from __future__ import annotations

import time

import pytest
import redis.exceptions

from heber.ops.metrics import consumer_last_xread_success_unixtime, consumer_loop_heartbeat_unixtime
from heber.writer.consumer import EventConsumer


@pytest.mark.unit
async def test_connect_retries_transient_redis_errors_then_succeeds(monkeypatch) -> None:
    """Startup rides out an AOF reload instead of exiting into a crash-loop."""
    consumer = EventConsumer()
    attempts = 0

    async def _flaky_xgroup_create(**_kwargs) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise redis.exceptions.ConnectionError(
                "Error 101 connecting to host.docker.internal:6379. Network is unreachable."
            )
        if attempts == 2:
            raise redis.exceptions.BusyLoadingError("Redis is loading the dataset in memory")
        return True

    async def _no_pending() -> int:
        return 0

    class _FakeRedis:
        xgroup_create = staticmethod(_flaky_xgroup_create)

    monkeypatch.setattr("heber.writer.consumer.redis.from_url", lambda _url: _FakeRedis())
    monkeypatch.setattr(consumer, "_recover_pending_messages", _no_pending)
    # Keep the test fast; the production backoff is seconds.
    monkeypatch.setattr("heber.writer.consumer.calculate_retry_delay", lambda **_kw: 0.0)

    await consumer.connect()

    assert attempts == 3, "connect() must retry transient Redis errors rather than propagate"


@pytest.mark.unit
async def test_connect_still_raises_non_transient_errors(monkeypatch) -> None:
    """A real misconfiguration must stay loud — only transient errors are retried."""
    consumer = EventConsumer()

    async def _fatal(**_kwargs) -> bool:
        raise redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")

    class _FakeRedis:
        xgroup_create = staticmethod(_fatal)

    monkeypatch.setattr("heber.writer.consumer.redis.from_url", lambda _url: _FakeRedis())
    monkeypatch.setattr("heber.writer.consumer.calculate_retry_delay", lambda **_kw: 0.0)

    with pytest.raises(redis.exceptions.ResponseError):
        await consumer.connect()


@pytest.mark.unit
async def test_idle_xread_advances_progress_gauge(monkeypatch) -> None:
    """An empty read still proves Redis is reachable, so it counts as progress."""
    consumer = EventConsumer()

    async def _empty_xreadgroup(**_kwargs) -> list:
        return []

    class _FakeRedis:
        xreadgroup = staticmethod(_empty_xreadgroup)

    consumer.redis = _FakeRedis()

    async def _no_flush(_force: bool = False) -> bool:
        return False

    monkeypatch.setattr(consumer, "_flush_layers_with_heartbeat", _no_flush)
    monkeypatch.setattr(consumer, "_should_force_flush", lambda: False)

    consumer_last_xread_success_unixtime.set(1000.0)  # long-stale
    await consumer._consume_iteration()

    assert consumer_last_xread_success_unixtime._value.get() > time.time() - 5


@pytest.mark.unit
async def test_failed_xread_leaves_progress_stale_while_heartbeat_is_fresh(monkeypatch) -> None:
    """The signal that makes a Redis stall visible.

    The healthcheck reads the heartbeat, which the run loop refreshes on every
    iteration including failed ones. Only the progress gauge distinguishes
    "spinning" from "consuming", so it must NOT advance when XREADGROUP fails.
    """
    consumer = EventConsumer()

    async def _unreachable(**_kwargs) -> list:
        raise redis.exceptions.ConnectionError("Network is unreachable")

    class _FakeRedis:
        xreadgroup = staticmethod(_unreachable)

    consumer.redis = _FakeRedis()

    consumer_last_xread_success_unixtime.set(1000.0)
    consumer_loop_heartbeat_unixtime.set(time.time())

    with pytest.raises(redis.exceptions.ConnectionError):
        await consumer._consume_iteration()

    assert consumer_last_xread_success_unixtime._value.get() == 1000.0, (
        "a failed read must not look like ingest progress"
    )
    assert consumer_loop_heartbeat_unixtime._value.get() > time.time() - 5, (
        "heartbeat stays fresh — this is exactly why it cannot detect the stall"
    )
