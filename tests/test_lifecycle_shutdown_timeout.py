"""Regression tests for lifecycle shutdown timeout status behavior."""

from __future__ import annotations

import pytest

from heber.ops.lifecycle import LifecycleManager, LifecycleState, ShutdownConfig


def test_execute_shutdown_returns_false_on_timeout() -> None:
    lifecycle = LifecycleManager(
        service_name="test-lifecycle-timeout-sync",
        config=ShutdownConfig(timeout_seconds=0, drain_poll_interval=0.0),
    )
    lifecycle._in_flight_count = 1

    success = lifecycle.execute_shutdown()

    assert success is False
    assert lifecycle.state == LifecycleState.STOPPED


@pytest.mark.asyncio
async def test_async_execute_shutdown_returns_false_on_timeout() -> None:
    lifecycle = LifecycleManager(
        service_name="test-lifecycle-timeout-async",
        config=ShutdownConfig(timeout_seconds=0, drain_poll_interval=0.0),
    )
    lifecycle._in_flight_count = 1

    success = await lifecycle.async_execute_shutdown()

    assert success is False
    assert lifecycle.state == LifecycleState.STOPPED


def test_execute_shutdown_returns_true_when_drained() -> None:
    lifecycle = LifecycleManager(
        service_name="test-lifecycle-drained",
        config=ShutdownConfig(timeout_seconds=0, drain_poll_interval=0.0),
    )
    lifecycle._in_flight_count = 0

    success = lifecycle.execute_shutdown()

    assert success is True
    assert lifecycle.state == LifecycleState.STOPPED
