"""Regression tests for lifecycle async shutdown wait behavior."""

from __future__ import annotations

import asyncio

import pytest

from heber.ops.lifecycle import LifecycleManager


@pytest.mark.asyncio
async def test_async_wait_returns_when_shutdown_already_signaled() -> None:
    lifecycle = LifecycleManager(service_name="test-lifecycle")
    lifecycle.initiate_shutdown(reason="unit_test")

    await asyncio.wait_for(lifecycle.async_wait_for_shutdown(), timeout=0.1)


@pytest.mark.asyncio
async def test_async_wait_unblocks_after_late_shutdown_signal() -> None:
    lifecycle = LifecycleManager(service_name="test-lifecycle")
    wait_task = asyncio.create_task(lifecycle.async_wait_for_shutdown())
    await asyncio.sleep(0)

    lifecycle.initiate_shutdown(reason="unit_test")

    await asyncio.wait_for(wait_task, timeout=0.1)
