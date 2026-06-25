"""The /api/v1/health/summary endpoint must not block the catalog event loop.

Each day's health partition holds >1000 small parquet files on the bind mount;
a single-day read measured 50-80s. Running that on the loop stalls the
single-worker catalog API (and its background discovery task) for the whole
read. The handler now offloads the blocking read to a worker thread.
"""

from __future__ import annotations

import asyncio
import time

import pandas as pd
import pytest

import heber.health_monitor.store as store_mod
from heber.catalog.api import health_summary


@pytest.mark.unit
async def test_health_summary_does_not_block_event_loop(monkeypatch) -> None:
    blocking_seconds = 0.3

    class _SlowStore:
        def read_results(self, _day):
            time.sleep(blocking_seconds)  # simulate the slow bind-mount parquet read
            return pd.DataFrame(
                {
                    "check_name": ["liveness"],
                    "feed": ["bars"],
                    "status": ["pass"],
                    "ts_checked": [pd.Timestamp.now(tz="UTC")],
                }
            )

    # health_summary imports HealthStore lazily inside the function, so patching
    # the module attribute is picked up at call time.
    monkeypatch.setattr(store_mod, "HealthStore", _SlowStore)

    ticks = 0

    async def _pinger() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    pinger = asyncio.create_task(_pinger())
    try:
        result = await health_summary(days=1)
    finally:
        pinger.cancel()

    # Correctness preserved by the extraction.
    assert result["summary"]["total"] == 1
    assert result["summary"]["pass"] == 1
    # If the read had run on the loop, the pinger would have ticked ~0 times
    # during the 0.3s block; off-loop it ticks ~30. Generous floor for CI jitter.
    assert ticks >= 5, f"event loop was blocked during the read (only {ticks} ticks)"
