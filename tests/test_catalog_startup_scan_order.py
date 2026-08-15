"""Startup coverage scan must publish freshness without blocking on verification.

The verification pass re-reads every Parquet footer. Measured on the production
mount at 11.3 files/sec, ``feed=quotes`` alone (~825k files) is ~20 hours, and
no coverage row is written until the whole 80-feed walk finishes. Running that
on the critical path meant coverage could not refresh at all for longer than
both the 6h staleness threshold and the container's uptime between restarts —
it sat frozen for 14h with the scan working the entire time.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from heber.catalog.api import _initial_coverage_scan, _verification_recount

pytestmark = pytest.mark.unit


def _seed_spy(hang_on_full: asyncio.Event | None = None):
    calls: list[bool] = []

    async def seed(_session, reuse_recorded=True):
        calls.append(reuse_recorded)
        if not reuse_recorded and hang_on_full is not None:
            await hang_on_full.wait()
        return 0

    seed.calls = calls
    return seed


@pytest.fixture
def _session():
    with patch("heber.catalog.api.async_session") as s:
        s.return_value.__aenter__ = AsyncMock(return_value=object())
        s.return_value.__aexit__ = AsyncMock(return_value=False)
        yield s


async def test_the_startup_scan_only_does_the_fast_pass(_session):
    """Coverage must become fresh in minutes, not after a 20-hour verification."""
    seed = _seed_spy()
    with patch("heber.catalog.api.seed_coverage_from_disk", seed):
        await _initial_coverage_scan()

    assert seed.calls == [True], "the startup path must not run a full recount inline"


async def test_verification_still_recounts_everything(_session):
    seed = _seed_spy()
    with patch("heber.catalog.api.seed_coverage_from_disk", seed):
        await _verification_recount()

    assert seed.calls == [False], "verification must ignore recorded counts"


async def test_a_running_verification_does_not_block_the_fast_pass(_session):
    """Enabling the integrity pass must not re-create the outage it is paired with."""
    still_running = asyncio.Event()
    seed = _seed_spy(hang_on_full=still_running)

    with patch("heber.catalog.api.seed_coverage_from_disk", seed):
        verification = asyncio.create_task(_verification_recount())
        await asyncio.sleep(0)  # let it start and block

        # The refresh has to complete while verification is still in flight.
        await asyncio.wait_for(_initial_coverage_scan(), timeout=1)

        assert True in seed.calls, "fast pass did not run while verification held"
        still_running.set()
        await verification


async def test_a_failing_pass_never_escapes(_session):
    """An exception here would kill the periodic loop that keeps coverage current."""

    async def seed(_session, reuse_recorded=True):
        raise RuntimeError("mount blipped")

    with patch("heber.catalog.api.seed_coverage_from_disk", seed):
        await _initial_coverage_scan()  # must not raise
        await _verification_recount()  # must not raise
