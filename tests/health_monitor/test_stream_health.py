"""Tests for Tier 1 — Stream Health Check."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from heber.health_monitor.checks.stream_health import (
    PENDING_CRITICAL,
    PENDING_WARN,
    run_stream_health_checks,
)
from heber.health_monitor.models import Severity, Status
from tests.health_monitor.conftest import ET, make_check_context, make_healthy_redis

# Market-open time: Wednesday 2026-03-25 10:00 ET
MARKET_OPEN_DT = datetime(2026, 3, 25, 10, 0, tzinfo=ET)
# Market-closed time: Saturday 2026-03-28 10:00 ET
MARKET_CLOSED_DT = datetime(2026, 3, 28, 10, 0, tzinfo=ET)

# Stream-health tests use a fixed tmp dir (no Parquet I/O).
_TMP = Path("/tmp/heber_test_stream")


def _make_ctx(redis_mock: AsyncMock):
    return make_check_context(
        _TMP,
        redis=redis_mock,
        settings_overrides={
            "redis_stream_name": "heber:events",
            "redis_consumer_group": "heber-writers",
            "redis_dlq_stream_name": "heber:events:dlq",
        },
    )


def _healthy_redis() -> AsyncMock:
    return make_healthy_redis()


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_healthy_stream_all_pass(_mock_now: MagicMock) -> None:
    """All checks pass for a healthy stream during market hours."""
    redis = _healthy_redis()
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    assert len(results) == 3
    statuses = {r.check_name: r.status for r in results}
    assert statuses["stream_reachable"] == Status.PASS
    assert statuses["consumer_lag"] == Status.PASS
    assert statuses["dlq_depth"] == Status.PASS


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_stream_unreachable_p0_fail(_mock_now: MagicMock) -> None:
    """When Redis stream is unreachable, return P0 FAIL and stop early."""
    redis = AsyncMock()
    redis.xinfo_stream = AsyncMock(side_effect=ConnectionError("Connection refused"))
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    assert len(results) == 1
    r = results[0]
    assert r.check_name == "stream_reachable"
    assert r.status == Status.FAIL
    assert r.severity == Severity.P0_CRITICAL
    assert "Connection refused" in r.message


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_dlq_nonempty_warn(_mock_now: MagicMock) -> None:
    """When DLQ has messages, dlq_depth check returns WARN."""
    redis = _healthy_redis()
    redis.xlen = AsyncMock(return_value=7)
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    dlq = next(r for r in results if r.check_name == "dlq_depth")
    assert dlq.status == Status.WARN
    assert dlq.severity == Severity.P1_WARNING
    assert dlq.details["depth"] == 7


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_CLOSED_DT)
async def test_severity_suppressed_outside_market_hours(_mock_now: MagicMock) -> None:
    """Outside market hours, all severities are downgraded to P2_INFO."""
    redis = AsyncMock()
    redis.xinfo_stream = AsyncMock(side_effect=ConnectionError("down"))
    ctx = _make_ctx(redis)
    # Calendar returns P2_INFO for everything outside market hours
    ctx.calendar.adjust_severity = MagicMock(return_value=Severity.P2_INFO)

    results = await run_stream_health_checks(ctx)
    assert len(results) == 1
    assert results[0].severity == Severity.P2_INFO
    assert results[0].status == Status.FAIL  # status unchanged, only severity suppressed


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_high_pending_warn(_mock_now: MagicMock) -> None:
    """High pending message count triggers WARN."""
    redis = _healthy_redis()
    redis.xinfo_groups = AsyncMock(
        return_value=[{"name": "heber-writers", "pending": PENDING_WARN + 1, "consumers": 1}]
    )
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    lag = next(r for r in results if r.check_name == "consumer_lag")
    assert lag.status == Status.WARN
    assert lag.severity == Severity.P1_WARNING


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_critical_pending_fail(_mock_now: MagicMock) -> None:
    """Critical pending message count triggers FAIL."""
    redis = _healthy_redis()
    redis.xinfo_groups = AsyncMock(
        return_value=[{"name": "heber-writers", "pending": PENDING_CRITICAL + 1, "consumers": 1}]
    )
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    lag = next(r for r in results if r.check_name == "consumer_lag")
    assert lag.status == Status.FAIL
    assert lag.severity == Severity.P0_CRITICAL


@pytest.mark.unit
@patch("heber.health_monitor.checks.stream_health._now_et", return_value=MARKET_OPEN_DT)
async def test_missing_consumer_group_warn(_mock_now: MagicMock) -> None:
    """Missing consumer group returns WARN and skips lag check."""
    redis = _healthy_redis()
    redis.xinfo_groups = AsyncMock(return_value=[{"name": "other-group", "pending": 0}])
    ctx = _make_ctx(redis)
    results = await run_stream_health_checks(ctx)

    names = [r.check_name for r in results]
    assert "consumer_group" in names
    assert "consumer_lag" not in names
    cg = next(r for r in results if r.check_name == "consumer_group")
    assert cg.status == Status.WARN
