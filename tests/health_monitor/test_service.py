"""Tests for HealthMonitorService orchestrator."""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from heber.config import Settings
from heber.health_monitor.models import CheckContext, CheckResult, Severity, Status
from heber.health_monitor.service import (
    HealthMonitorService,
    start_health_monitor,
    stop_health_monitor,
)


def _make_settings(**overrides) -> Settings:
    defaults = {
        "data_root": "/tmp/heber_test_service",
        "redis_url": "redis://localhost:6380",
        "redis_stream_name": "heber:events",
        "redis_consumer_group": "heber-writers",
        "health_monitor_enabled": True,
        "health_stream_check_interval_seconds": 30,
        "health_partition_check_interval_seconds": 60,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _sample_result(name: str = "test_check", status: Status = Status.PASS) -> CheckResult:
    return CheckResult(
        check_name=name,
        feed="bars",
        severity=Severity.P1_WARNING,
        status=status,
        message="ok",
        details={},
        ts_checked=datetime.utcnow(),
    )


@pytest.mark.unit
async def test_start_uses_an_isolated_notifier_state_file(tmp_path: Path) -> None:
    settings = _make_settings(
        data_root=str(tmp_path),
        alert_discord_enabled=True,
        alert_discord_webhook_url="https://discord.invalid/test",
    )
    service = HealthMonitorService(settings=settings)
    redis = AsyncMock()

    with (
        patch("redis.asyncio.from_url", return_value=redis),
        patch("heber.health_monitor.service.DiscordNotifier") as notifier,
    ):
        await service.start()
        notifier.assert_called_once_with(
            settings,
            state_path=tmp_path / "ops" / "alerts" / "health-monitor-state.json",
        )
        await service.stop()


# ---------------------------------------------------------------------------
# Test 1: Service starts and stops cleanly
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_service_start_stop() -> None:
    """Service starts three tier loops and stops cleanly."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_redis = AsyncMock()
    svc._redis = mock_redis
    svc._ctx = CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=mock_redis,
        calendar=MagicMock(),
        store=MagicMock(),
    )
    svc._running = True
    svc._tasks = [
        asyncio.create_task(_noop_loop(), name="health-tier1"),
        asyncio.create_task(_noop_loop(), name="health-tier2"),
        asyncio.create_task(_noop_loop(), name="health-tier3"),
    ]

    assert svc._running is True
    assert len(svc._tasks) == 3

    await svc.stop()

    assert svc._running is False
    assert len(svc._tasks) == 0


async def _noop_loop() -> None:
    """Dummy coroutine that sleeps forever (cancelled by stop)."""
    while True:
        await asyncio.sleep(100)


# ---------------------------------------------------------------------------
# Test 2: Tier 1 loop runs and produces results
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_tier1_runs_checks() -> None:
    """Tier 1 loop executes stream health checks and stores results."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_store = MagicMock()
    mock_calendar = MagicMock()
    mock_redis = AsyncMock()

    sample = [_sample_result("stream_reachable")]

    svc._running = True
    svc._ctx = CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=mock_redis,
        calendar=mock_calendar,
        store=mock_store,
    )
    svc._store = mock_store

    with patch(
        "heber.health_monitor.checks.stream_health.run_stream_health_checks",
        new_callable=AsyncMock,
        return_value=sample,
    ):
        # Run _tier1_loop for just one iteration by stopping after it sleeps
        task = asyncio.create_task(svc._tier1_loop())
        await asyncio.sleep(0.1)
        svc._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Store should have been called with the results
    mock_store.write_results.assert_called_once()
    written_results = mock_store.write_results.call_args[0][0]
    assert len(written_results) == 1
    assert written_results[0].check_name == "stream_reachable"


# ---------------------------------------------------------------------------
# Test 3: Tier 2 loop skips outside market hours
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_tier2_skips_outside_market_hours() -> None:
    """Tier 2 loop does not run checks when market is closed."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_store = MagicMock()
    mock_calendar = MagicMock()
    mock_calendar.is_trading_day.return_value = False
    mock_calendar.is_market_open.return_value = False
    mock_redis = AsyncMock()

    svc._running = True
    svc._ctx = CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=mock_redis,
        calendar=mock_calendar,
        store=mock_store,
    )
    svc._store = mock_store
    svc._calendar = mock_calendar

    with (
        patch(
            "heber.health_monitor.checks.partition.run_partition_checks",
            new_callable=AsyncMock,
        ) as mock_partition,
        patch(
            "heber.health_monitor.checks.volume.run_volume_checks",
            new_callable=AsyncMock,
        ) as mock_volume,
    ):
        task = asyncio.create_task(svc._tier2_loop())
        await asyncio.sleep(0.1)
        svc._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Neither check function should be called
    mock_partition.assert_not_called()
    mock_volume.assert_not_called()
    mock_store.write_results.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4: Error in one tier does not crash others
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_tier_error_isolation() -> None:
    """An exception in Tier 1 does not prevent the loop from continuing."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_store = MagicMock()

    svc._running = True
    svc._ctx = CheckContext(
        settings=settings,
        reader=MagicMock(),
        redis=AsyncMock(),
        calendar=MagicMock(),
        store=mock_store,
    )
    svc._store = mock_store

    call_count = 0

    async def _failing_checks(ctx):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("simulated check failure")
        return [_sample_result()]

    with patch(
        "heber.health_monitor.checks.stream_health.run_stream_health_checks",
        side_effect=_failing_checks,
    ):
        # Monkey-patch the interval attribute to be very short for fast iteration
        object.__setattr__(svc._settings, "health_stream_check_interval_seconds", 0)
        task = asyncio.create_task(svc._tier1_loop())
        await asyncio.sleep(0.3)
        svc._running = False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Should have called the check more than once (survived the error)
    assert call_count >= 2


# ---------------------------------------------------------------------------
# Test 5: Results are stored after each tier run
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_results_stored_with_metrics() -> None:
    """_record_and_store persists results and records Prometheus metrics."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_store = MagicMock()
    svc._store = mock_store

    results = [
        _sample_result("check_a", Status.PASS),
        _sample_result("check_b", Status.FAIL),
    ]

    with patch("heber.health_monitor.service.record_check") as mock_record:
        svc._record_and_store(results, 1.5, "tier1")

    assert mock_record.call_count == 2
    mock_store.write_results.assert_called_once()
    written = mock_store.write_results.call_args[0][0]
    assert len(written) == 2


# ---------------------------------------------------------------------------
# Test 6: Module-level start/stop singleton
# ---------------------------------------------------------------------------
@pytest.mark.unit
async def test_module_singleton_start_stop() -> None:
    """start_health_monitor / stop_health_monitor manage a singleton."""
    import heber.health_monitor.service as svc_mod

    settings = _make_settings(health_monitor_enabled=False)

    # Reset global state
    svc_mod._monitor = None

    monitor = await start_health_monitor(settings)
    assert monitor is not None
    assert svc_mod._monitor is monitor

    # Calling start again returns the same instance
    same = await start_health_monitor(settings)
    assert same is monitor

    await stop_health_monitor()
    assert svc_mod._monitor is None


# ---------------------------------------------------------------------------
# Test 7: Tier 3 _should_run_tier3 gating
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_should_run_tier3_already_ran_today() -> None:
    """_should_run_tier3 returns False if already ran today."""
    settings = _make_settings()
    svc = HealthMonitorService(settings=settings)

    mock_calendar = MagicMock()
    mock_calendar.is_trading_day.return_value = True
    svc._calendar = mock_calendar

    today = date.today()
    svc._last_tier3_date = today

    with patch("heber.health_monitor.service.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(today.year, today.month, today.day, 17, 0)
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
        # It should return False since we already ran today
        assert svc._should_run_tier3() is False
