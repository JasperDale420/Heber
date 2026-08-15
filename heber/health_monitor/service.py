"""Health Monitor Service orchestrator.

Runs as a long-lived async service with three tiered check loops:
  - Tier 1: Stream health (every 30s)
  - Tier 2: Partition + volume (every 15min, market hours only)
  - Tier 3: Schema + statistical + ML readiness (once daily after 16:35 ET)

Run with:
    python -m heber.health_monitor
"""

from __future__ import annotations

import asyncio
import time
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from heber.config import Settings, get_settings
from heber.health_monitor.calendar import HealthCalendar
from heber.health_monitor.metrics import record_check
from heber.health_monitor.models import CheckContext, CheckResult
from heber.health_monitor.store import HealthStore
from heber.ops.notifier import DiscordNotifier
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

ET = ZoneInfo("America/New_York")


def _summarise(results: list[CheckResult]) -> dict[str, int]:
    """Return a status -> count summary dict."""
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status.value] = counts.get(r.status.value, 0) + 1
    return counts


# Check names written only by the Tier 3 sweep. Tier 1 and 2 write into the same
# dated partition all day, so completion is identified by these rather than by
# the partition merely existing.
TIER3_CHECK_NAMES = frozenset(
    {
        "schema_drift",
        "statistical_null_rate",
        "statistical_mean_shift",
        "ml_cross_sectional",
        "ml_feature_nulls",
        "ml_label_stability",
        "ml_leakage_audit",
    }
)


class HealthMonitorService:
    """Async service that runs tiered data health checks.

    Follows the GoldFeaturePoller singleton pattern:
      - Module-level singleton with start/stop helpers
      - asyncio.create_task with infinite loops per tier
      - Configurable via Pydantic Settings (HEBER_HEALTH_*)
      - Market hours gating for Tier 2
      - EOD gating for Tier 3
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        self._reader = HeberReader()
        self._calendar = HealthCalendar()
        self._store = HealthStore(data_root=self._settings.data_root)
        self._redis: Any = None
        self._ctx: CheckContext | None = None
        self._last_tier3_date: date | None = None
        # Cleared with the process; the probe below recovers it from disk once.
        self._tier3_store_checked = False
        self._notifier: DiscordNotifier | None = None
        self._last_alert_ts: datetime | None = None

    async def start(self) -> None:
        """Create Redis connection, build CheckContext, launch tier loops."""
        if self._running:
            return

        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(
            self._settings.redis_url,
            decode_responses=True,
        )

        self._ctx = CheckContext(
            settings=self._settings,
            reader=self._reader,
            redis=self._redis,
            calendar=self._calendar,
            store=self._store,
        )

        if self._settings.alert_discord_enabled:
            self._notifier = DiscordNotifier(self._settings)

        self._running = True
        self._tasks = [
            asyncio.create_task(self._tier1_loop(), name="health-tier1"),
            asyncio.create_task(self._tier2_loop(), name="health-tier2"),
            asyncio.create_task(self._tier3_loop(), name="health-tier3"),
            asyncio.create_task(self._liveness_loop(), name="health-liveness"),
        ]

        logger.info(
            "health_monitor_started",
            tier1_interval=self._settings.health_stream_check_interval_seconds,
            tier2_interval=self._settings.health_partition_check_interval_seconds,
        )

    async def stop(self) -> None:
        """Cancel all tier loops and close Redis."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []

        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None

        logger.info("health_monitor_stopped")

    # ----- Tier 1: Stream Health (every ~30s) -----

    async def _tier1_loop(self) -> None:
        """Run stream health checks at the configured interval."""
        from heber.health_monitor.checks.stream_health import run_stream_health_checks

        interval = self._settings.health_stream_check_interval_seconds
        assert self._ctx is not None

        while self._running:
            try:
                t0 = time.monotonic()
                results = await run_stream_health_checks(self._ctx)
                elapsed = time.monotonic() - t0

                self._record_and_store(results, elapsed, "tier1")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("health_tier1_error", exc_info=True)

            await asyncio.sleep(interval)

    # ----- Tier 2: Partition + Volume (every ~15min, market hours) -----

    async def _tier2_loop(self) -> None:
        """Run partition and volume checks during market hours."""
        from heber.health_monitor.checks.partition import run_partition_checks
        from heber.health_monitor.checks.volume import run_volume_checks

        interval = self._settings.health_partition_check_interval_seconds
        assert self._ctx is not None

        while self._running:
            try:
                now_et = datetime.now(ET)
                today = now_et.date()

                if self._calendar.is_trading_day(today) and self._calendar.is_market_open(now_et):
                    t0 = time.monotonic()
                    results: list[CheckResult] = []
                    results.extend(await run_partition_checks(self._ctx, today))
                    results.extend(await run_volume_checks(self._ctx, today))
                    elapsed = time.monotonic() - t0

                    self._record_and_store(results, elapsed, "tier2")
                else:
                    logger.debug("health_tier2_skipped", reason="outside_market_hours")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("health_tier2_error", exc_info=True)

            await asyncio.sleep(interval)

    # ----- Tier 3: Schema + Statistical + ML Readiness (once daily after 16:35 ET) -----

    async def _tier3_loop(self) -> None:
        """Run deep checks once per day after EOD."""
        from heber.health_monitor.checks.ml_readiness import run_ml_readiness_checks
        from heber.health_monitor.checks.schema import run_schema_checks
        from heber.health_monitor.checks.statistical import run_statistical_checks
        from heber.health_monitor.checks.volume import write_volume_baseline

        assert self._ctx is not None
        # Check every 60 seconds whether we should trigger
        check_interval = 60

        while self._running:
            try:
                if self._should_run_tier3():
                    now_et = datetime.now(ET)
                    today = now_et.date()

                    t0 = time.monotonic()
                    results: list[CheckResult] = []
                    results.extend(await run_schema_checks(self._ctx, today))
                    results.extend(await run_statistical_checks(self._ctx, today))
                    results.extend(await run_ml_readiness_checks(self._ctx, today))
                    await write_volume_baseline(self._ctx, today)
                    elapsed = time.monotonic() - t0

                    self._record_and_store(results, elapsed, "tier3")
                    self._last_tier3_date = today
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("health_tier3_error", exc_info=True)

            await asyncio.sleep(check_interval)

    def _tier3_already_ran(self, today: date) -> bool:
        """Whether a Tier 3 sweep for *today* is already on disk.

        The in-memory marker is cleared by every restart, and this service
        restarted 59 times in a week, so without consulting what was persisted
        the daily sweep begins again each time. Results are already written to a
        dated partition, so no extra marker is needed.

        Read once per process, when the in-memory marker is empty — the loop
        ticks every 60s and the store lives on the slow bind mount. If the read
        fails, the answer is "not run": repeating the sweep is wasteful, but
        skipping one because the volume blipped would lose the day's checks.
        """
        if self._tier3_store_checked:
            return False
        self._tier3_store_checked = True
        try:
            stored = self._store.read_results(today)
        except Exception:
            logger.warning("health_tier3_completion_probe_failed", exc_info=True)
            return False
        if stored is None or stored.empty or "check_name" not in stored.columns:
            return False
        ran = bool(set(stored["check_name"]) & TIER3_CHECK_NAMES)
        if ran:
            logger.info("health_tier3_already_completed_today", date=str(today))
        return ran

    def _should_run_tier3(self) -> bool:
        """Return True if Tier 3 should run now (after 16:35 ET, once per day, trading day)."""
        now_et = datetime.now(ET)
        today = now_et.date()

        if self._last_tier3_date == today:
            return False

        if self._last_tier3_date is None and self._tier3_already_ran(today):
            self._last_tier3_date = today
            return False

        if not self._calendar.is_trading_day(today):
            return False

        trigger_hour = self._settings.gold_poller_eod_hour
        trigger_minute = self._settings.gold_poller_eod_minute
        if now_et.hour < trigger_hour or (now_et.hour == trigger_hour and now_et.minute < trigger_minute):
            return False

        return True

    # ----- Liveness: per-feed critical alarm (short interval, market days) -----

    async def _liveness_loop(self) -> None:
        """Run per-feed liveness checks and route criticals to Discord."""
        from heber.health_monitor.checks.liveness import run_liveness_checks

        interval = self._settings.alert_liveness_check_interval_seconds
        assert self._ctx is not None

        while self._running:
            try:
                now_et = datetime.now(ET)
                if self._calendar.is_trading_day(now_et.date()):
                    t0 = time.monotonic()
                    results = await run_liveness_checks(self._ctx, now=now_et)
                    elapsed = time.monotonic() - t0
                    self._record_and_store(results, elapsed, "liveness")
                    await self._maybe_alert(results)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("health_liveness_error", exc_info=True)

            await asyncio.sleep(interval)

    async def _maybe_alert(self, results: list[CheckResult]) -> None:
        """Dispatch results to the Discord notifier off the event loop."""
        if self._notifier is None:
            return
        try:
            await asyncio.to_thread(self._notifier.dispatch, results)
            self._last_alert_ts = datetime.now(ET)
        except Exception:
            logger.error("health_alert_dispatch_error", exc_info=True)

    # ----- Shared helpers -----

    def _record_and_store(self, results: list[CheckResult], elapsed: float, tier: str) -> None:
        """Record metrics, persist results, log summary."""
        today = datetime.now(ET).date()

        for r in results:
            record_check(
                check_name=r.check_name,
                feed=r.feed or "",
                status=r.status.value,
                duration=elapsed / max(len(results), 1),
            )

        self._store.write_results(results, today)

        summary = _summarise(results)
        logger.info(
            "health_tier_complete",
            tier=tier,
            checks=len(results),
            elapsed_seconds=round(elapsed, 2),
            **summary,
        )

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return current monitor state for admin/health endpoints."""
        return {
            "running": self._running,
            "enabled": self._settings.health_monitor_enabled,
            "last_tier3_date": self._last_tier3_date.isoformat() if self._last_tier3_date else None,
            "tier1_interval": self._settings.health_stream_check_interval_seconds,
            "tier2_interval": self._settings.health_partition_check_interval_seconds,
            "alerting_enabled": self._notifier is not None,
            "last_alert_ts": self._last_alert_ts.isoformat() if self._last_alert_ts else None,
        }


# ---------------------------------------------------------------------------
# Module-level singleton (Data-Gateway pattern)
# ---------------------------------------------------------------------------

_monitor: HealthMonitorService | None = None


def get_health_monitor() -> HealthMonitorService | None:
    """Get the global health monitor instance."""
    return _monitor


async def start_health_monitor(settings: Settings | None = None) -> HealthMonitorService:
    """Start the global health monitor service."""
    global _monitor
    if _monitor is not None:
        return _monitor

    s = settings or get_settings()
    if not s.health_monitor_enabled:
        logger.info("health_monitor_disabled")
        _monitor = HealthMonitorService(settings=s)
        return _monitor

    _monitor = HealthMonitorService(settings=s)
    await _monitor.start()
    return _monitor


async def stop_health_monitor() -> None:
    """Stop the global health monitor service."""
    global _monitor
    if _monitor is not None:
        await _monitor.stop()
        _monitor = None


def get_health_monitor_snapshot() -> dict[str, Any] | None:
    """Get runtime telemetry from the health monitor."""
    if _monitor is None:
        return None
    return _monitor.get_runtime_snapshot()
