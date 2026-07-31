"""Gold Feature Poller service.

Runs as a long-lived async service alongside heber-watch.  After each trading
day closes it executes every feature pipeline that feeds Orion model training,
writing results to the Gold layer.

Pattern follows Data-Gateway pollers: asyncio.create_task with infinite loop,
configurable via HEBER_GOLD_POLLER_* env vars, market-hours gating via
exchange_calendars.

Run with:
    python -m heber.gold_poller
"""

from __future__ import annotations

import asyncio
import errno
import multiprocessing
import queue
import threading
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

import structlog

from heber.config import Settings, get_settings
from heber.gold_poller.reconcile import reconcile_eod_feeds
from heber.ops.metrics import gold_poller_pipeline_outcomes_total
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

_SCHEDULE_DISABLED_PIPELINES = frozenset({"excursion_analytics"})


class _TerminalPipelineError(RuntimeError):
    """A pipeline failure that cannot improve through retry."""


class _TransientPipelineError(OSError):
    """An explicitly transient I/O failure eligible for retry."""


class _PipelineTimeoutError(TimeoutError):
    """The scheduler-enforced pipeline deadline, which is never retryable."""


_TRANSIENT_IO_ERRNOS = frozenset(
    {
        errno.EAGAIN,
        errno.EINTR,
        errno.ETIMEDOUT,
        errno.ECONNABORTED,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ENETDOWN,
        errno.ENETRESET,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
    }
)


def _is_transient_io_error(exc: OSError) -> bool:
    """Return whether a failed operation may succeed on a later attempt."""
    return isinstance(exc, _TransientPipelineError) or exc.errno in _TRANSIENT_IO_ERRNOS


def _remaining_deadline_seconds(deadline: float) -> float:
    """Return non-negative time remaining before an absolute monotonic deadline."""
    return max(0.0, deadline - time.monotonic())


def _stop_process_by_deadline(process: Any, deadline: float) -> bool:
    """Try to terminate and reap a child without waiting past ``deadline``."""
    if not process.is_alive():
        process.join(0)
        return True

    process.terminate()
    remaining = _remaining_deadline_seconds(deadline)
    if remaining:
        process.join(remaining)

    if process.is_alive():
        process.kill()
        remaining = _remaining_deadline_seconds(deadline)
        if remaining:
            process.join(remaining)
    if process.is_alive():
        return False
    process.join(0)
    return True


class _DeadlineProcessStart:
    """Start a process without allowing ``Process.start()`` to overrun a deadline."""

    def __init__(self, process: Any) -> None:
        self._process = process
        self._started = threading.Event()
        self._decision = threading.Event()
        self._cancelled = threading.Event()
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._launch, daemon=True)

    def _launch(self) -> None:
        try:
            self._process.start()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the scheduler thread
            self._error = exc
            self._started.set()
            return

        self._started.set()
        self._decision.wait()
        if not self._cancelled.is_set():
            return

        try:
            self._process.terminate()
        except Exception:
            logger.error("gold_poller_late_child_terminate_failed", exc_info=True)
        try:
            if self._process.is_alive():
                self._process.kill()
            self._process.join(None)
        except Exception:
            logger.error("gold_poller_late_child_reap_failed", exc_info=True)

    def start_by(self, deadline: float) -> bool:
        """Return false when launch misses the deadline; late children self-terminate."""
        self._thread.start()
        started = self._started.wait(_remaining_deadline_seconds(deadline))
        if not started:
            self._cancelled.set()
            self._decision.set()
            return False
        self._decision.set()
        if self._error is not None:
            raise self._error
        return True

    def is_alive(self) -> bool:
        """Report cleanup progress through the existing unreaped-child fence."""
        return self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)


# ---------------------------------------------------------------------------
# Pipeline registry — maps a human-readable name to its pipeline class import
# path and the datasets tuple it should run.  We use lazy imports so the
# service module itself stays lightweight.
# ---------------------------------------------------------------------------

PIPELINE_REGISTRY: list[dict[str, Any]] = [
    {
        "name": "equity_features",
        "module": "heber.features.pipelines.equity_features",
        "class": "EquityFeaturePipeline",
        "datasets": ("flow", "momentum", "volatility", "returns_1d", "returns_5d"),
        "gold_datasets": [
            "flow_features",
            "momentum_features",
            "volatility_features",
            "labels_returns_1d",
            "labels_returns_5d",
        ],
        "silver_sources": ("bars", "quotes", "flow_alerts"),
    },
    {
        "name": "gex_regime",
        "module": "heber.features.pipelines.gex_regime_features",
        "class": "GexRegimePipeline",
        "datasets": None,
        "gold_datasets": ["gex_regime_features"],
        "silver_sources": ("greek_exposure",),
    },
    {
        "name": "darkpool",
        "module": "heber.features.pipelines.darkpool_features",
        "class": "DarkpoolPipeline",
        "datasets": None,
        "gold_datasets": ["darkpool_features"],
        "silver_sources": ("darkpool", "flow_alerts"),
    },
    {
        "name": "flow_toxicity",
        "module": "heber.features.pipelines.flow_toxicity_features",
        "class": "FlowToxicityPipeline",
        "datasets": None,
        "gold_datasets": ["flow_toxicity_features"],
        "silver_sources": ("flow_alerts",),
    },
    {
        "name": "market_intel",
        "module": "heber.features.pipelines.market_intel_features",
        "class": "MarketIntelPipeline",
        # Explicitly exclude "darkpool" — the dedicated darkpool pipeline owns
        # darkpool_features with the per-ticker schema (darkpool_notional_1d /
        # darkpool_premium_ratio / darkpool_activity_zscore). Letting MarketIntel
        # default to ALL_DATASETS double-writes the same partition with an
        # incompatible schema (total_volume / total_notional / pct_above_nbbo)
        # which downstream readers then merge into a frame missing the columns
        # consumers actually configured against.
        "datasets": ("greek_exposure", "options_sentiment", "ftd"),
        "gold_datasets": [
            "greek_exposure_features",
            "options_sentiment_features",
            "ftd_features",
        ],
        "silver_sources": ("greek_exposure", "iv_rank", "market_tide", "sector_tide", "ftd"),
    },
    {
        "name": "market_tide_context",
        "module": "heber.features.pipelines.market_tide_context_features",
        "class": "MarketTideContextPipeline",
        "datasets": None,
        "gold_datasets": ["market_tide_context_features"],
        "silver_sources": ("market_tide",),
    },
    {
        "name": "sector_flow",
        "module": "heber.features.pipelines.sector_flow_features",
        "class": "SectorFlowPipeline",
        "datasets": None,
        "gold_datasets": ["sector_flow_features"],
        "silver_sources": ("flow_alerts", "sector_tide"),
    },
    {
        "name": "oi_momentum",
        "module": "heber.features.pipelines.oi_momentum_features",
        "class": "OiMomentumPipeline",
        "datasets": None,
        "gold_datasets": ["oi_momentum_features"],
        "silver_sources": ("oi_change",),
    },
    {
        "name": "straddle_momentum",
        "module": "heber.features.pipelines.straddle_momentum_features",
        "class": "StraddleMomentumPipeline",
        "datasets": None,
        "gold_datasets": ["straddle_momentum_features"],
        "silver_sources": ("option_chain_snapshot",),
    },
    {
        "name": "trend_scan",
        "module": "heber.features.pipelines.trend_scan_features",
        "class": "TrendScanPipeline",
        "datasets": None,
        "gold_datasets": ["trend_scan_features"],
        "silver_sources": ("bars",),
    },
    {
        "name": "flow_context",
        "module": "heber.features.pipelines.flow_context_features",
        "class": "FlowContextPipeline",
        "datasets": None,
        "gold_datasets": ["flow_context_features"],
        "silver_sources": ("flow_alerts",),
    },
    {
        "name": "market_regime",
        "module": "heber.features.pipelines.market_regime_features",
        "class": "MarketRegimePipeline",
        "datasets": None,
        "gold_datasets": ["market_regime_features"],
        "silver_sources": ("bars", "quotes", "treasury_yields"),
        "gold_sources": ({"dataset": "momentum_features", "project_from_settings": True},),
    },
    {
        "name": "iv_surface",
        "module": "heber.features.pipelines.iv_surface_features",
        "class": "IVSurfacePipeline",
        "datasets": None,
        "gold_datasets": ["iv_surface_features"],
        "silver_sources": ("iv_term_structure",),
    },
    {
        "name": "flow_normalization",
        "module": "heber.features.pipelines.flow_normalization_features",
        "class": "FlowNormalizationPipeline",
        "datasets": None,
        "gold_datasets": ["flow_normalization_features"],
        "silver_sources": ("flow_alerts",),
    },
    {
        "name": "ticker_base_rates",
        "module": "heber.features.pipelines.ticker_base_rates",
        "class": "TickerBaseRatesPipeline",
        "datasets": None,
        "gold_datasets": ["ticker_base_rates"],
        "gold_sources": ({"dataset": "labels_alert_barriers", "project": "watch"},),
    },
    {
        "name": "excursion_analytics",
        "module": "heber.features.pipelines.excursion_analytics",
        "class": "ExcursionAnalyticsPipeline",
        "datasets": None,
        "gold_datasets": ["excursion_analytics"],
    },
]


def _is_nyse_trading_day(dt: date) -> bool:
    """Check if a date is a NYSE trading day (weekday, not a US market holiday)."""
    try:
        import exchange_calendars as xcals

        nyse = xcals.get_calendar("XNYS")
        return nyse.is_session(dt)
    except Exception:
        # Fallback: weekdays only (no holiday awareness)
        return dt.weekday() < 5


def _instantiate_pipeline(entry: dict[str, Any], settings: Settings) -> Any:
    """Lazy-import and instantiate a pipeline class."""
    import importlib

    mod = importlib.import_module(entry["module"])
    cls = getattr(mod, entry["class"])
    return cls(
        project=settings.gold_poller_project,
        version=settings.gold_poller_version,
    )


def _compute_pipeline(entry: dict[str, Any], start_date: date, end_date: date) -> dict[str, Any]:
    """Instantiate and run a single pipeline synchronously.

    Module-level (not a method) so it can run inside an isolated ``spawn``
    subprocess, which re-imports this module and reconstructs settings.
    """
    settings = get_settings()
    pipeline = _instantiate_pipeline(entry, settings)

    kwargs: dict[str, Any] = {
        "start_date": datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC),
        "end_date": datetime(end_date.year, end_date.month, end_date.day, tzinfo=UTC),
    }
    if entry["datasets"] is not None:
        kwargs["datasets"] = entry["datasets"]

    return pipeline.run(**kwargs)


def _capture_source_partitions(
    reader: HeberReader,
    pipelines: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    poller_project: str,
) -> dict[str, dict[str, tuple[str, ...]]]:
    """Snapshot completed physical source partitions for one Gold run."""
    time_range = (
        datetime(start_date.year, start_date.month, start_date.day, tzinfo=UTC),
        datetime(end_date.year, end_date.month, end_date.day, tzinfo=UTC),
    )
    snapshots: dict[str, dict[str, tuple[str, ...]]] = {}
    for entry in pipelines:
        sources = {
            source: tuple(sorted(reader.completed_silver_partitions(source, time_range)))
            for source in entry.get("silver_sources", ())
        }
        for source in entry.get("gold_sources", ()):
            dataset = source["dataset"]
            project = poller_project if source.get("project_from_settings") else source.get("project")
            sources[f"gold:{dataset}"] = tuple(
                sorted(
                    reader.completed_gold_partitions(
                        dataset,
                        project=project,
                        version=source.get("version"),
                        time_range=time_range,
                    )
                )
            )
        snapshots[entry["name"]] = sources
    return snapshots


def _pipeline_subprocess_entry(
    entry: dict[str, Any],
    start_date: date,
    end_date: date,
    result_q: Any,
) -> None:
    """Child-process entrypoint for an isolated pipeline run.

    A hard crash here (OOM SIGKILL, segfault) is contained to this process and
    leaves no result on the queue — the parent detects the missing payload. Any
    Python-level error is reported back so the parent can log and continue.
    """
    try:
        result_q.put({"ok": True, "stats": _compute_pipeline(entry, start_date, end_date)})
    except BaseException as exc:  # noqa: BLE001 - report every failure to the parent
        error: dict[str, Any] = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "error_type": type(exc).__name__,
        }
        if isinstance(exc, OSError):
            error["errno"] = exc.errno
        result_q.put(error)


def _source_discovery_subprocess_entry(
    pipelines: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    poller_project: str,
    result_q: Any,
) -> None:
    """Child-process entrypoint for a killable source-partition snapshot."""
    try:
        sources = _capture_source_partitions(HeberReader(), pipelines, start_date, end_date, poller_project)
        result_q.put({"ok": True, "sources": sources})
    except BaseException as exc:  # noqa: BLE001 - report every failure to the parent
        result_q.put({"ok": False, "error": f"{type(exc).__name__}: {exc}"})


class GoldFeaturePoller:
    """Async service that refreshes Gold feature datasets after market close.

    Follows the Data-Gateway poller pattern:
      - Module-level singleton with start/stop helpers
      - asyncio.create_task with infinite _poll_loop
      - Configurable via Pydantic Settings (HEBER_GOLD_POLLER_*)
      - Market hours gating (only triggers after EOD)
      - Per-pipeline retry with exponential backoff
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._reader = HeberReader()
        self._last_run_date: date | None = None
        self._last_reconcile_date: date | None = None
        self._run_history: list[dict[str, Any]] = []
        self._unreaped_children: list[tuple[str, Any]] = []

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        disabled = self._settings.gold_poller_disabled_pipeline_set | _SCHEDULE_DISABLED_PIPELINES
        logger.info(
            "gold_poller_started",
            eod_trigger=f"{self._settings.gold_poller_eod_hour:02d}:{self._settings.gold_poller_eod_minute:02d} ET",
            check_interval=self._settings.gold_poller_check_interval_seconds,
            disabled=sorted(disabled),
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("gold_poller_stopped")

    # ----- Main loop -----

    async def _poll_loop(self) -> None:
        """Check every interval if an EOD run is due."""
        # Small startup delay so other services can initialize first
        await asyncio.sleep(10)

        while self._running:
            try:
                if self._should_run():
                    await self._run_all_pipelines()
                if self._should_reconcile():
                    await self._run_reconcile()
            except Exception:
                logger.error("gold_poller_loop_error", exc_info=True)

            await asyncio.sleep(self._settings.gold_poller_check_interval_seconds)

    def _should_run(self) -> bool:
        """Return True if we haven't yet run for the current trading day and
        the EOD trigger time has passed."""
        et_now = datetime.now(ZoneInfo("America/New_York"))
        today = et_now.date()

        # Already ran for today
        if self._last_run_date == today:
            return False

        # Not a trading day
        if not _is_nyse_trading_day(today):
            return False

        # Before the trigger time
        trigger_hour = self._settings.gold_poller_eod_hour
        trigger_minute = self._settings.gold_poller_eod_minute
        if et_now.hour < trigger_hour or (et_now.hour == trigger_hour and et_now.minute < trigger_minute):
            return False

        return True

    def _should_reconcile(self) -> bool:
        """Return True if the EOD self-heal reconcile is due (enabled, trading day,
        past its trigger time, not yet run today)."""
        if not self._settings.eod_reconcile_enabled:
            return False
        et_now = datetime.now(ZoneInfo("America/New_York"))
        today = et_now.date()
        if self._last_reconcile_date == today:
            return False
        if not _is_nyse_trading_day(today):
            return False
        hour = self._settings.eod_reconcile_hour
        minute = self._settings.eod_reconcile_minute
        if et_now.hour < hour or (et_now.hour == hour and et_now.minute < minute):
            return False
        return True

    async def _run_reconcile(self) -> None:
        """Re-pull any daily UW feed missing today's Silver via the gateway backfill.

        Marks today done even on failure so a persistent gateway/credential error
        does not retry-storm every check interval — the next attempt is tomorrow,
        and the liveness alarm still surfaces a genuinely missing feed today.
        """
        today = datetime.now(ZoneInfo("America/New_York")).date()
        try:
            await reconcile_eod_feeds(self._settings, self._reader, today=today)
        except Exception:
            logger.error("gold_poller_reconcile_error", exc_info=True)
        finally:
            self._last_reconcile_date = today

    # ----- Pipeline execution -----

    def _track_unreaped_child(self, child_name: str, process: Any) -> None:
        """Fence new subprocesses until a deadline-overrun child is reaped."""
        if any(tracked_process is process for _, tracked_process in self._unreaped_children):
            return
        self._unreaped_children.append((child_name, process))
        logger.critical("gold_poller_unreaped_child_fenced", child=child_name)

    def _reap_unreaped_children(self) -> bool:
        """Non-blockingly reap deferred children and report whether the fence is clear."""
        still_running: list[tuple[str, Any]] = []
        for child_name, process in self._unreaped_children:
            try:
                if process.is_alive():
                    still_running.append((child_name, process))
                    continue
                process.join(0)
                logger.warning("gold_poller_unreaped_child_reaped", child=child_name)
            except Exception:
                logger.error("gold_poller_unreaped_child_recheck_failed", child=child_name, exc_info=True)
                still_running.append((child_name, process))
        self._unreaped_children = still_running
        return not still_running

    def _raise_if_unreaped_children(self) -> None:
        """Prevent new work while a deadline-overrun child has not exited."""
        if not self._reap_unreaped_children():
            raise _TerminalPipelineError("Gold scheduler is fenced by an unreaped child process")

    def _stop_or_defer_child_reap(self, child_name: str, process: Any, deadline: float) -> bool:
        """Stop a child within its deadline or retain a fence for later nonblocking reaping."""
        try:
            stopped = _stop_process_by_deadline(process, deadline)
        except Exception:
            self._track_unreaped_child(child_name, process)
            logger.error("gold_poller_child_stop_failed", child=child_name, exc_info=True)
            return False
        if not stopped:
            self._track_unreaped_child(child_name, process)
        return stopped

    async def _run_all_pipelines(self) -> None:
        """Execute all registered pipelines for the lookback window."""
        settings = self._settings
        # _should_run() has already validated that today is a trading day whose
        # configured EOD trigger time has passed, so the just-closed session is
        # today. (Using the old _last_trading_day() helper here gated on a
        # hardcoded 17:00 ET and returned *yesterday* for the default 16:35
        # trigger — and because _last_run_date is then set to today, the 17:00
        # re-run never happened, leaving the lakehouse one trading day stale.)
        today = datetime.now(ZoneInfo("America/New_York")).date()
        lookback = settings.gold_poller_lookback_days
        start_date = today - timedelta(days=lookback)
        end_date = today

        disabled = settings.gold_poller_disabled_pipeline_set | _SCHEDULE_DISABLED_PIPELINES
        pipelines = list(PIPELINE_REGISTRY)
        enabled_pipeline_count = sum(1 for entry in pipelines if entry["name"] not in disabled)
        run_start = time.monotonic()
        run_deadline = run_start + settings.gold_poller_run_budget_seconds

        logger.info(
            "gold_poller_run_start",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            pipeline_count=enabled_pipeline_count,
            disabled=sorted(disabled),
            run_budget_seconds=settings.gold_poller_run_budget_seconds,
        )

        results: dict[str, dict[str, Any]] = {}
        if not self._reap_unreaped_children():
            for entry in pipelines:
                name = entry["name"]
                if name in disabled:
                    results[name] = {"status": "disabled", "reason": "scheduled_disabled", "attempts": 0}
                else:
                    results[name] = {
                        "status": "error",
                        "reason": "unreaped_child_fence",
                        "error": "Gold scheduler is fenced by an unreaped child process",
                        "attempts": 0,
                        "terminal": True,
                    }
        else:
            try:
                expected_source_partitions = await self._capture_expected_source_partitions_by_deadline(
                    pipelines,
                    start_date,
                    end_date,
                    run_deadline,
                )
            except _PipelineTimeoutError as exc:
                for entry in pipelines:
                    name = entry["name"]
                    if name in disabled:
                        results[name] = {"status": "disabled", "reason": "scheduled_disabled", "attempts": 0}
                    else:
                        results[name] = {
                            "status": "error",
                            "reason": "source_discovery_timeout",
                            "error": str(exc),
                            "attempts": 0,
                            "terminal": True,
                        }
            except Exception as exc:
                logger.error("gold_poller_source_discovery_error", exc_info=True)
                for entry in pipelines:
                    name = entry["name"]
                    if name in disabled:
                        results[name] = {"status": "disabled", "reason": "scheduled_disabled", "attempts": 0}
                    else:
                        results[name] = {
                            "status": "error",
                            "reason": "source_discovery_error",
                            "error": str(exc),
                            "attempts": 0,
                            "terminal": True,
                        }
            else:
                for entry in pipelines:
                    name = entry["name"]
                    if name in disabled:
                        results[name] = {"status": "disabled", "reason": "scheduled_disabled", "attempts": 0}
                    elif time.monotonic() >= run_deadline:
                        results[name] = {
                            "status": "error",
                            "reason": "run_budget_exhausted",
                            "attempts": 0,
                            "terminal": True,
                        }
                    else:
                        results[name] = await self._run_pipeline(
                            entry,
                            start_date,
                            end_date,
                            expected_source_partitions=expected_source_partitions[name],
                            run_deadline=run_deadline,
                        )

        for name, result in results.items():
            gold_poller_pipeline_outcomes_total.labels(pipeline=name, status=result["status"]).inc()

        elapsed = time.monotonic() - run_start
        success_count = sum(1 for r in results.values() if r["status"] == "success")
        error_count = sum(1 for r in results.values() if r["status"] == "error")
        no_data_count = sum(1 for r in results.values() if r["status"] == "no_data")
        disabled_count = sum(1 for r in results.values() if r["status"] == "disabled")

        self._last_run_date = end_date

        run_summary = {
            "date": end_date.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "pipelines": len(pipelines),
            "success": success_count,
            "errors": error_count,
            "no_data": no_data_count,
            "disabled": disabled_count,
            "run_budget_seconds": settings.gold_poller_run_budget_seconds,
            "budget_exhausted": elapsed >= settings.gold_poller_run_budget_seconds,
            "results": results,
        }
        self._run_history.append(run_summary)
        # Keep last 7 run records
        if len(self._run_history) > 7:
            self._run_history = self._run_history[-7:]

        logger.info(
            "gold_poller_run_complete",
            date=end_date.isoformat(),
            elapsed_seconds=round(elapsed, 1),
            success=success_count,
            errors=error_count,
            no_data=no_data_count,
            disabled=disabled_count,
            run_budget_seconds=settings.gold_poller_run_budget_seconds,
        )

        # Log individual failures for visibility — name the Gold datasets left
        # stale so the alert points straight at the affected downstream data.
        datasets_by_name = {p["name"]: p.get("gold_datasets", []) for p in pipelines}
        for name, result in results.items():
            if result["status"] == "error":
                logger.error(
                    "gold_poller_pipeline_failed",
                    pipeline=name,
                    gold_datasets=datasets_by_name.get(name, []),
                    error=result.get("error", "unknown"),
                    attempts=result.get("attempts", 0),
                )

    def _capture_expected_source_partitions(
        self,
        pipelines: list[dict[str, Any]],
        start_date: date,
        end_date: date,
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        """Snapshot completed physical source partitions once before the run."""
        return _capture_source_partitions(
            self._reader,
            pipelines,
            start_date,
            end_date,
            getattr(self._settings, "gold_poller_project", "watch"),
        )

    async def _capture_expected_source_partitions_by_deadline(
        self,
        pipelines: list[dict[str, Any]],
        start_date: date,
        end_date: date,
        run_deadline: float,
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        """Await a killable source snapshot without extending the scheduled run."""
        return await asyncio.to_thread(
            self._execute_source_discovery_isolated,
            pipelines,
            start_date,
            end_date,
            run_deadline=run_deadline,
        )

    def _execute_source_discovery_isolated(
        self,
        pipelines: list[dict[str, Any]],
        start_date: date,
        end_date: date,
        *,
        run_deadline: float,
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        """Capture source partitions in a spawned child bounded by ``run_deadline``."""
        self._raise_if_unreaped_children()
        if _remaining_deadline_seconds(run_deadline) <= 0:
            raise _PipelineTimeoutError("Gold run budget exhausted before source discovery")

        ctx = multiprocessing.get_context("spawn")
        result_q: Any = ctx.Queue()
        proc = ctx.Process(
            target=_source_discovery_subprocess_entry,
            args=(
                pipelines,
                start_date,
                end_date,
                getattr(self._settings, "gold_poller_project", "watch"),
                result_q,
            ),
            name="gold-source-discovery",
        )
        process_start = _DeadlineProcessStart(proc)
        if not process_start.start_by(run_deadline):
            self._track_unreaped_child("source_discovery:start", process_start)
            raise _PipelineTimeoutError("Gold source discovery process start exceeded the run budget")

        payload: dict[str, Any] | None = None
        timed_out = False
        while payload is None:
            remaining = _remaining_deadline_seconds(run_deadline)
            if remaining <= 0:
                timed_out = True
                break
            try:
                payload = result_q.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if not proc.is_alive():
                    try:
                        payload = result_q.get_nowait()
                    except queue.Empty:
                        break

        if timed_out:
            stopped = self._stop_or_defer_child_reap("source_discovery", proc, run_deadline)
            if not stopped:
                logger.error("gold_poller_source_discovery_reap_deadline_exceeded")
            raise _PipelineTimeoutError("Gold source discovery exceeded the run budget")

        if proc.is_alive():
            stopped = self._stop_or_defer_child_reap("source_discovery", proc, run_deadline)
            if not stopped:
                raise _PipelineTimeoutError("Gold source discovery subprocess could not be reaped before its deadline")
        else:
            proc.join(0)

        if payload is None:
            raise _TerminalPipelineError(
                f"Gold source discovery subprocess died without a result (exitcode={proc.exitcode})"
            )
        if not payload.get("ok"):
            raise _TerminalPipelineError(f"Gold source discovery failed: {payload.get('error')}")
        sources = payload.get("sources")
        if not isinstance(sources, dict):
            raise _TerminalPipelineError("Gold source discovery returned an invalid source-partition snapshot")
        return cast(dict[str, dict[str, tuple[str, ...]]], sources)

    @staticmethod
    def _classify_pipeline_stats(
        entry: dict[str, Any],
        stats: dict[str, Any],
        expected_source_partitions: dict[str, tuple[str, ...]],
    ) -> dict[str, Any]:
        """Reduce nested pipeline results to one explicit scheduler state."""
        if "status" in stats:
            raise _TerminalPipelineError(
                f"pipeline '{entry['name']}' returned a flat result; nested dataset results are required"
            )

        datasets = [value for value in stats.values() if isinstance(value, dict)]
        if not datasets:
            raise _TerminalPipelineError(f"pipeline '{entry['name']}' returned no dataset results")

        total_rows = sum(int(dataset.get("rows", 0)) for dataset in datasets)
        dataset_statuses = {str(dataset.get("status", "error")) for dataset in datasets}
        expected = {source: dates for source, dates in expected_source_partitions.items() if dates}

        if "error" in dataset_statuses:
            return {"status": "error", "rows": total_rows, "datasets": stats, "expected_source_partitions": expected}
        if total_rows == 0 and expected:
            return {
                "status": "error",
                "rows": 0,
                "datasets": stats,
                "expected_source_partitions": expected,
                "error": "completed Silver source partitions produced zero Gold rows",
            }
        if total_rows == 0:
            return {"status": "no_data", "rows": 0, "datasets": stats, "expected_source_partitions": expected}
        return {"status": "success", "rows": total_rows, "datasets": stats, "expected_source_partitions": expected}

    async def _run_pipeline(
        self,
        entry: dict[str, Any],
        start_date: date,
        end_date: date,
        *,
        expected_source_partitions: dict[str, tuple[str, ...]],
        run_deadline: float,
    ) -> dict[str, Any]:
        """Run a single pipeline with retry logic."""
        settings = self._settings
        max_retries = settings.gold_poller_retry_max
        backoff_base = settings.gold_poller_retry_backoff_seconds

        for attempt in range(1, max_retries + 1):
            if time.monotonic() >= run_deadline:
                return {
                    "status": "error",
                    "attempts": attempt - 1,
                    "error": "run_budget_exhausted",
                    "terminal": True,
                }
            if not self._reap_unreaped_children():
                return {
                    "status": "error",
                    "attempts": attempt - 1,
                    "error": "Gold scheduler is fenced by an unreaped child process",
                    "reason": "unreaped_child_fence",
                    "terminal": True,
                }
            try:
                logger.info(
                    "gold_poller_pipeline_start",
                    pipeline=entry["name"],
                    attempt=attempt,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                )

                # Run the pipeline in an isolated subprocess (awaited via a thread
                # so the event loop stays responsive). Isolation means a hard crash
                # — e.g. an OOM SIGKILL the try/except cannot catch — kills only the
                # child and surfaces here as an exception, instead of killing the
                # poller and starving every pipeline scheduled after this one.
                stats = await asyncio.to_thread(
                    self._execute_pipeline_isolated,
                    entry,
                    start_date,
                    end_date,
                    max_runtime_seconds=run_deadline - time.monotonic(),
                )

                result = self._classify_pipeline_stats(entry, stats, expected_source_partitions)

                logger.info(
                    "gold_poller_pipeline_done",
                    pipeline=entry["name"],
                    total_rows=result["rows"],
                    datasets=list(stats.keys()),
                    status=result["status"],
                )
                return {**result, "attempts": attempt}

            except (_PipelineTimeoutError, _TerminalPipelineError) as exc:
                logger.warning(
                    "gold_poller_pipeline_error",
                    pipeline=entry["name"],
                    attempt=attempt,
                    error=str(exc),
                    exc_info=True,
                )
                return {"status": "error", "attempts": attempt, "error": str(exc), "terminal": True}
            except OSError as exc:
                if not _is_transient_io_error(exc):
                    logger.warning(
                        "gold_poller_pipeline_terminal_io_error",
                        pipeline=entry["name"],
                        attempt=attempt,
                        error=str(exc),
                        exc_info=True,
                    )
                    return {"status": "error", "attempts": attempt, "error": str(exc), "terminal": True}
                logger.warning(
                    "gold_poller_pipeline_transient_io_error",
                    pipeline=entry["name"],
                    attempt=attempt,
                    error=str(exc),
                    exc_info=True,
                )
                if attempt == max_retries:
                    return {"status": "error", "attempts": attempt, "error": str(exc)}
                await asyncio.sleep(min(backoff_base * attempt, max(0.0, run_deadline - time.monotonic())))
            except Exception as exc:
                logger.warning(
                    "gold_poller_pipeline_error",
                    pipeline=entry["name"],
                    attempt=attempt,
                    error=str(exc),
                    exc_info=True,
                )
                return {"status": "error", "attempts": attempt, "error": str(exc), "terminal": True}

        return {
            "status": "error",
            "attempts": max_retries,
            "error": f"Failed after {max_retries} attempts",
        }

    def _execute_pipeline_isolated(
        self,
        entry: dict[str, Any],
        start_date: date,
        end_date: date,
        *,
        max_runtime_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Run a pipeline in an isolated subprocess (blocking; call via ``to_thread``).

        Containment: a hard crash in the child — most importantly an OOM SIGKILL,
        which the in-process try/except cannot catch — kills only the child. The
        parent converts the child's death (or a timeout) into an exception, so the
        scheduler records a terminal failure instead of letting one child overrun
        the nightly deadline and starve every pipeline scheduled after it.
        """
        self._raise_if_unreaped_children()
        timeout: float = self._settings.gold_poller_pipeline_timeout_seconds
        if max_runtime_seconds is not None:
            timeout = min(timeout, max_runtime_seconds)
        if timeout <= 0:
            raise _PipelineTimeoutError(f"pipeline '{entry['name']}' has no remaining run budget")
        deadline = time.monotonic() + timeout
        ctx = multiprocessing.get_context("spawn")
        result_q: Any = ctx.Queue()
        proc = ctx.Process(
            target=_pipeline_subprocess_entry,
            args=(entry, start_date, end_date, result_q),
            name=f"gold-pipeline-{entry['name']}",
        )
        process_start = _DeadlineProcessStart(proc)
        if not process_start.start_by(deadline):
            self._track_unreaped_child(f"pipeline:{entry['name']}:start", process_start)
            raise _PipelineTimeoutError(f"pipeline '{entry['name']}' process start exceeded {timeout}s timeout")

        payload: dict[str, Any] | None = None
        timed_out = False
        while payload is None:
            remaining = _remaining_deadline_seconds(deadline)
            if remaining <= 0:
                timed_out = True
                break
            try:
                # Drain the result first: a fast pipeline may still be winding
                # down (flushing the queue feeder) when we read its result, so a
                # delivered payload — not liveness — is what ends the wait.
                payload = result_q.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if not proc.is_alive():
                    try:
                        payload = result_q.get_nowait()
                    except queue.Empty:
                        break  # child died without delivering (e.g. OOM SIGKILL)

        if timed_out:
            stopped = self._stop_or_defer_child_reap(f"pipeline:{entry['name']}", proc, deadline)
            if not stopped:
                logger.error("gold_poller_pipeline_reap_deadline_exceeded", pipeline=entry["name"])
            raise _PipelineTimeoutError(f"pipeline '{entry['name']}' exceeded {timeout}s timeout")

        # Reap the child without allowing cleanup to run beyond the same absolute
        # deadline that bounded its computation.
        if proc.is_alive():
            stopped = self._stop_or_defer_child_reap(f"pipeline:{entry['name']}", proc, deadline)
            if not stopped:
                raise _PipelineTimeoutError(
                    f"pipeline '{entry['name']}' subprocess could not be reaped before its deadline"
                )
        else:
            proc.join(0)

        if payload is None:
            raise _TerminalPipelineError(
                f"pipeline '{entry['name']}' subprocess died without a result (exitcode={proc.exitcode})"
            )
        if not payload.get("ok"):
            child_errno = payload.get("errno")
            error_type = payload.get("error_type")
            if (
                error_type
                in {
                    "ConnectionError",
                    "ConnectionAbortedError",
                    "ConnectionRefusedError",
                    "ConnectionResetError",
                    "BrokenPipeError",
                }
                or child_errno in _TRANSIENT_IO_ERRNOS
            ):
                raise _TransientPipelineError(f"pipeline '{entry['name']}' failed: {payload.get('error')}")
            raise _TerminalPipelineError(f"pipeline '{entry['name']}' failed: {payload.get('error')}")
        return cast(dict[str, Any], payload["stats"])

    # ----- Telemetry -----

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return current poller state for admin/health endpoints."""
        disabled = self._settings.gold_poller_disabled_pipeline_set | _SCHEDULE_DISABLED_PIPELINES
        return {
            "running": self._running,
            "enabled": self._settings.gold_poller_enabled,
            "last_run_date": self._last_run_date.isoformat() if self._last_run_date else None,
            "eod_trigger": f"{self._settings.gold_poller_eod_hour:02d}:{self._settings.gold_poller_eod_minute:02d}",
            "check_interval_seconds": self._settings.gold_poller_check_interval_seconds,
            "pipeline_count": len(PIPELINE_REGISTRY) - len(disabled),
            "disabled_pipelines": sorted(disabled),
            "unreaped_children": [child_name for child_name, _ in self._unreaped_children],
            "recent_runs": self._run_history[-3:],
        }


# ---------------------------------------------------------------------------
# Module-level singleton (Data-Gateway pattern)
# ---------------------------------------------------------------------------

_poller: GoldFeaturePoller | None = None


def get_gold_poller() -> GoldFeaturePoller | None:
    """Get the global Gold feature poller instance."""
    return _poller


async def start_gold_poller(settings: Settings | None = None) -> GoldFeaturePoller:
    """Start the global Gold feature poller."""
    global _poller
    if _poller is not None:
        return _poller

    s = settings or get_settings()
    if not s.gold_poller_enabled:
        logger.info("gold_poller_disabled")
        _poller = GoldFeaturePoller(settings=s)
        return _poller

    _poller = GoldFeaturePoller(settings=s)
    await _poller.start()
    return _poller


async def stop_gold_poller() -> None:
    """Stop the global Gold feature poller."""
    global _poller
    if _poller is not None:
        await _poller.stop()
        _poller = None


def get_gold_poller_snapshot() -> dict[str, Any] | None:
    """Get runtime telemetry from the Gold feature poller."""
    if _poller is None:
        return None
    return _poller.get_runtime_snapshot()
