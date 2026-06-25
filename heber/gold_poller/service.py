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
import multiprocessing
import queue
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import structlog

from heber.config import Settings, get_settings
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

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
    },
    {
        "name": "gex_regime",
        "module": "heber.features.pipelines.gex_regime_features",
        "class": "GexRegimePipeline",
        "datasets": None,
        "gold_datasets": ["gex_regime_features"],
    },
    {
        "name": "darkpool",
        "module": "heber.features.pipelines.darkpool_features",
        "class": "DarkpoolPipeline",
        "datasets": None,
        "gold_datasets": ["darkpool_features"],
    },
    {
        "name": "flow_toxicity",
        "module": "heber.features.pipelines.flow_toxicity_features",
        "class": "FlowToxicityPipeline",
        "datasets": None,
        "gold_datasets": ["flow_toxicity_features"],
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
    },
    {
        "name": "market_tide_context",
        "module": "heber.features.pipelines.market_tide_context_features",
        "class": "MarketTideContextPipeline",
        "datasets": None,
        "gold_datasets": ["market_tide_context_features"],
    },
    {
        "name": "sector_flow",
        "module": "heber.features.pipelines.sector_flow_features",
        "class": "SectorFlowPipeline",
        "datasets": None,
        "gold_datasets": ["sector_flow_features"],
    },
    {
        "name": "oi_momentum",
        "module": "heber.features.pipelines.oi_momentum_features",
        "class": "OiMomentumPipeline",
        "datasets": None,
        "gold_datasets": ["oi_momentum_features"],
    },
    {
        "name": "straddle_momentum",
        "module": "heber.features.pipelines.straddle_momentum_features",
        "class": "StraddleMomentumPipeline",
        "datasets": None,
        "gold_datasets": ["straddle_momentum_features"],
    },
    {
        "name": "trend_scan",
        "module": "heber.features.pipelines.trend_scan_features",
        "class": "TrendScanPipeline",
        "datasets": None,
        "gold_datasets": ["trend_scan_features"],
    },
    {
        "name": "flow_context",
        "module": "heber.features.pipelines.flow_context_features",
        "class": "FlowContextPipeline",
        "datasets": None,
        "gold_datasets": ["flow_context_features"],
    },
    {
        "name": "market_regime",
        "module": "heber.features.pipelines.market_regime_features",
        "class": "MarketRegimePipeline",
        "datasets": None,
        "gold_datasets": ["market_regime_features"],
    },
    {
        "name": "iv_surface",
        "module": "heber.features.pipelines.iv_surface_features",
        "class": "IVSurfacePipeline",
        "datasets": None,
        "gold_datasets": ["iv_surface_features"],
    },
    {
        "name": "flow_normalization",
        "module": "heber.features.pipelines.flow_normalization_features",
        "class": "FlowNormalizationPipeline",
        "datasets": None,
        "gold_datasets": ["flow_normalization_features"],
    },
    {
        "name": "ticker_base_rates",
        "module": "heber.features.pipelines.ticker_base_rates",
        "class": "TickerBaseRatesPipeline",
        "datasets": None,
        "gold_datasets": ["ticker_base_rates"],
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
        self._run_history: list[dict[str, Any]] = []

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(
            "gold_poller_started",
            eod_trigger=f"{self._settings.gold_poller_eod_hour:02d}:{self._settings.gold_poller_eod_minute:02d} ET",
            check_interval=self._settings.gold_poller_check_interval_seconds,
            disabled=list(self._settings.gold_poller_disabled_pipeline_set),
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

    # ----- Pipeline execution -----

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

        disabled = settings.gold_poller_disabled_pipeline_set
        pipelines = [p for p in PIPELINE_REGISTRY if p["name"] not in disabled]

        logger.info(
            "gold_poller_run_start",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            pipeline_count=len(pipelines),
            disabled=list(disabled),
        )

        run_start = time.monotonic()
        results: dict[str, dict[str, Any]] = {}

        for entry in pipelines:
            result = await self._run_pipeline(entry, start_date, end_date)
            results[entry["name"]] = result

        elapsed = time.monotonic() - run_start
        success_count = sum(1 for r in results.values() if r["status"] == "success")
        error_count = sum(1 for r in results.values() if r["status"] == "error")

        self._last_run_date = datetime.now(ZoneInfo("America/New_York")).date()

        run_summary = {
            "date": end_date.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "pipelines": len(pipelines),
            "success": success_count,
            "errors": error_count,
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

    async def _run_pipeline(
        self,
        entry: dict[str, Any],
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        """Run a single pipeline with retry logic."""
        settings = self._settings
        max_retries = settings.gold_poller_retry_max
        backoff_base = settings.gold_poller_retry_backoff_seconds

        for attempt in range(1, max_retries + 1):
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
                )

                # Every pipeline `run()` returns the single nested result shape:
                # {gold_dataset_name: {"status", "rows", "path"}}. A flat
                # {"status": ..., "rows": N} at the top level is a contract
                # violation — historically it made `total_rows` always 0 and
                # logged `["status","rows","path"]` as the "datasets" list,
                # masking real successes. Reject it loudly instead of silently
                # papering over it.
                if isinstance(stats, dict) and "status" in stats:
                    raise ValueError(
                        f"pipeline '{entry['name']}' returned a flat result "
                        f"{{'status': ...}}; pipelines must return the nested shape "
                        f"{{gold_dataset_name: {{'status', 'rows', 'path'}}}}"
                    )

                total_rows = sum(s.get("rows", 0) for s in stats.values() if isinstance(s, dict))
                error_datasets = [k for k, v in stats.items() if isinstance(v, dict) and v.get("status") == "error"]

                if error_datasets:
                    logger.warning(
                        "gold_poller_pipeline_partial",
                        pipeline=entry["name"],
                        error_datasets=error_datasets,
                        total_rows=total_rows,
                    )

                logger.info(
                    "gold_poller_pipeline_done",
                    pipeline=entry["name"],
                    total_rows=total_rows,
                    datasets=list(stats.keys()),
                )

                return {
                    "status": "success",
                    "attempts": attempt,
                    "rows": total_rows,
                    "datasets": stats,
                }

            except Exception as exc:
                logger.warning(
                    "gold_poller_pipeline_error",
                    pipeline=entry["name"],
                    attempt=attempt,
                    error=str(exc),
                    exc_info=True,
                )
                if attempt < max_retries:
                    delay = backoff_base * attempt
                    await asyncio.sleep(delay)

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
    ) -> dict[str, Any]:
        """Run a pipeline in an isolated subprocess (blocking; call via ``to_thread``).

        Containment: a hard crash in the child — most importantly an OOM SIGKILL,
        which the in-process try/except cannot catch — kills only the child. The
        parent converts the child's death (or a timeout) into an exception, so the
        retry/continue logic in ``_run_pipeline`` keeps the run going instead of
        the whole poller dying and silently starving every pipeline after this one.
        """
        timeout = self._settings.gold_poller_pipeline_timeout_seconds
        ctx = multiprocessing.get_context("spawn")
        result_q: Any = ctx.Queue()
        proc = ctx.Process(
            target=_pipeline_subprocess_entry,
            args=(entry, start_date, end_date, result_q),
            name=f"gold-pipeline-{entry['name']}",
        )
        proc.start()

        payload: dict[str, Any] | None = None
        timed_out = False
        deadline = time.monotonic() + timeout
        while True:
            try:
                # Drain the result first: a fast pipeline may still be winding
                # down (flushing the queue feeder) when we read its result, so a
                # delivered payload — not liveness — is what ends the wait.
                payload = result_q.get(timeout=2.0)
                break
            except queue.Empty:
                if not proc.is_alive():
                    break  # child died without delivering (e.g. OOM SIGKILL)
                if time.monotonic() >= deadline:
                    timed_out = True
                    break

        if timed_out:
            # Wedged child — kill it so it cannot leak past the run.
            proc.terminate()
            proc.join(10)
            if proc.is_alive():
                proc.kill()
                proc.join()
            raise TimeoutError(f"pipeline '{entry['name']}' exceeded {timeout}s timeout")

        # Reap the child (it may be exiting after delivering its result).
        proc.join(5)
        if proc.is_alive():
            proc.terminate()
            proc.join(5)
            if proc.is_alive():
                proc.kill()
                proc.join()

        if payload is None:
            # Child died before reporting — almost always an OOM SIGKILL (-9).
            # This is the exact failure mode that used to take down the poller.
            raise RuntimeError(
                f"pipeline '{entry['name']}' subprocess died without a result "
                f"(exitcode={proc.exitcode}); likely OOM-killed"
            )
        if not payload.get("ok"):
            raise RuntimeError(f"pipeline '{entry['name']}' failed: {payload.get('error')}")
        return payload["stats"]

    # ----- Telemetry -----

    def get_runtime_snapshot(self) -> dict[str, Any]:
        """Return current poller state for admin/health endpoints."""
        return {
            "running": self._running,
            "enabled": self._settings.gold_poller_enabled,
            "last_run_date": self._last_run_date.isoformat() if self._last_run_date else None,
            "eod_trigger": f"{self._settings.gold_poller_eod_hour:02d}:{self._settings.gold_poller_eod_minute:02d}",
            "check_interval_seconds": self._settings.gold_poller_check_interval_seconds,
            "pipeline_count": len(PIPELINE_REGISTRY),
            "disabled_pipelines": list(self._settings.gold_poller_disabled_pipeline_set),
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
