"""Alert Watch Writer - Writes completed outcomes to Gold layer."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from heber.core.http_client import create_async_http_client
from heber.utils.durable_write import fsync_dir
from heber.watch.checker import BarrierChecker, outcome_to_label_row
from heber.watch.gateway import (
    GATEWAY_AUTH_HEADER_NAME,
    describe_gateway_auth_env,
    gateway_auth_headers,
    gateway_url_candidates,
)
from heber.watch.models import WatchOutcome

logger = structlog.get_logger(__name__)

DEFAULT_GATEWAY_URL = "http://localhost:8080"


def _resolve_gateway_api_key(explicit_gateway_api_key: str | None, settings_gateway_api_key: str | None) -> str:
    """Resolve and validate the gateway API key used by watch components."""
    from heber.config import get_settings

    candidate = explicit_gateway_api_key
    if candidate is None:
        candidate = settings_gateway_api_key
    normalized = (candidate or "").strip()
    if not normalized:
        if get_settings().environment == "prod":
            raise ValueError("HEBER_WATCH_GATEWAY_API_KEY (or DATA_GATEWAY_API_KEY) must be configured")
        logger.warning("gateway_api_key_missing", hint="Set HEBER_WATCH_GATEWAY_API_KEY for authenticated access")
        return ""
    return normalized


class LabelWriter:
    """Writes completed watch outcomes to Gold storage.

    Can write to either Parquet files or via HeberClient.
    """

    def __init__(
        self,
        output_path: Path | None = None,
        heber_client: Any | None = None,
        dataset: str = "labels_alert_barriers",
        project: str = "watch",
        version: str = "v1",
    ):
        """Initialize the writer.

        Args:
            output_path: Direct path to write Parquet (optional)
            heber_client: HeberClient instance (optional)
            dataset: Gold dataset name
            project: Project name
            version: Version string
        """
        self.output_path = output_path
        self.client = heber_client
        self.dataset = dataset
        self.project = project
        self.version = version
        self._buffer: list[dict] = []
        self._buffer_size = 100

    def write_outcome(self, outcome: WatchOutcome) -> None:
        """Write a single outcome (buffered).

        Args:
            outcome: Completed watch outcome
        """
        row = outcome_to_label_row(outcome)
        self._buffer.append(row)

        MAX_BUFFER_SIZE = 10000
        if len(self._buffer) > MAX_BUFFER_SIZE:
            logger.warning("label_buffer_overflow", size=len(self._buffer), max=MAX_BUFFER_SIZE)
            self._buffer = self._buffer[-MAX_BUFFER_SIZE:]

        if len(self._buffer) >= self._buffer_size:
            self.flush()

    def write_outcomes(self, outcomes: list[WatchOutcome]) -> None:
        """Write multiple outcomes.

        Args:
            outcomes: List of completed outcomes
        """
        for outcome in outcomes:
            row = outcome_to_label_row(outcome)
            self._buffer.append(row)

        self.flush()

    def flush(self) -> None:
        """Flush buffered outcomes to storage."""
        if not self._buffer:
            return

        df = pd.DataFrame(self._buffer)

        if self.client:
            self._write_via_client(df)
        elif self.output_path:
            self._write_to_parquet(df)
        else:
            logger.warning("No output configured, discarding labels", count=len(df))

        logger.info("Flushed labels to Gold", count=len(self._buffer))
        self._buffer = []

    def _write_via_client(self, df: pd.DataFrame) -> None:
        """Write using HeberClient."""
        self.client.write_gold(
            dataset=self.dataset,
            df=df,
            project=self.project,
            version=self.version,
        )

    def _write_to_parquet(self, df: pd.DataFrame) -> None:
        """Write directly to Parquet files."""
        # Partition by date.
        # Stage writes to temp files first so a mid-flush failure does not
        # partially commit some partitions and duplicate rows on retry.
        write_df = df.copy()
        write_df["_date"] = pd.to_datetime(write_df["ts_event"]).dt.date
        staged_files: list[tuple[Path, Path, int]] = []
        promoted_files: list[Path] = []
        current_tmp_file: Path | None = None

        try:
            for dt, group in write_df.groupby("_date"):
                partition_path = (
                    self.output_path
                    / f"dataset={self.dataset}"
                    / f"project={self.project}"
                    / f"version={self.version}"
                    / f"dt={dt}"
                )
                partition_path.mkdir(parents=True, exist_ok=True)

                ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
                # Include a unique suffix so multiple flushes within the same timestamp cannot overwrite files.
                final_file_path = partition_path / f"part-{ts}-{uuid.uuid4().hex[:8]}.parquet"
                current_tmp_file = partition_path / f".{final_file_path.name}.tmp"

                write_group = group.drop(columns=["_date"])

                # Audit for unexpected null values before writing
                from heber.quality.write_audit import audit_null_fields

                audit_null_fields(
                    write_group,
                    layer="gold",
                    dataset=self.dataset,
                    context={"date": str(dt), "path": str(partition_path)},
                )

                write_group.to_parquet(current_tmp_file, index=False, compression="snappy")
                # Flush before the rename can publish the name. The lakehouse
                # volume is exFAT with no journaling, so a rename that lands
                # ahead of the data publishes a zero-byte file — and one of
                # those makes the whole dataset unreadable to pyarrow.
                with current_tmp_file.open("rb+") as handle:
                    os.fsync(handle.fileno())  # noqa: PTH123 — flush before the rename publishes the name
                staged_files.append((current_tmp_file, final_file_path, len(group)))
                current_tmp_file = None

            for tmp_file, final_file, rows in staged_files:
                tmp_file.replace(final_file)
                promoted_files.append(final_file)
                logger.debug("Wrote partition", path=str(final_file), rows=rows)

            # One flush per distinct partition directory — a single flush
            # would leave every other date's rename undurable, and those
            # labels would vanish without even a zero-byte file to notice.
            for directory in {f.parent for f in promoted_files}:
                fsync_dir(directory)

        except Exception:
            self._cleanup_staged_files(current_tmp_file, staged_files, promoted_files)
            raise

    @staticmethod
    def _cleanup_staged_files(
        current_tmp_file: Path | None,
        staged_files: list[tuple[Path, Path, int]],
        promoted_files: list[Path],
    ) -> None:
        """Remove staged and promoted files on write failure."""
        if current_tmp_file is not None and current_tmp_file.exists():
            current_tmp_file.unlink()
        for tmp_file, _final_file, _rows in staged_files:
            if tmp_file.exists():
                tmp_file.unlink()
        for committed_file in reversed(promoted_files):
            if committed_file.exists():
                committed_file.unlink()


class WatchService:
    """Orchestrates all watch components.

    Runs consumer, poller, checker, writer, and enrichment backfill together.
    """

    def __init__(
        self,
        redis_client: Any,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        gateway_api_key: str | None = None,
        output_path: Path | None = None,
    ):
        """Initialize the watch service.

        Args:
            redis_client: Redis client
            gateway_url: Data Gateway URL
            gateway_api_key: Data Gateway API key for authenticated requests
            output_path: Path for Gold output
        """
        from heber.config import settings
        from heber.watch.backfill_scanner import EnrichmentBackfillScanner
        from heber.watch.consumer import AlertWatchConsumer
        from heber.watch.features import DEFAULT_FEATURES_OUTPUT_PATH, AlertFeatureExtractor
        from heber.watch.manager import WatchManager
        from heber.watch.poller import SnapshotPoller

        resolved_gateway_api_key = _resolve_gateway_api_key(
            gateway_api_key,
            settings.watch_gateway_api_key,
        )

        self._gateway_url = gateway_url
        self._gateway_api_key = resolved_gateway_api_key
        self._auth_preflight_timeout_seconds = 5.0
        self.redis = redis_client
        self.manager = WatchManager(redis_client)
        self.consumer = AlertWatchConsumer(
            redis_client,
            self.manager,
            gateway_url=gateway_url,
            gateway_api_key=resolved_gateway_api_key,
            legacy_route_fallback_enabled=settings.watch_gateway_legacy_fallback_enabled,
        )
        self.poller = SnapshotPoller(
            self.manager,
            gateway_url=gateway_url,
            gateway_api_key=resolved_gateway_api_key,
            legacy_route_fallback_enabled=settings.watch_gateway_legacy_fallback_enabled,
        )
        self.checker = BarrierChecker(self.manager)
        self.writer = LabelWriter(output_path=output_path)

        # Enrichment backfill scanner
        backfill_extractor = AlertFeatureExtractor(
            gateway_url=gateway_url,
            gateway_api_key=resolved_gateway_api_key,
            legacy_route_fallback_enabled=settings.watch_gateway_legacy_fallback_enabled,
            request_timeout_seconds=settings.watch_enrichment_timeout_seconds,
            request_timeout_option_chain_seconds=settings.watch_enrichment_option_chain_timeout_seconds,
        )
        self.backfill_scanner = EnrichmentBackfillScanner(
            feature_extractor=backfill_extractor,
            features_output_path=DEFAULT_FEATURES_OUTPUT_PATH,
            interval_seconds=settings.enrichment_backfill_interval,
            lookback_days=settings.enrichment_backfill_lookback_days,
            batch_size=settings.enrichment_backfill_batch_size,
        )
        self._backfill_enabled = settings.enrichment_backfill_enabled
        self._running = False
        self._stopped = False

    async def run(self) -> None:
        """Run all components concurrently."""
        import asyncio

        self._running = True

        logger.info(
            "Starting watch service",
            enrichment_backfill_enabled=self._backfill_enabled,
        )

        # Preflight: surface broken gateway auth at boot rather than discovering
        # it via cascading 401s and null Greeks downstream.
        await self._gateway_auth_preflight()

        coroutines = [
            self.consumer.run(),
            self.poller.run(),
            self._check_and_write_loop(),
        ]
        if self._backfill_enabled:
            coroutines.append(self.backfill_scanner.run())

        try:
            async with asyncio.TaskGroup() as tg:
                for coro in coroutines:
                    tg.create_task(coro)
        except ExceptionGroup as eg:
            # Re-raise the first exception for backward-compatible error handling
            raise eg.exceptions[0] from eg

    async def _gateway_auth_preflight(self) -> None:
        """One-shot auth check at boot. Surfaces 401s loudly without crashing.

        Hits a lightweight auth-required gateway endpoint (`uw/SPY/iv-rank`)
        and emits a CRITICAL-grade structured log on 401 — operators see the
        problem immediately instead of discovering it via null Greeks in Gold
        meta_label_features hours later.

        Failure modes:
          - 401: log critical, set self._auth_preflight_ok = False, but DO NOT
            raise. The service still starts in case the operator is reusing the
            container for non-enriched watches; downstream 401 fail-fast already
            kicks in after the configured threshold.
          - transport error / timeout: log warning, mark unknown.
          - 2xx / 5xx / other 4xx: log info, mark OK (gateway reachable + auth
            accepted; non-200 may be expected for the probe symbol).
        """
        self._auth_preflight_ok: bool | None = None
        auth_env = describe_gateway_auth_env()
        headers = gateway_auth_headers(self._gateway_api_key) or None
        routes = gateway_url_candidates(self._gateway_url, "/uw/SPY/iv-rank")
        probe_url = routes[0]

        log_ctx = {
            "gateway_url": self._gateway_url,
            "probe_route": probe_url,
            "auth_header_names_sent": [GATEWAY_AUTH_HEADER_NAME] if headers else [],
            "auth_env": auth_env,
        }

        try:
            async with create_async_http_client(timeout=self._auth_preflight_timeout_seconds) as client:
                resp = await client.get(probe_url, headers=headers)
        except Exception as exc:  # network/transport — non-fatal
            logger.warning(
                "Gateway auth preflight: transport error (auth state unknown)",
                error=str(exc),
                error_type=type(exc).__name__,
                **log_ctx,
            )
            return

        status_code = getattr(resp, "status_code", None)
        if status_code == 401:
            # This is the canary for the cascade. Make it impossible to miss.
            self._auth_preflight_ok = False
            logger.critical(
                "GATEWAY AUTH PREFLIGHT FAILED: gateway returned 401 unauthorized. "
                "Feature enrichment WILL fail and Gold meta_label_features WILL be "
                "written with null Greeks unless this is fixed. Check that the "
                "gateway API key is correct and not rotated.",
                status_code=status_code,
                **log_ctx,
            )
            return

        self._auth_preflight_ok = True
        logger.info(
            "Gateway auth preflight ok",
            status_code=status_code,
            **log_ctx,
        )

    async def _check_and_write_loop(self) -> None:
        """Periodically check for completed watches and write labels."""
        import asyncio

        while self._running:
            try:
                outcomes = await asyncio.to_thread(self.checker.check_all)

                if outcomes:
                    self.writer.write_outcomes(outcomes)
                    logger.info("Wrote outcomes", count=len(outcomes))

            except Exception as e:
                logger.error("Check/write error", error=str(e), exc_info=True)

            await asyncio.sleep(60)  # Check every minute

    def stop(self) -> None:
        """Stop all components.

        Idempotent: calling stop() twice (e.g. once from a SIGTERM handler
        and once from the ``finally`` block of the CLI entrypoint) returns
        cleanly on the second call instead of raising on already-closed
        redis clients or already-flushed writers. This is what eliminates
        the recurring "Watch service stop failed" log noise during normal
        SIGTERM-driven shutdown.
        """
        if self._stopped:
            return
        self._stopped = True
        self._running = False

        # Best-effort cleanup of each component — one failure must not skip
        # the others. We capture and log per-component errors and re-raise
        # the first one only if nothing else failed afterwards.
        errors: list[Exception] = []
        for component, action in (
            ("consumer", self.consumer.stop),
            ("poller", self.poller.stop),
            ("backfill_scanner", self.backfill_scanner.stop),
            ("writer_flush", self.writer.flush),
            ("redis_close", self.redis.close),
        ):
            try:
                action()
            except Exception as exc:  # noqa: BLE001 — cleanup must be exhaustive
                errors.append(exc)
                logger.warning(
                    "watch_component_stop_failed",
                    component=component,
                    error=str(exc),
                )

        logger.info("Watch service stopped")
        if errors:
            # Re-raise the first error so callers can react if they wish,
            # but the rest of the components are guaranteed to have run.
            raise errors[0]

    def get_stats(self) -> dict[str, Any]:
        """Get service statistics."""
        return {
            "watches": self.manager.get_stats(),
            "buffer_size": len(self.writer._buffer),
        }


def run_watch_service(
    redis_url: str = "redis://localhost:6379",
    gateway_url: str = DEFAULT_GATEWAY_URL,
    gateway_api_key: str | None = None,
    output_path: str | None = None,
) -> None:
    """CLI entry point for watch service.

    Args:
        redis_url: Redis connection URL
        gateway_url: Data Gateway URL
        gateway_api_key: Data Gateway API key for authenticated requests
        output_path: Path for Gold output
    """
    import asyncio

    import redis

    r = redis.from_url(redis_url)
    path = Path(output_path) if output_path else None

    service = WatchService(r, gateway_url=gateway_url, gateway_api_key=gateway_api_key, output_path=path)
    run_error: BaseException | None = None
    run_error_tb = None

    def _safe_stop() -> None:
        try:
            service.stop()
        except Exception as stop_error:
            logger.error("Watch service stop failed", error=str(stop_error))

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        run_error = exc
        run_error_tb = exc.__traceback__
    finally:
        _safe_stop()

    if run_error is not None:
        raise run_error.with_traceback(run_error_tb)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run alert watch service")
    parser.add_argument("--redis", default="redis://localhost:6379", help="Redis URL")
    parser.add_argument("--gateway", default=DEFAULT_GATEWAY_URL, help="Data Gateway URL")
    parser.add_argument("--output", help="Gold output path")

    args = parser.parse_args()

    run_watch_service(
        redis_url=args.redis,
        gateway_url=args.gateway,
        output_path=args.output,
    )
