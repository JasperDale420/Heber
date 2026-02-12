"""Compatibility facade for legacy writer.hotstore imports.

Hot Store sync/read/write logic now lives in `heber.hotstore.sync`.
This module preserves the prior import path while using the unified
implementation and `clickhouse-connect` client stack.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Callable

import structlog

from heber.hotstore.sync import (
    HotStoreReader,
    HotStoreSync,
    HotStoreSyncConfig,
    HotStoreTable,
    QueryType,
    SyncState,
    create_hot_store_syncer,
)
from heber.ops.metrics import start_metrics_server_from_env

logger = structlog.get_logger(__name__)


def _parse_datasets(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _build_parser() -> argparse.ArgumentParser:
    from heber.config import get_settings

    settings = get_settings()

    parser = argparse.ArgumentParser(description="Run Heber hotloader service.")
    parser.add_argument(
        "--datasets",
        default=settings.hotloader_datasets,
        help="Comma-separated datasets to sync",
    )
    parser.add_argument(
        "--silver-base-path",
        default=settings.hotloader_silver_base_path,
        help="Optional Silver base path override",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one sync pass and exit",
    )
    return parser


async def _run_hotloader(
    datasets: list[str],
    silver_base_path: str | None,
    once: bool,
    syncer_factory: Callable[..., HotStoreSync],
) -> None:
    syncer = syncer_factory(silver_base_path=silver_base_path)
    syncer.ensure_tables()

    if once:
        for dataset in datasets:
            syncer.sync_from_silver(dataset, silver_path=silver_base_path)
        syncer.flush()
        return

    stop_event: asyncio.Event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        syncer.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except (RuntimeError, ValueError):
            # Not all runtimes allow custom signal handlers.
            pass

    sync_task = asyncio.create_task(syncer.run_sync_loop(datasets, silver_base_path))
    sync_task.add_done_callback(lambda _task: stop_event.set())
    logger.info("hotloader_started", datasets=datasets, silver_base_path=silver_base_path)

    try:
        await stop_event.wait()
    finally:
        syncer.stop()
        await sync_task
        logger.info("hotloader_stopped")


def main(
    argv: list[str] | None = None,
    syncer_factory: Callable[..., HotStoreSync] = create_hot_store_syncer,
    metrics_server_starter: Callable[..., int | None] = start_metrics_server_from_env,
) -> int:
    """CLI entrypoint for `python -m heber.writer.hotstore`."""
    metrics_server_starter(default_port=9090)
    args = _build_parser().parse_args(argv)
    datasets = _parse_datasets(args.datasets)
    if not datasets:
        raise ValueError("At least one dataset must be provided for hotloader sync.")

    try:
        asyncio.run(
            _run_hotloader(
                datasets=datasets,
                silver_base_path=args.silver_base_path,
                once=args.once,
                syncer_factory=syncer_factory,
            )
        )
    except KeyboardInterrupt:
        pass
    return 0


__all__ = [
    "QueryType",
    "HotStoreTable",
    "HotStoreSyncConfig",
    "SyncState",
    "HotStoreSync",
    "HotStoreReader",
    "create_hot_store_syncer",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
