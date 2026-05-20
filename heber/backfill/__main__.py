"""Executable backfill service entrypoint."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from typing import Any

import structlog
from fastapi import FastAPI

from heber.backfill import create_backfill_router
from heber.ops.metrics import start_metrics_server_from_env

logger = structlog.get_logger(__name__)


def create_app() -> FastAPI:
    """Build the backfill API app."""
    app = FastAPI(title="Heber Backfill Service", version="0.1.0")
    app.include_router(create_backfill_router())

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready() -> dict[str, str]:
        return {"status": "ready"}

    return app


app = create_app()


def _build_parser() -> argparse.ArgumentParser:
    from heber.config import get_settings

    settings = get_settings()

    parser = argparse.ArgumentParser(description="Run Heber backfill service.")
    parser.add_argument(
        "--host",
        default=settings.backfill_host,
        help="Bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=settings.backfill_port,
        help="Bind port",
    )
    parser.add_argument(
        "--log-level",
        default=settings.backfill_log_level,
        help="Uvicorn log level",
    )
    return parser


def main(
    argv: list[str] | None = None,
    run_server: Callable[..., Any] | None = None,
    metrics_server_starter: Callable[..., int | None] = start_metrics_server_from_env,
) -> int:
    """CLI entrypoint for `python -m heber.backfill`."""
    try:
        metrics_server_starter(default_port=9090)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))
    args = _build_parser().parse_args(argv)
    runner = run_server
    if runner is None:
        import uvicorn

        runner = uvicorn.run

    runner(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
