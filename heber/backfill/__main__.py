"""Executable backfill service entrypoint."""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable
from typing import Any

from fastapi import FastAPI

from heber.backfill import create_backfill_router


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
    parser = argparse.ArgumentParser(description="Run Heber backfill service.")
    parser.add_argument(
        "--host",
        default=os.getenv("HEBER_BACKFILL_HOST", os.getenv("HEBER_API_HOST", "0.0.0.0")),
        help="Bind host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("HEBER_BACKFILL_PORT", "8080")),
        help="Bind port",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("HEBER_BACKFILL_LOG_LEVEL", "info"),
        help="Uvicorn log level",
    )
    return parser


def main(argv: list[str] | None = None, run_server: Callable[..., Any] | None = None) -> int:
    """CLI entrypoint for `python -m heber.backfill`."""
    args = _build_parser().parse_args(argv)
    runner = run_server
    if runner is None:
        import uvicorn

        runner = uvicorn.run

    runner(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
