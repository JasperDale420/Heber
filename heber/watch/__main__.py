"""Watch Service entry point.

Run with: python -m heber.watch

This starts the full watch service that:
1. Consumes flow_alerts from Redis stream
2. Polls option quotes during market hours
3. Checks barriers and writes labels to Gold
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def _get_defaults() -> tuple[str, str, str]:
    """Load defaults from settings to avoid module-level env reads."""
    from heber.config import get_settings

    s = get_settings()
    return s.watch_redis_url, s.watch_gateway_url, str(s.gold_path)


def _ensure_writable_output_path(output_path: str | None) -> Path | None:
    """Resolve, create, and validate write access to output path."""
    if not output_path:
        return None

    resolved = Path(output_path)
    resolved.mkdir(parents=True, exist_ok=True)

    probe_file = resolved / f".watch-write-probe-{os.getpid()}"
    try:
        with probe_file.open("w", encoding="utf-8") as handle:
            handle.write("ok")
    except OSError as exc:
        raise PermissionError(f"Output path is not writable: {resolved}") from exc
    finally:
        probe_file.unlink(missing_ok=True)

    return resolved


def run() -> None:
    """Run the watch service."""
    import redis

    from heber.ops.metrics import start_metrics_server_from_env
    from heber.watch.writer import WatchService

    redis_url, gateway_url, output_path = _get_defaults()

    parser = argparse.ArgumentParser(
        description="Run the alert watch service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python -m heber.watch --redis redis://localhost:6379 --gateway http://localhost:8000

Environment variables:
  HEBER_REDIS_URL      Redis connection URL
  DATA_GATEWAY_URL     Data Gateway URL for option quotes
  HEBER_GOLD_PATH      Gold layer output path
""",
    )
    parser.add_argument(
        "--redis",
        default=redis_url,
        help=f"Redis URL (default: {redis_url})",
    )
    parser.add_argument(
        "--gateway",
        default=gateway_url,
        help=f"Data Gateway URL (default: {gateway_url})",
    )
    parser.add_argument(
        "--output",
        default=output_path,
        help=f"Gold output path (default: {output_path})",
    )

    args = parser.parse_args()

    logger.info(
        "Starting watch service",
        redis_url=args.redis,
        gateway_url=args.gateway,
        output_path=args.output,
    )

    start_metrics_server_from_env(default_port=9090)

    r = redis.from_url(args.redis)
    output_path = _ensure_writable_output_path(args.output)

    service = WatchService(r, gateway_url=args.gateway, output_path=output_path)
    run_error: BaseException | None = None
    run_error_tb = None

    def _safe_stop(source: str) -> None:
        try:
            service.stop()
        except Exception as stop_error:
            logger.error("Watch service stop failed", source=source, error=str(stop_error))

    # Handle graceful shutdown
    def shutdown_handler(sig: int, frame: object) -> None:
        logger.info("Shutdown signal received", signal=sig)
        _safe_stop(source=f"signal:{sig}")

    try:
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
    except (ValueError, RuntimeError) as signal_error:
        logger.warning(
            "Signal handlers unavailable, continuing without process signal hooks",
            error=str(signal_error),
        )

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        run_error = exc
        run_error_tb = exc.__traceback__
    finally:
        _safe_stop(source="finally")
        logger.info("Watch service exited")

    if run_error is not None:
        raise run_error.with_traceback(run_error_tb)


if __name__ == "__main__":
    run()
