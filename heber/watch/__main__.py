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
import threading
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    handler: object,
) -> bool:
    """Register SIGTERM/SIGINT handlers via the event loop.

    Uses ``loop.add_signal_handler()`` rather than ``signal.signal()`` so the
    callback runs inside the event loop (well-defined async state) rather than
    in an OS-signal-interrupt context where awaiting/closing resources is
    racy.

    ``add_signal_handler`` itself still requires the *main thread* (Python's
    signal module raises ``ValueError: signal only works in main thread of
    the main interpreter`` otherwise). Returns ``True`` if installation
    succeeded, ``False`` if we were called off the main thread or on a
    platform without unix signals (e.g. Windows). The caller may proceed
    without signal hooks in that case — the service will still exit cleanly
    via the ``finally`` block once ``run()`` returns or raises.
    """
    if threading.current_thread() is not threading.main_thread():
        logger.warning(
            "watch_signal_install_skipped",
            reason="not_main_thread",
            thread=threading.current_thread().name,
        )
        return False

    success = False
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handler)  # type: ignore[arg-type]
            success = True
        except (NotImplementedError, RuntimeError, ValueError) as exc:
            # NotImplementedError: Windows / unsupported event loop.
            # RuntimeError/ValueError: signal module main-thread check.
            logger.warning(
                "watch_signal_install_skipped",
                reason="add_signal_handler_failed",
                signal=sig.name,
                error=str(exc),
            )
    return success


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

    # Initialize logging early
    # Note: We don't have access to settings instance here directly without importing config,
    # but _get_defaults used it. Let's just use the default log level or fetch settings.
    from heber.config import get_settings
    from heber.ops.logging import configure_logging

    settings = get_settings()
    configure_logging(service_name="heber-watch", log_level=settings.log_level, json_output=True)

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

    try:
        start_metrics_server_from_env(default_port=9090)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))

    r = redis.from_url(args.redis)
    resolved_output_path: Path | None = _ensure_writable_output_path(args.output)

    service = WatchService(r, gateway_url=args.gateway, output_path=resolved_output_path)

    def _safe_stop(source: str) -> None:
        try:
            service.stop()
        except Exception as stop_error:
            logger.warning("watch_service_stop_failed", source=source, error=str(stop_error))

    async def _run_with_signals() -> None:
        loop = asyncio.get_running_loop()

        def _on_signal() -> None:
            logger.info("watch_shutdown_signal_received")
            _safe_stop(source="signal")

        _install_signal_handlers(loop, _on_signal)
        await service.run()

    run_error: BaseException | None = None
    run_error_tb = None
    try:
        asyncio.run(_run_with_signals())
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
