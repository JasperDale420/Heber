"""Health Monitor entry point.

Run with: python -m heber.health_monitor

Starts a long-lived async service that runs tiered data health checks
against the Heber lakehouse.  Follows the same singleton/signal pattern
as heber-gold-poller.
"""

from __future__ import annotations

import argparse
import asyncio
import signal

import structlog

logger = structlog.get_logger(__name__)


def run() -> None:
    """Run the Health Monitor service."""
    from heber.config import get_settings
    from heber.health_monitor.service import start_health_monitor, stop_health_monitor
    from heber.ops.logging import configure_logging
    from heber.ops.metrics import start_metrics_server_from_env

    settings = get_settings()
    configure_logging(
        service_name="heber-health-monitor",
        log_level=settings.log_level,
        json_output=True,
    )

    parser = argparse.ArgumentParser(
        description="Run the Heber data health monitor service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Runs tiered data health checks against the Heber lakehouse:
  Tier 1 — Stream health (every 30s)
  Tier 2 — Partition + volume (every 15min, market hours)
  Tier 3 — Schema + statistical + ML readiness (once daily after EOD)

Example:
  python -m heber.health_monitor
  HEBER_HEALTH_STREAM_CHECK_INTERVAL_SECONDS=60 python -m heber.health_monitor

Environment variables:
  HEBER_HEALTH_MONITOR_ENABLED                  Enable/disable (default: true)
  HEBER_HEALTH_STREAM_CHECK_INTERVAL_SECONDS    Tier 1 interval (default: 30)
  HEBER_HEALTH_PARTITION_CHECK_INTERVAL_SECONDS  Tier 2 interval (default: 900)
""",
    )
    parser.parse_args()

    if not settings.health_monitor_enabled:
        logger.info("health_monitor_disabled_by_config")
        return

    logger.info(
        "health_monitor_starting",
        tier1_interval=settings.health_stream_check_interval_seconds,
        tier2_interval=settings.health_partition_check_interval_seconds,
    )

    start_metrics_server_from_env(default_port=9093)

    shutdown_event = asyncio.Event()

    async def _main() -> None:
        await start_health_monitor(settings)
        await shutdown_event.wait()
        await stop_health_monitor()
        logger.info("health_monitor_shutdown_complete")

    def shutdown_handler(sig: int, frame: object) -> None:
        logger.info("shutdown_signal_received", signal=sig)
        shutdown_event.set()

    try:
        signal.signal(signal.SIGTERM, shutdown_handler)
        signal.signal(signal.SIGINT, shutdown_handler)
    except (ValueError, RuntimeError) as exc:
        logger.warning("signal_handlers_unavailable", error=str(exc))

    run_error: BaseException | None = None
    run_error_tb = None

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        run_error = exc
        run_error_tb = exc.__traceback__
    finally:
        logger.info("health_monitor_exited")

    if run_error is not None:
        raise run_error.with_traceback(run_error_tb)


if __name__ == "__main__":
    run()
