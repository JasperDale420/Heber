"""Gold Feature Poller entry point.

Run with: python -m heber.gold_poller

Starts a long-lived async service that refreshes Gold feature datasets
after each trading day closes.  Follows the same singleton/signal pattern
as heber-watch.
"""

from __future__ import annotations

import argparse
import asyncio
import signal

import structlog

logger = structlog.get_logger(__name__)


def run() -> None:
    """Run the Gold feature poller service."""
    from heber.config import get_settings
    from heber.gold_poller.service import start_gold_poller, stop_gold_poller
    from heber.ops.logging import configure_logging
    from heber.ops.metrics import start_metrics_server_from_env

    settings = get_settings()
    configure_logging(
        service_name="heber-gold-poller",
        log_level=settings.log_level,
        json_output=True,
    )

    parser = argparse.ArgumentParser(
        description="Run the Gold feature poller service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Refreshes all registered Gold feature pipelines after each trading day.

Example:
  python -m heber.gold_poller
  HEBER_GOLD_POLLER_EOD_HOUR=17 python -m heber.gold_poller

Environment variables:
  HEBER_GOLD_POLLER_ENABLED              Enable/disable (default: true)
  HEBER_GOLD_POLLER_EOD_HOUR             Trigger hour in ET (default: 16)
  HEBER_GOLD_POLLER_EOD_MINUTE           Trigger minute (default: 35)
  HEBER_GOLD_POLLER_CHECK_INTERVAL_SECONDS  Check interval (default: 60)
  HEBER_GOLD_POLLER_LOOKBACK_DAYS        Days to recompute (default: 1)
  HEBER_GOLD_POLLER_DISABLED_PIPELINES   Comma-separated skip list
""",
    )
    parser.parse_args()

    if not settings.gold_poller_enabled:
        logger.info("gold_poller_disabled_by_config")
        return

    logger.info(
        "gold_poller_starting",
        eod_trigger=f"{settings.gold_poller_eod_hour:02d}:{settings.gold_poller_eod_minute:02d} ET",
        check_interval=settings.gold_poller_check_interval_seconds,
        lookback_days=settings.gold_poller_lookback_days,
        disabled=list(settings.gold_poller_disabled_pipeline_set),
    )

    try:
        start_metrics_server_from_env(default_port=9091)
    except Exception as exc:
        logger.warning("metrics_server_startup_skipped", error=str(exc))

    shutdown_event = asyncio.Event()

    async def _main() -> None:
        await start_gold_poller(settings)
        await shutdown_event.wait()
        await stop_gold_poller()
        logger.info("gold_poller_shutdown_complete")

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
        logger.info("gold_poller_exited")

    if run_error is not None:
        raise run_error.with_traceback(run_error_tb)


if __name__ == "__main__":
    run()
