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
import signal

import structlog

logger = structlog.get_logger(__name__)


def _get_defaults() -> tuple[str, str, str]:
    """Load defaults from settings to avoid module-level env reads."""
    from heber.config import get_settings

    s = get_settings()
    return s.watch_redis_url, s.watch_gateway_url, str(s.gold_path)


def run() -> None:
    """Run the watch service."""
    from pathlib import Path

    import redis

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

    r = redis.from_url(args.redis)
    output_path = Path(args.output) if args.output else None

    service = WatchService(r, gateway_url=args.gateway, output_path=output_path)

    # Handle graceful shutdown
    def shutdown_handler(sig: int, frame: object) -> None:
        logger.info("Shutdown signal received", signal=sig)
        service.stop()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        pass
    except Exception:
        raise
    finally:
        service.stop()
        logger.info("Watch service exited")


if __name__ == "__main__":
    run()
