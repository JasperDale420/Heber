"""Alert Watch Writer - Writes completed outcomes to Gold layer."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

from heber.watch.checker import BarrierChecker, outcome_to_label_row
from heber.watch.models import WatchOutcome

logger = structlog.get_logger(__name__)

DEFAULT_GATEWAY_URL = "http://localhost:8000"


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
        # Partition by date
        df["_date"] = pd.to_datetime(df["ts_event"]).dt.date

        for dt, group in df.groupby("_date"):
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
            file_path = partition_path / f"part-{ts}-{uuid.uuid4().hex[:8]}.parquet"

            group.drop(columns=["_date"]).to_parquet(file_path, compression="snappy")
            logger.debug("Wrote partition", path=str(file_path), rows=len(group))


class WatchService:
    """Orchestrates all watch components.

    Runs consumer, poller, checker, and writer together.
    """

    def __init__(
        self,
        redis_client: Any,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        output_path: Path | None = None,
    ):
        """Initialize the watch service.

        Args:
            redis_client: Redis client
            gateway_url: Data Gateway URL
            output_path: Path for Gold output
        """
        from heber.watch.consumer import AlertWatchConsumer
        from heber.watch.manager import WatchManager
        from heber.watch.poller import SnapshotPoller

        self.redis = redis_client
        self.manager = WatchManager(redis_client)
        self.consumer = AlertWatchConsumer(redis_client, self.manager, gateway_url=gateway_url)
        self.poller = SnapshotPoller(self.manager, gateway_url=gateway_url)
        self.checker = BarrierChecker(self.manager)
        self.writer = LabelWriter(output_path=output_path)
        self._running = False

    async def run(self) -> None:
        """Run all components concurrently."""
        import asyncio

        self._running = True

        logger.info("Starting watch service")

        # Run consumer, poller, and checker loop concurrently
        await asyncio.gather(
            self.consumer.run(),
            self.poller.run(),
            self._check_and_write_loop(),
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
                logger.error("Check/write error", error=str(e))

            await asyncio.sleep(60)  # Check every minute

    def stop(self) -> None:
        """Stop all components."""
        self._running = False
        self.consumer.stop()
        self.poller.stop()
        self.writer.flush()
        logger.info("Watch service stopped")

    def get_stats(self) -> dict[str, Any]:
        """Get service statistics."""
        return {
            "watches": self.manager.get_stats(),
            "buffer_size": len(self.writer._buffer),
        }


def run_watch_service(
    redis_url: str = "redis://localhost:6379",
    gateway_url: str = DEFAULT_GATEWAY_URL,
    output_path: str | None = None,
) -> None:
    """CLI entry point for watch service.

    Args:
        redis_url: Redis connection URL
        gateway_url: Data Gateway URL
        output_path: Path for Gold output
    """
    import asyncio

    import redis

    r = redis.from_url(redis_url)
    path = Path(output_path) if output_path else None

    service = WatchService(r, gateway_url=gateway_url, output_path=path)

    try:
        asyncio.run(service.run())
    except KeyboardInterrupt:
        service.stop()


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
