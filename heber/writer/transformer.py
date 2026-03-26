"""Bronze to Silver Transformer - Batch processing for medallion architecture.

Reads raw EventEnvelope data from Bronze layer and writes normalized
typed Parquet to Silver layer. This enables:
- Backfill Silver from historical Bronze data
- Reprocess after schema changes
- Fix Silver bugs without re-ingestion from source

Deduplication: Before writing, the transformer reads existing event_ids
from Silver partition files and skips events that are already present.
This prevents duplicates when re-running backfills over the same data.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import structlog
from pydantic import ValidationError

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import UnmappedFeedError, is_bronze_only_feed, resolve_silver_feed
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import (
    MissingRequiredFieldsError,
    enforce_required_non_null_fields,
    envelope_to_silver_row,
)
from heber.writer.utils import (
    build_silver_candidates,
    get_partition_key,
    write_silver_parquet,
)

logger = structlog.get_logger(__name__)


# Field mappings live in `heber.writer.ingest_contracts` and are consumed via
# `envelope_to_silver_row` so live and backfill follow the same normalization.


class BronzeToSilverTransformer:
    """Transforms Bronze JSONL.gz files to Silver Parquet.

    Usage:
        transformer = BronzeToSilverTransformer()
        stats = await transformer.transform_all()
        # Or transform specific feed/date:
        stats = await transformer.transform(feed="flow_alerts", dt="2026-02-05")
    """

    def __init__(
        self,
        bronze_path: Path | None = None,
        silver_path: Path | None = None,
        batch_size: int = 10_000,
    ):
        self.bronze_path = bronze_path or settings.bronze_path
        self.silver_path = silver_path or settings.silver_path
        self.batch_size = batch_size

    def transform_all(
        self,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> dict[str, int]:
        """Transform all Bronze data to Silver.

        Args:
            since: Only process files after this datetime
            until: Only process files before this datetime

        Returns:
            Dict of feed -> records transformed
        """
        stats: dict[str, int] = defaultdict(int)

        if not self.bronze_path.exists():
            logger.warning("Bronze path does not exist", path=str(self.bronze_path))
            return dict(stats)

        # Find all Bronze provider/feed directories
        for provider_dir in self.bronze_path.iterdir():
            if not provider_dir.is_dir() or not provider_dir.name.startswith("provider="):
                continue

            for feed_dir in provider_dir.iterdir():
                if not feed_dir.is_dir() or not feed_dir.name.startswith("feed="):
                    continue

                feed = feed_dir.name.split("=")[1]
                feed_stats = self._transform_feed(feed_dir, feed, since, until)
                stats[feed] += feed_stats

        logger.info("Transformation complete", stats=dict(stats))
        return dict(stats)

    def transform(
        self,
        feed: str,
        dt: str | None = None,
        provider: str = "unusual_whales",
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> int:
        """Transform a specific feed (optionally for a specific date).

        Args:
            feed: Feed name (e.g., "flow_alerts")
            dt: Optional date string "YYYY-MM-DD"
            provider: Provider name
            since: Only process files after this datetime
            until: Only process files before this datetime

        Returns:
            Number of records transformed
        """
        feed_dir = self.bronze_path / f"provider={provider}" / f"feed={feed}"

        if not feed_dir.exists():
            logger.warning("Feed directory not found", path=str(feed_dir))
            return 0

        if dt:
            # Transform specific date
            return self._transform_date_partition(feed_dir, feed, dt)
        else:
            # Transform entire feed with optional date filtering
            return self._transform_feed(feed_dir, feed, since, until)

    def _transform_feed(
        self,
        feed_dir: Path,
        feed: str,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> int:
        """Transform all data for a single feed."""
        total = 0

        for dt_dir in feed_dir.iterdir():
            if not dt_dir.is_dir() or not dt_dir.name.startswith("dt="):
                continue

            dt_str = dt_dir.name.split("=")[1]

            # Date filtering
            if since or until:
                dt_date = datetime.strptime(dt_str, "%Y-%m-%d")
                if since and dt_date < since:
                    continue
                if until and dt_date > until:
                    continue

            total += self._transform_date_partition(feed_dir, feed, dt_str)

        return total

    def _collect_existing_event_ids(self, partition_key: str) -> set[str]:
        """Read all event_ids already present in a Silver partition.

        Scans existing Parquet files in the target partition and returns
        a set of event_ids, enabling deduplication during backfill.
        """
        partition_path = self.silver_path / partition_key
        if not partition_path.exists():
            return set()

        existing_ids: set[str] = set()
        for parquet_file in partition_path.glob("*.parquet"):
            if parquet_file.name.startswith("."):
                continue
            try:
                pf = pq.ParquetFile(parquet_file)
                table = pf.read(columns=["event_id"])
                existing_ids.update(table.column("event_id").to_pylist())
            except Exception as exc:  # noqa: BLE001 — skip unreadable files
                logger.warning(
                    "backfill_dedup_read_failed",
                    file=str(parquet_file),
                    error=str(exc),
                )
        return existing_ids

    def _transform_date_partition(
        self,
        feed_dir: Path,
        feed: str,
        dt: str,
    ) -> int:
        """Transform a single date partition."""
        if is_bronze_only_feed(feed):
            logger.info(
                "Backfill skipping Silver write for Bronze-only feed",
                source_feed=feed,
                dt=dt,
            )
            return 0

        silver_feed = resolve_silver_feed(feed)
        if silver_feed is None:
            logger.warning("No schema for feed, skipping", feed=feed)
            return 0

        dt_dir = feed_dir / f"dt={dt}"
        if not dt_dir.exists():
            return 0

        buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        files_processed = 0
        total_records = 0
        skipped_duplicates = 0

        # Collect existing event_ids per partition for deduplication.
        # Lazily populated as new partition keys are encountered.
        existing_ids_cache: dict[str, set[str]] = {}

        # Process all hour directories and files
        for item in dt_dir.rglob("*.jsonl.gz"):
            for row in self._read_bronze_file(item, feed):
                # Calculate partition key for each row to match live ingestion layout
                # (e.g. handle hourly partitioning for quotes/trades)
                p_key = get_partition_key(
                    feed=silver_feed,
                    instrument_type=row.get("instrument_type", "unknown"),
                    ts_event=row["ts_event"],
                )

                # Lazy-load existing event_ids for this partition
                if p_key not in existing_ids_cache:
                    existing_ids_cache[p_key] = self._collect_existing_event_ids(p_key)

                # Skip events that already exist in Silver
                event_id = row.get("event_id")
                if event_id and event_id in existing_ids_cache[p_key]:
                    skipped_duplicates += 1
                    continue

                # Track newly written event_ids to deduplicate within the same batch
                if event_id:
                    existing_ids_cache[p_key].add(event_id)

                buffers[p_key].append(row)

                # Flush specific partition if big enough
                if len(buffers[p_key]) >= self.batch_size:
                    self._flush_buffer(buffers[p_key], p_key, silver_feed)
                    total_records += len(buffers[p_key])
                    buffers[p_key] = []

            files_processed += 1

        # Final flush
        for p_key, rows in buffers.items():
            if rows:
                self._flush_buffer(rows, p_key, silver_feed)
                total_records += len(rows)

        logger.info(
            "Transformed partition",
            source_feed=feed,
            feed=silver_feed,
            dt=dt,
            files=files_processed,
            records=total_records,
            skipped_duplicates=skipped_duplicates,
        )
        return total_records

    def _read_bronze_file(self, file_path: Path, feed: str) -> list[dict[str, Any]]:
        """Read and transform a single Bronze JSONL.gz file."""
        records: list[dict[str, Any]] = []

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        event_dict = json.loads(line.strip())
                        envelope = EventEnvelope.model_validate(event_dict)
                        # Use shared logic to explode aggregates (bars/trades)
                        candidates = build_silver_candidates(envelope, feed_override=feed)
                        for candidate in candidates:
                            row = self._envelope_to_silver_row(candidate, feed)
                            if row:
                                records.append(row)
                    except (json.JSONDecodeError, ValidationError, ValueError, KeyError) as e:
                        logger.warning("bronze_line_parse_failed", error=str(e), exc_info=True)
                        continue

        except OSError as e:
            logger.error("Failed to read Bronze file", path=str(file_path), error=str(e), exc_info=True)

        return records

    def _envelope_to_silver_row(self, envelope: EventEnvelope, feed: str) -> dict[str, Any] | None:
        """Convert EventEnvelope to Silver row format."""
        if resolve_silver_feed(feed) is None:
            logger.debug("No schema for feed, skipping", feed=feed)
            return None

        try:
            feed_scoped = envelope.model_copy(update={"feed": feed})
            normalized = normalize_envelope_for_silver(feed_scoped)
            row = envelope_to_silver_row(normalized)
            enforce_required_non_null_fields(feed=row["feed"], row=row, event_id=envelope.event_id)
            return row
        except MissingRequiredFieldsError as exc:
            logger.warning(
                "backfill_row_missing_required_fields",
                feed=feed,
                event_id=envelope.event_id,
                missing_fields=exc.missing_fields,
            )
            return None
        except UnmappedFeedError as exc:
            logger.debug("Failed to normalize Bronze row", feed=feed, error=str(exc))
            return None

    def _flush_buffer(
        self,
        rows: list[dict[str, Any]],
        partition_key: str,
        dataset: str,
    ) -> None:
        """Flush a buffer to Silver Parquet."""
        if not rows:
            return

        schema = SILVER_SCHEMAS.get(dataset)
        if not schema:
            logger.warning("No schema for feed", feed=dataset)
            return

        # Path management
        partition_path = self.silver_path / partition_key
        partition_path.mkdir(parents=True, exist_ok=True)

        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        file_path = partition_path / f"part-{ts}.parquet"

        write_silver_parquet(
            rows=rows,
            schema=schema,
            file_path=file_path,
            partition_key=partition_key,
            dataset=dataset,
        )


def backfill_silver(
    since: datetime | None = None,
    until: datetime | None = None,
) -> dict[str, int]:
    """Convenience function to backfill Silver from Bronze.

    Usage:
        from heber.writer.transformer import backfill_silver
        stats = backfill_silver(since=datetime(2026, 1, 1))
    """
    transformer = BronzeToSilverTransformer()
    return transformer.transform_all(since=since, until=until)
