"""Bronze to Silver Transformer - Batch processing for medallion architecture.

Reads raw EventEnvelope data from Bronze layer and writes normalized
typed Parquet to Silver layer. This enables:
- Backfill Silver from historical Bronze data
- Reprocess after schema changes
- Fix Silver bugs without re-ingestion from source
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.schemas.silver import SILVER_SCHEMAS
from heber.writer.ingest_contracts import UnmappedFeedError, resolve_silver_feed
from heber.writer.key_normalization import normalize_envelope_for_silver
from heber.writer.normalizer import envelope_to_silver_row

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

    def _transform_date_partition(
        self,
        feed_dir: Path,
        feed: str,
        dt: str,
    ) -> int:
        """Transform a single date partition."""
        silver_feed = resolve_silver_feed(feed)
        if silver_feed is None:
            logger.warning("No schema for feed, skipping", feed=feed)
            return 0

        dt_dir = feed_dir / f"dt={dt}"
        if not dt_dir.exists():
            return 0

        records: list[dict[str, Any]] = []
        files_processed = 0
        total_records = 0

        # Process all hour directories and files
        for item in dt_dir.rglob("*.jsonl.gz"):
            records.extend(self._read_bronze_file(item, feed))
            files_processed += 1

            # Flush in batches
            if len(records) >= self.batch_size:
                self._write_silver_batch(records, silver_feed, dt)
                total_records += len(records)
                records = []

        # Final flush
        if records:
            self._write_silver_batch(records, silver_feed, dt)
            total_records += len(records)

        logger.info(
            "Transformed partition",
            source_feed=feed,
            feed=silver_feed,
            dt=dt,
            files=files_processed,
            records=total_records,
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
                        candidates = self._build_silver_candidates(envelope, feed)
                        for candidate in candidates:
                            row = self._envelope_to_silver_row(candidate, feed)
                            if row:
                                records.append(row)
                    except Exception as e:
                        logger.debug("Failed to parse line", error=str(e))
                        continue

        except Exception as e:
            logger.error("Failed to read Bronze file", path=str(file_path), error=str(e))

        return records

    def _build_silver_candidates(self, envelope: EventEnvelope, feed: str) -> list[EventEnvelope]:
        """Build candidate envelopes for Silver writes.

        For aggregate REST payloads (bars/trades arrays), explode into one
        candidate envelope per item so backfill writes typed rows rather than
        null-heavy aggregate blobs.
        """
        payload = envelope.payload if isinstance(envelope.payload, dict) else {}
        if not isinstance(payload, dict):
            return [envelope]

        canonical_feed = resolve_silver_feed(feed)
        list_key: str | None = None
        context_keys: tuple[str, ...] = ()

        if canonical_feed == "bars":
            list_key = "bars"
            context_keys = ("symbol", "timeframe")
        elif canonical_feed == "trades":
            list_key = "trades"
            context_keys = ("symbol",)

        if list_key is None:
            return [envelope]

        raw_items = payload.get(list_key)
        if not isinstance(raw_items, list):
            return [envelope]
        if not raw_items:
            return []

        candidates: list[EventEnvelope] = []
        skipped = 0
        for idx, raw_item in enumerate(raw_items):
            if not isinstance(raw_item, dict):
                skipped += 1
                continue

            item_payload = dict(raw_item)
            for key in context_keys:
                value = payload.get(key)
                if value is not None and key not in item_payload:
                    item_payload[key] = value
            if "symbol" not in item_payload and envelope.symbol:
                item_payload["symbol"] = envelope.symbol

            item_ts_event = self._extract_item_event_timestamp(item_payload) or envelope.ts_event
            item_envelope = envelope.model_copy(
                update={
                    "event_id": f"{envelope.event_id}:{idx}",
                    "payload": item_payload,
                    "ts_event": item_ts_event,
                }
            )
            candidates.append(item_envelope)

        if skipped:
            logger.warning(
                "backfill_aggregate_payload_non_dict_items",
                event_id=envelope.event_id,
                feed=feed,
                total_items=len(raw_items),
                emitted=len(candidates),
                skipped=skipped,
            )

        return candidates

    @staticmethod
    def _extract_item_event_timestamp(payload: dict[str, Any]) -> datetime | None:
        """Parse item-level event timestamp from common provider keys."""
        raw = payload.get("timestamp") or payload.get("t") or payload.get("ts_event")
        if raw is None:
            return None
        if isinstance(raw, datetime):
            if raw.tzinfo is None:
                return raw.replace(tzinfo=UTC)
            return raw.astimezone(UTC)
        if isinstance(raw, int | float):
            epoch = float(raw)
            if epoch > 10_000_000_000:
                epoch /= 1000
            return datetime.fromtimestamp(epoch, tz=UTC)
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return None
            if text.isdigit():
                return BronzeToSilverTransformer._extract_item_event_timestamp({"timestamp": int(text)})
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        return None

    def _envelope_to_silver_row(self, envelope: EventEnvelope, feed: str) -> dict[str, Any] | None:
        """Convert EventEnvelope to Silver row format."""
        if resolve_silver_feed(feed) is None:
            logger.debug("No schema for feed, skipping", feed=feed)
            return None

        try:
            feed_scoped = envelope.model_copy(update={"feed": feed})
            normalized = normalize_envelope_for_silver(feed_scoped)
            return envelope_to_silver_row(normalized)
        except (UnmappedFeedError, ValueError) as exc:
            logger.debug("Failed to normalize Bronze row", feed=feed, error=str(exc))
            return None

    def _coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a value to match the expected Arrow type."""
        if value is None:
            return None

        try:
            if pa.types.is_floating(arrow_type):
                return float(value) if value != "" else None
            if pa.types.is_integer(arrow_type):
                return int(float(value)) if value != "" else None
            if pa.types.is_date(arrow_type):
                return self._coerce_to_date(value)
            if pa.types.is_timestamp(arrow_type):
                return self._coerce_to_timestamp(value)
            if pa.types.is_boolean(arrow_type):
                return bool(value)
            if pa.types.is_list(arrow_type):
                return list(value) if value else []
            return str(value) if value is not None else None
        except Exception:
            return None

    @staticmethod
    def _coerce_to_date(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        return value

    @staticmethod
    def _coerce_to_timestamp(value: Any) -> Any:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if isinstance(value, int | float):
            return datetime.fromtimestamp(value)
        return value

    def _write_silver_batch(
        self,
        records: list[dict[str, Any]],
        feed: str,
        dt: str,
    ) -> None:
        """Write a batch of records to Silver Parquet."""
        if not records:
            return

        schema = SILVER_SCHEMAS.get(feed)
        if not schema:
            logger.warning("No schema for feed", feed=feed)
            return

        # Build partition path
        instrument_type = records[0].get("instrument_type", "unknown")
        partition_path = self.silver_path / f"feed={feed}" / f"instrument_type={instrument_type}" / f"dt={dt}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Generate unique filename
        ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
        file_path = partition_path / f"part-{ts}.parquet"

        try:
            table = pa.Table.from_pylist(records, schema=schema)
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                row_group_size=100_000,
            )
            logger.debug("Wrote Silver batch", path=str(file_path), records=len(records))
        except Exception as e:
            logger.error("Failed to write Silver batch", error=str(e), exc_info=True)
            raise


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
