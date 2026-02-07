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

logger = structlog.get_logger(__name__)


# Field mappings: payload field -> Silver schema field
# These handle naming differences between provider payloads and our schema
FIELD_MAPPINGS: dict[str, dict[str, str]] = {
    # Options Flow
    "flow_alerts": {
        "price": "contract_px",
        "underlying_price": "spot_px",
        "option_chain": "occ_symbol",
        "symbol": "underlying",
        "alert_rule": "alert_type",
        "ticker": "underlying",
    },
    # Darkpool
    "darkpool": {
        "symbol": "underlying",
        "exchange": "venue",
        "tracking_id": "print_id",
        "ask": "nbbo_ask",
        "bid": "nbbo_bid",
    },
    # Market Tide
    "market_tide": {
        "net_call_premium": "total_call_premium",
        "net_put_premium": "total_put_premium",
    },
    # Bars (Alpaca)
    "bars": {
        "t": "bar_start_ts",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "n": "trade_count",
        "vw": "vwap",
    },
    # Quotes (Alpaca)
    "quotes": {
        "bp": "bid_px",
        "bs": "bid_sz",
        "ap": "ask_px",
        "as": "ask_sz",
        "bx": "bid_exchange",
        "ax": "ask_exchange",
    },
    # Trades (Alpaca)
    "trades": {
        "p": "price",
        "s": "size",
        "x": "exchange",
        "i": "trade_id",
        "z": "tape",
    },
    # Insider Trades
    "insider_trades": {
        "transaction_date": "ts_event",
        "name": "insider_name",
        "title": "insider_title",
        "type": "transaction_type",
        "shares": "shares",
        "price_per_share": "price",
        "total_value": "value",
    },
    # Institution Holdings
    "institution_holdings": {
        "institution": "institution_name",
        "cik": "institution_id",
        "shares": "shares",
        "value": "market_value",
        "percent_portfolio": "portfolio_pct",
        "percent_outstanding": "outstanding_pct",
    },
    # Politician Trades
    "politician_trades": {
        "politician": "politician_name",
        "transaction_date": "ts_event",
        "type": "transaction_type",
        "amount": "amount_range",
        "asset": "asset_type",
    },
    # Congress Trades
    "congress_trades": {
        "representative": "representative_name",
        "transaction_date": "ts_event",
        "type": "transaction_type",
        "amount": "amount_range",
        "asset": "asset_type",
    },
    # Stock Fundamentals
    "stock_fundamentals": {
        "market_cap": "market_cap",
        "pe_ratio": "pe_ratio",
        "dividend_yield": "dividend_yield",
    },
    # News
    "news": {
        "headline": "headline",
        "summary": "summary",
        "url": "url",
        "author": "author",
        "published_at": "ts_event",
    },
    # Earnings
    "earnings": {
        "report_date": "ts_event",
        "quarter": "fiscal_quarter",
        "year": "fiscal_year",
        "eps_estimate": "eps_estimate",
        "eps_actual": "eps_actual",
        "revenue_estimate": "revenue_estimate",
        "revenue_actual": "revenue_actual",
    },
}


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

    async def transform_all(
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
                feed_stats = await self._transform_feed(feed_dir, feed, since, until)
                stats[feed] += feed_stats

        logger.info("Transformation complete", stats=dict(stats))
        return dict(stats)

    async def transform(
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
            return await self._transform_date_partition(feed_dir, feed, dt)
        else:
            # Transform entire feed with optional date filtering
            return await self._transform_feed(feed_dir, feed, since, until)

    async def _transform_feed(
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

            total += await self._transform_date_partition(feed_dir, feed, dt_str)

        return total

    async def _transform_date_partition(
        self,
        feed_dir: Path,
        feed: str,
        dt: str,
    ) -> int:
        """Transform a single date partition."""
        dt_dir = feed_dir / f"dt={dt}"
        if not dt_dir.exists():
            return 0

        records: list[dict] = []
        files_processed = 0

        # Process all hour directories and files
        for item in dt_dir.rglob("*.jsonl.gz"):
            records.extend(self._read_bronze_file(item, feed))
            files_processed += 1

            # Flush in batches
            if len(records) >= self.batch_size:
                await self._write_silver_batch(records, feed, dt)
                records = []

        # Final flush
        if records:
            await self._write_silver_batch(records, feed, dt)

        logger.info(
            "Transformed partition",
            feed=feed,
            dt=dt,
            files=files_processed,
            records=len(records),
        )
        return len(records)

    def _read_bronze_file(self, file_path: Path, feed: str) -> list[dict]:
        """Read and transform a single Bronze JSONL.gz file."""
        records = []

        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        event_dict = json.loads(line.strip())
                        envelope = EventEnvelope.model_validate(event_dict)
                        row = self._envelope_to_silver_row(envelope, feed)
                        if row:
                            records.append(row)
                    except Exception as e:
                        logger.debug("Failed to parse line", error=str(e))
                        continue

        except Exception as e:
            logger.error("Failed to read Bronze file", path=str(file_path), error=str(e))

        return records

    def _envelope_to_silver_row(self, envelope: EventEnvelope, feed: str) -> dict[str, Any] | None:
        """Convert EventEnvelope to Silver row format."""
        if feed not in SILVER_SCHEMAS:
            logger.debug("No schema for feed, skipping", feed=feed)
            return None

        # Base columns from envelope
        row = {
            "event_id": envelope.event_id,
            "provider": envelope.provider,
            "feed": envelope.feed,
            "instrument_type": envelope.instrument_type,
            "instrument_key": envelope.instrument_key,
            "symbol": envelope.symbol,
            "ts_event": envelope.ts_event,
            "ts_ingest": envelope.ts_ingest,
            "ts_available": envelope.ts_available or envelope.ts_ingest,
            "source": envelope.source,
            "schema_version": envelope.schema_version,
            "quality_flags": envelope.quality_flags,
        }

        # Map payload fields using field mappings
        payload = envelope.payload
        mappings = FIELD_MAPPINGS.get(feed, {})
        schema = SILVER_SCHEMAS[feed]

        for field in schema:
            if field.name in row:
                continue  # Already set from envelope

            # Try mapped name first, then direct name
            source_name = next((k for k, v in mappings.items() if v == field.name), field.name)
            value = payload.get(source_name)

            # Type coercion
            if value is not None:
                value = self._coerce_value(value, field.type)

            row[field.name] = value

        return row

    def _coerce_value(self, value: Any, arrow_type: pa.DataType) -> Any:
        """Coerce a value to match the expected Arrow type."""
        if value is None:
            return None

        try:
            if pa.types.is_floating(arrow_type):
                return float(value) if value != "" else None
            elif pa.types.is_integer(arrow_type):
                return int(float(value)) if value != "" else None
            elif pa.types.is_date(arrow_type):
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    return datetime.strptime(value[:10], "%Y-%m-%d").date()
                return value
            elif pa.types.is_timestamp(arrow_type):
                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    return datetime.fromisoformat(value.replace("Z", "+00:00"))
                if isinstance(value, int | float):
                    return datetime.fromtimestamp(value)
                return value
            elif pa.types.is_boolean(arrow_type):
                return bool(value)
            elif pa.types.is_list(arrow_type):
                return list(value) if value else []
            else:
                return str(value) if value is not None else None
        except Exception:
            return None

    async def _write_silver_batch(
        self,
        records: list[dict],
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


async def backfill_silver(
    since: datetime | None = None,
    until: datetime | None = None,
) -> dict[str, int]:
    """Convenience function to backfill Silver from Bronze.

    Usage:
        from heber.writer.transformer import backfill_silver
        stats = await backfill_silver(since=datetime(2026, 1, 1))
    """
    transformer = BronzeToSilverTransformer()
    return await transformer.transform_all(since=since, until=until)
