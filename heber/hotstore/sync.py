"""Hot Store sync/service utilities.

This module is the single Hot Store write/sync implementation. It uses
`clickhouse-connect` through `HotStoreClient` and offers async-compatible
wrappers for event-driven callers.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

from heber.config import settings
from heber.hotstore.client import HotStoreClient, get_hotstore_client
from heber.hotstore.tables import create_all_tables

logger = structlog.get_logger(__name__)

_metrics = {
    "rows_synced_total": 0,
    "sync_failures_total": 0,
}


class QueryType(str, Enum):
    """Query behavior profile per PRD §12.10.1."""

    REALTIME_DASHBOARD = "realtime_dashboard"
    STRATEGY_SIGNALS = "strategy_signals"
    BACKTEST_RESEARCH = "backtest_research"


class HotStoreTable(str, Enum):
    """Canonical Hot Store table names."""

    QUOTES = "quotes_hot"
    TRADES = "trades_hot"
    BARS = "bars_hot"


@dataclass
class HotStoreSyncConfig:
    """Sync configuration for Hot Store workflows."""

    rolling_window_days: int = 7
    max_lag_seconds: float = 300.0
    sync_interval_seconds: float = 30.0
    silver_base_path: str = str(settings.silver_path)
    event_batch_max_rows: int = 250
    event_batch_max_wait_seconds: float = 1.0


@dataclass
class SyncState:
    """Sync state for a dataset."""

    dataset: str
    last_sync: datetime | None = None
    rows_synced: int = 0


class HotStoreSync:
    """Hot Store writer/sync service.

    Supports:
    - Event-based writes (quote/trade/bar payloads)
    - Batch writes from flat or payload-style records
    - Silver catch-up sync loops
    """

    def __init__(
        self,
        client: HotStoreClient | None = None,
        config: HotStoreSyncConfig | None = None,
    ):
        self.client = client or get_hotstore_client()
        self.config = config or HotStoreSyncConfig()
        self._running = False
        self._sync_state: dict[str, SyncState] = {}
        self._event_buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._event_buffer_started_at: dict[str, datetime] = {}

    @staticmethod
    def get_table_for_dataset(dataset: str) -> HotStoreTable:
        name = dataset.lower()
        if "quote" in name:
            return HotStoreTable.QUOTES
        if "trade" in name:
            return HotStoreTable.TRADES
        return HotStoreTable.BARS

    @staticmethod
    def _dataset_name_for_table(table: HotStoreTable) -> str:
        if table == HotStoreTable.QUOTES:
            return "quotes"
        if table == HotStoreTable.TRADES:
            return "trades"
        return "bars"

    @staticmethod
    def _to_utc_datetime(value: Any, fallback: datetime | None = None) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        if isinstance(value, str):
            normalized = value.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(normalized)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=UTC)
                return dt.astimezone(UTC)
            except ValueError:
                pass
        if fallback is not None:
            return fallback
        return datetime.now(UTC)

    @staticmethod
    def _columns_for_table(table: HotStoreTable) -> list[str]:
        common = [
            "event_id",
            "provider",
            "feed",
            "instrument_type",
            "instrument_key",
            "symbol",
            "ts_event",
            "ts_ingest",
            "ts_available",
            "source",
            "schema_version",
            "quality_flags",
            "lineage",
        ]
        if table == HotStoreTable.QUOTES:
            return common + ["bid_px", "bid_sz", "ask_px", "ask_sz", "bid_exchange", "ask_exchange"]
        if table == HotStoreTable.TRADES:
            return common + ["price", "size", "trade_id", "exchange", "tape"]
        return common + ["timeframe", "bar_start_ts", "open", "high", "low", "close", "volume", "trade_count", "vwap"]

    @staticmethod
    def _normalize_quality_flags(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    @staticmethod
    def _normalize_lineage(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        try:
            return json.dumps(value, sort_keys=True, default=str)
        except TypeError:
            return str(value)

    def _build_row(self, table: HotStoreTable, record: dict[str, Any]) -> tuple[Any, ...]:
        payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
        ts_event = self._to_utc_datetime(record.get("ts_event"), datetime.now(UTC))
        ts_ingest = self._to_utc_datetime(record.get("ts_ingest"), ts_event)
        ts_available = self._to_utc_datetime(record.get("ts_available"), ts_ingest)

        instrument_key = record.get("instrument_key") or payload.get("instrument_key") or ""
        symbol = record.get("symbol") or payload.get("symbol") or record.get("underlying") or ""
        provider = record.get("provider") or payload.get("provider") or ""
        feed = record.get("feed") or payload.get("feed") or self._dataset_name_for_table(table)
        instrument_type = record.get("instrument_type") or payload.get("instrument_type") or "equity"
        source = record.get("source") or payload.get("source") or "unknown"
        schema_version = record.get("schema_version") or payload.get("schema_version") or "v1"
        quality_flags = self._normalize_quality_flags(record.get("quality_flags", payload.get("quality_flags")))
        lineage = self._normalize_lineage(record.get("lineage", payload.get("lineage")))
        event_id = record.get("event_id") or payload.get("event_id") or ""

        common = (
            event_id,
            provider,
            feed,
            instrument_type,
            instrument_key,
            symbol,
            ts_event,
            ts_ingest,
            ts_available,
            source,
            schema_version,
            quality_flags,
            lineage,
        )

        if table == HotStoreTable.QUOTES:
            return common + (
                payload.get("bid_px", record.get("bid_px", 0.0)),
                payload.get("bid_sz", record.get("bid_sz", 0.0)),
                payload.get("ask_px", record.get("ask_px", 0.0)),
                payload.get("ask_sz", record.get("ask_sz", 0.0)),
                payload.get("bid_exchange", record.get("bid_exchange")),
                payload.get("ask_exchange", record.get("ask_exchange")),
            )

        if table == HotStoreTable.TRADES:
            return common + (
                payload.get("price", record.get("price", 0.0)),
                payload.get("size", record.get("size", 0.0)),
                payload.get("trade_id", record.get("trade_id")),
                payload.get("exchange", record.get("exchange")),
                payload.get("tape", record.get("tape")),
            )

        bar_start = payload.get("bar_start_ts", record.get("bar_start_ts")) or ts_event
        return common + (
            payload.get("timeframe", record.get("timeframe", "1Min")),
            self._to_utc_datetime(bar_start, ts_event),
            payload.get("open", record.get("open", 0.0)),
            payload.get("high", record.get("high", 0.0)),
            payload.get("low", record.get("low", 0.0)),
            payload.get("close", record.get("close", 0.0)),
            payload.get("volume", record.get("volume", 0.0)),
            payload.get("trade_count", record.get("trade_count")),
            payload.get("vwap", record.get("vwap")),
        )

    def ensure_tables(self) -> None:
        """Ensure Hot Store tables/views exist."""
        create_all_tables(self.client.client)

    def ensure_table(self, _table: HotStoreTable | None = None) -> None:
        """Backwards-compatible alias used by legacy call sites."""
        self.ensure_tables()

    def write_batch(self, dataset: str, records: list[dict[str, Any]]) -> int:
        """Write batch records to the matching Hot Store table."""
        if not records:
            return 0

        table = self.get_table_for_dataset(dataset)
        columns = self._columns_for_table(table)
        rows = [self._build_row(table, record) for record in records]

        self.client.client.insert(table.value, rows, column_names=columns)
        rows_written = len(rows)
        _metrics["rows_synced_total"] += rows_written

        state = self._sync_state.setdefault(dataset, SyncState(dataset=dataset))
        state.last_sync = datetime.now(UTC)
        state.rows_synced += rows_written
        return rows_written

    def _flush_event_buffer(self, dataset: str) -> int:
        records = self._event_buffers.get(dataset, [])
        if not records:
            return 0

        rows_written = self.write_batch(dataset, records)
        self._event_buffers[dataset] = []
        self._event_buffer_started_at.pop(dataset, None)
        return rows_written

    def _flush_due_event_buffers(self) -> int:
        now = datetime.now(UTC)
        total_flushed = 0
        for dataset, records in list(self._event_buffers.items()):
            if not records:
                continue
            started_at = self._event_buffer_started_at.get(dataset, now)
            elapsed_seconds = (now - started_at).total_seconds()
            if elapsed_seconds >= self.config.event_batch_max_wait_seconds:
                total_flushed += self._flush_event_buffer(dataset)
        return total_flushed

    def flush(self) -> int:
        """Flush all buffered event writes."""
        total_flushed = 0
        for dataset in list(self._event_buffers):
            total_flushed += self._flush_event_buffer(dataset)
        return total_flushed

    def _buffer_event(self, dataset: str, event: dict[str, Any]) -> int:
        if dataset not in self._event_buffer_started_at:
            self._event_buffer_started_at[dataset] = datetime.now(UTC)

        buffered = self._event_buffers[dataset]
        buffered.append(event)

        if len(buffered) >= self.config.event_batch_max_rows:
            return self._flush_event_buffer(dataset)

        return self._flush_due_event_buffers()

    def get_row_count(self, dataset: str) -> int:
        """Get row count from Hot Store for a dataset."""
        table = self.get_table_for_dataset(dataset)
        return self.client.get_row_count(self._dataset_name_for_table(table))

    def sync_from_silver(
        self,
        dataset: str,
        silver_path: str | None = None,
        since: datetime | None = None,
    ) -> int:
        """Sync records from Silver parquet partitions into Hot Store."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            logger.warning("pyarrow_not_available")
            return 0

        silver_root = Path(silver_path or self.config.silver_base_path)
        dataset_dir = silver_root / f"feed={dataset}"
        if not dataset_dir.exists():
            logger.warning("silver_path_not_found", path=str(dataset_dir))
            return 0

        if since is None:
            since = datetime.now(UTC) - timedelta(days=self.config.rolling_window_days)

        total = 0
        for part in sorted(dataset_dir.glob("dt=*")):
            for pq_file in sorted(part.rglob("*.parquet")):
                table = pq.read_table(pq_file)
                records = table.to_pylist()
                filtered: list[dict[str, Any]] = []
                for record in records:
                    ts_event = self._to_utc_datetime(record.get("ts_event"))
                    if ts_event >= since:
                        filtered.append(record)
                total += self.write_batch(dataset, filtered)
        return total

    async def run_sync_loop(
        self,
        datasets: list[str],
        silver_base_path: str | None = None,
    ) -> None:
        """Continuously sync configured datasets from Silver."""
        self._running = True
        self.ensure_tables()

        while self._running:
            for dataset in datasets:
                try:
                    state = self._sync_state.get(dataset)
                    last_sync = state.last_sync if state else None
                    self.sync_from_silver(dataset, silver_base_path, last_sync)
                except Exception as exc:
                    _metrics["sync_failures_total"] += 1
                    logger.error("hot_store_sync_failed", dataset=dataset, error=str(exc))
            self._flush_due_event_buffers()
            await asyncio.sleep(self.config.sync_interval_seconds)

        # Best-effort flush of any event-buffered rows when loop exits.
        try:
            self.flush()
        except Exception as exc:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_buffer_flush_failed", error=str(exc))

    def stop(self) -> None:
        self._running = False
        # Stop can be called without a running loop; flush buffered events now.
        try:
            self.flush()
        except Exception as exc:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_buffer_flush_failed", error=str(exc))

    async def sync_quote(self, event: dict[str, Any]) -> None:
        self._buffer_event("quotes", event)

    async def sync_trade(self, event: dict[str, Any]) -> None:
        self._buffer_event("trades", event)

    async def sync_bar(self, event: dict[str, Any]) -> None:
        self._buffer_event("bars", event)

    async def sync_event(self, event: dict[str, Any]) -> None:
        feed = str(event.get("feed", "")).lower()
        if "quote" in feed:
            await self.sync_quote(event)
        elif "trade" in feed:
            await self.sync_trade(event)
        elif "bar" in feed:
            await self.sync_bar(event)
        else:
            logger.debug("hot_store_skip", feed=feed, reason="lake-only dataset")

    def get_metrics_snapshot(self) -> dict[str, Any]:
        """Return sync counters and lag metrics."""
        return {
            "rows_synced_total": _metrics["rows_synced_total"],
            "sync_failures_total": _metrics["sync_failures_total"],
            "quotes_lag_seconds": self.client.get_sync_lag_seconds("quotes"),
            "trades_lag_seconds": self.client.get_sync_lag_seconds("trades"),
            "bars_lag_seconds": self.client.get_sync_lag_seconds("bars"),
        }

    async def get_metrics(self) -> dict[str, Any]:
        return self.get_metrics_snapshot()


class HotStoreReader:
    """Read helper with Hot Store/Silver fallback behavior."""

    def __init__(
        self,
        syncer: HotStoreSync | None = None,
        silver_base_path: str | None = None,
    ):
        self.syncer = syncer or HotStoreSync()
        self.silver_base_path = silver_base_path or self.syncer.config.silver_base_path

    async def query(self, dataset: str, query_type: QueryType, **filters) -> list[dict[str, Any]]:
        if query_type == QueryType.BACKTEST_RESEARCH:
            return self._query_silver(dataset, **filters)

        hot = self._query_hot_store(dataset, **filters)
        if query_type == QueryType.REALTIME_DASHBOARD:
            return hot

        if hot:
            return hot
        return self._query_silver(dataset, **filters)

    def _query_hot_store(self, dataset: str, **filters) -> list[dict[str, Any]]:
        table = self.syncer.get_table_for_dataset(dataset).value
        where = ["1=1"]
        params: dict[str, Any] = {}
        if "instrument_key" in filters:
            where.append("instrument_key = %(instrument_key)s")
            params["instrument_key"] = filters["instrument_key"]
        query = f"SELECT * FROM {table} WHERE {' AND '.join(where)} ORDER BY ts_event"  # nosec B608 — table from HotStoreTable enum
        result = self.syncer.client.client.query(query, parameters=params)
        return [dict(zip(result.column_names, row, strict=False)) for row in result.result_rows]

    def _query_silver(self, dataset: str, **filters) -> list[dict[str, Any]]:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            logger.warning("pyarrow_not_available")
            return []

        dataset_dir = Path(self.silver_base_path) / f"feed={dataset}"
        if not dataset_dir.exists():
            return []

        rows: list[dict[str, Any]] = []
        for part in sorted(dataset_dir.glob("dt=*")):
            for pq_file in sorted(part.rglob("*.parquet")):
                rows.extend(pq.read_table(pq_file).to_pylist())
        if "instrument_key" in filters:
            key = filters["instrument_key"]
            rows = [row for row in rows if row.get("instrument_key") == key]
        return rows


def create_hot_store_syncer(
    silver_base_path: str | None = None,
    config: HotStoreSyncConfig | None = None,
    client: HotStoreClient | None = None,
) -> HotStoreSync:
    """Factory for compatibility with legacy writer.hotstore imports."""
    merged = config or HotStoreSyncConfig()
    if silver_base_path:
        merged.silver_base_path = silver_base_path
    return HotStoreSync(client=client, config=merged)
