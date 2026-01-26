"""Hot Store sync strategy for Heber per PRD §12.10.

Provides:
- ClickHouse integration for low-latency queries
- Sync from event bus or Silver partitions
- Rolling window retention (TTL managed by ClickHouse)
- Lag monitoring and alerting
- Silver fallback for missing data
"""

import asyncio
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Protocol

import structlog
from prometheus_client import Counter, Gauge, Histogram

logger = structlog.get_logger(__name__)


# Prometheus metrics per PRD §12.10.1
hot_store_lag_seconds = Gauge(
    "heber_hot_store_lag_seconds",
    "Hot Store lag behind Silver in seconds",
    ["dataset"],
)

hot_store_sync_failures = Counter(
    "heber_hot_store_sync_failures_total",
    "Hot Store sync failure count",
    ["dataset", "error_type"],
)

hot_store_sync_success = Counter(
    "heber_hot_store_sync_success_total",
    "Hot Store sync success count",
    ["dataset"],
)

hot_store_row_count = Gauge(
    "heber_hot_store_row_count",
    "Row count in Hot Store",
    ["dataset"],
)

silver_row_count = Gauge(
    "heber_silver_row_count",
    "Row count in Silver for comparison",
    ["dataset"],
)

hot_store_sync_duration = Histogram(
    "heber_hot_store_sync_duration_seconds",
    "Time to sync batch to Hot Store",
    ["dataset"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
)


class QueryType(str, Enum):
    """Query types with different Hot Store behavior per PRD §12.10.1."""

    REALTIME_DASHBOARD = "realtime_dashboard"  # Hot Store only
    STRATEGY_SIGNALS = "strategy_signals"  # Hot Store + Silver fallback
    BACKTEST_RESEARCH = "backtest_research"  # Silver only


class HotStoreTable(str, Enum):
    """ClickHouse Hot Store tables per PRD §12.10."""

    QUOTES = "quotes_hot"
    TRADES = "trades_hot"
    BARS = "bars_hot"


@dataclass
class HotStoreSyncConfig:
    """Hot Store sync configuration per PRD §12.10."""

    # ClickHouse connection
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 9000
    clickhouse_database: str = "heber"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Sync settings
    source: str = "event_bus"  # "event_bus" or "silver"
    rolling_window_days: int = 7  # Default retention window

    # TTL per PRD §12.10.1
    quotes_ttl_days: int = 7
    trades_ttl_days: int = 7
    bars_ttl_days: int = 30

    # Lag SLA
    max_lag_seconds: float = 300.0  # 5 minutes per PRD

    # Sync batching
    batch_size: int = 10000
    sync_interval_seconds: float = 30.0


@dataclass
class SyncState:
    """Tracks sync state per dataset."""

    dataset: str
    last_sync: datetime | None = None
    last_event_ts: datetime | None = None
    rows_synced: int = 0
    errors: int = 0


class ClickHouseClient(Protocol):
    """Protocol for ClickHouse client."""

    async def execute(self, query: str, params: dict | None = None) -> Any: ...
    async def insert(self, table: str, data: list[dict]) -> int: ...


class HotStoreWriter:
    """Writes data to Hot Store (ClickHouse) per PRD §12.10."""

    def __init__(
        self,
        config: HotStoreSyncConfig | None = None,
        client: ClickHouseClient | None = None,
    ):
        self.config = config or HotStoreSyncConfig()
        self._client = client
        self._sync_states: dict[str, SyncState] = {}

    def get_client(self) -> ClickHouseClient:
        """Get or create ClickHouse client."""
        if self._client is None:
            # Lazy import to avoid hard dependency
            try:
                from clickhouse_driver import Client as CHClient

                self._client = CHClient(
                    host=self.config.clickhouse_host,
                    port=self.config.clickhouse_port,
                    database=self.config.clickhouse_database,
                    user=self.config.clickhouse_user,
                    password=self.config.clickhouse_password,
                )
            except ImportError:
                logger.warning("clickhouse_driver not available, using mock")
                self._client = MockClickHouseClient()
        return self._client

    def get_table_for_dataset(self, dataset: str) -> HotStoreTable:
        """Map dataset name to Hot Store table."""
        if "quote" in dataset.lower():
            return HotStoreTable.QUOTES
        elif "trade" in dataset.lower():
            return HotStoreTable.TRADES
        else:
            return HotStoreTable.BARS

    def get_ttl_days(self, table: HotStoreTable) -> int:
        """Get TTL days for table per PRD §12.10.1."""
        if table == HotStoreTable.QUOTES:
            return self.config.quotes_ttl_days
        elif table == HotStoreTable.TRADES:
            return self.config.trades_ttl_days
        else:
            return self.config.bars_ttl_days

    async def ensure_table(self, table: HotStoreTable) -> None:
        """Create table if not exists with TTL."""
        client = self.get_client()
        ttl_days = self.get_ttl_days(table)

        # Table creation DDL per PRD requirements
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table.value} (
            event_id String,
            symbol String,
            ts_event DateTime64(9, 'UTC'),
            ts_ingest DateTime64(9, 'UTC'),
            ts_available DateTime64(9, 'UTC'),
            provider String,
            data String,  -- JSON payload
            dt Date MATERIALIZED toDate(ts_event)
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY dt
        ORDER BY (symbol, ts_event, event_id)
        TTL dt + INTERVAL {ttl_days} DAY DELETE
        SETTINGS index_granularity = 8192
        """

        try:
            await client.execute(ddl)
            logger.info("table_ensured", table=table.value, ttl_days=ttl_days)
        except Exception as e:
            logger.error("table_creation_failed", table=table.value, error=str(e))
            raise

    async def write_batch(
        self,
        dataset: str,
        records: list[dict[str, Any]],
    ) -> int:
        """Write a batch of records to Hot Store.

        Args:
            dataset: Dataset name
            records: Records to write

        Returns:
            Number of records written
        """
        if not records:
            return 0

        table = self.get_table_for_dataset(dataset)
        client = self.get_client()

        start_time = asyncio.get_event_loop().time()

        try:
            # Prepare records for ClickHouse
            prepared = []
            for record in records:
                prepared.append(
                    {
                        "event_id": record.get("event_id", ""),
                        "symbol": record.get("symbol", ""),
                        "ts_event": record.get("ts_event"),
                        "ts_ingest": record.get("ts_ingest"),
                        "ts_available": record.get("ts_available"),
                        "provider": record.get("provider", ""),
                        "data": str(record.get("data", {})),
                    }
                )

            # Insert batch
            rows_written = await client.insert(table.value, prepared)

            # Update metrics
            duration = asyncio.get_event_loop().time() - start_time
            hot_store_sync_duration.labels(dataset=dataset).observe(duration)
            hot_store_sync_success.labels(dataset=dataset).inc()

            # Update sync state
            if dataset not in self._sync_states:
                self._sync_states[dataset] = SyncState(dataset=dataset)
            state = self._sync_states[dataset]
            state.last_sync = datetime.now(UTC)
            state.rows_synced += rows_written

            logger.info(
                "batch_written",
                dataset=dataset,
                rows=rows_written,
                duration_ms=duration * 1000,
            )

            return rows_written

        except Exception as e:
            hot_store_sync_failures.labels(
                dataset=dataset,
                error_type=type(e).__name__,
            ).inc()
            logger.error("write_failed", dataset=dataset, error=str(e))
            raise

    async def get_row_count(self, dataset: str) -> int:
        """Get current row count in Hot Store."""
        table = self.get_table_for_dataset(dataset)
        client = self.get_client()

        try:
            result = await client.execute(f"SELECT count() FROM {table.value}")
            count = result[0][0] if result else 0
            hot_store_row_count.labels(dataset=dataset).set(count)
            return count
        except Exception as e:
            logger.error("row_count_failed", dataset=dataset, error=str(e))
            return 0


class HotStoreSyncer:
    """Syncs data to Hot Store from event bus or Silver per PRD §12.10."""

    def __init__(
        self,
        writer: HotStoreWriter,
        config: HotStoreSyncConfig | None = None,
    ):
        self.writer = writer
        self.config = config or HotStoreSyncConfig()
        self._running = False
        self._last_sync_time: dict[str, datetime] = {}

    async def sync_from_silver(
        self,
        dataset: str,
        silver_path: str,
        since: datetime | None = None,
    ) -> int:
        """Sync data from Silver partitions to Hot Store.

        Args:
            dataset: Dataset name
            silver_path: Path to Silver directory
            since: Only sync records after this timestamp

        Returns:
            Number of records synced
        """
        from pathlib import Path

        silver_dir = Path(silver_path) / dataset
        if not silver_dir.exists():
            logger.warning("silver_path_not_found", path=str(silver_dir))
            return 0

        # Calculate window
        if since is None:
            since = datetime.now(UTC) - timedelta(days=self.config.rolling_window_days)

        total_synced = 0

        # Find partitions within window
        for dt_dir in silver_dir.glob("dt=*"):
            dt_str = dt_dir.name.replace("dt=", "")
            try:
                partition_date = datetime.fromisoformat(dt_str)
                if partition_date.replace(tzinfo=UTC) < since:
                    continue
            except ValueError:
                continue

            # Read and sync each partition
            records = self._read_partition(dt_dir)
            if records:
                synced = await self.writer.write_batch(dataset, records)
                total_synced += synced

        return total_synced

    def _read_partition(self, partition_path) -> list[dict[str, Any]]:
        """Read records from a partition."""
        try:
            import pyarrow.parquet as pq

            records = []
            for pq_file in partition_path.glob("*.parquet"):
                table = pq.read_table(str(pq_file))
                records.extend(table.to_pylist())
            return records
        except ImportError:
            logger.warning("pyarrow_not_available")
            return []
        except Exception as e:
            logger.error("partition_read_failed", path=str(partition_path), error=str(e))
            return []

    def calculate_lag(self, dataset: str, latest_event_ts: datetime) -> float:
        """Calculate lag in seconds.

        Per PRD §12.10.1: Hot Store lags Silver by ≤5 minutes
        """
        now = datetime.now(UTC)
        lag = (now - latest_event_ts).total_seconds()
        hot_store_lag_seconds.labels(dataset=dataset).set(lag)

        if lag > self.config.max_lag_seconds:
            logger.warning(
                "hot_store_lag_exceeded",
                dataset=dataset,
                lag_seconds=lag,
                max_lag=self.config.max_lag_seconds,
            )

        return lag

    async def run_sync_loop(
        self,
        datasets: list[str],
        silver_base_path: str,
    ) -> None:
        """Run continuous sync loop.

        Args:
            datasets: List of datasets to sync
            silver_base_path: Base path to Silver storage
        """
        self._running = True

        # Ensure tables exist
        for dataset in datasets:
            table = self.writer.get_table_for_dataset(dataset)
            await self.writer.ensure_table(table)

        while self._running:
            for dataset in datasets:
                try:
                    # Get last sync time
                    since = self._last_sync_time.get(dataset)

                    # Sync from Silver
                    synced = await self.sync_from_silver(
                        dataset,
                        silver_base_path,
                        since,
                    )

                    if synced > 0:
                        self._last_sync_time[dataset] = datetime.now(UTC)
                        logger.info("sync_complete", dataset=dataset, rows=synced)

                except Exception as e:
                    logger.error("sync_failed", dataset=dataset, error=str(e))

            await asyncio.sleep(self.config.sync_interval_seconds)

    def stop(self) -> None:
        """Stop the sync loop."""
        self._running = False


class HotStoreReader:
    """Reads from Hot Store with Silver fallback per PRD §12.10.1."""

    def __init__(
        self,
        writer: HotStoreWriter,
        silver_base_path: str,
    ):
        self.writer = writer
        self.silver_base_path = silver_base_path

    async def query(
        self,
        dataset: str,
        query_type: QueryType,
        **filters,
    ) -> list[dict[str, Any]]:
        """Query data based on query type per PRD §12.10.1.

        Staleness Handling:
        - REALTIME_DASHBOARD: Hot Store only (accepts staleness)
        - STRATEGY_SIGNALS: Hot Store with Silver fallback
        - BACKTEST_RESEARCH: Silver only (never Hot Store)
        """
        if query_type == QueryType.BACKTEST_RESEARCH:
            # Silver only - never use Hot Store
            return self._query_silver(dataset, **filters)

        elif query_type == QueryType.REALTIME_DASHBOARD:
            # Hot Store only - accept staleness
            return await self._query_hot_store(dataset, **filters)

        else:  # STRATEGY_SIGNALS
            # Hot Store with Silver fallback
            results = await self._query_hot_store(dataset, **filters)

            # Check for gaps and fallback to Silver
            if self._has_gaps(results):
                silver_results = self._query_silver(dataset, **filters)
                results = self._merge_results(results, silver_results)

            return results

    async def _query_hot_store(
        self,
        dataset: str,
        **filters,
    ) -> list[dict[str, Any]]:
        """Query Hot Store (ClickHouse)."""
        table = self.writer.get_table_for_dataset(dataset)
        client = await self.writer.get_client()

        # Build query
        where_clauses = []
        if "symbol" in filters:
            where_clauses.append(f"symbol = '{filters['symbol']}'")
        if "start_ts" in filters:
            where_clauses.append(f"ts_event >= '{filters['start_ts']}'")
        if "end_ts" in filters:
            where_clauses.append(f"ts_event <= '{filters['end_ts']}'")

        where = " AND ".join(where_clauses) if where_clauses else "1=1"
        query = f"SELECT * FROM {table.value} WHERE {where} ORDER BY ts_event"

        try:
            result = await client.execute(query)
            return [dict(row) for row in result] if result else []
        except Exception as e:
            logger.error("hot_store_query_failed", error=str(e))
            return []

    def _query_silver(
        self,
        dataset: str,
        **filters,
    ) -> list[dict[str, Any]]:
        """Query Silver (Parquet files)."""
        # Read from Silver partitions
        from pathlib import Path

        silver_dir = Path(self.silver_base_path) / dataset
        records = []

        for dt_dir in silver_dir.glob("dt=*"):
            try:
                import pyarrow.parquet as pq

                for pq_file in dt_dir.glob("**/*.parquet"):
                    table = pq.read_table(str(pq_file))
                    records.extend(table.to_pylist())
            except Exception:
                pass

        # Apply filters
        if "symbol" in filters:
            records = [r for r in records if r.get("symbol") == filters["symbol"]]
        if "start_ts" in filters:
            records = [r for r in records if r.get("ts_event") >= filters["start_ts"]]
        if "end_ts" in filters:
            records = [r for r in records if r.get("ts_event") <= filters["end_ts"]]

        return records

    def _has_gaps(self, results: list[dict]) -> bool:
        """Check if results have gaps that need Silver fallback."""
        if not results:
            return True
        # Simple gap detection - could be enhanced
        return len(results) == 0

    def _merge_results(
        self,
        hot_results: list[dict],
        silver_results: list[dict],
    ) -> list[dict]:
        """Merge Hot Store and Silver results, deduping by event_id."""
        seen_ids = {r.get("event_id") for r in hot_results}
        merged = list(hot_results)

        for record in silver_results:
            if record.get("event_id") not in seen_ids:
                merged.append(record)
                seen_ids.add(record.get("event_id"))

        return sorted(merged, key=lambda r: r.get("ts_event", ""))


class MockClickHouseClient:
    """Mock ClickHouse client for development without ClickHouse."""

    def __init__(self):
        self._tables: dict[str, list[dict]] = {}

    async def execute(self, query: str, params: dict | None = None) -> Any:  # noqa: ARG002
        """Execute a query."""
        if query.strip().upper().startswith("CREATE"):
            return None
        if query.strip().upper().startswith("SELECT COUNT"):
            table = query.split("FROM")[1].strip().split()[0]
            return [[len(self._tables.get(table, []))]]
        if query.strip().upper().startswith("SELECT"):
            table = query.split("FROM")[1].strip().split()[0]
            return self._tables.get(table, [])
        return None

    async def insert(self, table: str, data: list[dict]) -> int:
        """Insert data."""
        if table not in self._tables:
            self._tables[table] = []
        self._tables[table].extend(data)
        return len(data)


# Factory functions


def create_hot_store_syncer(
    silver_base_path: str = "/data/heber/silver",
    config: HotStoreSyncConfig | None = None,
) -> HotStoreSyncer:
    """Create a Hot Store syncer.

    Reads configuration from environment:
    - CLICKHOUSE_HOST
    - CLICKHOUSE_PORT
    - CLICKHOUSE_DATABASE
    - CLICKHOUSE_USER
    - CLICKHOUSE_PASSWORD
    """
    if config is None:
        config = HotStoreSyncConfig(
            clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT", "9000")),
            clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "heber"),
            clickhouse_user=os.environ.get("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        )

    writer = HotStoreWriter(config)
    return HotStoreSyncer(writer, config)
