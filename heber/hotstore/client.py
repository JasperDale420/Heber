"""ClickHouse client for Hot Store queries per PRD §7.6 and §12.10.

Provides:
- Connection management
- Query methods with fallback to Silver
- Metrics for sync lag monitoring
"""

from datetime import datetime, timedelta
from typing import Any

import structlog
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

from heber.config import settings

logger = structlog.get_logger(__name__)


class HotStoreClient:
    """ClickHouse client for Hot Store queries.
    
    Per PRD §12.10.1, Hot Store supports different query modes:
    - Real-time dashboard: Hot Store only (accepts staleness)
    - Strategy signals: Hot Store with Silver fallback
    - Backtest/research: Silver only (never Hot Store)
    """

    def __init__(self, client: Client | None = None):
        self._client = client
        
    @property
    def client(self) -> Client:
        """Lazy-initialize ClickHouse client."""
        if self._client is None:
            self._client = get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
            )
        return self._client

    async def get_latest_quote(self, instrument_key: str) -> dict[str, Any] | None:
        """Get latest quote for an instrument from Hot Store.
        
        Args:
            instrument_key: Canonical instrument key (e.g., equity:AAPL)
            
        Returns:
            Latest quote or None if not found
        """
        query = """
        SELECT *
        FROM quotes_hot
        WHERE instrument_key = %(key)s
        ORDER BY ts_event DESC
        LIMIT 1
        """
        result = self.client.query(query, parameters={"key": instrument_key})
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return None

    async def get_latest_bar(
        self, 
        instrument_key: str, 
        timeframe: str = "1Min"
    ) -> dict[str, Any] | None:
        """Get latest bar for an instrument from Hot Store.
        
        Args:
            instrument_key: Canonical instrument key
            timeframe: Bar timeframe (1Min, 5Min, 1Hour, etc)
            
        Returns:
            Latest bar or None if not found
        """
        query = """
        SELECT *
        FROM bars_hot
        WHERE instrument_key = %(key)s
          AND timeframe = %(tf)s
        ORDER BY bar_start_ts DESC
        LIMIT 1
        """
        result = self.client.query(
            query, 
            parameters={"key": instrument_key, "tf": timeframe}
        )
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return None

    async def get_quotes_range(
        self,
        instrument_key: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Get quotes for a time range.
        
        Args:
            instrument_key: Canonical instrument key
            start: Start timestamp
            end: End timestamp
            
        Returns:
            List of quote records
        """
        query = """
        SELECT *
        FROM quotes_hot
        WHERE instrument_key = %(key)s
          AND ts_event >= %(start)s
          AND ts_event <= %(end)s
        ORDER BY ts_event
        """
        result = self.client.query(
            query,
            parameters={"key": instrument_key, "start": start, "end": end}
        )
        return [
            dict(zip(result.column_names, row))
            for row in result.result_rows
        ]

    async def get_trades_range(
        self,
        instrument_key: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Get trades for a time range.
        
        Args:
            instrument_key: Canonical instrument key
            start: Start timestamp
            end: End timestamp
            
        Returns:
            List of trade records
        """
        query = """
        SELECT *
        FROM trades_hot
        WHERE instrument_key = %(key)s
          AND ts_event >= %(start)s
          AND ts_event <= %(end)s
        ORDER BY ts_event
        """
        result = self.client.query(
            query,
            parameters={"key": instrument_key, "start": start, "end": end}
        )
        return [
            dict(zip(result.column_names, row))
            for row in result.result_rows
        ]

    async def get_sync_lag_seconds(self, dataset: str) -> float:
        """Get sync lag between Silver and Hot Store (PRD §12.10.1).
        
        Args:
            dataset: Dataset name (quotes, trades, bars)
            
        Returns:
            Lag in seconds (Hot Store should be ≤300s behind Silver)
        """
        table = f"{dataset}_hot"
        query = f"""
        SELECT 
            now() - max(ts_available) as lag_seconds
        FROM {table}
        WHERE ts_event > now() - INTERVAL 1 HOUR
        """
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0]:
            return float(result.result_rows[0][0])
        return float("inf")  # No recent data

    async def get_row_count(self, dataset: str, days: int = 7) -> int:
        """Get row count for a dataset in the retention window.
        
        Args:
            dataset: Dataset name (quotes, trades, bars)
            days: Number of days to count
            
        Returns:
            Row count
        """
        table = f"{dataset}_hot"
        query = f"""
        SELECT count()
        FROM {table}
        WHERE dt >= today() - INTERVAL {days} DAY
        """
        result = self.client.query(query)
        return result.result_rows[0][0] if result.result_rows else 0


def get_hotstore_client() -> HotStoreClient:
    """Get a HotStoreClient instance."""
    return HotStoreClient()
