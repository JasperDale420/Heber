"""Hot Store sync service per PRD §12.10.

Syncs data from event bus or Silver to Hot Store (ClickHouse).
Maintains ≤5 minute lag SLA under normal operation.
"""

from datetime import datetime, UTC
from typing import Any

import structlog

from heber.config import settings
from heber.hotstore.client import HotStoreClient, get_hotstore_client

logger = structlog.get_logger(__name__)

# Metrics counters (will be replaced with Prometheus metrics)
_metrics = {
    "rows_synced_total": 0,
    "sync_failures_total": 0,
}


class HotStoreSync:
    """Syncs data from event bus to Hot Store.
    
    Per PRD §12.10:
    - Source: event bus (preferred) or recently written Silver partitions
    - Window: rolling last N days per dataset
    - Correctness: Hot Store is read-only for queries
    """

    def __init__(self, client: HotStoreClient | None = None):
        self.client = client or get_hotstore_client()
        self._last_sync: dict[str, datetime] = {}

    async def sync_quote(self, event: dict[str, Any]) -> None:
        """Sync a quote event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing quote data
        """
        try:
            insert_query = """
            INSERT INTO quotes_hot (
                event_id, provider, feed, instrument_type, instrument_key, symbol,
                ts_event, ts_ingest, ts_available, source, schema_version,
                bid_px, bid_sz, ask_px, ask_sz, bid_exchange, ask_exchange
            ) VALUES
            """
            
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("bid_px", 0),
                payload.get("bid_sz", 0),
                payload.get("ask_px", 0),
                payload.get("ask_sz", 0),
                payload.get("bid_exchange"),
                payload.get("ask_exchange"),
            )
            
            self.client.client.insert("quotes_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="quotes", error=str(e))
            raise

    async def sync_trade(self, event: dict[str, Any]) -> None:
        """Sync a trade event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing trade data
        """
        try:
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("price", 0),
                payload.get("size", 0),
                payload.get("trade_id"),
                payload.get("exchange"),
                payload.get("tape"),
            )
            
            self.client.client.insert("trades_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="trades", error=str(e))
            raise

    async def sync_bar(self, event: dict[str, Any]) -> None:
        """Sync a bar event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing bar data
        """
        try:
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("timeframe", "1Min"),
                payload.get("bar_start_ts") or event["ts_event"],
                payload.get("open", 0),
                payload.get("high", 0),
                payload.get("low", 0),
                payload.get("close", 0),
                payload.get("volume", 0),
                payload.get("trade_count"),
                payload.get("vwap"),
            )
            
            self.client.client.insert("bars_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="bars", error=str(e))
            raise

    async def sync_event(self, event: dict[str, Any]) -> None:
        """Route an event to the appropriate sync method.
        
        Args:
            event: EventEnvelope dict
        """
        feed = event.get("feed", "")
        
        if feed == "quotes":
            await self.sync_quote(event)
        elif feed == "trades":
            await self.sync_trade(event)
        elif feed == "bars":
            await self.sync_bar(event)
        else:
            # Flow alerts, darkpool, etc. stay lake-only per PRD §7.6
            logger.debug("hot_store_skip", feed=feed, reason="lake-only dataset")

    async def get_metrics(self) -> dict[str, Any]:
        """Get sync metrics per PRD §12.10.1.
        
        Returns:
            Dict with lag, row counts, and failure stats
        """
        return {
            "rows_synced_total": _metrics["rows_synced_total"],
            "sync_failures_total": _metrics["sync_failures_total"],
            "quotes_lag_seconds": await self.client.get_sync_lag_seconds("quotes"),
            "trades_lag_seconds": await self.client.get_sync_lag_seconds("trades"),
            "bars_lag_seconds": await self.client.get_sync_lag_seconds("bars"),
        }
