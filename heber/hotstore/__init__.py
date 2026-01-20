"""Hot Store module - ClickHouse integration for real-time queries.

Per PRD §7.6 and §12.10, Hot Store provides:
- Sub-second query latency for recent data
- Rolling window cache (7 days quotes/trades, 30 days bars)
- ClickHouse manages TTL eviction

Silver is always the source of truth. Hot Store is read-only for queries.
"""

from heber.hotstore.client import HotStoreClient, get_hotstore_client
from heber.hotstore.sync import HotStoreSync
from heber.hotstore.tables import (
    QUOTES_HOT_DDL,
    TRADES_HOT_DDL,
    BARS_HOT_DDL,
    create_all_tables,
)

__all__ = [
    "HotStoreClient",
    "get_hotstore_client",
    "HotStoreSync",
    "QUOTES_HOT_DDL",
    "TRADES_HOT_DDL",
    "BARS_HOT_DDL",
    "create_all_tables",
]
