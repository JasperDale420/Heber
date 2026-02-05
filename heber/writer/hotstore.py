"""Compatibility facade for legacy writer.hotstore imports.

Hot Store sync/read/write logic now lives in `heber.hotstore.sync`.
This module preserves the prior import path while using the unified
implementation and `clickhouse-connect` client stack.
"""

from __future__ import annotations

from heber.hotstore.sync import (
    HotStoreReader,
    HotStoreSync,
    HotStoreSyncConfig,
    HotStoreTable,
    QueryType,
    SyncState,
    create_hot_store_syncer,
)

# Backward-compatible aliases
HotStoreWriter = HotStoreSync
HotStoreSyncer = HotStoreSync

__all__ = [
    "QueryType",
    "HotStoreTable",
    "HotStoreSyncConfig",
    "SyncState",
    "HotStoreSync",
    "HotStoreWriter",
    "HotStoreSyncer",
    "HotStoreReader",
    "create_hot_store_syncer",
]
