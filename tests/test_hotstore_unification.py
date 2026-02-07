"""Regression tests for unified Hot Store implementation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from heber.hotstore.sync import HotStoreSync, HotStoreSyncConfig
from heber.hotstore.tables import (
    BARS_HOT_DDL,
    QUOTES_HOT_DDL,
    TRADES_HOT_DDL,
    create_all_tables,
    create_all_tables_async,
)


@dataclass
class _QueryResult:
    result_rows: list[tuple]
    column_names: list[str]


class _StubClickHouseClient:
    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[tuple], list[str]]] = []

    def command(self, statement: str) -> None:
        self.commands.append(statement)

    def insert(self, table: str, rows: list[tuple], column_names: list[str]) -> int:
        self.inserts.append((table, rows, column_names))
        return len(rows)

    def query(self, _query: str, parameters=None):  # noqa: ARG002
        return _QueryResult(result_rows=[(1,)], column_names=["count"])


class _StubAsyncClickHouseClient:
    def __init__(self):
        self.commands: list[str] = []

    async def execute(self, statement: str) -> None:
        self.commands.append(statement)


class _StubHotStoreClient:
    def __init__(self):
        self.client = _StubClickHouseClient()

    def get_sync_lag_seconds(self, _dataset: str) -> float:
        return 12.5

    def get_row_count(self, _dataset: str, days: int = 7) -> int:  # noqa: ARG002
        return 42


def test_create_all_tables_uses_sync_client_command() -> None:
    stub = _StubClickHouseClient()
    create_all_tables(stub)
    assert len(stub.commands) == 5


def test_create_all_tables_rejects_async_client_result() -> None:
    async_stub = _StubAsyncClickHouseClient()

    try:
        create_all_tables(async_stub)
    except TypeError as exc:
        assert "create_all_tables()" in str(exc)
    else:
        raise AssertionError("Expected TypeError when sync helper receives async client result")


def test_create_all_tables_async_awaits_async_client_execute() -> None:
    async_stub = _StubAsyncClickHouseClient()
    asyncio.run(create_all_tables_async(async_stub))
    assert len(async_stub.commands) == 5


def test_hotstore_sync_write_batch_uses_unified_insert_path() -> None:
    client = _StubHotStoreClient()
    syncer = HotStoreSync(client=client)

    rows_written = syncer.write_batch(
        "quotes",
        [
            {
                "event_id": "evt-1",
                "provider": "alpaca",
                "feed": "quotes",
                "instrument_type": "equity",
                "instrument_key": "equity:AAPL",
                "symbol": "AAPL",
                "payload": {"bid_px": 180.0, "ask_px": 180.1},
            }
        ],
    )

    assert rows_written == 1
    assert len(client.client.inserts) == 1
    table, rows, columns = client.client.inserts[0]
    assert table == "quotes_hot"
    assert len(rows) == 1
    assert "bid_px" in columns
    assert "ask_px" in columns
    assert "quality_flags" in columns
    assert "lineage" in columns
    row = rows[0]
    assert row[columns.index("quality_flags")] == []
    assert row[columns.index("lineage")] is None


def test_hotstore_ddl_includes_quality_and_lineage_columns() -> None:
    assert "quality_flags Array(String)" in QUOTES_HOT_DDL
    assert "lineage Nullable(String)" in QUOTES_HOT_DDL
    assert "quality_flags Array(String)" in TRADES_HOT_DDL
    assert "lineage Nullable(String)" in TRADES_HOT_DDL
    assert "quality_flags Array(String)" in BARS_HOT_DDL
    assert "lineage Nullable(String)" in BARS_HOT_DDL


def test_hotstore_sync_serializes_lineage_dict() -> None:
    client = _StubHotStoreClient()
    syncer = HotStoreSync(client=client)

    rows_written = syncer.write_batch(
        "quotes",
        [
            {
                "event_id": "evt-1",
                "provider": "alpaca",
                "feed": "quotes",
                "instrument_type": "equity",
                "instrument_key": "equity:AAPL",
                "symbol": "AAPL",
                "quality_flags": ["validated", "deduped"],
                "lineage": {"source_event_id": "raw-1"},
                "payload": {"bid_px": 180.0, "ask_px": 180.1},
            }
        ],
    )

    assert rows_written == 1
    _, rows, columns = client.client.inserts[0]
    row = rows[0]
    assert row[columns.index("quality_flags")] == ["validated", "deduped"]
    assert row[columns.index("lineage")] == '{"source_event_id": "raw-1"}'


def test_hotstore_sync_metrics_no_await_mismatch() -> None:
    client = _StubHotStoreClient()
    syncer = HotStoreSync(client=client)
    metrics = asyncio.run(syncer.get_metrics())
    assert metrics["quotes_lag_seconds"] == 12.5
    assert metrics["trades_lag_seconds"] == 12.5
    assert metrics["bars_lag_seconds"] == 12.5


def test_hotstore_event_sync_batches_before_insert() -> None:
    client = _StubHotStoreClient()
    config = HotStoreSyncConfig(event_batch_max_rows=3, event_batch_max_wait_seconds=60.0)
    syncer = HotStoreSync(client=client, config=config)

    asyncio.run(syncer.sync_quote({"event_id": "evt-1", "feed": "quotes", "instrument_key": "equity:AAPL"}))
    asyncio.run(syncer.sync_quote({"event_id": "evt-2", "feed": "quotes", "instrument_key": "equity:AAPL"}))
    assert len(client.client.inserts) == 0

    asyncio.run(syncer.sync_quote({"event_id": "evt-3", "feed": "quotes", "instrument_key": "equity:AAPL"}))
    assert len(client.client.inserts) == 1
    table, rows, _ = client.client.inserts[0]
    assert table == "quotes_hot"
    assert len(rows) == 3


def test_hotstore_event_sync_stop_flushes_pending_buffer() -> None:
    client = _StubHotStoreClient()
    config = HotStoreSyncConfig(event_batch_max_rows=100, event_batch_max_wait_seconds=60.0)
    syncer = HotStoreSync(client=client, config=config)

    asyncio.run(syncer.sync_trade({"event_id": "evt-1", "feed": "trades", "instrument_key": "equity:MSFT"}))
    assert len(client.client.inserts) == 0

    syncer.stop()
    assert len(client.client.inserts) == 1
    table, rows, _ = client.client.inserts[0]
    assert table == "trades_hot"
    assert len(rows) == 1
