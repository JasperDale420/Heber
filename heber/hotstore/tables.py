"""ClickHouse table DDL for Hot Store per PRD §12.10.

Tables:
- quotes_hot: Last 7 days of quote data
- trades_hot: Last 7 days of trade data
- bars_hot: Last 30 days of bar data

Retention is managed by ClickHouse TTL, not Heber.
"""

from __future__ import annotations

import inspect

# Quotes Hot Table (PRD §12.10)
QUOTES_HOT_DDL = """
CREATE TABLE IF NOT EXISTS quotes_hot (
    -- Base columns
    event_id String,
    provider LowCardinality(String),
    feed LowCardinality(String),
    instrument_type LowCardinality(String),
    instrument_key String,
    symbol LowCardinality(String),
    ts_event DateTime64(6, 'UTC'),
    ts_ingest DateTime64(6, 'UTC'),
    ts_available DateTime64(6, 'UTC'),
    source LowCardinality(String),
    schema_version LowCardinality(String),
    quality_flags Array(String),
    lineage Nullable(String),

    -- Quote-specific
    bid_px Float64,
    bid_sz Float64,
    ask_px Float64,
    ask_sz Float64,
    bid_exchange Nullable(String),
    ask_exchange Nullable(String),

    -- Partitioning
    dt Date MATERIALIZED toDate(ts_event)
)
ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (instrument_key, ts_event)
TTL dt + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
"""

# Trades Hot Table (PRD §12.10)
TRADES_HOT_DDL = """
CREATE TABLE IF NOT EXISTS trades_hot (
    -- Base columns
    event_id String,
    provider LowCardinality(String),
    feed LowCardinality(String),
    instrument_type LowCardinality(String),
    instrument_key String,
    symbol LowCardinality(String),
    ts_event DateTime64(6, 'UTC'),
    ts_ingest DateTime64(6, 'UTC'),
    ts_available DateTime64(6, 'UTC'),
    source LowCardinality(String),
    schema_version LowCardinality(String),
    quality_flags Array(String),
    lineage Nullable(String),

    -- Trade-specific
    price Float64,
    size Float64,
    trade_id Nullable(String),
    exchange Nullable(String),
    tape Nullable(String),

    -- Partitioning
    dt Date MATERIALIZED toDate(ts_event)
)
ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (instrument_key, ts_event)
TTL dt + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192;
"""

# Bars Hot Table (PRD §12.10) - 30 day retention
BARS_HOT_DDL = """
CREATE TABLE IF NOT EXISTS bars_hot (
    -- Base columns
    event_id String,
    provider LowCardinality(String),
    feed LowCardinality(String),
    instrument_type LowCardinality(String),
    instrument_key String,
    symbol LowCardinality(String),
    ts_event DateTime64(6, 'UTC'),
    ts_ingest DateTime64(6, 'UTC'),
    ts_available DateTime64(6, 'UTC'),
    source LowCardinality(String),
    schema_version LowCardinality(String),
    quality_flags Array(String),
    lineage Nullable(String),

    -- Bar-specific
    timeframe LowCardinality(String),
    bar_start_ts DateTime64(6, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count Nullable(Int64),
    vwap Nullable(Float64),

    -- Partitioning
    dt Date MATERIALIZED toDate(bar_start_ts)
)
ENGINE = MergeTree()
PARTITION BY dt
ORDER BY (instrument_key, timeframe, bar_start_ts)
TTL dt + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
"""

# "Latest" materialized views for fast point lookups
LATEST_QUOTES_VIEW_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS quotes_latest
ENGINE = ReplacingMergeTree()
ORDER BY instrument_key
AS SELECT
    instrument_key,
    argMax(bid_px, ts_event) as bid_px,
    argMax(ask_px, ts_event) as ask_px,
    argMax(bid_sz, ts_event) as bid_sz,
    argMax(ask_sz, ts_event) as ask_sz,
    max(ts_event) as ts_event
FROM quotes_hot
GROUP BY instrument_key;
"""

LATEST_BARS_VIEW_DDL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS bars_latest
ENGINE = ReplacingMergeTree()
ORDER BY (instrument_key, timeframe)
AS SELECT
    instrument_key,
    timeframe,
    argMax(open, bar_start_ts) as open,
    argMax(high, bar_start_ts) as high,
    argMax(low, bar_start_ts) as low,
    argMax(close, bar_start_ts) as close,
    argMax(volume, bar_start_ts) as volume,
    max(bar_start_ts) as bar_start_ts
FROM bars_hot
GROUP BY instrument_key, timeframe;
"""


def _execute_statement(client, statement: str):
    """Execute DDL statement against sync or async client interfaces."""
    for method_name in ("command", "execute", "query"):
        method = getattr(client, method_name, None)
        if callable(method):
            return method(statement)
    raise AttributeError("Hot Store client must expose command(), execute(), or query()")


def create_all_tables(client) -> None:
    """Create all Hot Store tables and views using a synchronous client."""
    statements = [
        QUOTES_HOT_DDL,
        TRADES_HOT_DDL,
        BARS_HOT_DDL,
        LATEST_QUOTES_VIEW_DDL,
        LATEST_BARS_VIEW_DDL,
    ]

    for stmt in statements:
        result = _execute_statement(client, stmt)
        if inspect.isawaitable(result):
            # Close coroutine objects to avoid un-awaited coroutine warnings on the sync guard path.
            close = getattr(result, "close", None)
            if callable(close):
                close()
            raise TypeError("create_all_tables() received an async client result. Use create_all_tables_async().")


async def create_all_tables_async(client) -> None:
    """Create all Hot Store tables and views using an async-capable client."""
    statements = [
        QUOTES_HOT_DDL,
        TRADES_HOT_DDL,
        BARS_HOT_DDL,
        LATEST_QUOTES_VIEW_DDL,
        LATEST_BARS_VIEW_DDL,
    ]

    for stmt in statements:
        result = _execute_statement(client, stmt)
        if inspect.isawaitable(result):
            await result
