# Hot Store (ClickHouse)

Hot Store provides low-latency access to recent quotes, trades, and bars for dashboards and signals.

## Components

- Client: `heber/hotstore/client.py`
- Sync helper: `heber/hotstore/sync.py`
- Table definitions: `heber/hotstore/tables.py`

## Tables

Expected tables:

- `quotes_hot`
- `trades_hot`
- `bars_hot`

Retention is enforced by ClickHouse TTLs (see table definitions).

## Sync Behavior

`HotStoreSync` accepts `EventEnvelope` dicts and inserts rows into Hot Store:

- Quotes: `feed == "quotes"`
- Trades: `feed == "trades"`
- Bars: `feed == "bars"`
- All other feeds are lake-only (skip)

`get_metrics()` returns sync lag and counters; `HotStoreClient.get_sync_lag_seconds()` uses `ts_available` to compute lag.

## Deployment

Hot Store containers run in `docker-compose.yml` (ClickHouse only). The sync service is not currently started by default.

To deploy a sync process, run a service wrapper that reads events from Redis Streams or recent Silver partitions and calls `HotStoreSync.sync_event()` per event. This repo provides the sync logic, not a long-running service.

## Query Patterns

Hot Store is intended for:

- Real-time dashboards (Hot Store only)
- Strategy signals (Hot Store with Silver fallback)

Backtests and research should use Silver/Gold only.
