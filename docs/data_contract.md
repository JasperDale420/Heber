# Data Contract

Canonical event and dataset contract for Heber ingestion and storage.

## EventEnvelope (Canonical)

Source: `heber/models/envelope.py`

Required fields:

- `event_id` (str): idempotency hash
- `provider` (str): data provider (alpaca, unusual_whales, etc)
- `feed` (str): feed type (bars, quotes, trades, flow_alerts, etc)
- `source` (str): delivery method (`websocket` or `rest`)
- `instrument_type` (str): `equity|option|crypto|forex`
- `instrument_key` (str): canonical key (e.g., `equity:AAPL`)
- `symbol` (str): human-readable symbol
- `ts_event` (datetime): event time from provider
- `ts_ingest` (datetime): gateway receive/process time
- `payload` (dict): normalized event payload

Optional fields:

- `schema_version` (str, default `v1`)
- `lineage` (dict)
- `quality_flags` (list[str])
- `ts_available` (datetime): first safe time this record is queryable (set by Heber on write)
- `raw` (dict): original provider message (Bronze fidelity)
- `processing_delay_ms` (int, default `0`)

Zero-leakage rule: reads for training/backtests must filter `ts_available <= asof_time`.

## Instrument Key Formats

Regex validation in `validate_instrument_key()`:

- `equity:SYMBOL` (1-5 uppercase letters)
- `crypto:BASE-QUOTE` (e.g., `crypto:BTC-USD`)
- `forex:BASE-QUOTE` (e.g., `forex:EUR-USD`)
- `option:OCC:...` (OCC standard option symbol)

## Storage Layers

- Bronze: raw envelope (JSONL.gz) partitioned by `provider/feed/dt/hour`
- Silver: normalized Parquet partitioned by `feed/instrument_type/dt` (and `hour` for quotes/trades)
- Gold: Parquet partitioned by `dataset/project/version/dt`

## Silver Schemas (Parquet Writer)

Source: `heber/writer/silver.py`

All feeds include base fields:

- `event_id`, `provider`, `feed`, `instrument_type`, `instrument_key`, `symbol`
- `ts_event`, `ts_ingest`, `ts_available`
- `source`, `schema_version`, `quality_flags`

Feed-specific fields:

### bars

- `timeframe`, `bar_start_ts`, `open`, `high`, `low`, `close`, `volume`, `trade_count`, `vwap`

### quotes

- `bid_px`, `bid_sz`, `ask_px`, `ask_sz`, `bid_exchange`, `ask_exchange`

### trades

- `trade_id`, `price`, `size`, `exchange`, `tape`

### flow_alerts

- `underlying`, `occ_symbol`, `expiry`, `strike`, `put_call`
- `premium`, `volume`, `open_interest`
- `spot_px`, `contract_px`
- `alert_type`, `side`, `aggressor`

Unknown feeds are stored using `DEFAULT_SCHEMA` with a `payload_json` field.

## Gold Datasets

Gold writes require:

- `instrument_key`, `ts_event`, `ts_available`

Partition key: `dt` derived from `ts_event`. The SDK enforces `ts_available >= ts_event`.

## SDK Semantics

- `read_silver()`: reads Parquet from local filesystem.
- `read_asof()`: enforces `ts_available <= asof_time`.
- `write_gold()`: enforces zero-leakage and writes partitioned Parquet.
- `read_gold_versioned()`: resolves via lakeFS tags if configured, otherwise filesystem.
