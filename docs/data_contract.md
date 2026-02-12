# Data Contract

Canonical event and dataset contract for Heber ingestion and storage.

## Plain-English Terms

- **Feed**: event type label (example: `bars`, `flow_alerts`)
- **Bronze**: raw, always-write storage
- **Silver**: strict normalized schema used for analytics and ML training
- **Alias**: incoming feed name remapped to canonical Silver dataset name
- **DLQ**: dead-letter queue for events kept in Bronze but blocked from Silver

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
- Gold (SDK writer): Parquet partitioned by `dataset={name}/project={name}/version={version}/dt={date}`
- Gold (label writer): Parquet partitioned by `dataset={name}/type=label/version={version}`

Ingestion policy:

- Bronze-first: every valid envelope is persisted to Bronze before Silver normalization.
- Silver-strict: only contracted raw feeds with a valid canonical key are written to Silver.
- Uncontracted feed policy: write Bronze, emit DLQ reason `uncontracted_feed`, and skip Silver write.
- Unmapped feed policy: for contracted feeds that do not map to a Silver schema, write Bronze, emit DLQ reason `unmapped_feed`, and skip Silver write.

## Data Gateway Feed Coverage

Training-feed raw contract (stream + UW poller + backfill):

- `bars -> bars`
- `quotes -> quotes`
- `trades -> trades`
- `news -> news`
- `flow_alerts -> flow_alerts`
- `darkpool -> darkpool`
- `market_tide -> market_tide`
- `sector_tide -> sector_tide`
- `greek_exposure -> greek_exposure`
- `iv_rank -> iv_rank`
- `oi_change -> oi_change`
- `historic_option_volume -> historic_option_volume`
- `short_interest -> short_data`
- `short_volume -> short_data`
- `ftds -> ftd`
- `congress_trades -> congress_trades`
- `insider_trades -> insider_trades`
- `option_trades -> trades`
- `crypto_bars -> bars`
- `crypto_trades -> trades`
- `ticker_flow -> flow_alerts`
- `darkpool_ticker -> darkpool`
- `institutions -> institution_holdings`
- `earnings -> earnings`

Feed aliases are defined in `heber/writer/ingest_contracts.py`.

## Silver Schemas (Parquet Writer)

Source: `heber/schemas/silver.py`

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

### historic_option_volume

- `hov_date`, `expiry`, `volume`, `open_interest`, `call_volume`, `put_volume`, `premium`

### Key Synthesis Rules

Key normalization and synthesis for Silver path is implemented in `heber/writer/key_normalization.py`:

- `flow_alerts`: derive OCC key from `option_chain` when present, otherwise synthesize from `symbol/expiry/put_call/strike`.
- `market_tide`: normalize to ETF proxy key `equity:SPY`.
- `sector_tide`: normalize sector names to ETF proxies (`XLK`, `XLF`, `XLY`, `XLC`, `XLV`, `XLI`, `XLP`, `XLE`, `XLB`, `XLRE`, `XLU`), fallback `SPY`.
- `congress_trades` and `insider_trades`: derive symbol from `ticker` when `symbol` is missing.
- `news`: derive symbol from `symbols[0]` when `symbol` is missing.

## Gold Datasets

Gold writes require:

- `instrument_key`, `ts_event`, `ts_available`

SDK path convention:

- `gold/dataset={dataset}/project={project}/version={version}/dt={YYYY-MM-DD}/part-*.parquet`

Label path convention:

- `gold/dataset={dataset}/type=label/version={version}/data.parquet`

Partition key for SDK writer: `dt` derived from `ts_event`. The SDK enforces `ts_available >= ts_event`.

## SDK Semantics

- `read_silver()`: reads Parquet from local filesystem.
- `read_asof()`: enforces `ts_available <= asof_time`.
- `write_gold()`: enforces zero-leakage and writes partitioned Parquet.
- `read_gold_versioned()`: resolves via lakeFS tags if configured, otherwise filesystem.
