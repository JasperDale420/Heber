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

## Known Coverage Gaps

Permanent, non-recoverable Silver gaps. These are documented so downstream
consumers treat them as source-data limitations, not pipeline bugs.

| Feed | Missing partitions | Cause | Recoverable? |
|------|--------------------|-------|--------------|
| `iv_rank` | `dt=2026-07-20`, `dt=2026-07-21` | The 07-20/07-21 EOD publishes were evicted from the live stream (overload). The `/api/stock/{sym}/iv-rank` endpoint returns HTTP 422 for a historical `date` param, so the client falls back to the **current** snapshot (`gateway/providers/uw/options.py:383`). A re-pull can only ever produce today's value — verified: a 2026-07-22 backfill re-landed iv_rank only under `dt=2026-07-22`, never 07-20/07-21. | **No** — permanent |
| `iv_term_structure` | `dt=2026-07-20`, `dt=2026-07-21` | Same eviction event. The provider method takes **no date param** — snapshot-only, returns the current term structure regardless of the requested range (`gateway/core/backfill.py:248`). The lost dates cannot be re-fetched. | **No** — permanent |

| `option_chain_snapshot` | `dt=2026-08-06`, `dt=2026-08-07` | Two-day hole between `dt=2026-08-05` and `dt=2026-08-10`; Silver `quotes` has the same hole while every other feed kept ingesting. | **No** — permanent |

Only 07-20 and 07-21 are lost for the IV feeds; both resume from the next daily snapshot.
Other feeds hit by the same overload (`oi_change`, `darkpool`,
`greek_exposure`, `historic_option_volume`, `short_data`, `ftd`) **are**
recoverable — their endpoints accept a `date_str` param (or are trailing-series
pulls), so per-date backfill restores them. See
`docs/operations/postmortem-2026-07-19-power-outage.md` and the
2026-07-21 CHANGELOG entries for the overload remediation.

### Gold `meta_label_features`: Greeks are not reconstructable for past dates

Option Greeks (`delta`, `gamma`, `theta`, `vega`, `iv`) and the `iv_rank`, `gex`,
`vex`, `max_pain_*` and `market_tide_*` enrichments are captured live at alert
arrival. **No historical source exists for any of them:**

- The gateway's `/alpaca/options/chain/{underlying}` route accepts only
  expiration and strike filters — no as-of parameter — and is backed by Alpaca's
  live-only `/v1beta1/options/snapshots/{underlying}`.
- The genuinely historical option routes (`/options/{contract}/bars`,
  `/options/{contract}/quotes/historical`, `/options/trades`) carry NBBO and
  OHLC only, with no Greeks and no IV.
- Heber's own `option_chain_snapshot` feed does carry per-contract Greeks inside
  `chain_json`, but it covers only SPY/QQQ/IWM (≈3.6% of alerted underlyings on
  a representative day) and has no data at all for 2026-08-06 / 2026-08-07.

Consequently a backfill of a past date **cannot** produce point-in-time Greeks.
`AlertFeatureExtractor` refuses to run those enrichments beyond
`HEBER_WATCH_LIVE_ENRICHMENT_MAX_AGE_MINUTES` (default 60) and writes the row
with those fields null, flagged in `quality_flags`:

| Flag | Meaning |
|------|---------|
| `enrichment_skipped_stale` | The alert was older than the bound, so **every** live-only step was skipped: Greeks, `iv_rank`, `gex`/`vex`, `max_pain_*`, `market_tide_*`. Filter on this flag when the model consumes any of those beyond Greeks. |
| `greeks_no_point_in_time_source` | Greeks specifically are null because no as-of source exists for this alert's timestamp. `MetaLabelDatasetBuilder` **drops these rows by default** (`DatasetConfig.include_unrecoverable_greeks=False`). |
| `market_tide_recovered_from_silver` | `market_tide_*` was asof-joined from the Silver `market_tide` feed rather than captured live. Point-in-time correct, but derived from the stored feed rather than the provider response the live path reads. |
| `gex_recovered_from_silver` | `gex`/`vex` was asof-joined from Silver `greek_exposure`. That feed is a daily snapshot, so a recovered value has coarser grain than a live per-alert fetch. |
| `enrichment_captured_late` | The row's live-fetched enrichment (Greeks, iv_rank, max pain) was captured later than the freshness bound — `ts_available - alert_time` exceeded it. Values are real but not point-in-time. The measure spans enrichment *and* the write that followed, so this set is a **superset** of true contamination; recompute `ts_available - alert_time` to apply a stricter threshold. |
| `enrichment_provenance_unknown` | No usable `ts_available`, so capture lag cannot be measured. Applies to partitions written before Heber recorded feature write time (through 2026-03-10). |

`heber_watch_enrichment_skipped_stale_total` counts every alert the gate fires
on, so the live path degrading into flagged rows is visible rather than silent.

`gex`/`vex`/`market_tide_*` remain recoverable point-in-time from Silver
`greek_exposure` and `market_tide` via `backfill_uw_fields()`
(`scripts/backfill_features.py --fill-nulls`). `iv_rank` is not: Silver
`iv_rank` is a once-daily snapshot stamped at midnight UTC of the trade date, so
applying it to an intraday alert would itself be an intraday look-ahead.

**Rows predating this contract carry no flag.** Their write lag
(`ts_available - ts_event`) exposes the same problem: a lag of hours or days
means the Greeks on that row were fetched long after the alert. Eleven of the 94
partitions have a median lag above 12 hours (largest: `dt=2026-03-11`, 2,963
rows at ~18h; `dt=2026-03-20`, `dt=2026-03-27`, `dt=2026-07-17` at ~65h), and
partitions written before `ts_available` existed cannot be assessed at all.

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
