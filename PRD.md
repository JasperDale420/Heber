# Heber Data Lakehouse PRD + Technical Specification (Hybrid)

**Owner:** Jacob\
**Doc Type:** PRD + Technical Spec (Hybrid)\
**Systems:** Data Gateway (producer), **Heber** (LakeWriter + lakehouse), Heber Catalog, Hot Store\
**Status:** Draft v0.1

---

## 1) Problem Statement

Jacob has multiple trading projects ingesting market + intelligence data from **Alpaca** and **Unusual Whales** (and more in the future). Each project currently pulls different subsets of data (bars/quotes/trades/options flow/darkpool/option greeks/chain snapshots/news, etc.). The system must:

- Store all inbound data in a **shared lake** so that projects can reuse data across projects.
- Provide a **future-proof** architecture where adding a new project or a new feed type does **not** require a schema redesign.
- Enforce **zero-leakage / point-in-time correctness** for backtests and ML workflows.

---

## 1.1 Assumptions & Constraints

- ✅ **Data Gateway is already built** and operating as the ingestion/normalization layer.
  - It already supports Alpaca + Unusual Whales and emits a normalized **EventEnvelope** with deterministic `event_id`.
  - It already has a WebSocket multiplexer for Alpaca (stocks/options/crypto/news) and supports subscribe/unsubscribe.
  - It already has validation + quality flags + idempotency behavior.
- 🚫 We are **not** redesigning Gateway core behavior in this project.
- ✅ Allowed Gateway changes: **small, backwards-compatible augmentation** to support Heber integration (e.g., adding correlation metadata, adding an optional Heber sink publisher).
- ✅ Heber is a **separate application/service** (LakeWriter + Catalog), not a feature bolted into every trading project.

---

## 2) Goals

### 2.1 Product Goals

1. **Unified storage** for all market + UW intelligence feeds.
2. **Cross-project reuse**: any project can query shared canonical datasets.
3. **Future-proof ingest**: new feeds should auto-route into storage with minimal/no new code.
4. **Zero leakage guarantee**: strictly prevent time-travel in features, labels, and training datasets.
5. **Operational reliability**: ingestion continues even if storage layers degrade.

### 2.2 Technical Goals

1. Implement a lakehouse with **Bronze / Silver / Gold** layers.
2. Introduce a universal **EventEnvelope** metadata contract.
3. Use stable **instrument\_key** and deterministic **event\_id** for idempotency.
4. Add a lightweight **Catalog** for dataset discovery and schema registry.
5. Enable **Hot Store** for real-time querying while preserving the lake as truth.

---

## 3) Non-Goals

- Building a full UI data warehouse explorer (initially).
- Guaranteeing ultra-low latency query performance from the lake itself (Hot Store covers this).
- Supporting every possible vendor/provider on day 1 (architecture must be ready, not fully implemented).

---

## 4) System Overview

### 4.1 High-Level Architecture

**Data Gateway (producer)**

- Connects to providers via WebSocket + REST
- Normalizes events into canonical models
- Emits events downstream as **EventEnvelope**

**Event Bus / Transport (recommended)**

- Redis Streams / NATS JetStream / Kafka (choose 1)
- Provides buffering + replay

**Heber (LakeWriter service)**

- Subscribes to EventEnvelope stream
- Writes to:
  - **Bronze:** raw provider payloads (append-only)
  - **Silver:** normalized canonical Parquet datasets
  - **Gold:** project-derived datasets (features/labels/signal tables)

**Heber Catalog (metadata DB)**

- Stores dataset registry + schema versions + storage paths
- Stores instrument registry and provider mappings

**Hot Store (cache / realtime)**

- ClickHouse (preferred) or TimescaleDB
- Only stores recent history and “latest” views

---

## 5) Design Principles

1. **Store by meaning, not ownership:**

   - **Never** partition the lake by project for shared market data.
   - Partition by **feed + instrument + time**.

2. **Separation of concerns:**

   - Gateway = ingestion + normalization + emission
   - Heber = storage + compaction + retention

3. **Point-in-time correctness by default:**

   - Every record must support **as-of queries**.

4. **Schema evolution is normal:**

   - Everything must be versioned.

---

## 6) Canonical Event Contract

### 6.1 EventEnvelope (required for every emitted event)

**Purpose:** universal routing, storage, idempotency, discoverability.

**Compatibility note (Gateway reality):**

- The Gateway already emits a validated EventEnvelope with deterministic `event_id`.
- Heber will **accept the Gateway envelope as-is**.
- If additional Heber-required fields are missing (ex: `ts_available`), Heber will **derive/fill** them during write.

**Fields (minimum):**

- `event_id: str` (deterministic hash)
- `provider: str` (alpaca | unusual\_whales | ...)
- `feed: str` (bars | quotes | trades | flow | darkpool | greeks | chain\_snapshots | news | ...)
- `instrument_type: str` (equity | option | crypto | forex)
- `instrument_key: str` (stable canonical)
- `symbol: str` (human-friendly)
- `ts_event: datetime` (provider event time)
- `ts_ingest: datetime` (gateway receive time)

**Optional but strongly recommended (Heber extensions):**

- `ts_available: datetime` (first safe time this record is queryable)
  - If not provided, **Heber sets**`` (time it was written successfully).
- `schema_version: str` (v1, v2, ...)
- `lineage: dict` (sequence counters, stream ids, reconnect info)
- `quality_flags: list[str]` (validated, deduped, cached, etc.)

**Correlation metadata (so projects can find “their” data without partitioning the lake by project):** Store these inside `lineage` (keeps Gateway changes backwards-compatible):

- `lineage.client_id` (API key owner / consumer identity)
- `lineage.project` (kairos | nightwatch | …)
- `lineage.request_id` (UUID per REST pull or websocket subscription)
- `lineage.subscription_id` (stable id for a streaming session)

**Payload:**

- `payload: dict` (normalized event fields)

### 6.2 instrument\_key standard

**Equity:** `equity:<SYMBOL>`\
**Crypto:** `crypto:<BASE>-<QUOTE>` (normalize provider formatting)\
**Forex:** `forex:<BASE>-<QUOTE>`\
**Option:** `option:OCC:<CONTRACT_SYMBOL>`

### 6.3 event\_id standard

`event_id = SHA256(provider|feed|instrument_key|ts_event|uniques...)`

**Unique fields rules:**

- Trades: include `trade_id` if present
- Bars: include `timeframe` + open time
- Quotes: include bid/ask px+sz + ts\_event
- Flow alerts: include underlying|expiry|strike|put\_call|premium|volume|ts\_event

### 6.4 Timestamp semantics (anti-leakage critical)

Heber and downstream systems must support **point-in-time (as-of) correctness**.

**Timestamps**

- `ts_event`: when the provider says the event occurred
- `ts_ingest`: when Gateway received the event
- `ts_commit`: when Heber successfully wrote the record to durable storage *(Heber-only internal value)*
- `ts_available`: when downstream systems are allowed to use this record for as-of queries

**Rules**

- `ts_event <= ts_ingest <= ts_commit`
- If Gateway does not supply `ts_available`, Heber sets:
  - `ts_available = ts_commit` (safe and conservative)
- Optional latency budget for realism:
  - `ts_effective = ts_available + processing_delay_ms`

### 6.5 Feed taxonomy and mapping

To avoid ambiguity, `feed` values are standardized where possible.

**Recommended canonical feeds (v1)**

- `bars`
- `quotes`
- `trades`
- `option_contracts`
- `option_chain_snapshots`
- `greeks`
- `flow_alerts`
- `darkpool_trades`
- `market_tide`
- `news_articles`
- `news_entities`

**Provider reality**

- Providers may emit different names (e.g., UW `flow`, UW `darkpool`).
- Heber must accept any string for `feed` and store it in Bronze unchanged.
- Silver dataset names are **canonical**, and mapping is recorded in Heber Catalog:
  - `gateway_feed` → `silver_dataset_name`

### 6.6 Payload requirements

**payload** must contain the normalized event fields required to query and join.

**Minimum payload expectations**

- For time-series (bars/quotes/trades): must include the numeric fields and any IDs the provider gives (trade\_id, exchange, conditions, etc.).
- For options: must include OCC symbol and/or a complete contract tuple (underlying, expiry, strike, put/call).
- For news: must include URL, headline, and publish timestamp (`ts_published`).

### 6.7 Optional raw payload capture (Bronze fidelity)

Bronze is most valuable when it contains the **original provider payload**.

**Option A (preferred): Gateway includes raw** Add an optional envelope field:

- `raw: dict | None` (original provider message)

Heber writes `raw` into Bronze and writes `payload` into Silver.

**Option B (fallback): No raw** If Gateway cannot include raw without performance impact, Bronze stores:

- the EventEnvelope + normalized payload only

This is still usable for research/backtests, but replay fidelity is lower.

### 6.8 EventEnvelope examples (JSON)

**A) Equity 1m bar (Alpaca)**

```json
{
  "event_id": "...",
  "provider": "alpaca",
  "feed": "bars",
  "instrument_type": "equity",
  "instrument_key": "equity:AAPL",
  "symbol": "AAPL",
  "ts_event": "2026-01-17T18:31:00Z",
  "ts_ingest": "2026-01-17T18:31:00.120Z",
  "ts_available": "2026-01-17T18:31:00.250Z",
  "schema_version": "v1",
  "lineage": {
    "project": "kairos",
    "request_id": "2b1d...",
    "subscription_id": "sub_...",
    "sequence": 81231
  },
  "quality_flags": ["validated"],
  "payload": {
    "timeframe": "1Min",
    "open": 187.12,
    "high": 187.30,
    "low": 187.10,
    "close": 187.22,
    "volume": 12034
  }
}
```

**B) Equity quote (Alpaca)**

```json
{
  "event_id": "...",
  "provider": "alpaca",
  "feed": "quotes",
  "instrument_type": "equity",
  "instrument_key": "equity:SPY",
  "symbol": "SPY",
  "ts_event": "2026-01-17T18:31:03.010Z",
  "ts_ingest": "2026-01-17T18:31:03.040Z",
  "ts_available": "2026-01-17T18:31:03.090Z",
  "schema_version": "v1",
  "payload": {
    "bid_px": 482.11,
    "bid_sz": 900,
    "ask_px": 482.12,
    "ask_sz": 600
  }
}
```

**C) Options flow alert (Unusual Whales)**

```json
{
  "event_id": "...",
  "provider": "unusual_whales",
  "feed": "flow_alerts",
  "instrument_type": "option",
  "instrument_key": "option:OCC:AAPL260116C00200000",
  "symbol": "AAPL",
  "ts_event": "2026-01-17T18:32:11Z",
  "ts_ingest": "2026-01-17T18:32:11.300Z",
  "ts_available": "2026-01-17T18:32:11.600Z",
  "schema_version": "v1",
  "payload": {
    "underlying": "AAPL",
    "expiry": "2026-01-16",
    "strike": 200,
    "put_call": "C",
    "premium": 315000,
    "volume": 1200,
    "open_interest": 5400,
    "alert_type": "SWEEP"
  }
}
```

---

## 7) Heber Lakehouse Storage Model

### 7.1 Bronze (raw)

- Immutable append-only
- Stores original provider payload alongside EventEnvelope metadata

**Recommended format:** JSONL/NDJSON + gzip

**Partitioning:**

- provider
- feed
- dt/hour

**Example path:** `bronze/provider=alpaca/feed=quotes/dt=2026-01-17/hour=18/part-0001.jsonl.gz`

### 7.2 Silver (canonical normalized)

- Canonical normalized events (queryable, joinable)

**Recommended format:** Parquet

**Partitioning:**

- feed
- instrument\_type
- dt (optionally hour for very high volume)

**Example path:** `silver/feed=trades/instrument_type=equity/dt=2026-01-17/part-0001.parquet`

### 7.3 Gold (derived datasets)

- Project-owned datasets: features, labels, signals, predictions

**Partitioning:**

- dataset
- project
- version
- dt

**Example path:** `gold/dataset=features_intraday_v3/project=kairos/version=v3/dt=2026-01-17/part-0001.parquet`

### 7.4 Partitioning & File Layout Strategy (per feed)

Heber must be optimized for two different query patterns:

- **Backtest/training scans** (wide date ranges, many symbols)
- **As-of lookups** (narrow time windows, a small symbol universe)

**Guiding rules**

- Partition by **time first** (dt/hour) for write efficiency.
- Partition by **instrument\_type** to avoid mixed schemas.
- Avoid over-partitioning by symbol (explodes file counts).
- Solve symbol selectivity using **Hot Store** and/or **bucketed files**.

#### Silver partition defaults

| Dataset                  | Default Partitions                                   | Notes                                                       |
| ------------------------ | ---------------------------------------------------- | ----------------------------------------------------------- |
| `bars`                   | `feed`, `instrument_type`, `dt`                      | Bars are low enough volume for daily partitions.            |
| `quotes`                 | `feed`, `instrument_type`, `dt`, `hour`              | Quotes are high volume; hour partition prevents huge files. |
| `trades`                 | `feed`, `instrument_type`, `dt`, `hour`              | Trades are high volume; hour partition by default.          |
| `greeks`                 | `feed`, `instrument_type`, `dt`, `hour` *(optional)* | If sampled frequently, use hour.                            |
| `option_chain_snapshots` | `feed`, `instrument_type`, `dt`                      | Snapshot cadence (5–15m) usually OK daily.                  |
| `flow_alerts`            | `feed`, `instrument_type`, `dt`                      | Volume manageable; daily is fine.                           |
| `darkpool_trades`        | `feed`, `instrument_type`, `dt`                      | Daily is fine.                                              |
| `news_articles`          | `feed`, `dt`                                         | Document stream; daily.                                     |
| `news_entities`          | `feed`, `dt`                                         | Daily.                                                      |

#### Optional: Symbol bucketing (only if needed)

If `quotes/trades` become too expensive to query from Parquet alone, add a *non-symbol* bucketing key:

- `bucket = hash(instrument_key) % N`

Partition:

- `dt/hour/bucket=0..N-1`

This avoids creating partitions per symbol, while still improving selectivity.

**Recommendation:** start without bucketing; add later when volume justifies.

### 7.5 File sizing, batching, and compaction (small-file control)

Parquet lakes die from “too many tiny files.” Heber must enforce batching and compaction.

**Write batching targets (Silver)**

- Aim for **128–512 MB** Parquet files per partition
- Flush by whichever occurs first:
  - `max_rows` (e.g., 250k–2M depending on dataset)
  - `max_bytes` (target file size)
  - `max_time` (e.g., 5–30 seconds) to bound latency

**Row group target**

- 64–256 MB row groups (tune later)

**Compaction job (Heber Compactor)**

- Runs periodically per partition (dt/hour)
- Merges small files into target-sized Parquet
- Must preserve:
  - `event_id` uniqueness (dedupe)
  - `ts_commit` / `ts_available` semantics

### 7.6 Hot Store policy (required for live systems)

The lake is the source of truth, but high-frequency queries should hit a hot cache.

**Recommended Hot Store:** ClickHouse (preferred) or TimescaleDB

**What goes into Hot Store**

- `quotes` (last 1–7 days)
- `trades` (last 1–7 days)
- `bars` (last 30–90 days)
- optional: latest greeks / latest chain snapshot pointers

**What stays lake-only**

- UW flow alerts, darkpool, news (usually fine in Parquet)
- Long-range history beyond hot retention

**Deployment priority** Hot Store is essential when any of the following is true:

- `quotes/trades` volume makes as-of queries slow (> \~1–2 seconds for common workloads)
- You need sub-second dashboards or live strategy windows
- You routinely query “last N minutes” across many symbols

---

## 8) Dataset Inventory (v1 Scope vs Planned)

This section locks down what Heber must support **now** vs what is **planned**. The goal is to ship a coherent core that covers current projects (Alpaca + UW) while remaining extensible.

### 8.1 Dataset Classification

- **V1 (Required):** must be stored, queryable, and point-in-time safe from day one.
- **V1.5 (Near-term):** needed for options intelligence + ML workflows.
- **V2 (Planned):** future enhancements (forex, advanced surfaces, etc.).

### 8.2 V1 Required Datasets (Silver)

These are the shared canonical tables all projects can depend on.

#### 8.2.1 Core Market Data (Alpaca primarily)

1. `bars` *(equity/crypto/option/forex via instrument\_type)*
2. `quotes` *(equity/crypto/option/forex)*
3. `trades` *(equity/crypto/option/forex)*

**Notes**

- Options contract bars/quotes/trades live in the same datasets, differentiated by `instrument_type=option` and `instrument_key=option:OCC:...`.
- This design allows any project to join contract-level microstructure ↔ underlying price action via `instrument_key` + mapping tables.

#### 8.2.2 Unusual Whales Intelligence (must store everything UW emits)

1. `flow_alerts`
2. `darkpool_trades`

**Notes**

- UW intelligence is long-lived and will be enriched later; do not discard.
- Enrichment outputs must be written to **Gold** to avoid leakage.

#### 8.2.3 Options Reference + Tracking (minimal v1)

1. `option_contracts` *(reference / slowly changing)*

**Notes**

- This is needed so downstream projects can resolve OCC symbols, underlyings, expiries, strikes consistently.

### 8.3 V1.5 Near-Term Datasets

These unlock more powerful option modeling + flow enrichment.

1. `greeks` *(time-series per option contract)*
2. `option_chain_snapshots` *(snapshot stream, 5–15m cadence)*
3. `market_tide` *(UW REST snapshot; periodic ingest)*

### 8.4 V2 Planned Datasets

These are optional expansions that should not require architectural changes.

1. `news_articles`
2. `news_entities`
3. `fundamentals` *(if/when sourced; revision-safe)*
4. `corporate_actions` *(splits/dividends; revision-safe)*
5. `forex_*` feeds *(no new tables required; just instrument\_type=forex)*

### 8.5 Gold Datasets (project-owned but shareable)

Gold datasets are versioned and governed. They can be consumed cross-project.

**Required patterns**

- `features_<scope>_v<version>`
- `labels_<task>_v<version>`
- `signals_<strategy>_v<version>`
- `predictions_<model>_v<version>`

**Zero-leakage constraints**

- Gold builds must record `feature_time`, `max_ts_available_used`, `code_version`.
- Builds fail if point-in-time rules are violated.

### 8.6 Dataset Summary Table (v1 oriented)

| Dataset                  | Layer  | Provider(s)  | Update Mode   | Point-in-time Gate          | v1 Priority |
| ------------------------ | ------ | ------------ | ------------- | --------------------------- | ----------- |
| `bars`                   | Silver | Alpaca       | WS + REST     | `ts_available <= T`         | Required    |
| `quotes`                 | Silver | Alpaca       | WS            | `ts_available <= T`         | Required    |
| `trades`                 | Silver | Alpaca       | WS            | `ts_available <= T`         | Required    |
| `flow_alerts`            | Silver | UW           | REST/stream   | `ts_available <= T`         | Required    |
| `darkpool_trades`        | Silver | UW           | REST/stream   | `ts_available <= T`         | Required    |
| `option_contracts`       | Silver | Alpaca/UW    | REST          | validity windows if revised | Required    |
| `greeks`                 | Silver | Alpaca/other | REST/periodic | `ts_available <= T`         | Near-term   |
| `option_chain_snapshots` | Silver | Alpaca/other | periodic      | `ts_available <= T`         | Near-term   |
| `market_tide`            | Silver | UW           | periodic      | `ts_available <= T`         | Near-term   |
| `news_articles/entities` | Silver | TBD          | periodic      | revision-safe               | Planned     |

### 8.7 v1 Silver Schema Contracts (Build-Ready)

This section defines the **column-level schema** for the v1 Required Silver datasets. These schemas are designed for:

- fast filtering by `instrument_key` + time
- safe point-in-time queries via `ts_available`
- cross-project joins with minimal ambiguity

#### 8.7.1 Shared base columns (present in *every* Silver dataset)

All Silver tables MUST include these columns.

| Column            | Type      | Required | Description                                                |
| ----------------- | --------- | -------- | ---------------------------------------------------------- |
| `event_id`        | string    | ✅        | Deterministic idempotency key (SHA256)                     |
| `provider`        | string    | ✅        | `alpaca`, `unusual_whales`, …                              |
| `feed`            | string    | ✅        | Canonical feed name (`bars`, `quotes`, …)                  |
| `instrument_type` | string    | ✅        | `equity`\|`option`\|`crypto`\|`forex`                      |
| `instrument_key`  | string    | ✅        | Stable canonical instrument key                            |
| `symbol`          | string    | ✅        | Human-friendly symbol (underlying for options)             |
| `ts_event`        | timestamp | ✅        | Provider event timestamp                                   |
| `ts_ingest`       | timestamp | ✅        | Gateway receive timestamp                                  |
| `ts_available`    | timestamp | ✅        | Earliest safe-use timestamp (anti-leakage gate)            |
| `source`          | string    | ✅        | `websocket`\|`rest`                                        |
| `schema_version`  | string    | ✅        | Dataset schema version (`v1`, `v2`, …)                     |
| `quality_flags`   | array     | ✅        | e.g., `validated`, `deduped`, `late`                       |
| `lineage`         | json/map  | ◻︎       | Optional correlation metadata (`project`, `request_id`, …) |

**Point-in-time rule (hard):** any training/backtest read at time `T` must enforce:

- `WHERE ts_available <= T`

---

#### 8.7.2 `bars` (Silver)

**Primary key (logical):** (`instrument_key`, `timeframe`, `bar_start_ts`)\
**Idempotency key:** `event_id`

| Column         | Type      | Required | Notes                                  |
| -------------- | --------- | -------- | -------------------------------------- |
| `timeframe`    | string    | ✅        | `1Min`, `5Min`, `1Hour`, …             |
| `bar_start_ts` | timestamp | ✅        | Bar start/open time (anchor for joins) |
| `open`         | float     | ✅        |                                        |
| `high`         | float     | ✅        |                                        |
| `low`          | float     | ✅        |                                        |
| `close`        | float     | ✅        |                                        |
| `volume`       | float/int | ✅        | Use float if crypto/forex              |
| `trade_count`  | int       | ◻︎       | If provider supplies                   |
| `vwap`         | float     | ◻︎       | If provider supplies                   |

**Notes**

- For “as-of” joins, prefer `bar_start_ts` as the time key.

---

#### 8.7.3 `quotes` (Silver)

**Primary key (logical):** (`instrument_key`, `ts_event`)\
**Idempotency key:** `event_id`

| Column         | Type      | Required | Notes                    |
| -------------- | --------- | -------- | ------------------------ |
| `bid_px`       | float     | ✅        |                          |
| `bid_sz`       | float/int | ✅        |                          |
| `ask_px`       | float     | ✅        |                          |
| `ask_sz`       | float/int | ✅        |                          |
| `bid_exchange` | string    | ◻︎       | If available             |
| `ask_exchange` | string    | ◻︎       | If available             |
| `conditions`   | array     | ◻︎       | Provider condition codes |

**Notes**

- `quotes` is high volume: default partitions include `hour`.

---

#### 8.7.4 `trades` (Silver)

**Primary key (logical):** (`instrument_key`, `ts_event`, `trade_id?`)\
**Idempotency key:** `event_id`

| Column       | Type      | Required | Notes                   |
| ------------ | --------- | -------- | ----------------------- |
| `trade_id`   | string    | ◻︎       | If provider supplies    |
| `price`      | float     | ✅        |                         |
| `size`       | float/int | ✅        |                         |
| `exchange`   | string    | ◻︎       |                         |
| `conditions` | array     | ◻︎       |                         |
| `tape`       | string    | ◻︎       | If equities tape exists |

---

#### 8.7.5 `flow_alerts` (Silver, Unusual Whales)

**Primary key (logical):** (`event_id`)\
**Idempotency key:** `event_id`

| Column          | Type      | Required | Notes                                      |
| --------------- | --------- | -------- | ------------------------------------------ |
| `underlying`    | string    | ✅        | Underlying symbol                          |
| `occ_symbol`    | string    | ◻︎       | If provided; else derive via tuple         |
| `expiry`        | date      | ✅        |                                            |
| `strike`        | float     | ✅        |                                            |
| `put_call`      | string    | ✅        | `P` or `C`                                 |
| `premium`       | float     | ✅        | Notional premium / \$ value                |
| `volume`        | float/int | ✅        |                                            |
| `open_interest` | float/int | ◻︎       |                                            |
| `spot_px`       | float     | ◻︎       | Underlying spot at alert time              |
| `contract_px`   | float     | ◻︎       | Fill price if supplied                     |
| `alert_type`    | string    | ✅        | `SWEEP`, `BLOCK`, …                        |
| `side`          | string    | ◻︎       | `bullish`/`bearish`/`neutral` if available |
| `aggressor`     | string    | ◻︎       | `bid`/`ask`/`mid` if available             |
| `tags`          | array     | ◻︎       | Any provider tags                          |

**Strict leakage rule**

- Any post-event outcomes (PnL, follow-through, “worked/failed”) must be written to **Gold**, never merged into this Silver dataset.

---

#### 8.7.6 `darkpool_trades` (Silver, Unusual Whales)

**Primary key (logical):** (`event_id`)\
**Idempotency key:** `event_id`

| Column       | Type      | Required | Notes                   |
| ------------ | --------- | -------- | ----------------------- |
| `underlying` | string    | ✅        |                         |
| `price`      | float     | ✅        | Print price             |
| `size`       | float/int | ✅        | Shares/contracts        |
| `notional`   | float     | ◻︎       | If precomputed          |
| `venue`      | string    | ◻︎       | ATS/venue if supplied   |
| `print_id`   | string    | ◻︎       | Provider id if supplied |
| `conditions` | array     | ◻︎       |                         |

---

#### 8.7.7 `option_contracts` (Silver, reference table)

This is the canonical options reference dataset to keep options consistent across projects.

**Primary key:** `instrument_key` (option\:OCC:...)\
**Idempotency key:** `event_id` (for updates)

| Column          | Type      | Required | Notes                        |
| --------------- | --------- | -------- | ---------------------------- |
| `occ_symbol`    | string    | ✅        | OCC formatted symbol         |
| `underlying`    | string    | ✅        |                              |
| `expiry`        | date      | ✅        |                              |
| `strike`        | float     | ✅        |                              |
| `put_call`      | string    | ✅        | `P`/`C`                      |
| `multiplier`    | int       | ◻︎       | Usually 100                  |
| `currency`      | string    | ◻︎       | Default USD                  |
| `first_seen_ts` | timestamp | ✅        | When Heber first observed it |
| `last_seen_ts`  | timestamp | ✅        | Updated when observed        |
| `status`        | string    | ◻︎       | active/expired/delisted      |

**Validity windows (if revised)** If provider revises contract metadata:

- `valid_from`, `valid_to`, `revision_id`

---

#### 8.7.8 Near-term schemas (V1.5) preview

These are included for planning and consistency (not required day-1).

`` (time-series)

- `iv`, `delta`, `gamma`, `theta`, `vega`, `rho`
- keys: `instrument_key`, `ts_event`

`` (snapshot stream)

- `underlying`, `ts_event`, `snapshot_id`
- contract-level rows preferred for queryability (one row per contract per snapshot)

`` (periodic snapshot)

- snapshot-style dataset keyed by `ts_event`

---

## 9) News Handling (No-Leakage Safe)

### 9.1 Storage

**news\_articles**

- `news_id` (hash URL+title+publish time)
- `provider`
- `ts_published`
- `ts_ingest`
- `ts_available`
- `headline`
- `summary`
- `body` (optional; subject to licensing)
- `url`
- `source_name`

**news\_entities**

- `news_id`
- `instrument_key`
- `entity_type`
- `confidence`
- `match_method` (provider\_tags | NER | keywords)

### 9.2 Revisions

If headlines/metadata/body arrive at different times, treat as revisions:

- `valid_from`, `valid_to`, `revision_id`

---

## 10) Zero-Leakage Firewall (Build-Ready Spec)

This section is the **non-negotiable guardrail** that prevents silent lookahead bias and target leakage across all projects.

### 10.1 Leakage threat model (what we are preventing)

1. **Transport/arrival leakage**
   - Using a record at time T that was only received/written later.
2. **Revision leakage**
   - Using corrected/backfilled/revised historical values that were not available at the time.
3. **Enrichment leakage**
   - Accidentally mixing “outcome” or post-event information back into Silver.
4. **Label/target leakage**
   - Labels or future-derived statistics accidentally included in features.
5. **Split leakage**
   - Overlapping windows across train/test causing information bleed.

### 10.2 Mandatory timestamps

Every Silver row MUST include:

- `ts_event`
- `ts_ingest`
- `ts_available`

**Hard rule:** at feature time **T**, a job may only use rows where:

- `ts_available <= T`

**Defaults**

- If Gateway does not provide `ts_available`, Heber sets `ts_available = ts_commit`.

### 10.3 As-of query contract (canonical semantics)

All reads used for research/backtests/ML must be performed **AS-OF** a specific time.

**Definition:**`` A dataset read is point-in-time correct if it filters:

- `WHERE ts_available <= T`

**Never allowed:** querying Silver without an ASOF cutoff for training/backtest pipelines.

### 10.4 As-of join contract (no time travel joins)

All joins across time-series datasets must use **as-of joins**.

**As-of join definition:** For a left table row at time `T_left`, join the most recent prior row from the right table such that:

- `ts_event_right <= T_left`
- `ts_available_right <= T_left`

**Tie-breaking rule:**

- If multiple rows match, choose the row with the **max(**``**)**.

### 10.5 Derived time keys (bars vs ticks)

Different feeds join on different time anchors:

- **Bars:** use `bar_start_ts` (or bar end, but must be consistent) as the join key.
- **Trades/Quotes/Greeks/Flow:** use `ts_event`.

**Rule:** feature generators must document and standardize the time anchor they use.

### 10.6 Reference tables and revisions (validity windows)

Any dataset that can change historically MUST be modeled as a **slowly changing dimension**.

**Required fields (when applicable)**

- `valid_from`
- `valid_to` (nullable)
- `revision_id`

**As-of rule for reference tables:**

- select row where `valid_from <= T AND (valid_to IS NULL OR valid_to > T)`

Applies to (now or future):

- `option_contracts` (rare but possible)
- `fundamentals`
- `corporate_actions`
- `news_articles` revisions

### 10.7 Late-arriving + backfilled data policy

Some events arrive late (reconnect gaps, REST backfills).

**Rules:**

- Silver is append-only with idempotent dedupe on `event_id`.
- Late records are allowed but MUST be tagged:
  - `quality_flags += ["late"]`

**Critical:** Late data does NOT break correctness because ASOF reads gate on `ts_available`.

### 10.8 Enrichment separation (Silver vs Gold)

To prevent “outcome leakage,” enrichments must never overwrite canonical events.

**Rule:**

- Silver datasets store *what was known at the time*.
- Gold datasets store computed enrichments and outcomes.

Examples of Gold-only fields:

- UW alert PnL / follow-through
- “worked/failed” classifications
- post-event max favorable excursion
- future returns or future vol

### 10.9 Gold dataset build gates (must fail loudly)

Every Gold dataset build MUST record the following metadata:

- `feature_time` (anchor)
- `max_ts_event_used`
- `max_ts_available_used`
- `dataset_version`
- `code_version` (git SHA)
- `input_datasets` (names + schema versions)

**Hard gates (fail build):**

- `max_ts_available_used > feature_time`
- any input dataset missing `ts_available`
- any join performed without ASOF cutoff (SDK enforces)

### 10.10 Train/test split safety (time-series specific)

For ML evaluation to be valid:

**Required**

- **Purged splits:** remove overlapping windows around the boundary
- **Embargo:** hold out an additional period after the split

**Minimum recommended defaults**

- Purge = max feature lookback window
- Embargo = max label horizon

### 10.11 Heber SDK enforcement (how we make this unavoidable)

Projects should not hand-roll ASOF logic. The SDK is the enforcement tool.

**Required primitives**

- `read_asof(dataset, asof_time, filters…)`
  - automatically applies `ts_available <= asof_time`
- `asof_join(left_df, right_df, left_time_col, right_time_col, key_cols…)`
  - joins using `ts_event_right <= left_time AND ts_available_right <= left_time`
- `build_gold(dataset_name, df, metadata)`
  - validates lineage + gates before commit

**Safe default behavior**

- If a project calls `read()` without an `asof_time` in a training context, SDK should either:
  - require an explicit override, or
  - throw an error (recommended)

### 10.12 Automated leakage tests (CI + runtime)

Heber must ship automated checks so leakage cannot creep in quietly.

**CI unit tests (SDK + pipelines)**

- As-of reads always filter `ts_available`
- As-of joins never join future rows
- Gold build fails on gate violations

**Runtime monitors**

- Percent of late-arriving events by feed
- Distribution of `(ts_available - ts_event)` by feed
- Alerts when availability lag spikes (provider issues)

### 10.13 Example: as-of query and join (pseudo-SQL)

**As-of filter**

```sql
SELECT *
FROM silver.quotes
WHERE instrument_key = 'equity:SPY'
  AND ts_event BETWEEN :t0 AND :t1
  AND ts_available <= :asof_time;
```

**As-of join (last known quote before each trade)**

```sql
-- Pseudocode pattern: join trade rows to the latest quote <= trade time
SELECT t.*, q.bid_px, q.ask_px
FROM trades t
ASOF JOIN quotes q
  ON t.instrument_key = q.instrument_key
 AND q.ts_event <= t.ts_event
 AND q.ts_available <= t.ts_event;
```

---

## 11) Catalog & Discoverability (Build-Ready)

Heber must be discoverable and self-describing so new projects can integrate without hardcoding paths or schemas.

### 11.1 Core responsibilities

The Catalog provides:

- **Dataset registry** (what exists, where it lives, how it’s partitioned)
- **Schema registry** (dataset schemas + versions)
- **Provider/feed mapping** (provider feed names → canonical Silver dataset)
- **Instrument registry** (canonical `instrument_key` + provider mappings)
- **Project correlation** (optional metadata for subscriptions/requests so producers can trace what they asked for)

### 11.2 Catalog DB (Postgres recommended)

#### 11.2.1 Tables (minimal required)

**datasets**

- `dataset_id` (uuid, pk)
- `dataset_name` (text, unique) e.g. `bars`, `quotes`
- `layer` (text) `bronze|silver|gold`
- `owner` (text) `shared|project`
- `description` (text)
- `storage_root` (text) e.g. `s3://heber/silver/` or `minio://heber/silver/`
- `path_template` (text) partition template
- `partition_cols` (jsonb) e.g. `["dt","hour","instrument_type"]`
- `primary_keys` (jsonb) logical keys
- `retention_policy` (jsonb)
- `is_active` (bool)
- `created_at`, `updated_at`

**dataset\_versions**

- `dataset_version_id` (uuid, pk)
- `dataset_name` (fk → datasets.dataset\_name)
- `schema_version` (text) e.g. `v1`
- `schema_json` (jsonb)
- `writer_min_version` (text) *(optional)*
- `reader_min_version` (text) *(optional)*
- `is_current` (bool)
- `created_at`

**feed\_mappings**

- `provider` (text) e.g. `unusual_whales`
- `gateway_feed` (text) e.g. `flow`
- `silver_dataset_name` (text) e.g. `flow_alerts`
- `notes` (text)

**instrument\_registry**

- `instrument_key` (text, pk)
- `instrument_type` (text)
- `canonical_symbol` (text) e.g. `AAPL`
- `underlying_key` (text, nullable) e.g. equity key for an option
- `occ_symbol` (text, nullable)
- `expiry` (date, nullable)
- `strike` (numeric, nullable)
- `put_call` (text, nullable)
- `multiplier` (int, nullable)
- `currency` (text, nullable)
- `created_at`, `updated_at`

**instrument\_provider\_map**

- `instrument_key` (fk)
- `provider` (text)
- `provider_symbol` (text)
- `provider_id` (text, nullable)
- `is_primary` (bool)

#### 11.2.2 Tables (recommended for scale and usability)

**data\_coverage** (fast “what data exists” lookup)

- `dataset_name`
- `instrument_key`
- `dt_min` (date)
- `dt_max` (date)
- `last_updated_ts` (timestamp)
- `approx_row_count` (bigint)

**projects**

- `project_id` (uuid, pk)
- `project_name` (text, unique) e.g. `kairos`
- `description`
- `created_at`

**requests** (REST pulls and one-shot queries)

- `request_id` (text, pk)
- `project_name` (fk)
- `provider`
- `feed`
- `params_json` (jsonb)
- `created_at`
- `status` (text)

**subscriptions** (WebSocket streams)

- `subscription_id` (text, pk)
- `project_name` (fk)
- `provider`
- `feed`
- `instrument_keys` (jsonb array)
- `started_at`
- `ended_at` (nullable)

> **Note:** Requests/subscriptions are *optional* for correctness. They exist to improve discoverability and auditability, and to let projects trace “what did I ask the Gateway to subscribe to?”.

### 11.3 Indexes (required)

- `datasets(dataset_name)` unique
- `dataset_versions(dataset_name, schema_version)` unique
- `feed_mappings(provider, gateway_feed)` unique
- `instrument_registry(instrument_key)` pk
- `instrument_provider_map(provider, provider_symbol)`
- `data_coverage(dataset_name, instrument_key)`
- `subscriptions(project_name, provider, feed)`
- `requests(project_name, provider, feed, created_at)`

### 11.4 Dataset URNs + path conventions

To make discovery stable, consumers should refer to datasets using a URN-like identifier:

- `heber://silver/bars@v1`
- `heber://silver/quotes@v1`
- `heber://silver/flow_alerts@v1`

**Path template examples**

- Silver bars:
  - `silver/feed=bars/instrument_type={instrument_type}/dt={dt}/part-*.parquet`
- Silver quotes/trades:
  - `silver/feed=quotes/instrument_type={instrument_type}/dt={dt}/hour={hour}/part-*.parquet`

### 11.5 How projects find data (canonical patterns)

Projects should query by **meaning** (instrument + time), not by which project requested it.

#### Pattern A: “Give me SPY quotes for the last hour ASOF now”

- Filter: `instrument_key='equity:SPY'` + time range
- Gate: `ts_available <= T`

#### Pattern B: “Give me UW flow alerts for AAPL today”

- Filter: `symbol='AAPL'` or option `instrument_key` + date range
- Gate: `ts_available <= T`

#### Pattern C: “Show me the data tied to my request/subscription” (audit/debug)

Use `lineage.request_id` / `lineage.subscription_id` for tracing.

**Important performance note**

- Lineage lookups across large Parquet tables may be expensive.
- For operational tracing, prefer Catalog tables `requests`/`subscriptions` and/or a small **Gold audit index**.

**Recommended audit index (Gold)**

- `gold/dataset=request_event_index/project=<project>/dt=<dt>`
  - stores: `request_id`, `subscription_id`, `provider`, `feed`, `instrument_key`, `ts_event`, `event_id`

### 11.6 Heber SDK API surface (v1)

The SDK is the ergonomic layer that makes Heber safe and easy across projects.

**Discovery**

- `catalog.discover(dataset_name, layer="silver", schema_version="latest") → {paths, schema, partitions}`
- `catalog.resolve_feed(provider, gateway_feed) → silver_dataset_name`
- `catalog.list_datasets(filters) → […]`
- `catalog.instrument_lookup(symbol|occ_symbol|instrument_key) → instrument_key`

**Safe reads**

- `read_asof(dataset_name, asof_time, instrument_keys, time_range, columns=None)`
  - applies `ts_available <= asof_time`

**Safe joins**

- `asof_join(left, right, on_keys=[instrument_key], left_time=…, right_time=…)`

**Gold writes**

- `write_gold(dataset_name, df, project, version, metadata)`
  - enforces leakage gates

### 11.7 Catalog REST API Contract

The Catalog exposes a REST API for SDK and service integration.

#### 11.7.1 Base URL

- Local dev: `http://localhost:8080/api/v1`
- Production: `https://heber-catalog.internal/api/v1`

#### 11.7.2 Authentication

**MVP:** API key in header

```
Authorization: Bearer <HEBER_API_KEY>
```

**Future:** JWT with project-scoped claims, mTLS for service-to-service.

#### 11.7.3 Endpoints

**Dataset Discovery**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/datasets` | List all datasets (filterable by layer) |
| `GET` | `/datasets/{name}` | Get dataset metadata |
| `GET` | `/datasets/{name}/versions` | List schema versions |
| `GET` | `/datasets/{name}/versions/{version}` | Get specific schema |
| `GET` | `/datasets/{name}/coverage` | Get data coverage (date ranges, instruments) |

**Instrument Registry**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/instruments/{key}` | Get instrument by key |
| `POST` | `/instruments/lookup` | Batch lookup (body: `{symbols: [...]}`) |
| `GET` | `/instruments/search` | Search instruments (query params) |
| `PUT` | `/instruments/{key}` | Upsert instrument (internal use) |

**Feed Mappings**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/feeds` | List all feed mappings |
| `GET` | `/feeds/resolve?provider={p}&feed={f}` | Resolve gateway feed to Silver dataset |

**Backfill Jobs** (internal)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/backfill` | Create backfill job |
| `GET` | `/backfill/{id}` | Get backfill status |
| `GET` | `/backfill` | List backfill jobs |

#### 11.7.4 Response Format

All responses use JSON envelope:

```json
{
  "data": { ... },
  "meta": {
    "request_id": "...",
    "ts": "2026-01-17T12:00:00Z"
  }
}
```

**Error responses:**

```json
{
  "error": {
    "code": "DATASET_NOT_FOUND",
    "message": "Dataset 'foo' not found",
    "details": {}
  },
  "meta": { ... }
}
```

#### 11.7.5 Error Codes

| HTTP | Code | Meaning |
|------|------|---------|
| 400 | `INVALID_REQUEST` | Malformed request body/params |
| 401 | `UNAUTHORIZED` | Missing or invalid API key |
| 403 | `FORBIDDEN` | API key lacks permission |
| 404 | `NOT_FOUND` | Resource doesn't exist |
| 409 | `CONFLICT` | Version conflict (concurrent update) |
| 429 | `RATE_LIMITED` | Too many requests |
| 500 | `INTERNAL_ERROR` | Server error |

#### 11.7.6 Rate Limits

| Endpoint Group | Limit |
|----------------|-------|
| Read endpoints | 1000 req/min per API key |
| Write endpoints | 100 req/min per API key |
| Batch lookup | 50 req/min, max 1000 items per request |

---

### 11.8 SDK Distribution & Versioning

The Heber SDK is the primary interface for downstream projects.

#### 11.8.1 Package Info

- **Name:** `heber-sdk`
- **Language:** Python 3.10+
- **Distribution:** Internal PyPI (e.g., Artifactory, CodeArtifact)

```bash
pip install heber-sdk --index-url https://pypi.internal.example.com/simple
```

#### 11.8.2 Version Semantics

**Format:** `MAJOR.MINOR.PATCH`

| Change Type | Version Bump | Compatibility |
|-------------|--------------|---------------|
| Breaking API change | MAJOR | Not backward compatible |
| New feature (additive) | MINOR | Backward compatible |
| Bug fix | PATCH | Backward compatible |

**Compatibility Matrix:**

SDK version must be compatible with Catalog schema version.

| SDK Version | Min Catalog Schema | Max Catalog Schema |
|-------------|-------------------|-------------------|
| 1.x | v1.0 | v1.x |
| 2.x | v2.0 | v2.x |

#### 11.8.3 Version Pinning Guidance

Projects MUST pin SDK versions in `requirements.txt` / `pyproject.toml`:

```toml
[project]
dependencies = [
    "heber-sdk>=1.2.0,<2.0.0"
]
```

**Upgrade policy:**

- PATCH: auto-upgrade safe
- MINOR: test before upgrade
- MAJOR: scheduled migration with deprecation period

#### 11.8.4 Deprecation Policy

- Deprecated APIs remain functional for 2 MINOR versions
- Deprecation warnings logged on use
- Removed in next MAJOR version

#### 11.8.5 SDK Configuration

```python
from heber.reader import HeberReader

client = HeberReader(
    catalog_url="https://heber-catalog.internal/api/v1",
    api_key=os.environ["HEBER_API_KEY"],
    storage_endpoint=os.environ.get("HEBER_STORAGE_ENDPOINT"),
    # Optional: override defaults
    cache_ttl_seconds=300,
    timeout_seconds=30,
)
```

---

### 11.9 Access control (optional, future)

Heber can remain a single-tenant system initially. If/when multi-tenant is needed:

- restrict Gold datasets per project
- keep Silver datasets shared
- enforce access via SDK tokens or storage policies

---

## 12) Operational Requirements

### 12.1 Reliability

- Heber should never block Gateway ingestion.
- Use event bus buffering and idempotent writes.

### 12.2 Idempotency + dedupe

- Use `event_id` as the primary dedupe key.
- Writes must be safe across reconnects and retries.

### 12.3 Logging (robust, structured)

**Gateway logs (per emitted event):**

- event\_id, provider, feed, instrument\_key
- ts\_event, ts\_ingest, ts\_available
- schema\_version, quality\_flags

**Heber logs (per write batch):**

- feed, dt partition, file count, rows written
- latency metrics (ingest → available)
- error counts, retries, dead-letter stats

### 12.4 Error handling

- Dead-letter queue for malformed events
- Quarantine storage bucket for schema mismatches
- Backoff + retry with jitter

### 12.5 Observability (Build-Ready)

Comprehensive observability is required for operating Heber reliably.

#### 12.5.1 Metrics Stack

**Format:** Prometheus exposition format
**Scrape endpoint:** `/metrics` on `HEBER_METRICS_PORT` (default 9100)

#### 12.5.2 Required Metrics (all services)

**Naming convention:** `heber_<service>_<metric_name>{<labels>}`

**Consumer Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `heber_consumer_events_received_total` | counter | `feed`, `provider` | Events received from bus |
| `heber_consumer_events_processed_total` | counter | `feed`, `provider`, `status` | Events processed (success/error) |
| `heber_consumer_batch_size` | histogram | `feed` | Batch sizes |
| `heber_consumer_lag_seconds` | gauge | `stream` | Consumer lag behind stream head |
| `heber_consumer_dedupe_drops_total` | counter | `feed` | Bloom filter drops |

**Writer Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `heber_writer_rows_written_total` | counter | `layer`, `dataset` | Rows written |
| `heber_writer_bytes_written_total` | counter | `layer`, `dataset` | Bytes written |
| `heber_writer_files_written_total` | counter | `layer`, `dataset` | Files created |
| `heber_writer_flush_duration_seconds` | histogram | `layer` | Time to flush batch |
| `heber_writer_errors_total` | counter | `layer`, `error_type` | Write failures |

**Compactor Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `heber_compactor_runs_total` | counter | `dataset`, `status` | Compaction runs |
| `heber_compactor_files_merged_total` | counter | `dataset` | Files merged |
| `heber_compactor_bytes_reclaimed_total` | counter | `dataset` | Space saved |
| `heber_compactor_duration_seconds` | histogram | `dataset` | Compaction duration |

**Catalog Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `heber_catalog_requests_total` | counter | `endpoint`, `status_code` | API requests |
| `heber_catalog_request_duration_seconds` | histogram | `endpoint` | Request latency |
| `heber_catalog_db_connections_active` | gauge | | Active DB connections |

**Hot Store Metrics**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `heber_hotstore_rows_synced_total` | counter | `dataset` | Rows synced to Hot Store |
| `heber_hotstore_lag_seconds` | gauge | `dataset` | Sync lag behind Silver |
| `heber_hotstore_sync_errors_total` | counter | `dataset`, `error_type` | Sync failures |

#### 12.5.3 Latency Metrics (anti-leakage monitoring)

These are critical for validating point-in-time correctness:

| Metric | Type | Description |
|--------|------|-------------|
| `heber_ingest_lag_seconds` | histogram | `ts_ingest - ts_event` |
| `heber_availability_lag_seconds` | histogram | `ts_available - ts_event` |
| `heber_commit_lag_seconds` | histogram | `ts_commit - ts_ingest` |

**Labels:** `feed`, `provider`

#### 12.5.4 Alerting Thresholds

| Alert | Condition | Severity |
|-------|-----------|----------|
| `HeberConsumerLagHigh` | `heber_consumer_lag_seconds > 60` for 5m | warning |
| `HeberConsumerLagCritical` | `heber_consumer_lag_seconds > 300` for 5m | critical |
| `HeberWriteErrorRateHigh` | `rate(heber_writer_errors_total[5m]) > 0.01` | warning |
| `HeberHotStoreLagHigh` | `heber_hotstore_lag_seconds > 300` for 5m | warning |
| `HeberAvailabilityLagSpike` | `heber_availability_lag_seconds{quantile="0.99"} > 30` | warning |
| `HeberDLQGrowing` | `rate(heber_dlq_events_total[5m]) > 0` for 10m | warning |
| `HeberCatalogDown` | `up{job="heber-catalog"} == 0` for 1m | critical |
| `HeberCompactionFailed` | `heber_compactor_runs_total{status="error"} > 0` | warning |

#### 12.5.5 Logging

**Format:** JSON structured logs

**Required fields (all services):**

```json
{
  "ts": "2026-01-17T12:00:00.123Z",
  "level": "info",
  "service": "heber-consumer",
  "instance_id": "consumer-01",
  "trace_id": "abc123...",
  "span_id": "def456...",
  "message": "Batch processed",
  "feed": "bars",
  "rows": 1500,
  "duration_ms": 45
}
```

**Log levels:**

| Level | Usage |
|-------|-------|
| `error` | Unrecoverable failures, DLQ events |
| `warn` | Retries, degraded state, schema mismatches |
| `info` | Normal operations (batch processed, file written) |
| `debug` | Detailed per-event logging (disabled in prod) |

#### 12.5.6 Distributed Tracing

**Protocol:** OpenTelemetry (OTLP)

**Trace context propagation:**

- Trace ID is generated at Gateway and passed through EventEnvelope `lineage.trace_id`
- All Heber services propagate trace context
- Export to: Jaeger, Tempo, or cloud provider (X-Ray, Cloud Trace)

**Key spans:**

| Service | Span Name | Attributes |
|---------|-----------|------------|
| consumer | `process_batch` | `feed`, `batch_size` |
| consumer | `dedupe_check` | `bloom_size`, `drops` |
| writer | `write_bronze` | `rows`, `bytes` |
| writer | `write_silver` | `rows`, `bytes`, `partition` |
| catalog | `api_request` | `endpoint`, `status` |

**Sampling:** Head-based sampling at 1% in prod; 100% in dev/staging.

---

### 12.5.7 Required Dashboards

**Heber Overview Dashboard**

- Consumer lag (all streams)
- Events processed rate (by feed)
- Write throughput (rows/sec, bytes/sec)
- Error rate (by type)
- Hot Store sync lag

**Heber Latency Dashboard**

- Ingest lag histogram (p50, p95, p99)
- Availability lag histogram
- Write duration histogram
- API response time histogram

**Heber Health Dashboard**

- Service health (up/down)
- DLQ growth rate
- Compaction status
- Catalog connection pool

### 12.6 Runtime + Deployment Spec (Build-Ready)

This section locks the operational topology for Heber so it can be deployed consistently across environments.

#### 12.6.1 Components

**Heber LakeWriter** is composed of these services:

1. **heber-consumer**

   - Subscribes to the event bus
   - Validates EventEnvelope + schema
   - Batches events by target partition (feed/instrument\_type/dt/hour)

2. **heber-writer**

   - Writes Bronze + Silver files to object storage
   - Updates Catalog metadata (dataset versions, coverage)
   - Emits write metrics

3. **heber-compactor**

   - Periodically compacts small Parquet files into target sizes
   - Operates per-partition (dt/hour)

4. **heber-catalog**

   - Postgres DB for metadata + discovery
   - Optional lightweight API (REST) for SDK use

5. **Optional: heber-hotloader**

   - Tails the event bus (or Silver)
   - Loads recent windows into ClickHouse/Timescale

#### 12.6.2 Deployment targets

- **Local Dev:** Docker Compose (MinIO + Postgres + Redis + Heber services)
- **Single-node Prod:** same topology, persistent volumes
- **Scale-out Prod:** multiple consumers/writers + partition-aware batching

#### 12.6.3 Scaling model

- Scale horizontally at **heber-consumer** layer using consumer groups.
- Scale writers by shard key:
  - `feed + instrument_type + dt/hour` partitions
- Compactor scales independently and is usually CPU + I/O bound.

---

### 12.7 Event Bus Decision + Spec

Heber assumes an event bus between Gateway and storage so ingestion never blocks.

#### 12.7.1 Default recommendation (MVP)

✅ **Redis Streams** for Slice 1–3

- Simple
- Good enough throughput for early stages
- Consumer groups provide replay + acknowledgment

Design note: **Heber must hide the bus behind an interface** so we can upgrade to NATS/Kafka later without rewriting writers.

#### 12.7.2 Stream topology

Two acceptable patterns:

**Pattern A (recommended): one stream per canonical feed**

- `stream:market.bars`
- `stream:market.quotes`
- `stream:market.trades`
- `stream:intel.flow_alerts`
- `stream:intel.darkpool_trades`

Pros: easier consumer scaling + backpressure isolation Cons: more streams

**Pattern B: one stream for everything**

- `stream:gateway.events`

Pros: simple Cons: noisy neighbors (quotes can drown everything)

**Recommendation:** start with Pattern A.

#### 12.7.3 Consumer group behavior

- Each Heber consumer runs in a **consumer group** per stream.
- Ack only after:
  - successful write to object storage
  - successful Catalog update
- On crash/restart, unacked messages replay.

#### 12.7.4 Ordering guarantees

Market data streams do not guarantee total order across symbols.

**Heber ordering rules**

- Preserve **per-message timestamps** (`ts_event`, `ts_ingest`, `ts_available`) as truth.
- Do not assume sequence ordering beyond what the provider gives.
- Downstream joins must always be ASOF-safe (Section 10).

---

### 12.8 Backpressure, Retries, and DLQ (must be explicit)

#### 12.8.1 Backpressure

When write throughput < ingest throughput:

- consumer lag grows (visible metric)
- system must NOT drop data

Mitigations:

- scale consumers
- widen batch sizes
- add Hot Store only for queries (not to “fix ingestion”)

#### 12.8.2 Retry policy

Retries are handled at the consumer/writer boundary.

**Retryable errors** (retry with jitter)

- transient object storage failures
- transient DB connection failures

**Non-retryable errors** (DLQ / quarantine)

- schema mismatch
- malformed EventEnvelope
- missing required timestamps

Default retry settings (tunable):

- max retries: 10
- backoff: exponential + jitter (100ms → 30s)

#### 12.8.3 Dead Letter Queue (DLQ)

DLQ is a separate stream + storage path:

- `stream:heber.dlq`
- `quarantine/provider=.../feed=.../dt=.../`

DLQ payload must include:

- original EventEnvelope
- error type
- error message
- stack trace
- first\_seen\_ts
- retry\_count

#### 12.8.4 Schema mismatch quarantine

If a record cannot be parsed into the expected Silver schema:

- write it to **Bronze** (if possible)
- write failure record to **quarantine**
- emit alert and metric increment

---

### 12.9 Compaction schedule (operational default)

Compaction is critical for Parquet health.

**Default policy**

- Compact hourly partitions after they close
- Example: compact `dt=YYYY-MM-DD/hour=18` at 18:10–18:30

**Compactor invariants**

- Must preserve `event_id` uniqueness
- Must not change `ts_available`
- Must write atomically (temp path then rename/commit)

---

### 12.10 Hot Store sync strategy

Hot Store is a required component of Heber. Configuration:

- **Source:** event bus (preferred) or recently written Silver partitions
- **Window:** rolling last N days per dataset

**ClickHouse recommended tables**

- `quotes_hot` (partitioned by date)
- `trades_hot` (partitioned by date)
- `bars_hot` (partitioned by date)

**Correctness rule:**

- Hot Store is **read-only for queries**; Silver is always the source of truth.
- If a record exists in Silver but not Hot Store, the query must fall back to Silver.

#### 12.10.1 Hot Store Consistency Model

**Consistency SLA**

- Hot Store lags Silver by **≤5 minutes** under normal operation.
- During backpressure or recovery, lag may spike but must be monitored and alerted.

**Staleness Handling**

| Query Type | Behavior |
|------------|----------|
| Real-time dashboard | Hot Store only (accepts staleness) |
| Strategy signals | Hot Store with Silver fallback for missing data |
| Backtest/research | Silver only (never Hot Store) |

**Sync Metrics (required)**

- `hot_store_lag_seconds` (per dataset)
- `hot_store_sync_failures` (counter)
- `hot_store_row_count` vs `silver_row_count` (for same time window)

**Retention Ownership**

- Hot Store retention is managed by **ClickHouse TTL** (not Heber).
- Default TTL: 7 days for quotes/trades, 30 days for bars.
- Heber only writes; ClickHouse handles eviction.

---

### 12.11 Dedupe Strategy (Build-Ready)

Deduplication is critical for correctness and must happen at multiple layers.

#### 12.11.1 Dedupe Locations

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| **heber-consumer** | In-memory bloom filter | Fast approximate dedupe to reduce write volume |
| **heber-writer** | Upsert on `event_id` (if supported) or append-only | Ensures no duplicates in a single batch |
| **heber-compactor** | Exact dedupe during merge | Final guarantee of uniqueness |

#### 12.11.2 Bloom Filter Spec (Consumer Layer)

**Configuration**

- Expected items: 10M per filter (tunable per feed)
- False positive rate: 1%
- Memory: ~12MB per filter
- Rotation: new filter every hour (old filter retained for 1 additional hour)

**Behavior**

- If `event_id` is probably in the filter → drop silently
- If `event_id` is definitely not in the filter → process and add to filter

**Limitation:** Bloom filters have false positives. A small percentage of valid events may be incorrectly dropped. This is acceptable for high-volume feeds (quotes/trades) where duplicates are common.

#### 12.11.3 Compaction Dedupe (Final Guarantee)

During compaction:

1. Read all Parquet files in the partition
2. Sort by `event_id`
3. Drop duplicates, keeping the row with the **earliest `ts_ingest`**
4. Write deduplicated Parquet

**Invariant:** after compaction, `event_id` is unique within a partition.

---

### 12.12 Service Healthcheck Contract

All Heber services must expose consistent health endpoints.

#### 12.12.1 Endpoint Spec

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `/health` | Liveness probe | `200 OK` if process is running |
| `/ready` | Readiness probe | `200 OK` if ready to accept traffic |
| `/metrics` | Prometheus metrics | Prometheus exposition format |

#### 12.12.2 Liveness Check (`/health`)

Returns `200 OK` if the process is alive. Does NOT check dependencies.

**Response:**

```json
{
  "status": "ok",
  "service": "heber-consumer",
  "instance_id": "consumer-01",
  "uptime_seconds": 86400,
  "version": "1.2.3"
}
```

**Use:** Kubernetes livenessProbe, load balancer health checks.

#### 12.12.3 Readiness Check (`/ready`)

Returns `200 OK` only if the service is ready to handle requests.

**Readiness criteria by service:**

| Service | Ready When |
|---------|------------|
| `heber-consumer` | Connected to event bus + object storage writable |
| `heber-writer` | Object storage writable + Catalog reachable |
| `heber-compactor` | Object storage readable/writable |
| `heber-catalog` | Database connection pool healthy |
| `heber-hotloader` | Hot Store writable + event bus connected |

**Response (ready):**

```json
{
  "status": "ready",
  "checks": {
    "event_bus": "ok",
    "object_storage": "ok",
    "catalog": "ok"
  }
}
```

**Response (not ready):**

```json
{
  "status": "not_ready",
  "checks": {
    "event_bus": "ok",
    "object_storage": "error",
    "catalog": "ok"
  }
}
```

HTTP status: `503 Service Unavailable`

**Use:** Kubernetes readinessProbe (traffic routing), graceful startup.

#### 12.12.4 Startup Probe (optional)

For slow-starting services (e.g., compactor loading state):

| Endpoint | Purpose |
|----------|---------|
| `/startup` | Returns `200` when initialization complete |

**Kubernetes config:**

```yaml
startupProbe:
  httpGet:
    path: /startup
    port: 8080
  failureThreshold: 30
  periodSeconds: 10
```

---

### 12.13 Dependency Degradation Matrix

Heber must define graceful degradation when dependencies fail.

#### 12.13.1 Dependency Classification

| Dependency | Type | Description |
|------------|------|-------------|
| **Hard** | Required | Service cannot function without it |
| **Soft** | Degradable | Service can continue with reduced functionality |

#### 12.13.2 Degradation Matrix

| Service | Dependency | Hard/Soft | Degraded Behavior |
|---------|------------|-----------|-------------------|
| `heber-consumer` | Event Bus | Hard | Crash/restart (bus buffers data) |
| `heber-consumer` | Object Storage | Hard | Retry + DLQ after max attempts |
| `heber-consumer` | Catalog | Soft | Cache-only; skip coverage updates |
| `heber-consumer` | Bloom Filter State | Soft | Rebuild from scratch (memory only) |
| `heber-writer` | Object Storage | Hard | Retry + DLQ |
| `heber-writer` | Catalog | Soft | Continue writes; queue metadata updates |
| `heber-compactor` | Object Storage | Hard | Wait and retry |
| `heber-compactor` | Catalog | Soft | Skip catalog updates |
| `heber-catalog` | Postgres | Hard | Return 503 on all requests |
| `heber-hotloader` | Hot Store | Hard | Skip hot writes; emit alert |
| `heber-hotloader` | Event Bus | Hard | Crash/restart |
| SDK | Catalog API | Soft | Use local cache if available |
| SDK | Object Storage | Hard | Fail read/write operations |

#### 12.13.3 Circuit Breaker Settings

For soft dependencies, implement circuit breakers:

**Default settings:**

| Parameter | Value |
|-----------|-------|
| Failure threshold | 5 consecutive failures |
| Open duration | 30 seconds |
| Half-open probes | 3 |
| Success threshold | 2 to close |

**Circuit states:**

- **Closed:** Normal operation
- **Open:** Bypass dependency, use degraded path
- **Half-open:** Testing if dependency recovered

#### 12.13.4 Degraded Mode Indicators

When operating in degraded mode:

1. Emit metric: `heber_degraded_mode{dependency="catalog"} = 1`
2. Log warning: `"Operating in degraded mode: Catalog unreachable"`
3. Set response header (for Catalog API): `X-Heber-Degraded: catalog`

---

### 12.14 Rolling Upgrade Strategy

Service deployments must not cause data loss or inconsistency.

#### 12.14.1 Guiding Principles

1. **Zero-downtime:** New versions deploy alongside old
2. **Backward compatibility:** New services must read old data
3. **Graceful drain:** Old instances finish in-flight work before terminating
4. **Rollback-ready:** Previous version can be restored without data migration

#### 12.14.2 Deployment Strategy by Service

| Service | Strategy | Notes |
|---------|----------|-------|
| `heber-consumer` | Rolling (Kubernetes) | Consumer group handles rebalancing |
| `heber-writer` | Rolling | In-flight batches flush before shutdown |
| `heber-compactor` | Rolling | Only one compactor per partition active |
| `heber-catalog` | Rolling | Stateless; DB handles connection handoff |
| `heber-hotloader` | Rolling | Event bus consumer group rebalancing |

#### 12.14.3 Graceful Shutdown Sequence

All services MUST implement:

1. **SIGTERM received:** Start shutdown
2. **Readiness = false:** Stop accepting new work
3. **Drain in-flight:** Complete current batch/request
4. **Flush buffers:** Write any buffered data
5. **Close connections:** Gracefully close DB/storage/bus connections
6. **Exit:** Process terminates

**Shutdown timeout:** 30 seconds (configurable via `HEBER_SHUTDOWN_TIMEOUT_SECONDS`)

#### 12.14.4 Consumer Group Rebalancing

When consumers restart:

1. Unacked messages replay automatically (at-least-once)
2. New consumer joins group, receives partition assignments
3. Old consumer leaves group, partitions reassigned

**Key invariant:** No messages lost during rebalancing.

#### 12.14.5 Schema Migration During Upgrade

If a new version includes schema changes:

1. **Minor schema change:**
   - Deploy new version (writes new schema)
   - Old data remains readable (backward compat)

2. **Major schema change (rare):**
   - Deploy new dataset version (e.g., `bars_v2`)
   - Run backfill from old to new
   - Migrate consumers to new dataset
   - Deprecate old dataset

#### 12.14.6 Canary Deployment (Recommended)

For risk mitigation:

1. Deploy new version to 10% of instances
2. Monitor error rate, latency, lag for 15 minutes
3. If healthy, proceed to 50%, then 100%
4. If unhealthy, rollback immediately

**Canary metrics to watch:**

- `heber_writer_errors_total` (should not spike)
- `heber_consumer_lag_seconds` (should not grow)
- `heber_catalog_request_duration_seconds` (should not increase)

---

## 13) Historical Ingestion & Backfill Patterns

This section addresses bulk historical data loading, which is distinct from real-time event streaming.

### 13.1 Use Cases

1. **Initial load:** Populate lake with historical data before going live
2. **Provider migration:** Onboard a new provider with historical backfill
3. **Gap recovery:** Replay missed data after outage or reconnect
4. **Schema migration:** Re-ingest after schema changes

### 13.2 Backfill vs Streaming (Key Differences)

| Aspect | Streaming (normal) | Backfill (historical) |
|--------|-------------------|----------------------|
| Source | Event bus | REST API / file dumps |
| Path | heber-consumer → heber-writer | heber-backfill → heber-writer |
| Rate | Real-time | Controlled batch rate |
| `ts_available` | Set to `ts_commit` | Set to `ts_commit` (NOT historical time) |
| Dedupe | Bloom filter + compaction | Compaction only |

### 13.3 `ts_available` Rule for Historical Data

**Critical:** Historical data must NOT have `ts_available` set to historical timestamps.

**Rule:** `ts_available = ts_commit` (time Heber wrote the record), regardless of how old `ts_event` is.

**Rationale:**

- This ensures that as-of queries at historical time T do not suddenly "gain" data that was backfilled later.
- A backtest run yesterday and a backtest run today will produce the same results for the same `asof_time`.

**Exception:** If you want historical data to be usable for historical as-of queries (e.g., "simulate what we would have known on Jan 1"), you must explicitly set:

- `ts_available = ts_event + processing_delay_assumption`

This is opt-in and must be documented per backfill job.

### 13.4 Backfill Pipeline (heber-backfill)

**Components:**

1. **Backfill Job Definition**
   - Provider + feed
   - Date range
   - Symbol universe (optional)
   - `ts_available` policy (default: `ts_commit`)

2. **Backfill Coordinator**
   - Chunks work by date/symbol
   - Rate limits API calls (respects provider limits)
   - Tracks progress (resumable)

3. **Backfill Writer**
   - Writes directly to Bronze/Silver (bypasses event bus)
   - Tags records: `quality_flags += ["backfill"]`
   - Updates Catalog coverage

### 13.5 Backfill Metadata (required per job)

Every backfill job must record:

- `backfill_id` (uuid)
- `provider`, `feed`
- `date_range_start`, `date_range_end`
- `ts_available_policy` (commit | event | custom)
- `started_at`, `completed_at`
- `rows_written`, `files_written`
- `status` (running | completed | failed)

Store in Catalog table: `backfill_jobs`

### 13.6 Backfill Isolation

To prevent backfill from interfering with real-time ingestion:

- Backfill writes to **separate temp partitions**, then atomically swaps/merges
- Backfill runs at lower priority (nice'd processes, rate-limited)
- Compactor handles merge of backfill + streaming data

---

## 14) Schema Evolution Policy

Schema changes are inevitable. This section defines how Heber handles them without breaking readers or corrupting data.

### 14.1 Guiding Principles

1. **Backward compatibility:** New readers must be able to read old data.
2. **Forward compatibility:** Old readers should gracefully handle new data (ignore unknown columns).
3. **No in-place mutation:** Never modify existing Parquet files. Write new files with new schema.

### 14.2 Allowed Schema Changes (Backward-Compatible)

| Change Type | Allowed | Notes |
|-------------|---------|-------|
| Add optional column | ✅ | Must have default value |
| Add required column | ❌ | Breaks backward compat |
| Remove column | ⚠️ | Deprecate first, then remove after N versions |
| Rename column | ❌ | Add new + deprecate old instead |
| Change column type (widening) | ✅ | e.g., int32 → int64 |
| Change column type (narrowing) | ❌ | e.g., int64 → int32 (data loss) |
| Change column type (incompatible) | ❌ | e.g., string → int |

### 14.3 Schema Version Semantics

**Version format:** `v<major>.<minor>` (e.g., `v1.0`, `v1.1`, `v2.0`)

- **Minor bump:** Backward-compatible changes (add optional column)
- **Major bump:** Breaking changes (new required column, type change)

**Coexistence rule:**

- Partitions may contain files with different minor versions (e.g., v1.0 and v1.1)
- Major version changes require a new dataset namespace (e.g., `bars_v2`)

### 14.4 Schema Registry Integration

Heber Catalog serves as the schema registry.

**`dataset_versions` table responsibilities:**

- Store JSON schema per version
- Track `is_current` flag
- Record `writer_min_version` (minimum SDK version that can write this schema)
- Record `reader_min_version` (minimum SDK version that can read this schema)

**SDK behavior:**

- On read: check `reader_min_version`, warn if SDK is too old
- On write: check `writer_min_version`, fail if SDK is too old

### 14.5 Schema Migration Workflow

When a schema change is needed:

1. **Add new version** to `dataset_versions` with `is_current = false`
2. **Deploy new writers** that emit the new schema
3. **Set `is_current = true`** on new version
4. **Run backfill/re-transform** if historical data needs new columns
5. **Deprecate old version** (set `deprecated_at` timestamp)
6. **Remove old version** after grace period (30+ days)

### 14.6 Handling Mixed-Version Reads

When reading a partition with mixed schema versions:

1. SDK reads schema version from Parquet metadata
2. SDK applies schema normalization (fill missing optional columns with defaults)
3. Return unified DataFrame

**Required SDK function:**

```python
def normalize_schema(df: DataFrame, target_version: str) -> DataFrame:
    """Fill missing columns, cast types, handle defaults."""
```

---

## 15) Retention & Lifecycle Management

Data retention must be explicit to control costs and comply with any data policies.

### 15.1 Retention Policies by Layer

| Layer | Default Retention | Rationale |
|-------|-------------------|-----------|
| **Bronze** | 90 days | Raw replay window; cost-sensitive |
| **Silver** | Forever (or 5+ years) | Source of truth for research/backtests |
| **Gold** | Per-version (configurable) | Old feature versions can be pruned |
| **Hot Store** | 7-30 days | Real-time queries only |
| **DLQ/Quarantine** | 30 days | Debug window |

### 15.2 Retention Policy Schema

In Catalog `datasets.retention_policy` (jsonb):

```json
{
  "bronze": {
    "retention_days": 90,
    "action": "delete"
  },
  "silver": {
    "retention_days": null,  // null = forever
    "action": "archive"      // archive to cold storage
  },
  "gold": {
    "retention_versions": 5, // keep last 5 versions
    "retention_days": 365,   // or 1 year
    "action": "delete"
  }
}
```

### 15.3 Lifecycle Actions

| Action | Meaning |
|--------|---------|
| `delete` | Permanently remove files |
| `archive` | Move to cold storage (S3 Glacier, etc.) |
| `compress` | Re-encode with higher compression |

### 15.4 Retention Enforcement (heber-reaper)

A separate service/job enforces retention:

**Components:**

- **Reaper Scheduler:** Runs daily (configurable)
- **Reaper Worker:** Scans partitions, applies policies

**Workflow:**

1. Query Catalog for datasets with retention policies
2. For each dataset, list partitions older than retention window
3. Apply action (delete, archive, compress)
4. Update Catalog `data_coverage` table
5. Emit metrics: `files_deleted`, `bytes_reclaimed`

### 15.5 Deletion Safety Gates

Before deleting any data:

- **Verify no active queries** (optional, if query tracking exists)
- **Verify not referenced by Gold lineage** (prevent orphaned dependencies)
- **Dry-run mode:** Log what would be deleted without acting

### 15.6 Gold Version Retention

Gold datasets are versioned. Retention strategies:

| Strategy | Rule |
|----------|------|
| Keep N versions | Delete versions older than the Nth most recent |
| Keep N days | Delete versions older than N days |
| Pinned versions | Never delete versions marked as `pinned` |

**Pinning:** Production models should pin their dependent Gold versions to prevent accidental deletion.

---

## 16) Compaction Commit Protocol (Atomicity Guarantee)

This section specifies exactly how compaction achieves atomic file replacement.

### 16.1 The Problem

Parquet lakes suffer from "too many small files." Compaction merges them. But:

- S3 does not have atomic rename
- Crash during compaction can leave orphaned files
- Concurrent reads during compaction must not see partial state

### 16.2 Solution: Manifest-Based Commits

Heber uses a **manifest file** to track the "current" set of files per partition.

**Manifest path:** `<partition_path>/_manifest.json`

**Manifest structure:**

```json
{
  "version": 42,
  "created_at": "2026-01-17T12:00:00Z",
  "files": [
    {"path": "part-0001.parquet", "rows": 250000, "bytes": 134217728},
    {"path": "part-0002.parquet", "rows": 250000, "bytes": 134217728}
  ],
  "pending_deletes": []
}
```

### 16.3 Compaction Workflow

1. **Read current manifest** (or list files if no manifest exists)
2. **Read all Parquet files** in the manifest
3. **Merge, dedupe, re-partition** into new files
4. **Write new files** to temp paths: `_compact_tmp/part-*.parquet`
5. **Write new manifest** (atomically):
   - Include new file paths
   - Set `pending_deletes` = old file paths
6. **Move new files** from `_compact_tmp/` to partition root
7. **Update manifest** (remove temp prefix from paths)
8. **Delete old files** listed in `pending_deletes`
9. **Clear `pending_deletes`** in manifest

### 16.4 Crash Recovery

On startup, compactor checks for incomplete compactions:

- If `_compact_tmp/` exists with files → resume from step 6
- If `pending_deletes` is non-empty → resume from step 8

**Invariant:** The manifest always reflects a consistent, complete state.

### 16.5 Reader Behavior

Readers MUST:

1. Read `_manifest.json` first
2. Only read files listed in the manifest
3. Ignore any other files in the partition (orphaned/temp)

If no manifest exists (legacy partition), fall back to listing all Parquet files.

### 16.6 Alternative: Delta Lake / Iceberg

For production scale, consider adopting **Delta Lake** or **Apache Iceberg** instead of a custom manifest. Benefits:

- Battle-tested transaction log
- Time travel / versioning built-in
- ACID guarantees
- Community support

**Recommendation:** Start with custom manifests for simplicity; migrate to Delta Lake when complexity justifies.

---

## 17) Summary: Gap Resolutions

| Gap | Section | Resolution |
|-----|---------|------------|
| Backfill strategy | 13 | Dedicated backfill pipeline with `ts_available = ts_commit` rule |
| Compaction atomicity | 16 | Manifest-based commit protocol with crash recovery |
| Hot Store consistency | 12.10.1 | ≤5 min SLA, clear query-type behavior, ClickHouse TTL ownership |
| Dedupe strategy | 12.11 | Bloom filter at consumer + exact dedupe at compaction |
| Schema evolution | 14 | Backward-compat only, version semantics, migration workflow |
| Retention policy | 15 | Per-layer defaults, reaper service, pinned Gold versions |

---

## 18) Configuration & Environment Variables (must be explicit)

Heber should be configurable via **env vars + a single YAML config**, with sane defaults.

### 12.11.1 Required env vars

**Storage (S3/MinIO compatible)**

- `HEBER_STORAGE_ENDPOINT`
- `HEBER_STORAGE_BUCKET`
- `HEBER_STORAGE_ACCESS_KEY`
- `HEBER_STORAGE_SECRET_KEY`
- `HEBER_STORAGE_REGION` *(optional)*

**Catalog (Postgres)**

- `HEBER_CATALOG_DSN` *(postgres connection string)*

**Event bus (Redis Streams MVP)**

- `HEBER_REDIS_URL`
- `HEBER_STREAM_PREFIX` *(default: `stream:`)*
- `HEBER_CONSUMER_GROUP` *(default: `heber-writers`)*

**Runtime identity**

- `HEBER_ENV` *(local|dev|prod)*
- `HEBER_INSTANCE_ID` *(unique per process/container)*

### 12.11.2 Recommended env vars

**Writer batching + file sizing**

- `HEBER_MAX_ROWS_PER_FLUSH`
- `HEBER_MAX_BYTES_PER_FILE`
- `HEBER_MAX_FLUSH_INTERVAL_MS`

**Compaction**

- `HEBER_COMPACTION_ENABLED` *(true/false)*
- `HEBER_COMPACTION_TARGET_MB` *(default 256)*
- `HEBER_COMPACTION_LAG_MINUTES` *(default 10)*

**DLQ / quarantine**

- `HEBER_DLQ_STREAM` *(default `stream:heber.dlq`)*
- `HEBER_QUARANTINE_PREFIX` *(default `quarantine/`)*

**Observability**

- `HEBER_LOG_LEVEL` *(info|debug|warning|error)*
- `HEBER_METRICS_PORT` *(default 9100)*
- `HEBER_TRACING_ENABLED` *(true/false)*

---

## 12.12 Heber config file (YAML schema)

Example: `heber.yaml`

```yaml
env: prod

storage:
  endpoint: "http://minio:9000"
  bucket: "heber"
  region: "us-west-2"
  format:
    bronze: "jsonl.gz"
    silver: "parquet"

catalog:
  dsn: "postgresql://heber:heber@postgres:5432/heber"

bus:
  type: "redis_streams"
  streams:
    - name: "stream:market.bars"
      dataset: "bars"
    - name: "stream:market.quotes"
      dataset: "quotes"
    - name: "stream:market.trades"
      dataset: "trades"
    - name: "stream:intel.flow_alerts"
      dataset: "flow_alerts"
    - name: "stream:intel.darkpool_trades"
      dataset: "darkpool_trades"

writer:
  flush:
    max_rows: 750000
    max_bytes: 268435456   # 256MB
    max_interval_ms: 15000
  partitions:
    quotes:
      by: ["feed", "instrument_type", "dt", "hour"]
    trades:
      by: ["feed", "instrument_type", "dt", "hour"]

compaction:
  enabled: true
  target_mb: 256
  lag_minutes: 10

dlq:
  stream: "stream:heber.dlq"
  quarantine_prefix: "quarantine/"

leakage_firewall:
  enforce_asof_reads: true
  require_ts_available: true

hot_store:
  enabled: false
  type: "clickhouse"
  rolling_days:
    quotes: 7
    trades: 7
    bars: 90
```

---

## 12.13 Structured logging spec (robust error logging)

Heber services must emit **structured JSON logs** to support debugging at scale.

### 12.13.1 Every log line should include

- `service` (heber-consumer|heber-writer|heber-compactor|heber-catalog)
- `env`
- `instance_id`
- `batch_id`
- `provider`, `feed`, `dataset`
- `partition` (dt/hour/instrument_type)
- `event_count`
- `min_ts_event`, `max_ts_event`
- `duration_ms`

### 12.13.2 Error logs MUST also include

- `error_type`
- `error_message`
- `stack_trace`
- `retry_count`
- `dlq_written` (true/false)

### 12.13.3 Correlation fields (when available)

- `lineage.project`
- `lineage.request_id`
- `lineage.subscription_id`

---

## 12.14 Alerts, SLOs, and dashboards

### 12.14.1 Core SLOs (initial)

- **Ingestion durability:** 99.99% of events successfully written to Bronze+Silver
- **Freshness:** P95 `(ts_available - ts_event)` < 2s for bars, < 5s for quotes/trades *(tunable)*
- **Backlog:** consumer lag does not grow unbounded for > 10 minutes

### 12.14.2 Recommended alerts

**Red alerts (page-level)**

- Writer failure rate > 1% over 5m
- DLQ rate spikes > baseline threshold
- Catalog DB unreachable > 60s
- Storage write failures > N/min

**Yellow alerts (ticket-level)**

- Availability lag P95 doubles vs 24h baseline
- Late-arrival rate increases (quality_flags contains `late`)
- Compactor falling behind schedule

### 12.14.3 Dashboards

- Throughput per feed (events/sec)
- Consumer lag per stream
- Write latency histogram
- Availability lag histogram
- DLQ/quarantine volumes
- Parquet file counts per partition (small file detection)

---

## 12.15 Runbook (common incidents + fixes)

### Incident A: Consumer lag rising

**Symptoms**

- Stream lag increases
- Freshness SLO degrades

**Actions**

1. Increase `heber-consumer` replicas
2. Increase writer batch size (rows/bytes) cautiously
3. Verify storage endpoint performance (MinIO/S3)
4. Confirm quotes/trades are on separate streams (avoid noisy neighbor)

### Incident B: DLQ spike

**Symptoms**

- `stream:heber.dlq` growing fast

**Actions**

1. Sample DLQ messages and identify error type
2. If schema mismatch: update schema registry or mapping
3. If malformed envelope: fix Gateway emitter or add tolerant parser
4. Reprocess quarantined events after patch

### Incident C: Too many small Parquet files

**Symptoms**

- Query performance degrades
- Object store listing becomes slow

**Actions**

1. Ensure compaction enabled
2. Increase flush thresholds (max_rows/max_bytes)
3. Reduce partition granularity (avoid excessive buckets)

### Incident D: Suspected leakage

**Symptoms**

- Backtest results look “too good”

**Actions**

1. Verify all reads use `ASOF(T)`
2. Check `max_ts_available_used <= feature_time` in Gold metadata
3. Run leakage test suite on the pipeline
4. Audit enrichment fields not merged into Silver

---

## 13) Retention & Cost Controls

Retention must balance cost, query performance, and replay/debug value.

### 13.1 Default retention by layer

**Bronze (raw)**

- Purpose: replay + debugging + forensic audits
- High-volume streams are expensive to keep forever

**Silver (canonical Parquet)**

- Purpose: shared truth for backtests/research
- Prefer long retention for “strategy-relevant” datasets

**Gold (derived)**

- Purpose: reproducible model/strategy inputs
- Keep at least as long as the experiments that depend on them

### 13.2 Recommended starting retention matrix

| Dataset | Bronze retention | Silver retention | Notes |
|---|---:|---:|---|
| `quotes` | 3–14 days | 90 days → expand later | Quotes explode storage; keep Silver longer than Bronze. |
| `trades` | 3–14 days | 180 days → expand later | Trades are high value for microstructure. |
| `bars` (1m+) | 30 days | **Forever** | Bars are compact and essential. |
| `flow_alerts` | 180 days | **Forever** | UW intelligence is long-term valuable. |
| `darkpool_trades` | 180 days | **Forever** | Same. |
| `greeks` | 30–90 days | 180 days → expand | Depends on sampling frequency + storage. |
| `option_chain_snapshots` | 30–90 days | 180 days → expand | Snapshots enable surface features. |
| `news_articles/entities` | 90 days | 1–3 years | Depends on provider licensing. |

### 13.3 Downsampling strategy (if storage pressure rises)

If Silver becomes too large, prefer **derived rollups** rather than deleting canonical truth:

- Create `bars_5m`, `bars_15m`, `bars_1h` as Gold/Silver-derived datasets
- Keep 1m bars forever, prune only the highest-frequency tick data if needed

### 13.4 Hot Store rolling window

Hot Store (ClickHouse/Timescale) is a cache:

- `quotes/trades`: 1–7 days
- `bars`: 30–90 days
- Evict by time; the lake remains the source of truth

---

## 14) Vertical Slice Implementation Plan (Ship in slices)

### Slice 0 (Already done): Gateway exists

- Data Gateway ingests Alpaca + Unusual Whales via WS/REST
- Normalizes events and emits EventEnvelope
- Supports subscribe/unsubscribe and multi-stream routing

### Slice 1 (MVP): Equity 1m bars → Bronze + Silver + Catalog

**Scope**

- Stream: `stream:market.bars`
- Bronze write for bars
- Silver `bars` Parquet with `dt` partitioning
- Catalog registry entry for `bars@v1`

**Acceptance**

- Idempotent writes
- ASOF reads work via `ts_available`
- Compaction produces healthy Parquet sizes

### Slice 2: Quotes + Trades (hour partitions + compaction)

**Scope**

- Streams: `stream:market.quotes`, `stream:market.trades`
- Silver partitions include `hour`

**Acceptance**

- Consumer lag observable
- Small-file control works under load

### Slice 3: UW intelligence (flow + darkpool)

- Streams: `stream:intel.flow_alerts`, `stream:intel.darkpool_trades`
- Silver canonical datasets: `flow_alerts`, `darkpool_trades`

### Slice 4: Options reference + options time-series

- `option_contracts` population
- Near-term: `greeks`, `option_chain_snapshots`

### Slice 5: Gold scaffolding + Leakage firewall enforcement

- Ship SDK primitives: `read_asof`, `asof_join`, `write_gold`
- Gold metadata gates enforced

### Slice 6: Hot Store Integration

- ClickHouse tables for recent `quotes/trades/bars`
- Verified ASOF correctness

---

## 15) Open Questions / Decisions

1. **Bus upgrade path:** when to move from Redis Streams → NATS JetStream or Redpanda
2. **Bronze retention:** how many days of raw provider payload are affordable
3. **Schema evolution protocol:** how strict to be with backward compatibility
4. **Coverage indexing:** when to add `data_coverage` materialization jobs
5. **Hot store correctness:** ensure `ts_available` is not bypassed for live reads

---

## 19) Container Build & Registry

### 19.1 Base Images

| Service | Base Image | Rationale |
|---------|------------|-----------|
| `heber-consumer` | `python:3.11-slim-bookworm` | Slim for size, Debian for compatibility |
| `heber-writer` | `python:3.11-slim-bookworm` | Same as consumer |
| `heber-compactor` | `python:3.11-slim-bookworm` | Same as consumer |
| `heber-catalog` | `python:3.11-slim-bookworm` | Same as consumer |
| `heber-hotloader` | `python:3.11-slim-bookworm` | Same as consumer |
| `heber-backfill` | `python:3.11-slim-bookworm` | Same as consumer |

**Future:** migrate to `gcr.io/distroless/python3` for reduced attack surface.

### 19.2 Multi-Stage Build

All Dockerfiles MUST use multi-stage builds:

```dockerfile
# Build stage
FROM python:3.11-slim-bookworm AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# Runtime stage
FROM python:3.11-slim-bookworm
WORKDIR /app
COPY --from=builder /app/deps /app/deps
COPY src/ /app/src/
ENV PYTHONPATH=/app/deps
USER nobody
ENTRYPOINT ["python", "-m", "heber.consumer"]
```

### 19.3 Container Security

| Requirement | Implementation |
|-------------|----------------|
| Non-root user | `USER nobody` in Dockerfile |
| Read-only filesystem | `readOnlyRootFilesystem: true` in K8s |
| No privilege escalation | `allowPrivilegeEscalation: false` |
| Drop all capabilities | `drop: ["ALL"]` |
| Scan for CVEs | Trivy in CI before push |

### 19.4 Image Registry

**Production:** AWS ECR (or equivalent)

- Repository per service: `heber-consumer`, `heber-writer`, etc.
- Region: same as deployment region

**Local dev:** Local Docker registry or direct build

### 19.5 Image Tagging Strategy

Every image is tagged with **both**:

1. **Git SHA** (immutable): `sha-abc1234`
2. **Semver** (for releases): `v1.2.3`

**Branch tags:**

- `main` → `latest` (mutable, for dev)
- `release/*` → semver tag

**Tagging workflow:**

```bash
docker build -t heber-consumer:sha-$(git rev-parse --short HEAD) .
docker tag heber-consumer:sha-abc1234 heber-consumer:v1.2.3
docker push $REGISTRY/heber-consumer:sha-abc1234
docker push $REGISTRY/heber-consumer:v1.2.3
```

---

## 20) Kubernetes Deployment

### 20.1 Namespace Strategy

| Environment | Namespace |
|-------------|-----------|
| Local dev | `heber-dev` |
| Staging | `heber-staging` |
| Production | `heber-prod` |

### 20.2 Resource Requirements

| Service | CPU Request | CPU Limit | Memory Request | Memory Limit | Replicas (prod) |
|---------|-------------|-----------|----------------|--------------|-----------------|
| `heber-consumer` | 500m | 2000m | 512Mi | 2Gi | 3 |
| `heber-writer` | 500m | 2000m | 1Gi | 4Gi | 3 |
| `heber-compactor` | 1000m | 4000m | 2Gi | 8Gi | 1 |
| `heber-catalog` | 250m | 1000m | 256Mi | 1Gi | 2 |
| `heber-hotloader` | 500m | 2000m | 512Mi | 2Gi | 2 |
| `heber-backfill` | 500m | 2000m | 1Gi | 4Gi | 1 (on-demand) |

**Notes:**

- Consumer memory scales with bloom filter size
- Writer memory scales with batch buffer size
- Compactor memory scales with partition size being compacted

### 20.3 Pod Disruption Budget

Ensure HA during rolling deploys:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: heber-consumer-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: heber-consumer
```

| Service | minAvailable |
|---------|--------------|
| `heber-consumer` | 2 |
| `heber-writer` | 2 |
| `heber-catalog` | 1 |
| `heber-compactor` | 0 (single instance OK) |
| `heber-hotloader` | 1 |

### 20.4 Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: heber-consumer-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: heber-consumer
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: External
      external:
        metric:
          name: heber_consumer_lag_seconds
        target:
          type: AverageValue
          averageValue: "30"
```

**Scaling triggers:**

| Service | Metric | Scale-up Threshold |
|---------|--------|-------------------|
| `heber-consumer` | `consumer_lag_seconds` | > 30s |
| `heber-writer` | `pending_batch_rows` | > 100k |
| `heber-catalog` | `request_latency_p99` | > 500ms |

### 20.5 Service Mesh

**Recommendation:** Start without service mesh. Add if/when needed for:

- mTLS between services
- Advanced traffic management
- Distributed tracing injection

**If adopted:** Linkerd (simpler) or Istio (more features)

### 20.6 Kubernetes Labels & Annotations

Standard labels for all resources:

```yaml
labels:
  app.kubernetes.io/name: heber-consumer
  app.kubernetes.io/version: "1.2.3"
  app.kubernetes.io/component: consumer
  app.kubernetes.io/part-of: heber
  app.kubernetes.io/managed-by: helm
```

---

## 21) Secrets Management

### 21.1 Secrets Inventory

| Secret | Used By | Rotation Frequency |
|--------|---------|-------------------|
| `HEBER_STORAGE_ACCESS_KEY` | consumer, writer, compactor | 90 days |
| `HEBER_STORAGE_SECRET_KEY` | consumer, writer, compactor | 90 days |
| `HEBER_CATALOG_DSN` | all services | On credential change |
| `HEBER_REDIS_URL` | consumer, hotloader | On credential change |
| `HEBER_API_KEY` (Catalog) | SDK clients | Per-client, revocable |
| `HEBER_CLICKHOUSE_DSN` | hotloader | On credential change |

### 21.2 Secrets Backend by Environment

| Environment | Backend | Notes |
|-------------|---------|-------|
| Local dev | `.env` file | Gitignored |
| Staging | AWS Secrets Manager | Rotated manually |
| Production | AWS Secrets Manager + External Secrets Operator | Auto-synced to K8s |

### 21.3 Kubernetes Secrets Sync

Use **External Secrets Operator** to sync from AWS Secrets Manager:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: heber-secrets
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  target:
    name: heber-secrets
  data:
    - secretKey: HEBER_STORAGE_SECRET_KEY
      remoteRef:
        key: heber/prod/storage
        property: secret_key
```

### 21.4 Secret Rotation

**Rotation workflow:**

1. Generate new credential in Secrets Manager
2. Update secret (new version)
3. External Secrets Operator syncs to K8s
4. Rolling restart of affected pods (automatic via annotation hash)
5. Revoke old credential after grace period (24h)

**Pod restart on secret change:**

```yaml
spec:
  template:
    metadata:
      annotations:
        secrets-hash: "{{ sha256sum .Values.secrets }}"
```

---

## 22) Infrastructure as Code

### 22.1 IaC Tooling

| Component | Tool |
|-----------|------|
| Cloud infrastructure | Terraform |
| Kubernetes manifests | Helm charts |
| Secrets | Terraform + External Secrets Operator |

### 22.2 Repository Structure

```
infrastructure/
├── terraform/
│   ├── modules/
│   │   ├── vpc/
│   │   ├── eks/
│   │   ├── rds/
│   │   ├── s3/
│   │   ├── elasticache/
│   │   └── ecr/
│   ├── environments/
│   │   ├── dev/
│   │   ├── staging/
│   │   └── prod/
│   └── main.tf
├── helm/
│   └── heber/
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── values-staging.yaml
│       ├── values-prod.yaml
│       └── templates/
│           ├── deployment.yaml
│           ├── service.yaml
│           ├── configmap.yaml
│           ├── hpa.yaml
│           └── pdb.yaml
└── scripts/
    ├── apply.sh
    └── plan.sh
```

### 22.3 Environment Differences

| Resource | Dev | Staging | Prod |
|----------|-----|---------|------|
| EKS node count | 2 | 3 | 6+ |
| RDS instance | db.t3.small | db.t3.medium | db.r6g.large |
| S3 replication | None | None | Cross-region |
| Redis | Elasticache t3.micro | t3.small | r6g.large cluster |
| ClickHouse | Single node | Single node | 3-node cluster |

### 22.4 Terraform State

- **Backend:** S3 + DynamoDB for locking
- **State per environment:** `s3://heber-terraform-state/{env}/terraform.tfstate`
- **Workspaces:** Not used (explicit env directories instead)

---

## 23) CI/CD Pipeline

### 23.1 Pipeline Tool

**GitHub Actions** (or equivalent)

### 23.2 Pipeline Stages

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Build   │───▸│   Test   │───▸│   Scan   │───▸│   Push   │───▸│  Deploy  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
```

### 23.3 Build Stage

```yaml
build:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Build Docker images
      run: |
        docker build -t heber-consumer:${{ github.sha }} -f docker/consumer/Dockerfile .
        docker build -t heber-writer:${{ github.sha }} -f docker/writer/Dockerfile .
        # ... other services
```

### 23.4 Test Stage

```yaml
test:
  runs-on: ubuntu-latest
  services:
    postgres:
      image: postgres:15
    redis:
      image: redis:7
    minio:
      image: minio/minio
  steps:
    - name: Run unit tests
      run: pytest tests/unit -v
    - name: Run integration tests
      run: pytest tests/integration -v
    - name: Run leakage tests
      run: pytest tests/leakage -v
```

### 23.5 Scan Stage

```yaml
scan:
  runs-on: ubuntu-latest
  steps:
    - name: Run Trivy vulnerability scan
      uses: aquasecurity/trivy-action@master
      with:
        image-ref: heber-consumer:${{ github.sha }}
        severity: HIGH,CRITICAL
        exit-code: 1
```

### 23.6 Deploy Stage

```yaml
deploy-staging:
  needs: [build, test, scan]
  runs-on: ubuntu-latest
  environment: staging
  steps:
    - name: Deploy to staging
      run: |
        helm upgrade --install heber ./helm/heber \
          -n heber-staging \
          -f values-staging.yaml \
          --set image.tag=${{ github.sha }}

deploy-prod:
  needs: [deploy-staging]
  runs-on: ubuntu-latest
  environment: production
  steps:
    - name: Deploy canary (10%)
      run: |
        helm upgrade --install heber ./helm/heber \
          -n heber-prod \
          -f values-prod.yaml \
          --set image.tag=${{ github.sha }} \
          --set canary.enabled=true \
          --set canary.weight=10
    - name: Wait and validate
      run: sleep 900 && ./scripts/validate-canary.sh
    - name: Promote to 100%
      run: |
        helm upgrade --install heber ./helm/heber \
          -n heber-prod \
          -f values-prod.yaml \
          --set image.tag=${{ github.sha }} \
          --set canary.enabled=false
```

### 23.7 Rollback

**Automatic rollback triggers:**

- Error rate > 1% for 5 minutes after deploy
- p99 latency > 2x baseline for 5 minutes

**Manual rollback:**

```bash
helm rollback heber <revision> -n heber-prod
```

---

## 24) Backup & Disaster Recovery

### 24.1 RTO/RPO Targets

| Component | RPO | RTO | Priority |
|-----------|-----|-----|----------|
| Catalog (Postgres) | 1 hour | 4 hours | Critical |
| Silver (S3) | 0 (durable) | N/A | Critical |
| Bronze (S3) | 0 (durable) | N/A | High |
| Hot Store (ClickHouse) | 24 hours | 8 hours | Medium |
| Redis (event bus) | 0 (ephemeral OK) | 1 hour | Medium |

### 24.2 Backup Strategy

#### Catalog (Postgres)

| Backup Type | Frequency | Retention |
|-------------|-----------|-----------|
| Automated snapshots (RDS) | Daily | 30 days |
| Point-in-time recovery | Continuous | 7 days |
| Cross-region replica | Async | Warm standby |

#### Object Storage (S3)

| Feature | Configuration |
|---------|---------------|
| Versioning | Enabled |
| Cross-region replication | prod only, to disaster recovery region |
| Lifecycle rules | Bronze: transition to IA after 30 days, delete after 90 days |

#### ClickHouse (Hot Store)

- Daily backups via `clickhouse-backup` tool
- Stored in S3
- Retention: 7 days

### 24.3 Disaster Recovery Runbook

**Scenario: Primary region failure**

1. **Assess:** Confirm region is down (AWS status page, monitoring)
2. **Failover Postgres:** Promote cross-region replica
3. **Update DNS:** Point to DR region endpoints
4. **Deploy services:** Helm install in DR cluster
5. **Verify:** Run smoke tests
6. **Notify:** Alert stakeholders

**Estimated RTO:** 2-4 hours (depending on automation level)

### 24.4 Backup Validation

- **Monthly:** Restore Catalog backup to test environment
- **Quarterly:** Full DR drill (failover to secondary region)

---

## 25) Network Topology

### 25.1 VPC Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ VPC: 10.0.0.0/16                                                │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Public Subnet           │   │ Public Subnet           │     │
│  │ 10.0.1.0/24 (AZ-a)      │   │ 10.0.2.0/24 (AZ-b)      │     │
│  │                         │   │                         │     │
│  │  ┌─────────────────┐    │   │  ┌─────────────────┐    │     │
│  │  │ Load Balancer   │    │   │  │ Load Balancer   │    │     │
│  │  └─────────────────┘    │   │  └─────────────────┘    │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Private Subnet          │   │ Private Subnet          │     │
│  │ 10.0.10.0/24 (AZ-a)     │   │ 10.0.11.0/24 (AZ-b)     │     │
│  │                         │   │                         │     │
│  │  ┌─────────────────┐    │   │  ┌─────────────────┐    │     │
│  │  │ EKS Nodes       │    │   │  │ EKS Nodes       │    │     │
│  │  │ (Heber services)│    │   │  │ (Heber services)│    │     │
│  │  └─────────────────┘    │   │  └─────────────────┘    │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Data Subnet             │   │ Data Subnet             │     │
│  │ 10.0.20.0/24 (AZ-a)     │   │ 10.0.21.0/24 (AZ-b)     │     │
│  │                         │   │                         │     │
│  │  ┌────────┐ ┌────────┐  │   │  ┌────────┐ ┌────────┐  │     │
│  │  │Postgres│ │ Redis  │  │   │  │Postgres│ │ Redis  │  │     │
│  │  │ (RDS)  │ │(Elasti)│  │   │  │(standby)│ │(replica)│ │     │
│  │  └────────┘ └────────┘  │   │  └────────┘ └────────┘  │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

### 25.2 Subnet Purpose

| Subnet Type | CIDR | Contains | Internet Access |
|-------------|------|----------|-----------------|
| Public | 10.0.1-2.0/24 | Load balancers, NAT gateways | Yes (IGW) |
| Private | 10.0.10-11.0/24 | EKS worker nodes, services | Outbound only (NAT) |
| Data | 10.0.20-21.0/24 | RDS, ElastiCache, ClickHouse | None |

### 25.3 Security Groups

| Security Group | Inbound Rules | Outbound Rules |
|----------------|---------------|----------------|
| `heber-alb` | 443 from 0.0.0.0/0 | All to VPC |
| `heber-services` | All from `heber-alb` | All to VPC, 443 to 0.0.0.0/0 |
| `heber-postgres` | 5432 from `heber-services` | None |
| `heber-redis` | 6379 from `heber-services` | None |
| `heber-clickhouse` | 8123, 9000 from `heber-services` | None |
| `heber-s3-endpoint` | 443 from VPC | N/A (VPC endpoint) |

### 25.4 VPC Endpoints

For private access to AWS services:

| Service | Endpoint Type |
|---------|---------------|
| S3 | Gateway endpoint |
| ECR | Interface endpoint |
| Secrets Manager | Interface endpoint |
| CloudWatch Logs | Interface endpoint |

### 25.5 mTLS (Future)

When service mesh is adopted:

- All service-to-service traffic encrypted
- Certificates managed by cert-manager + Linkerd/Istio
- Automatic rotation every 24 hours

---

## 26) Cost Estimates (Monthly, Production)

### 26.1 Compute

| Resource | Spec | Quantity | Est. Cost |
|----------|------|----------|-----------|
| EKS cluster | Control plane | 1 | $72 |
| EKS nodes | m5.large | 6 | $540 |
| ClickHouse | r6g.large | 3 | $330 |

**Compute subtotal:** ~$950/month

### 26.2 Storage

| Resource | Spec | Est. Cost |
|----------|------|-----------|
| S3 (Silver) | 1 TB | $23 |
| S3 (Bronze) | 500 GB | $12 |
| S3 (Gold) | 200 GB | $5 |
| S3 cross-region replication | 1 TB | $20 |
| RDS (Postgres) | db.r6g.large, 100 GB | $200 |
| ElastiCache (Redis) | r6g.large cluster | $200 |

**Storage subtotal:** ~$460/month

### 26.3 Networking & Other

| Resource | Est. Cost |
|----------|-----------|
| NAT Gateway (2x, data transfer) | $100 |
| Load Balancer | $20 |
| Secrets Manager | $5 |
| CloudWatch Logs | $30 |
| ECR storage | $10 |

**Other subtotal:** ~$165/month

### 26.4 Total Estimate

| Category | Monthly Cost |
|----------|--------------|
| Compute | $950 |
| Storage | $460 |
| Other | $165 |
| **Total** | **~$1,575/month** |

**Notes:**

- Costs will scale with data volume and traffic
- Staging environment adds ~30% of prod cost
- Local dev: effectively free (Docker Compose)

---

## 27) Summary: Infrastructure Gap Resolutions

| Gap | Section | Resolution |
|-----|---------|------------|
| Container build | 19 | Multi-stage builds, security hardening, ECR registry |
| Kubernetes | 20 | Resource limits, HPA, PDB, namespace strategy |
| Secrets | 21 | AWS Secrets Manager + External Secrets Operator |
| IaC | 22 | Terraform + Helm, environment separation |
| CI/CD | 23 | GitHub Actions, canary deploy, auto-rollback |
| Backup/DR | 24 | RDS snapshots, S3 replication, DR runbook |
| Networking | 25 | VPC topology, security groups, VPC endpoints |
| Cost | 26 | ~$1,575/month baseline estimate |

---

# Part IV: Research Workflow (ML/Quant)

## 28) Gold Dataset Versioning & Reproducibility

### 28.1 The Problem

Without explicit versioning, researchers may:

- Train on `features@v3`, deploy on `features@v4` (silently different)
- Re-run a backtest and get different results
- Break models when upstream Silver schema changes

### 28.2 Version Pinning API

```python
# Explicit version pinning (recommended for production)
df = client.read_gold(
    dataset="momentum_features",
    version="v3.2.1",           # Exact version
    asof_time="2025-01-15T10:00:00Z",
    time_range=("2024-01-01", "2025-01-01")
)

# Latest within major version (for research)
df = client.read_gold(
    dataset="momentum_features",
    version="v3.*",             # Latest v3.x
    asof_time="2025-01-15T10:00:00Z"
)

# Default: latest (for interactive exploration only)
df = client.read_gold(dataset="momentum_features", ...)  # Uses latest
```

### 28.3 Version Compatibility Check

```python
# Check if model trained on v3.2 is compatible with v3.5
compat = client.check_version_compatibility(
    dataset="momentum_features",
    from_version="v3.2.1",
    to_version="v3.5.0"
)
# Returns:
# {
#   "compatible": True,
#   "changes": [
#     {"type": "added_column", "column": "momentum_20d"},
#     {"type": "deprecated_column", "column": "momentum_5d_legacy"}
#   ],
#   "breaking": False
# }
```

### 28.4 Version Lineage

Every Gold version tracks:

```json
{
  "version": "v3.2.1",
  "created_at": "2025-01-15T12:00:00Z",
  "created_by": "alpha_team",
  "upstream_deps": [
    {"dataset": "bars", "layer": "silver", "version": "v1.4"},
    {"dataset": "trades", "layer": "silver", "version": "v1.2"}
  ],
  "code_commit": "abc123",
  "config_hash": "def456"
}
```

### 28.5 Immutability Guarantee

**Rule:** Once a Gold version is published, its contents are immutable.

- Fixes require a new patch version (v3.2.1 → v3.2.2)
- Schema changes require minor/major bump
- This enables reproducible backtests

---

## 29) Label Management

### 29.1 The Problem

Labels (target variables) are forward-looking by nature. Without careful handling:

- You compute "5-day return" using future data → leakage
- Labels become "available" at wrong timestamps → inconsistent with features

### 29.2 Label Dataset Schema

Labels are stored as Gold datasets with special metadata:

```python
{
  "dataset_type": "label",
  "forward_window": "5d",           # How far forward the label looks
  "label_horizon": "close_to_close", # What it measures
  "availability_lag": "0s"          # When label becomes observable
}
```

### 29.3 Label Write API

```python
from heber_sdk import write_label

write_label(
    dataset="returns_5d",
    df=labels_df,

    # Column mappings
    instrument_key_col="instrument_key",
    label_time_col="ts_label",       # Feature cutoff time (T)
    forward_window="5d",             # Label observes T to T+5d

    # Daily windows resolve on TRADING sessions (not calendar days):
    #   ts_available = close of the Nth trading session after ts_label (+ lag),
    #   using the session's real close (handles weekends, holidays, 13:00 ET
    #   half-days). E.g. for T=Fri 2025-01-10, forward_window=5d the label
    #   becomes available at the Fri 2025-01-17 close. Intraday windows
    #   ("Nh"/"Nm") use plain wall-clock arithmetic.
)
```

### 29.4 Label Read API

```python
# Labels are aligned with feature asof_time
features = client.read_gold("momentum_features", asof_time=T, ...)
labels = client.read_label("returns_5d", asof_time=T, ...)

# The SDK enforces: labels.ts_available <= T
# Which means: label's forward_window must have elapsed by T
```

### 29.5 Label Alignment Rules

| Feature asof_time | Label forward_window | Label available? |
|-------------------|---------------------|------------------|
| 2025-01-15 | 5d | Only labels where ts_label <= 2025-01-10 |
| 2025-01-15 | 1d | Only labels where ts_label <= 2025-01-14 |
| 2025-01-15 | 0d (same-day) | Only labels where ts_label < 2025-01-15 (intraday cutoff) |

**Key insight:** Reading labels at asof_time T means you only get labels whose forward-looking window has fully elapsed by T.

---

## 30) Train/Test Split Utilities

### 30.1 Walk-Forward Splits

```python
from heber_sdk import walk_forward_splits

splits = walk_forward_splits(
    start="2020-01-01",
    end="2025-01-01",
    train_period="12M",    # Training window
    test_period="3M",      # Testing window
    step="3M",             # Step between splits
    embargo="5d"           # Gap between train and test (prevents leakage)
)

# Returns:
# [
#   (TrainRange(2020-01-01, 2020-12-31), TestRange(2021-01-06, 2021-03-31)),
#   (TrainRange(2020-04-01, 2021-03-31), TestRange(2021-04-06, 2021-06-30)),
#   ...
# ]
```

### 30.2 Embargo Period

The embargo prevents leakage at split boundaries:

```text
Train Window          Embargo    Test Window
[=================]   [===]      [===============]
     12 months         5 days        3 months
```

**Why:** Autocorrelation in financial data means observations near the boundary are not independent.

### 30.3 Expanding Window Splits

```python
splits = expanding_window_splits(
    start="2020-01-01",
    end="2025-01-01",
    min_train_period="12M",  # Minimum training data
    test_period="3M",
    embargo="5d"
)
# Train window grows with each split
```

### 30.4 Holdout Set

```python
holdout = HoldoutSet(
    start="2024-07-01",
    end="2025-01-01",
    purpose="final_validation"
)

# SDK warns if you access holdout data outside final eval:
client.read_gold(..., time_range=("2024-08-01", "2024-09-01"))
# Warning: "Accessing holdout period data. Are you sure?"
```

### 30.5 Split Usage Pattern

```python
for train_range, test_range in splits:
    # Read features for training (asof = end of train)
    train_features = client.read_gold(
        "momentum_features",
        asof_time=train_range.end,
        time_range=train_range
    )
    train_labels = client.read_label(
        "returns_5d",
        asof_time=train_range.end,
        time_range=train_range
    )

    # Read features for testing (asof = end of test)
    test_features = client.read_gold(
        "momentum_features",
        asof_time=test_range.end,
        time_range=test_range
    )
    test_labels = client.read_label(
        "returns_5d",
        asof_time=test_range.end,
        time_range=test_range
    )

    # Train and evaluate
    model.fit(train_features, train_labels)
    predictions = model.predict(test_features)
    metrics.append(evaluate(predictions, test_labels))
```

---

## 31) Feast Feature Store Integration

### 31.1 Overview

Heber integrates **Feast** (Feature Store for Machine Learning) as the centralized feature management layer. This ensures:

- **Training-serving consistency**: Same features used in backtests and production
- **Point-in-time correctness**: Enforced through `ts_available` semantics
- **Cross-project reuse**: All projects share a single feature definition
- **Low-latency serving**: Online store for real-time inference

> [!IMPORTANT]
> Feast is the **only** supported mechanism for feature access in production. Direct reads from Gold Parquet are discouraged except for exploratory analysis.

### 31.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           HEBER DATA LAKEHOUSE                          │
│  ┌─────────────┐   ┌─────────────┐   ┌───────────────────────────────┐  │
│  │   Bronze    │ → │   Silver    │ → │            Gold               │  │
│  │   (raw)     │   │ (canonical) │   │  ┌─────────────────────────┐  │  │
│  └─────────────┘   └─────────────┘   │  │   Feast Offline Store   │  │  │
│                                       │  │   (Parquet feature      │  │  │
│                                       │  │    datasets)            │  │  │
│                                       │  └─────────────────────────┘  │  │
│                                       └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                                    │
                                                    ▼ Materialize
┌─────────────────────────────────────────────────────────────────────────┐
│                        Feast Online Store                                │
│                    (ClickHouse / Redis)                                  │
│                  Latest feature values for inference                     │
└─────────────────────────────────────────────────────────────────────────┘
                          │                              │
              ┌───────────┴───────────┐      ┌───────────┴───────────┐
              │       KAIROS          │      │      NIGHTWATCH       │
              │  feast.get_online()   │      │  feast.get_online()   │
              │  feast.get_historical │      │  feast.get_historical │
              └───────────────────────┘      └───────────────────────┘
```

### 31.3 Feast Components

| Component | Heber Integration | Purpose |
|-----------|------------------|---------|
| **Feature Repository** | `heber/features/` directory | Feature definitions in Python |
| **Registry** | Heber Catalog (Postgres) | Feature metadata + versions |
| **Offline Store** | Gold layer (Parquet) | Historical features for training |
| **Online Store** | Hot Store (ClickHouse) | Latest values for inference |
| **Feature Server** | Heber API | REST/gRPC serving endpoint |

### 31.4 Feature Definition (Python)

All features are defined in the Feast feature repository:

```python
# heber/features/momentum_features.py
from feast import Entity, Feature, FeatureView, Field, FileSource
from feast.types import Float32, String
from datetime import timedelta

# Entity: what we're computing features for
equity = Entity(
    name="instrument_key",
    description="Canonical instrument identifier",
)

# Source: where the feature data lives (Gold Parquet)
momentum_source = FileSource(
    name="momentum_source",
    path="s3://heber/gold/dataset=momentum_features/",
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: the feature set
momentum_features = FeatureView(
    name="momentum_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="momentum_5d", dtype=Float32),
        Field(name="momentum_10d", dtype=Float32),
        Field(name="momentum_20d", dtype=Float32),
        Field(name="volatility_20d", dtype=Float32),
        Field(name="rsi_14", dtype=Float32),
    ],
    source=momentum_source,
    online=True,  # Materialize to online store
    tags={
        "owner": "quant_team",
        "category": "technical",
    },
)
```

### 31.5 Point-in-Time Correctness (Anti-Leakage)

Feast enforces Heber's zero-leakage guarantee through the `created_timestamp_column`:

```python
# This is how point-in-time joins work:
#
# For each row in entity_df at time T:
#   1. Find feature rows where ts_event <= T
#   2. Further filter: ts_available <= T  (Heber's anti-leakage gate)
#   3. Return the most recent qualifying row
```

**Critical mapping:**

| Feast Concept | Heber Equivalent | Purpose |
|---------------|-----------------|---------|
| `timestamp_field` | `ts_event` | Event occurrence time |
| `created_timestamp_column` | `ts_available` | When data became observable |

### 31.6 Offline Store (Historical Reads)

For training and backtesting, use `get_historical_features`:

```python
from feast import FeatureStore
from datetime import datetime

store = FeatureStore(repo_path="heber/features/")

# Entity DataFrame: (instrument_key, timestamp) tuples to retrieve features for
entity_df = pd.DataFrame({
    "instrument_key": ["equity:AAPL", "equity:MSFT", "equity:GOOG"],
    "event_timestamp": [datetime(2025, 1, 15, 16, 0)] * 3,
})

# Get historical features (point-in-time correct)
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "momentum_features:momentum_10d",
        "momentum_features:volatility_20d",
        "momentum_features:rsi_14",
    ],
).to_df()
```

### 31.7 Online Store (Real-Time Inference)

For production inference, use `get_online_features`:

```python
# Materialize features to online store (run periodically or on-demand)
store.materialize(
    start_date=datetime(2025, 1, 1),
    end_date=datetime.now(),
)

# Get latest feature values for inference (low-latency)
online_features = store.get_online_features(
    features=[
        "momentum_features:momentum_10d",
        "momentum_features:volatility_20d",
    ],
    entity_rows=[
        {"instrument_key": "equity:AAPL"},
        {"instrument_key": "equity:SPY"},
    ],
).to_dict()

# Response latency: 5-15ms with ClickHouse/Redis
```

### 31.8 Online Store Configuration

**ClickHouse (recommended for Heber):**

```yaml
# heber/features/feature_store.yaml
project: heber
provider: local
registry: postgresql://heber-catalog/feast_registry
online_store:
  type: clickhouse
  host: heber-hotstore.internal
  port: 9000
  database: feast_online
  user: feast
  password_env: FEAST_CLICKHOUSE_PASSWORD
offline_store:
  type: file  # Parquet files in S3/MinIO
```

**Redis (alternative for lower latency):**

```yaml
online_store:
  type: redis
  redis_type: redis_cluster
  connection_string: redis://heber-redis.internal:6379
```

### 31.9 Materialization Pipeline

Features are materialized from offline (Parquet) to online (ClickHouse) on a schedule:

```python
# heber/pipelines/feast_materialize.py
from feast import FeatureStore
from datetime import datetime, timedelta

def materialize_features():
    """Run hourly to keep online store fresh."""
    store = FeatureStore(repo_path="heber/features/")

    # Incremental materialization
    store.materialize_incremental(
        end_date=datetime.now(),
        feature_views=["momentum_features", "flow_features"],
    )
```

**Schedule (Kubernetes CronJob):**

```yaml
# Materialize every hour for intraday features
schedule: "0 * * * *"
# Materialize daily for end-of-day features
schedule: "0 17 * * 1-5"  # 5 PM ET on trading days
```

### 31.10 Feature Registry

The Feature Registry is backed by Heber Catalog (Postgres) and provides:

- **Feature discovery**: Search by tags, owner, category
- **Schema tracking**: Version history and compatibility
- **Lineage**: Trace features back to Silver sources
- **Quality metrics**: Staleness, fill rates, coverage

**Feature Metadata Schema:**

```json
{
  "feature_id": "momentum_10d",
  "feature_view": "momentum_features",
  "owner": "quant_team",
  "description": "10-day price momentum: close / close.shift(10) - 1",
  "dtype": "Float32",
  "dependencies": ["silver.bars.close"],
  "tags": ["momentum", "technical", "daily"],
  "quality": {
    "staleness_sla_hours": 24,
    "expected_fill_rate": 0.98,
    "coverage": "US equities"
  },
  "created_at": "2024-06-01",
  "version": "v2.1.0"
}
```

**Registry API:**

```python
from feast import FeatureStore

store = FeatureStore(repo_path="heber/features/")

# List all feature views
feature_views = store.list_feature_views()

# Get specific feature view metadata
fv = store.get_feature_view("momentum_features")
print(fv.entities, fv.features, fv.tags)

# Search by tags (custom query on Catalog)
from heber_sdk import search_features
features = search_features(tags=["momentum"], owner="quant_team")
```

### 31.11 Label Store Integration

Labels (target variables) are managed as special Feast Feature Views with forward-looking semantics:

```python
# heber/features/label_features.py
from feast import FeatureView, Field, FileSource
from feast.types import Float32
from datetime import timedelta

returns_source = FileSource(
    name="returns_5d_source",
    path="s3://heber/gold/dataset=labels_returns_5d/",
    timestamp_field="ts_label",            # When the label was computed FOR
    created_timestamp_column="ts_available", # When it became observable (ts_label + 5d)
)

returns_5d = FeatureView(
    name="labels_returns_5d",
    entities=[equity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="return_5d", dtype=Float32),
        Field(name="return_5d_excess", dtype=Float32),
    ],
    source=returns_source,
    online=False,  # Labels usually not needed online
    tags={
        "dataset_type": "label",
        "forward_window": "5d",
        "label_horizon": "close_to_close",
    },
)
```

**Reading Labels with Features:**

```python
# Labels are aligned by ts_available, ensuring forward window has elapsed
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "momentum_features:momentum_10d",
        "labels_returns_5d:return_5d",
    ],
).to_df()
# Only returns labels where ts_available <= event_timestamp
```

### 31.12 Feature Computation Pipelines

Feature computation jobs read from Silver and write to Gold Parquet:

```python
# heber/pipelines/compute_momentum.py
from heber.reader import HeberReader
import pandas as pd

def compute_momentum_features():
    """Daily job to compute momentum features."""
    client = HeberReader()

    # Read from Silver (point-in-time correct)
    bars = client.read_silver(
        dataset="bars",
        instrument_type="equity",
        time_range=("2025-01-01", "2025-01-15"),
    )

    # Compute features
    features = bars.groupby("instrument_key").apply(
        lambda df: pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["bar_start_ts"],
            "ts_available": pd.Timestamp.now(tz="UTC"),  # Available now
            "momentum_5d": df["close"] / df["close"].shift(5) - 1,
            "momentum_10d": df["close"] / df["close"].shift(10) - 1,
            "momentum_20d": df["close"] / df["close"].shift(20) - 1,
            "volatility_20d": df["close"].pct_change().rolling(20).std(),
        })
    )

    # Write to Gold (Feast offline store)
    client.write_gold(
        dataset="momentum_features",
        df=features,
        version="v2",
    )
```

**Pipeline Orchestration (Airflow DAG):**

```python
# dags/heber_features.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("heber_momentum_features", schedule_interval="0 18 * * 1-5") as dag:

    compute = PythonOperator(
        task_id="compute_momentum",
        python_callable=compute_momentum_features,
    )

    materialize = PythonOperator(
        task_id="materialize_to_online",
        python_callable=materialize_features,
    )

    compute >> materialize
```

### 31.13 Feature Server (REST API)

For production serving, deploy a Feast Feature Server:

```yaml
# kubernetes/feast-server.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: feast-server
  namespace: heber
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: feast-server
        image: feastdev/feature-server:0.38.0
        args: ["serve", "-h", "0.0.0.0", "-p", "6566"]
        env:
        - name: FEAST_REPO_PATH
          value: /app/features
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
```

**REST API Usage:**

```bash
# Get online features via HTTP
curl -X POST http://feast-server.heber:6566/get-online-features \
  -H "Content-Type: application/json" \
  -d '{
    "features": [
      "momentum_features:momentum_10d",
      "momentum_features:volatility_20d"
    ],
    "entities": {
      "instrument_key": ["equity:AAPL", "equity:SPY"]
    }
  }'
```

### 31.14 Project Setup

Each project integrates with Feast via the Heber SDK:

```python
# kairos/config.py
from heber.reader import HeberReader
from feast import FeatureStore

# Heber client for Silver/Gold data
heber = HeberReader(
    catalog_url="https://heber-catalog.internal/api/v1",
    api_key=os.environ["HEBER_API_KEY"],
)

# Feast store for feature access
feast_store = FeatureStore(repo_path="heber/features/")

# Training: Use Feast historical features
def get_training_data(symbols, start_date, end_date):
    entity_df = pd.DataFrame({
        "instrument_key": symbols,
        "event_timestamp": [end_date] * len(symbols),
    })
    return feast_store.get_historical_features(
        entity_df=entity_df,
        features=["momentum_features:momentum_10d", ...],
    ).to_df()

# Inference: Use Feast online features
def get_inference_features(symbols):
    return feast_store.get_online_features(
        features=["momentum_features:momentum_10d", ...],
        entity_rows=[{"instrument_key": s} for s in symbols],
    ).to_dict()
```

### 31.15 Feature Lineage

Track feature provenance from Silver to Gold to consumption:

```python
# Get lineage for a feature
lineage = heber.get_feature_lineage("momentum_features:momentum_10d")
# {
#   "feature": "momentum_10d",
#   "feature_view": "momentum_features@v2",
#   "sources": [
#     {"layer": "silver", "dataset": "bars", "columns": ["close", "bar_start_ts"]}
#   ],
#   "pipeline": "compute_momentum_features",
#   "schedule": "daily 18:00 ET",
#   "consumers": ["kairos", "nightwatch"]
# }
```

---

## 32) Feature Template Library

This section provides ready-to-use feature templates. Copy, modify, and register with Feast.

### 32.1 Technical Momentum Features

```python
# heber/features/templates/momentum.py
"""
Momentum features for equity/crypto price action.
Dependencies: Silver bars dataset
"""
import pandas as pd
import numpy as np

def compute_momentum_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute momentum features for each instrument.

    Input: Silver bars with columns [instrument_key, bar_start_ts, open, high, low, close, volume]
    Output: Gold features with ts_available set to computation time
    """
    def calc_features(df):
        close = df["close"]
        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["bar_start_ts"],
            "ts_available": pd.Timestamp.now(tz="UTC"),

            # Price momentum (returns over lookback)
            "momentum_1d": close.pct_change(1),
            "momentum_5d": close / close.shift(5) - 1,
            "momentum_10d": close / close.shift(10) - 1,
            "momentum_20d": close / close.shift(20) - 1,
            "momentum_60d": close / close.shift(60) - 1,

            # Rate of change
            "roc_5d": (close - close.shift(5)) / close.shift(5) * 100,
            "roc_20d": (close - close.shift(20)) / close.shift(20) * 100,

            # RSI (Relative Strength Index)
            "rsi_14": compute_rsi(close, 14),
            "rsi_28": compute_rsi(close, 28),

            # MACD
            "macd": close.ewm(span=12).mean() - close.ewm(span=26).mean(),
            "macd_signal": (close.ewm(span=12).mean() - close.ewm(span=26).mean()).ewm(span=9).mean(),
        })

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_features)

def compute_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))
```

### 32.2 Volatility Features

```python
# heber/features/templates/volatility.py
"""
Volatility features for risk management and position sizing.
Dependencies: Silver bars dataset
"""

def compute_volatility_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    def calc_features(df):
        close = df["close"]
        high = df["high"]
        low = df["low"]
        returns = close.pct_change()

        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["bar_start_ts"],
            "ts_available": pd.Timestamp.now(tz="UTC"),

            # Realized volatility (annualized)
            "vol_5d": returns.rolling(5).std() * np.sqrt(252),
            "vol_20d": returns.rolling(20).std() * np.sqrt(252),
            "vol_60d": returns.rolling(60).std() * np.sqrt(252),

            # Volatility ratio (short/long)
            "vol_ratio_5_20": returns.rolling(5).std() / returns.rolling(20).std(),
            "vol_ratio_20_60": returns.rolling(20).std() / returns.rolling(60).std(),

            # Parkinson volatility (uses high/low)
            "parkinson_vol_20d": compute_parkinson_vol(high, low, 20),

            # Average True Range (ATR)
            "atr_14": compute_atr(high, low, close, 14),
            "atr_20": compute_atr(high, low, close, 20),

            # Bollinger Band width (volatility proxy)
            "bb_width_20": (close.rolling(20).mean() + 2*close.rolling(20).std() -
                           (close.rolling(20).mean() - 2*close.rolling(20).std())) / close.rolling(20).mean(),

            # Z-score of price
            "price_zscore_20d": (close - close.rolling(20).mean()) / close.rolling(20).std(),
            "price_zscore_60d": (close - close.rolling(60).mean()) / close.rolling(60).std(),
        })

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_features)

def compute_parkinson_vol(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    log_hl = np.log(high / low)
    return np.sqrt((log_hl ** 2).rolling(window).mean() / (4 * np.log(2))) * np.sqrt(252)

def compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()
```

### 32.3 Options Flow Features (Unusual Whales)

```python
# heber/features/templates/flow_features.py
"""
Options flow intelligence features from Unusual Whales data.
Dependencies: Silver flow_alerts, darkpool_trades datasets
"""

def compute_flow_features(
    flow_df: pd.DataFrame,
    bars_df: pd.DataFrame,
    lookback_hours: int = 24
) -> pd.DataFrame:
    """
    Compute flow-based features aggregated per underlying per timestamp.
    """
    # Merge flow with underlying bars for context
    flow = flow_df.merge(
        bars_df[["instrument_key", "bar_start_ts", "close", "volume"]].rename(
            columns={"instrument_key": "underlying_key"}
        ),
        left_on=["underlying", "ts_event"],
        right_on=["underlying_key", "bar_start_ts"],
        how="left"
    )

    def calc_features(df):
        return pd.DataFrame({
            "instrument_key": f"equity:{df['underlying'].iloc[0]}",
            "ts_event": df["ts_event"],
            "ts_available": pd.Timestamp.now(tz="UTC"),

            # Premium aggregates
            "total_premium_24h": df["premium"].rolling(f"{lookback_hours}h").sum(),
            "call_premium_24h": df[df["put_call"] == "C"]["premium"].rolling(f"{lookback_hours}h").sum(),
            "put_premium_24h": df[df["put_call"] == "P"]["premium"].rolling(f"{lookback_hours}h").sum(),

            # Call/Put ratio
            "call_put_premium_ratio": (
                df[df["put_call"] == "C"]["premium"].rolling(f"{lookback_hours}h").sum() /
                df[df["put_call"] == "P"]["premium"].rolling(f"{lookback_hours}h").sum().replace(0, np.nan)
            ),

            # Sweep activity
            "sweep_count_24h": (df["alert_type"] == "SWEEP").rolling(f"{lookback_hours}h").sum(),
            "sweep_premium_24h": df[df["alert_type"] == "SWEEP"]["premium"].rolling(f"{lookback_hours}h").sum(),

            # Premium as % of underlying volume (normalized)
            "premium_to_volume_ratio": df["premium"] / (df["close"] * df["volume"]).replace(0, np.nan),

            # OTM/ITM breakdown
            "otm_call_premium": df[(df["put_call"] == "C") & (df["strike"] > df["spot_px"])]["premium"].sum(),
            "itm_put_premium": df[(df["put_call"] == "P") & (df["strike"] > df["spot_px"])]["premium"].sum(),
        })

    return flow.groupby("underlying", group_keys=False).apply(calc_features)
```

### 32.4 Microstructure Features

```python
# heber/features/templates/microstructure.py
"""
Market microstructure features from quotes and trades.
Dependencies: Silver quotes, trades datasets
"""

def compute_microstructure_features(
    quotes_df: pd.DataFrame,
    trades_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute market microstructure features.
    Useful for execution quality and short-term alpha.
    """
    def calc_features(df):
        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["ts_event"],
            "ts_available": pd.Timestamp.now(tz="UTC"),

            # Spread metrics
            "bid_ask_spread": df["ask_px"] - df["bid_px"],
            "spread_bps": (df["ask_px"] - df["bid_px"]) / df["mid_px"] * 10000,
            "spread_avg_5m": ((df["ask_px"] - df["bid_px"]) / df["mid_px"] * 10000).rolling("5min").mean(),

            # Depth metrics
            "bid_depth": df["bid_sz"],
            "ask_depth": df["ask_sz"],
            "depth_imbalance": (df["bid_sz"] - df["ask_sz"]) / (df["bid_sz"] + df["ask_sz"]),

            # Quote intensity
            "quote_count_1m": df["event_id"].rolling("1min").count(),
            "quote_count_5m": df["event_id"].rolling("5min").count(),

            # Price impact proxy
            "mid_px": (df["bid_px"] + df["ask_px"]) / 2,
            "mid_change_1m": ((df["bid_px"] + df["ask_px"]) / 2).diff(periods=60),  # Assuming 1s data
        })

    quotes_df["mid_px"] = (quotes_df["bid_px"] + quotes_df["ask_px"]) / 2
    return quotes_df.groupby("instrument_key", group_keys=False).apply(calc_features)
```

### 32.5 Cross-Asset / Relative Features

```python
# heber/features/templates/cross_asset.py
"""
Cross-asset and relative value features.
Dependencies: Silver bars for multiple instruments
"""

def compute_relative_features(
    bars_df: pd.DataFrame,
    benchmark_key: str = "equity:SPY"
) -> pd.DataFrame:
    """
    Compute features relative to a benchmark (e.g., SPY).
    """
    # Get benchmark data
    benchmark = bars_df[bars_df["instrument_key"] == benchmark_key][
        ["bar_start_ts", "close"]
    ].rename(columns={"close": "benchmark_close"})

    # Merge with all instruments
    merged = bars_df.merge(benchmark, on="bar_start_ts", how="left")

    def calc_features(df):
        returns = df["close"].pct_change()
        bench_returns = df["benchmark_close"].pct_change()

        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["bar_start_ts"],
            "ts_available": pd.Timestamp.now(tz="UTC"),

            # Relative strength
            "rel_strength_20d": (df["close"] / df["close"].shift(20)) / (df["benchmark_close"] / df["benchmark_close"].shift(20)),

            # Beta (rolling)
            "beta_60d": returns.rolling(60).cov(bench_returns) / bench_returns.rolling(60).var(),

            # Alpha (excess return vs benchmark)
            "alpha_20d": returns.rolling(20).mean() - bench_returns.rolling(20).mean(),

            # Correlation to benchmark
            "corr_spy_20d": returns.rolling(20).corr(bench_returns),
            "corr_spy_60d": returns.rolling(60).corr(bench_returns),

            # Idiosyncratic volatility
            "idio_vol_20d": (returns - bench_returns).rolling(20).std() * np.sqrt(252),
        })

    return merged[merged["instrument_key"] != benchmark_key].groupby(
        "instrument_key", group_keys=False
    ).apply(calc_features)
```

### 32.6 Label Templates

```python
# heber/features/templates/labels.py
"""
Common label (target variable) computations.
Remember: ts_available = ts_label + forward_window
"""

def compute_return_labels(bars_df: pd.DataFrame, horizons: list = [1, 5, 10, 20]) -> pd.DataFrame:
    """
    Compute forward-looking return labels.
    """
    def calc_labels(df):
        close = df["close"]
        result = {
            "instrument_key": df["instrument_key"],
            "ts_label": df["bar_start_ts"],  # Feature cutoff time
        }

        for h in horizons:
            # Forward return (what we're predicting)
            result[f"return_{h}d"] = close.shift(-h) / close - 1
            # ts_available = ts_label + horizon (label only observable after horizon passes)
            result[f"ts_available_{h}d"] = df["bar_start_ts"] + pd.Timedelta(days=h)

        return pd.DataFrame(result)

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_labels)

def compute_classification_labels(bars_df: pd.DataFrame, threshold: float = 0.02) -> pd.DataFrame:
    """
    Compute classification labels (up/down/flat).
    """
    def calc_labels(df):
        ret_5d = df["close"].shift(-5) / df["close"] - 1

        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_label": df["bar_start_ts"],
            "ts_available": df["bar_start_ts"] + pd.Timedelta(days=5),

            # Binary: up or not
            "label_up_5d": (ret_5d > threshold).astype(int),

            # Ternary: up/down/flat
            "label_direction_5d": pd.cut(
                ret_5d,
                bins=[-np.inf, -threshold, threshold, np.inf],
                labels=[-1, 0, 1]
            ).astype(int),
        })

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_labels)
```

### 32.7 Feast Registration Template

```python
# heber/features/register_features.py
"""
Template for registering computed features with Feast.
"""
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int32, String
from datetime import timedelta

# Entity (shared across all feature views)
equity = Entity(name="instrument_key", description="Canonical instrument identifier")

# Feature View Template
def create_feature_view(
    name: str,
    features: list,
    source_path: str,
    ttl_days: int = 90,
    online: bool = True,
    tags: dict = None
) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[equity],
        ttl=timedelta(days=ttl_days),
        schema=[Field(name=f, dtype=Float32) for f in features],
        source=FileSource(
            name=f"{name}_source",
            path=source_path,
            timestamp_field="ts_event",
            created_timestamp_column="ts_available",
        ),
        online=online,
        tags=tags or {},
    )

# Register all feature views
momentum_fv = create_feature_view(
    name="momentum_features",
    features=["momentum_1d", "momentum_5d", "momentum_10d", "momentum_20d", "rsi_14", "macd"],
    source_path="s3://heber/gold/dataset=momentum_features/",
    tags={"category": "technical", "owner": "quant_team"},
)

volatility_fv = create_feature_view(
    name="volatility_features",
    features=["vol_5d", "vol_20d", "vol_60d", "atr_14", "price_zscore_20d"],
    source_path="s3://heber/gold/dataset=volatility_features/",
    tags={"category": "risk", "owner": "quant_team"},
)

flow_fv = create_feature_view(
    name="flow_features",
    features=["total_premium_24h", "call_put_premium_ratio", "sweep_count_24h"],
    source_path="s3://heber/gold/dataset=flow_features/",
    tags={"category": "alternative", "owner": "alpha_team"},
)
```

---

## 33) Data Quality Contracts

### 33.1 Contract Definition

```json
{
  "dataset": "bars",
  "layer": "silver",
  "contracts": {
    "fill_rate": {
      "metric": "rows_per_symbol_per_day",
      "min": 0.95,
      "description": "At least 95% of expected trading days have data"
    },
    "completeness": {
      "metric": "non_null_rate",
      "columns": ["open", "high", "low", "close", "volume"],
      "min": 0.99
    },
    "freshness": {
      "metric": "max_lag_hours",
      "max": 2,
      "description": "Data available within 2 hours of market close"
    },
    "gap_duration": {
      "metric": "max_gap_seconds",
      "max": 86400,
      "description": "No gaps longer than 1 trading day"
    }
  }
}
```

### 33.2 Contract Validation API

```python
# Check data quality for a time range
violations = client.check_data_quality(
    dataset="bars",
    time_range=("2025-01-01", "2025-01-15")
)

# Returns:
# {
#   "passed": False,
#   "violations": [
#     {
#       "contract": "fill_rate",
#       "actual": 0.92,
#       "expected": 0.95,
#       "affected_symbols": ["XYZ", "ABC"],
#       "affected_dates": ["2025-01-03", "2025-01-10"]
#     }
#   ]
# }
```

### 33.3 Quality Metrics in Catalog

```sql
-- Catalog table: data_quality_metrics
CREATE TABLE data_quality_metrics (
    dataset VARCHAR NOT NULL,
    date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value FLOAT NOT NULL,
    contract_threshold FLOAT,
    passed BOOLEAN NOT NULL,
    PRIMARY KEY (dataset, date, metric_name)
);
```

### 33.4 Automated Quality Gates

- Backfill jobs fail if quality contracts are violated
- Alerts fire when production data violates contracts
- Gold feature pipelines can skip days with quality violations

---

## 34) Backtest Integration

### 33.1 Scope

Heber provides **data** for backtesting, not a full backtest engine.

**Heber provides:**

- Point-in-time correct data via `read_asof`
- Labels with proper forward-looking semantics
- Train/test split utilities
- Data quality validation

**User/external provides:**

- Backtest execution (loop over time)
- Portfolio simulation
- Order execution simulation
- Performance metrics

### 33.2 Integration Pattern

```python
from heber.reader import HeberReader
import mlflow  # or W&B, custom tracker

client = HeberReader(...)

# Log experiment metadata
with mlflow.start_run():
    mlflow.log_params({
        "feature_dataset": "momentum_features",
        "feature_version": "v3.2.1",
        "label_dataset": "returns_5d",
        "train_period": "12M",
        "test_period": "3M"
    })

    for train_range, test_range in splits:
        # Heber: data access
        train_data = client.read_gold(...)
        test_data = client.read_gold(...)

        # User: training and evaluation
        model.fit(train_data)
        metrics = evaluate(model, test_data)
        mlflow.log_metrics(metrics)
```

### 33.3 Recommended Experiment Trackers

| Tool | Use Case |
|------|----------|
| MLflow | Full ML lifecycle, model registry |
| Weights & Biases | Experiment tracking, visualizations |
| Custom | Lightweight metadata logging |

### 33.4 Backtest Reproducibility Checklist

For any backtest, log:

- [ ] Feature dataset + version
- [ ] Label dataset + version
- [ ] asof_time used for each read
- [ ] Train/test split parameters
- [ ] Model hyperparameters
- [ ] Random seeds
- [ ] Code commit hash

---

## 34) Streaming Feature Access

### 34.1 Batch vs Real-Time Boundary

| Layer | Update Frequency | Use Case |
|-------|-----------------|----------|
| Gold (batch) | Daily/hourly | Research, backtesting |
| Hot Store | Sub-minute | Production inference |

### 34.2 Latest Value API

```python
# Get most recent feature values (from Hot Store)
latest = client.get_latest(
    dataset="momentum_features",
    symbols=["AAPL", "MSFT", "GOOGL"],
    columns=["momentum_10d", "momentum_20d"]
)

# Returns DataFrame with one row per symbol, most recent values
# Note: these are point-in-time values as of now
```

### 34.3 Hot Store Feature Sync

For Gold features that need real-time access:

```yaml
hot_store_sync:
  momentum_features:
    sync: true
    retention: 7d
    refresh_frequency: 15m   # Re-sync every 15 minutes
    columns:
      - momentum_10d
      - momentum_20d
      - volume_zscore
```

### 34.4 Real-Time Feature Computation (Future)

For sub-second features (not in Heber MVP scope):

- Use streaming compute (Flink, Spark Streaming)
- Push to Hot Store directly
- Heber SDK reads via `get_latest()`

---

## 35) Survivor Bias Handling

### 35.1 The Problem

Backtesting on "current" universe ignores:

- Stocks that delisted (bankruptcy, M&A)
- Stocks that were added recently
- This creates look-ahead bias → inflated backtest returns

### 35.2 Instruments Table Extensions

```sql
-- Add to instruments table
ALTER TABLE instruments ADD COLUMN list_date DATE;
ALTER TABLE instruments ADD COLUMN delist_date DATE;
ALTER TABLE instruments ADD COLUMN delist_reason VARCHAR;
-- delist_reason: 'bankruptcy', 'merger', 'acquisition', 'voluntary', etc.
```

### 35.3 Point-in-Time Universe

```python
# Get universe as it existed on a specific date
universe = client.get_universe(
    asof_date="2023-06-15",
    filter={
        "asset_class": "equity",
        "exchange": ["NYSE", "NASDAQ"],
        "min_market_cap": 1e9
    }
)
# Returns only symbols that were listed AND not delisted as of 2023-06-15
```

### 35.4 Read with Universe Filtering

```python
# Automatically filter to point-in-time universe
df = client.read_gold(
    dataset="momentum_features",
    asof_time="2023-06-15T16:00:00Z",
    universe_asof="2023-06-15",    # Only symbols in universe on this date
    exclude_future_delistings=True  # Exclude symbols that will delist later
)
```

### 35.5 Delist Handling Modes

| Mode | Behavior |
|------|----------|
| `exclude_future_delistings=True` | Drop symbols that delist after asof_date (strict) |
| `exclude_future_delistings=False` | Include all symbols (may have survivor bias) |
| `mark_delistings=True` | Include column `will_delist_within_30d` for signals |

### 35.6 Corporate Actions Integration

For splits, dividends, mergers:

```python
# Read with adjustment factors applied
df = client.read_gold(
    dataset="bars",
    asof_time="2024-01-15",
    adjust_for=["splits", "dividends"]
)

# Read raw (unadjusted) for specific use cases
df = client.read_gold(
    dataset="bars",
    asof_time="2024-01-15",
    adjust_for=None
)
```

---

## 36) Summary: ML/Quant Gap Resolutions

| Gap | Section | Resolution |
|-----|---------|------------|
| Gold versioning | 28 | Explicit version pinning + compatibility check + immutability |
| Label management | 29 | Forward-window semantics + availability alignment |
| Train/test splits | 30 | Walk-forward + embargo + expanding window utilities |
| Feature registry | 31 | Searchable metadata + lineage + ownership |
| Data quality | 32 | Contracts + validation API + automated gates |
| Backtest integration | 33 | Clear boundary: Heber = data, external = execution |
| Streaming features | 34 | `get_latest()` API + Hot Store sync |
| Survivor bias | 35 | Point-in-time universe + delist tracking + adjustment factors |

---

# Part V: Reliability & Operations (SRE)

## 37) SLO Framework

### 37.1 SLO Definitions

| SLO Name | Indicator (SLI) | Target | Window |
|----------|-----------------|--------|--------|
| Ingestion Availability | `heber_consumer_events_processed_total{status="success"} / total` | 99.9% | 30d |
| Write Success Rate | `heber_writer_rows_written_total / rows_attempted` | 99.95% | 30d |
| Read Latency (p99) | `heber_sdk_read_latency_seconds{quantile="0.99"}` | < 500ms | 7d |
| Data Freshness | `max(now() - max(ts_available))` per dataset | < 2 hours | 30d |
| Hot Store Sync Lag | `heber_hotstore_lag_seconds` | < 5 min | 7d |
| Catalog Availability | `up{job="heber-catalog"}` | 99.9% | 30d |
| Catalog Latency (p99) | `heber_catalog_request_duration_seconds{quantile="0.99"}` | < 200ms | 7d |

### 37.2 SLI Calculation Details

**Ingestion Availability:**

```promql
sum(rate(heber_consumer_events_processed_total{status="success"}[30d]))
/
sum(rate(heber_consumer_events_processed_total[30d]))
```

**Data Freshness:**

```promql
max by (dataset) (
  time() - heber_dataset_latest_ts_available_timestamp_seconds
)
```

### 37.3 SLO Dashboard Requirements

- Real-time SLI values
- Error budget remaining (percentage)
- Burn rate (30d, 7d, 1d)
- Historical SLO compliance
- Per-dataset freshness heatmap

### 37.4 Alerting on SLO Burn Rate

| Burn Rate | Window | Severity | Action |
|-----------|--------|----------|--------|
| 14x | 1h | Critical | Page on-call |
| 6x | 6h | Warning | Notify in Slack |
| 3x | 1d | Info | Review in standup |
| 1x | 3d | Info | Track in weekly review |

**Example alert:**

```yaml
- alert: HeberIngestionSLOBurnRateHigh
  expr: |
    (
      sum(rate(heber_consumer_events_processed_total{status="error"}[1h]))
      /
      sum(rate(heber_consumer_events_processed_total[1h]))
    ) > (14 * 0.001)  # 14x burn rate on 99.9% target
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "Ingestion SLO burning at 14x rate"
```

---

## 38) Error Budget Policy

### 38.1 Error Budget Calculation

```
Monthly error budget = (1 - SLO target) × total requests

For 99.9% availability with 100M events/month:
Error budget = 0.001 × 100,000,000 = 100,000 failed events
```

### 38.2 Error Budget States

| State | Budget Remaining | Response |
|-------|------------------|----------|
| **Healthy** | > 50% | Normal operations, feature velocity |
| **Warning** | 25-50% | Pause risky deploys, prioritize reliability |
| **Critical** | < 25% | Freeze features, all-hands on reliability |
| **Exhausted** | 0% | Incident review required before resuming |

### 38.3 Error Budget Decision Tree

```
Budget > 50%?
├─ Yes → Normal development
└─ No → Budget > 25%?
    ├─ Yes → Pause risky deploys
    └─ No → Budget > 0%?
        ├─ Yes → Feature freeze, fix reliability
        └─ No → Mandatory incident review
                 No deploys until postmortem complete
```

### 38.4 Error Budget Spend Authorization

| Action | Budget Cost (estimate) | Approval Required |
|--------|------------------------|-------------------|
| Standard deploy | 0-1% | None |
| High-risk deploy | 1-5% | Tech lead |
| Breaking change | 5-10% | Engineering manager |
| Infrastructure migration | 10-25% | Director |

### 38.5 Error Budget Review

- **Weekly:** Review burn rate, upcoming risks
- **Monthly:** Full SLO compliance report
- **Quarterly:** SLO target adjustment (if needed)

---

## 39) Incident Runbooks

### 39.1 Consumer Lag Spike

**Alert:** `HeberConsumerLagHigh` or `HeberConsumerLagCritical`

**Symptoms:**

- `heber_consumer_lag_seconds` > 60s (warning) or > 300s (critical)
- Data freshness degraded

**Triage:**

1. Check consumer pod health: `kubectl get pods -l app=heber-consumer`
2. Check Redis Streams backlog: `redis-cli XPENDING stream:market.bars heber-writers`
3. Check if rebalancing: consumer logs for "rebalance" messages

**Common Causes:**

- Consumer pod restart / OOM kill
- Redis Streams slow (network, memory)
- Upstream provider burst

**Resolution:**

| Cause | Fix |
|-------|-----|
| Pod OOM | Increase memory limit, restart |
| Redis slow | Check ElastiCache metrics, scale if needed |
| Provider burst | Verify burst is temporary, consider scaling consumers |
| Rebalancing | Wait for rebalance to complete (~30s) |

**Escalation:** If unresolved in 15 minutes → page secondary on-call

---

### 39.2 DLQ Growing

**Alert:** `HeberDLQGrowing`

**Symptoms:**

- `heber_dlq_events_total` increasing
- Events not reaching Silver

**Triage:**

1. Sample DLQ events: check `quarantine/` bucket
2. Identify error pattern: schema mismatch? malformed JSON?
3. Check upstream provider for changes

**Common Causes:**

- Provider schema change (new field, type change)
- Malformed events from gateway
- Heber consumer bug

**Resolution:**

| Cause | Fix |
|-------|-----|
| Schema change | Update Heber schema, reprocess DLQ |
| Malformed events | Fix gateway, purge bad events |
| Consumer bug | Fix, deploy, reprocess DLQ |

**DLQ Reprocessing:**

```bash
./scripts/reprocess-dlq.sh --stream stream:heber.dlq --dry-run
./scripts/reprocess-dlq.sh --stream stream:heber.dlq --confirm
```

---

### 39.3 Hot Store Sync Failure

**Alert:** `HeberHotStoreLagHigh` or `HeberHotStoreSyncError`

**Symptoms:**

- `heber_hotstore_lag_seconds` > 300s
- Hot Store queries return stale data

**Triage:**

1. Check hotloader pod: `kubectl logs -l app=heber-hotloader`
2. Check ClickHouse health: `SELECT 1` on CH cluster
3. Check network between EKS and ClickHouse

**Common Causes:**

- ClickHouse cluster unhealthy
- Hotloader pod crash
- Network partition

**Resolution:**

| Cause | Fix |
|-------|-----|
| ClickHouse down | Check CH logs, restart if needed |
| Hotloader OOM | Increase memory, restart |
| Network issue | Check security groups, VPC endpoints |

**Fallback:** If Hot Store is down, queries should fall back to Silver (slower but correct)

---

### 39.4 Catalog Unreachable

**Alert:** `HeberCatalogDown`

**Symptoms:**

- `up{job="heber-catalog"} == 0`
- SDK discovery calls fail

**Triage:**

1. Check catalog pods: `kubectl get pods -l app=heber-catalog`
2. Check RDS health: AWS console or `pg_isready`
3. Check network/security groups

**Impact:**

- New dataset discovery fails
- Writers continue (degraded mode, skip catalog updates)
- SDK uses cached metadata

**Resolution:**

| Cause | Fix |
|-------|-----|
| Pod crash | Check logs, restart |
| RDS down | AWS console, failover to standby |
| Connection pool exhausted | Increase pool size, check for leaks |

---

### 39.5 Compaction Stuck

**Alert:** `HeberCompactionFailed`

**Symptoms:**

- `heber_compactor_runs_total{status="error"}` increasing
- Small files accumulating in partitions

**Triage:**

1. Check compactor logs for error
2. Check manifest file: `s3 cat s3://heber/silver/bars/.../manifest.json`
3. Check for orphaned files

**Common Causes:**

- Corrupted Parquet file
- Manifest lock stuck
- S3 rate limiting

**Resolution:**

| Cause | Fix |
|-------|-----|
| Corrupted file | Identify and quarantine, recompact |
| Stuck lock | Check lock timestamp, force release if stale |
| S3 throttle | Backoff, spread compaction windows |

---

### 39.6 Leakage Violation Detected

**Alert:** `HeberLeakageViolation`

**Symptoms:**

- `heber_leakage_violations_total` > 0
- Query attempted with `ts_available > asof_time`

**Severity:** CRITICAL — data correctness at risk

**Triage:**

1. Identify source: which SDK client? which query?
2. Check if data was actually used (audit log)
3. Assess impact on downstream systems

**Immediate Actions:**

1. Block violating client (if malicious or buggy)
2. Notify affected downstream users
3. Review recent Gold outputs for contamination

**Root Cause Analysis:**

- SDK bug? (should be impossible if SDK is correct)
- Direct S3 access bypassing SDK?
- Clock skew between systems?

**Postmortem required:** Any leakage violation triggers mandatory postmortem.

---

## 40) On-Call & Escalation

### 40.1 On-Call Rotation

| Role | Coverage | Responsibilities |
|------|----------|------------------|
| Primary | 24/7 (weekly rotation) | First response, triage, resolve P2/P3 |
| Secondary | Business hours backup | Escalation, P1 support |
| Tech Lead | Escalation | Architecture decisions, major incidents |

### 40.2 Escalation Timeline

| Severity | Initial Response | Escalation Trigger |
|----------|------------------|-------------------|
| P1 (Critical) | 5 min | Unresolved in 15 min → Secondary |
| P2 (High) | 15 min | Unresolved in 1 hour → Secondary |
| P3 (Medium) | 1 hour | Unresolved in 4 hours → Tech Lead |
| P4 (Low) | Next business day | Track in backlog |

### 40.3 Severity Definitions

| Severity | Definition | Examples |
|----------|------------|----------|
| P1 | Data loss risk or total outage | Ingestion stopped, leakage detected |
| P2 | Significant degradation | Lag > 30 min, Hot Store down |
| P3 | Partial impact | Single feed slow, Catalog errors |
| P4 | Minor issue | Dashboard broken, cosmetic bugs |

### 40.4 Communication Channels

| Channel | Use For |
|---------|---------|
| PagerDuty | P1/P2 alerts |
| Slack #heber-incidents | Real-time incident coordination |
| Slack #heber-alerts | Non-paging alerts |
| Email | Postmortem distribution |

---

## 41) Chaos Engineering

### 41.1 Fault Injection Goals

Validate that:

1. Graceful degradation works as designed
2. Consumer group rebalancing is seamless
3. DLQ captures malformed events
4. Circuit breakers trip and recover

### 41.2 Chaos Experiments

| Experiment | Target | Expected Outcome |
|------------|--------|------------------|
| Kill consumer pod | `heber-consumer` | Rebalance in <30s, no message loss |
| Kill writer pod | `heber-writer` | In-flight batch to DLQ, restart clean |
| Throttle S3 | Object storage | Backpressure, writes queue, no crash |
| Block Catalog | `heber-catalog` | Degraded mode, cache-only, no crash |
| Inject bad event | Event bus | Event to DLQ, others unaffected |
| Network partition | ClickHouse | Hot Store fails, Silver fallback works |
| High CPU | Any service | Graceful slowdown, no OOM |

### 41.3 Chaos Schedule

| Frequency | Scope | Environment |
|-----------|-------|-------------|
| Weekly | Single pod failures | Staging |
| Monthly | Network partitions | Staging |
| Quarterly | Multi-component failures | Staging (extended window) |
| Annually | Full DR drill | Production (planned maintenance) |

### 41.4 Chaos Tools

- **Kubernetes:** `kubectl delete pod` (manual)
- **Advanced:** Litmus Chaos, Chaos Monkey
- **Network:** `tc` for latency injection

### 41.5 Chaos Runbook Template

```markdown
## Experiment: [Name]

**Hypothesis:** When [condition], the system should [expected behavior].

**Procedure:**
1. Establish baseline metrics
2. Inject fault: [command]
3. Observe for [duration]
4. Remove fault
5. Verify recovery

**Success Criteria:**
- [ ] No data loss
- [ ] Recovery within [X] minutes
- [ ] Alerts fired correctly
- [ ] Degraded mode worked

**Results:**
- Date: ____
- Outcome: PASS / FAIL
- Notes: ____
```

---

## 42) Capacity Planning

### 42.1 Current Baseline (Estimates)

| Metric | Value | Source |
|--------|-------|--------|
| Events/day | 50M | bars + quotes + trades |
| Peak events/sec | 10,000 | Market open |
| Silver storage/day | 5 GB | Parquet, compressed |
| Silver storage/year | 1.8 TB | |
| Hot Store rows/day | 50M | 7-day retention = 350M rows |

### 42.2 Scaling Triggers

| Metric | Threshold | Action |
|--------|-----------|--------|
| Consumer CPU | > 70% sustained (15m) | Add consumer replicas |
| Consumer lag | > 60s sustained (10m) | Add consumer replicas |
| Writer memory | > 80% | Increase memory limit |
| Compactor duration | > 30 min/partition | Increase CPU/memory |
| RDS connections | > 80% max | Increase max_connections or scale |
| S3 request rate | > 3500/sec/prefix | Re-partition prefixes |
| ClickHouse query latency | > 1s p99 | Scale ClickHouse cluster |

### 42.3 Capacity Forecast

| Quarter | Events/day | Storage | Compute |
|---------|------------|---------|---------|
| Q1 2026 | 50M | 1.8 TB | 6 nodes |
| Q2 2026 | 75M (+50%) | 2.7 TB | 8 nodes |
| Q3 2026 | 100M (+33%) | 3.6 TB | 10 nodes |
| Q4 2026 | 150M (+50%) | 5.4 TB | 12 nodes |

### 42.4 Bottleneck Analysis

| Component | CPU-bound? | Memory-bound? | I/O-bound? |
|-----------|------------|---------------|------------|
| Consumer | Medium | High (bloom filter) | Low |
| Writer | Low | High (batch buffer) | High (S3) |
| Compactor | High | Very High | High (S3) |
| Catalog | Low | Low | Medium (Postgres) |
| Hotloader | Low | Medium | High (ClickHouse) |

### 42.5 Cost Scaling

```
Base cost: ~$1,575/month (Section 26)

At 3x volume:
- EKS nodes: $540 → $1,080 (+$540)
- S3: $40 → $120 (+$80)
- RDS: $200 → $400 (+$200)
- ClickHouse: $330 → $660 (+$330)

3x volume cost: ~$2,725/month (+73%)
```

---

## 43) Dependency SLAs & Composite Availability

### 43.1 External Dependency SLAs

| Dependency | Published SLA | Our Assumption |
|------------|---------------|----------------|
| AWS S3 | 99.99% | 99.99% |
| AWS RDS | 99.95% | 99.95% |
| AWS ElastiCache | 99.9% | 99.9% |
| ClickHouse (self-managed) | N/A | 99.5% (estimated) |

### 43.2 Composite Availability

**Serial dependencies** (all must be up):

- Ingestion: Consumer + Event Bus + S3 + Catalog
- Read: SDK + S3 + Catalog (or cache)

**Ingestion path:**

```
A(ingestion) = A(consumer) × A(redis) × A(s3) × A(catalog_degraded)
             = 0.999 × 0.999 × 0.9999 × 0.999
             = 0.996 (99.6%)
```

**Read path (with Catalog cache):**

```
A(read) = A(sdk) × A(s3) × max(A(catalog), A(cache))
        = 0.9999 × 0.9999 × 0.999
        = 0.9988 (99.88%)
```

### 43.3 Dependency Risk Matrix

| Dependency | Impact if Down | Likelihood | Risk Score |
|------------|----------------|------------|------------|
| S3 | Total data loss | Very Low | Medium |
| RDS (Catalog) | Degraded (cache fallback) | Low | Low |
| Redis | Ingestion stops | Low | High |
| ClickHouse | Hot Store down (Silver fallback) | Medium | Medium |
| Event Bus (upstream) | No new data | Medium | High |

### 43.4 Dependency Health Dashboard

Monitor all dependencies in single view:

- `aws_s3_availability`
- `aws_rds_connections`
- `redis_connected_clients`
- `clickhouse_uptime`
- Latency to each dependency (p50, p99)

---

## 44) Summary: Reliability Gap Resolutions

| Gap | Section | Resolution |
|-----|---------|------------|
| SLO/SLI | 37 | 7 SLOs defined with targets, burn rate alerting |
| Error budgets | 38 | Budget states, decision tree, spend authorization |
| Runbooks | 39 | 6 incident runbooks with triage + resolution |
| On-call | 40 | Rotation, escalation timeline, severity definitions |
| Chaos | 41 | 7 experiments, schedule, runbook template |
| Capacity | 42 | Baseline, triggers, forecast, bottleneck analysis |
| Dependency SLAs | 43 | External SLAs, composite availability, risk matrix |

---

# Part VI: Quality Assurance & Testing

## 45) Test Strategy & Pyramid

### 45.1 Test Pyramid

```
                    ┌───────────────┐
                    │   E2E Tests   │  5%
                    │  (10-20 tests)│
                    ├───────────────┤
                    │  Integration  │  25%
                    │  (100+ tests) │
                    ├───────────────┤
                    │  Unit Tests   │  70%
                    │ (500+ tests)  │
                    └───────────────┘
```

### 45.2 Testing Philosophy

1. **Test the invariants** — especially zero-leakage
2. **Fast feedback** — unit tests < 10s, integration < 2min
3. **Deterministic** — no flaky tests allowed in main branch
4. **Isolated** — tests don't depend on external services (mocked)

### 45.3 Coverage Requirements

| Component | Min Line Coverage | Min Branch Coverage |
|-----------|-------------------|---------------------|
| `heber-sdk` | 90% | 85% |
| `heber-consumer` | 80% | 75% |
| `heber-writer` | 80% | 75% |
| `heber-compactor` | 75% | 70% |
| `heber-catalog` | 85% | 80% |
| `heber-hotloader` | 75% | 70% |

### 45.4 Test Categories

| Category | Purpose | Runs In |
|----------|---------|---------|
| Unit | Test functions in isolation | CI (every commit) |
| Integration | Test component interactions | CI (every commit) |
| E2E | Test full data pipeline | CI (merge to main) |
| Leakage | Validate zero-leakage invariant | CI (every commit) |
| Performance | Validate latency/throughput SLOs | Nightly / pre-release |
| Chaos | Validate failure handling | Weekly (staging) |

---

## 46) Unit Test Requirements

### 46.1 What to Unit Test

| Module | Key Unit Tests |
|--------|----------------|
| Event parsing | Schema validation, field extraction, error handling |
| Timestamp logic | `ts_event`, `ts_available`, `ts_commit` calculations |
| Bloom filter | Insert, lookup, false positive rate |
| Batch accumulator | Size limits, flush triggers, ordering |
| Manifest operations | Read, write, merge, rollback |
| SDK query builder | asof_time filtering, partition pruning |
| Schema evolution | Backward/forward compatibility checks |

### 46.2 Mocking Strategy

| Dependency | Mock Approach |
|------------|---------------|
| S3 | `moto` (S3 mock) or `localstack` |
| Redis | `fakeredis` or `testcontainers` |
| Postgres | `testcontainers` with ephemeral DB |
| ClickHouse | Mock or `testcontainers` |

### 46.3 Unit Test Examples

**Timestamp Calculation:**

```python
def test_ts_available_for_realtime():
    event = EventEnvelope(ts_event="2025-01-15T10:00:00Z", ...)
    # For realtime, ts_available = time of receipt
    assert event.ts_available == event.ts_ingest

def test_ts_available_for_backfill():
    event = EventEnvelope(ts_event="2024-01-15T10:00:00Z", ...)
    # For backfill, ts_available = ts_commit (set at write time)
    event.mark_as_backfill()
    assert event.ts_available == event.ts_commit
```

**Bloom Filter:**

```python
def test_bloom_filter_insert_and_lookup():
    bf = BloomFilter(expected_items=1000, fp_rate=0.01)
    bf.add("event_id_123")
    assert bf.might_contain("event_id_123") == True
    assert bf.might_contain("event_id_unknown") == False  # May be True (FP)
```

---

## 47) Integration Test Suite

### 47.1 Integration Test Scope

| Test Suite | Components Tested |
|------------|-------------------|
| Consumer Integration | Event Bus → Consumer → Internal queue |
| Writer Integration | Consumer → Writer → S3 (mocked) |
| Compactor Integration | S3 → Compactor → S3 (manifest updates) |
| Catalog Integration | Catalog API → Postgres |
| SDK Integration | SDK → Catalog + S3 |
| Hot Store Integration | Hotloader → ClickHouse |

### 47.2 Test Fixtures

**Docker Compose for Integration Tests:**

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: heber_test
  redis:
    image: redis:7
  minio:
    image: minio/minio
    command: server /data
  clickhouse:
    image: clickhouse/clickhouse-server
```

### 47.3 Key Integration Tests

| Test Case | Steps | Assertion |
|-----------|-------|-----------|
| Event ingestion | Push event to Redis → Run consumer → Check internal queue | Event parsed correctly |
| S3 write | Accumulate batch → Trigger flush → Check S3 | Parquet file valid, manifest updated |
| Compaction | Create small files → Run compactor → Check S3 | Files merged, old files deleted |
| Catalog CRUD | Create dataset → Read → Update → Delete | All operations succeed |
| SDK read | Write test data → SDK read_asof() → Verify | Data matches, asof filter applied |

### 47.4 Integration Test Isolation

- Each test gets fresh database (schema migration)
- Each test gets fresh S3 bucket (MinIO)
- Tests run in parallel with unique prefixes
- Cleanup on teardown

---

## 48) E2E Test Scenarios

### 48.1 Critical E2E Flows

| Flow | Scenario | Success Criteria |
|------|----------|------------------|
| Happy path | Event → Bronze → Silver → SDK read | Data available within SLO |
| Malformed event | Bad JSON → DLQ | Event in DLQ, others unaffected |
| Duplicate event | Same event_id twice | Single row in Silver |
| Schema evolution | Add new field → Verify backward read | Old SDK can read new data |
| Backfill | Load historical → Verify ts_available | ts_available = ts_commit |
| Compaction | Many small files → Compacted | File count reduced, data intact |
| Hot Store | Silver → ClickHouse → get_latest() | Latest values correct |

### 48.2 E2E Test Implementation

```python
@pytest.mark.e2e
def test_event_ingestion_happy_path():
    # Arrange
    event = create_test_event(symbol="AAPL", ts_event="2025-01-15T10:00:00Z")

    # Act
    publish_to_redis(event)
    wait_for_consumer_processing(timeout=30)

    # Assert
    df = sdk_client.read_asof(
        dataset="bars",
        asof_time="2025-01-15T11:00:00Z",
        symbols=["AAPL"]
    )
    assert len(df) == 1
    assert df.iloc[0]["symbol"] == "AAPL"
```

### 48.3 E2E Test Schedule

- **On merge to main:** Core happy path (5 tests)
- **Nightly:** Full E2E suite (20 tests)
- **Pre-release:** Full suite + performance

---

## 49) Leakage Validation Suite

### 49.1 Purpose

The zero-leakage invariant is the most critical property. These tests validate that **future data is never exposed**.

### 49.2 Leakage Test Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| LK-001 | `read_asof(asof_time=T)` with `ts_available > T` | No future rows returned |
| LK-002 | `asof_join()` with mismatched timestamps | Uses earlier ts_available |
| LK-003 | Backfill with `ts_available = now()` | Rejected or corrected to ts_commit |
| LK-004 | Gold write with future-looking feature | Lineage validation fails |
| LK-005 | Direct S3 read bypassing SDK | Audit log created / blocked |
| LK-006 | Clock skew simulation | ts_available still correct |
| LK-007 | SDK version mismatch | Compatibility check enforced |

### 49.3 Leakage Test Implementation

```python
@pytest.mark.leakage
@pytest.mark.critical
def test_lk001_no_future_data_returned():
    """LK-001: read_asof must not return rows where ts_available > asof_time"""
    # Arrange: Insert data with ts_available in the future
    insert_test_data(
        symbol="TEST",
        ts_event="2025-01-15T10:00:00Z",
        ts_available="2025-01-20T10:00:00Z"  # Future
    )

    # Act: Query at asof_time before ts_available
    df = sdk_client.read_asof(
        dataset="bars",
        asof_time="2025-01-18T00:00:00Z",  # Before ts_available
        symbols=["TEST"]
    )

    # Assert: No rows returned (data not yet "available")
    assert len(df) == 0

@pytest.mark.leakage
@pytest.mark.critical
def test_lk003_backfill_ts_available():
    """LK-003: Backfill data must have ts_available = ts_commit"""
    # Arrange
    backfill_event = create_backfill_event(
        ts_event="2020-01-01T10:00:00Z"  # Historical
    )

    # Act
    ts_commit = run_backfill(backfill_event)

    # Assert
    row = read_raw_from_s3("bars", symbol="TEST", date="2020-01-01")
    assert row["ts_available"] == ts_commit  # Not now(), not ts_event
```

### 49.4 Leakage Test Enforcement

- **All leakage tests must pass to merge**
- **0% tolerance** for leakage test failures
- **Weekly audit:** Review for new leakage vectors

---

## 50) Test Data Strategy

### 50.1 Test Data Sources

| Source | Use Case | Characteristics |
|--------|----------|-----------------|
| Synthetic | Unit/integration tests | Deterministic, fast, no external deps |
| Golden dataset | Regression tests | Fixed, versioned, known outputs |
| Sampled production | E2E/performance tests | Anonymized, represents real patterns |
| Edge cases | Boundary testing | Holidays, splits, delistings, gaps |

### 50.2 Synthetic Data Generator

```python
class TestDataGenerator:
    def generate_bars(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        frequency: str = "1min"
    ) -> pd.DataFrame:
        """Generate realistic bar data for testing."""
        ...

    def generate_with_gaps(self, gap_dates: List[str]) -> pd.DataFrame:
        """Generate data with intentional gaps for testing."""
        ...

    def generate_with_splits(self, split_events: List[dict]) -> pd.DataFrame:
        """Generate data with stock splits."""
        ...
```

### 50.3 Golden Dataset

**Location:** `s3://heber-test-data/golden/v1/`

**Contents:**

- `bars_sample.parquet` — 1M rows, 100 symbols, 30 days
- `quotes_sample.parquet` — 10M rows, 50 symbols, 7 days
- `trades_sample.parquet` — 5M rows, 50 symbols, 7 days
- `expected_outputs/` — Pre-computed Gold features for regression

**Versioning:**

- Golden dataset is versioned (v1, v2, ...)
- Schema changes → new version
- Old versions retained for backward compat testing

### 50.4 Edge Case Library

| Edge Case | Test Data |
|-----------|-----------|
| Market holiday | 2024-12-25 (NYSE closed) |
| Stock split | AAPL 4:1 split on 2020-08-31 |
| Delisting | Lehman Brothers 2008-09-15 |
| IPO | Rivian 2021-11-10 |
| Ticker change | FB → META 2022-06-09 |
| Data gap | 2-hour gap in quotes |
| Extreme values | Price = $0.0001 (penny stock) |

---

## 51) Performance Testing

### 51.1 Performance SLOs (from Section 37)

| Metric | Target | Test Scenario |
|--------|--------|---------------|
| Ingestion throughput | 10,000 events/sec | Sustained load test |
| Write latency (p99) | < 5s per batch | Batch write benchmark |
| Read latency (p99) | < 500ms | SDK read benchmark |
| Compaction time | < 30 min/partition | Compaction benchmark |
| Hot Store query | < 100ms | ClickHouse benchmark |

### 51.2 Load Test Scenarios

| Scenario | Configuration | Duration |
|----------|---------------|----------|
| Baseline | 1,000 events/sec | 10 min |
| Normal load | 5,000 events/sec | 30 min |
| Peak load | 10,000 events/sec | 15 min |
| Burst | 20,000 events/sec | 5 min |
| Sustained | 5,000 events/sec | 4 hours |

### 51.3 Performance Test Tools

| Tool | Purpose |
|------|---------|
| `locust` | HTTP load testing (Catalog API) |
| Custom harness | Event ingestion load |
| `pytest-benchmark` | Micro-benchmarks |
| Prometheus/Grafana | Metrics collection |

### 51.4 Performance Regression Detection

```yaml
# .github/workflows/performance.yml
- name: Run performance benchmarks
  run: pytest tests/performance --benchmark-json=results.json

- name: Compare with baseline
  run: |
    python scripts/compare_benchmarks.py \
      --current results.json \
      --baseline baseline.json \
      --threshold 10%  # Fail if > 10% regression
```

---

## 52) Test Environments

### 52.1 Environment Matrix

| Environment | Purpose | Data Source | Isolation |
|-------------|---------|-------------|-----------|
| Local (dev) | Developer testing | Synthetic | Full (Docker Compose) |
| CI | Automated tests | Synthetic + Golden | Ephemeral containers |
| Staging | Pre-production validation | Sampled production | Shared, refreshed weekly |
| Production | Live system | Real data | N/A |

### 52.2 Local Development Setup

```bash
# Start local environment
docker compose -f docker-compose.test.yml up -d

# Run tests
pytest tests/unit -v
pytest tests/integration -v

# Teardown
docker compose -f docker-compose.test.yml down -v
```

### 52.3 CI Environment

**GitHub Actions runners with:**

- Docker for service containers
- Ephemeral databases (testcontainers)
- MinIO for S3 mocking
- 10 concurrent test jobs

### 52.4 Staging Environment

| Component | Config |
|-----------|--------|
| EKS cluster | 3 nodes (smaller than prod) |
| RDS | db.t3.small (Postgres) |
| Redis | t3.micro |
| S3 | Separate bucket (`heber-staging`) |
| ClickHouse | Single node |

**Data refresh:**

- Weekly snapshot from production (anonymized)
- Synthetic data for sensitive fields

---

## 53) CI Test Gates

### 53.1 PR Merge Gates

| Gate | Tests | Must Pass |
|------|-------|-----------|
| Lint | `ruff`, `mypy` | Yes |
| Unit tests | `pytest tests/unit` | Yes |
| Leakage tests | `pytest tests/leakage` | Yes (0% tolerance) |
| Integration tests | `pytest tests/integration` | Yes |
| Coverage | Line coverage >= threshold | Yes |

### 53.2 Merge to Main Gates

| Gate | Tests | Must Pass |
|------|-------|-----------|
| All PR gates | — | Yes |
| E2E tests | `pytest tests/e2e` | Yes |
| Schema compatibility | Backward compat check | Yes |

### 53.3 Deploy Gates

| Gate | Tests | Environment |
|------|-------|-------------|
| Staging deploy | E2E on staging | Staging |
| Staging smoke | Health + basic queries | Staging |
| Prod canary | Health + latency check | Prod (10%) |
| Prod full | Monitor for 15 min | Prod (100%) |

### 53.4 Flaky Test Policy

| Flake Rate | Action |
|------------|--------|
| < 1% | Monitor |
| 1-5% | Investigate within 1 week |
| 5-10% | Quarantine, fix within 3 days |
| > 10% | Immediate quarantine, P2 bug |

**Quarantine process:**

1. Move test to `tests/quarantine/`
2. Remove from CI gates
3. Track in bug tracker
4. Fix and restore within SLA

---

## 54) Summary: QA/Testing Gap Resolutions

| Gap | Section | Resolution |
|-----|---------|------------|
| Test strategy | 45 | Test pyramid, philosophy, coverage requirements |
| Unit tests | 46 | Module coverage, mocking strategy, examples |
| Integration tests | 47 | Component suites, fixtures, isolation |
| E2E tests | 48 | Critical flows, implementation patterns, schedule |
| Leakage tests | 49 | 7 test cases, zero-tolerance enforcement |
| Test data | 50 | Synthetic, golden dataset, edge case library |
| Performance | 51 | SLOs, load scenarios, regression detection |
| Environments | 52 | Local, CI, staging matrix |
| CI gates | 53 | PR, merge, deploy gates, flaky test policy |

---

# Part VII: Data Source Inventory

## 55) Data-Gateway Providers

Heber receives data from the Data-Gateway, which aggregates multiple upstream providers.

### 55.1 Provider Inventory

| Provider | Capabilities | Priority | Streaming |
|----------|--------------|----------|-----------|
| **Alpaca** | Bars, quotes, trades, options, crypto, news | 1 | Yes |
| **Unusual Whales** | Flow alerts, darkpool trades, congress, lobbying | 1 | No |
| **Finnhub** | Bars, quotes, news, sentiment | 2 | Yes |
| **Alpha Vantage** | Forex, crypto, economic indicators | 3 | No |
| **yFinance** | Historical bars (fallback) | 2 | No |
| **News API** | News articles, headlines | 1 | No |
| **SEC Edgar** | Filings (10-K, 10-Q, 8-K, 13F), company info | 1 | No |

### 55.2 Data Type Classification

| Data Type | Storage | Format | Query Pattern |
|-----------|---------|--------|---------------|
| **Market data** (bars, quotes, trades) | Heber Silver | Parquet | Columnar analytics, ASOF |
| **Options** (chains, greeks) | Heber Silver | Parquet | Columnar analytics |
| **Flow/Darkpool** | Heber Silver | Parquet | Columnar analytics |
| **Fundamentals** (revenue, EPS, ratios) | Heber Silver | Parquet | Point-in-time lookups |
| **Economic indicators** (GDP, CPI) | Heber Silver | Parquet | Time-series analysis |
| **Forex/Crypto rates** | Heber Silver | Parquet | Time-series analysis |
| **News metadata** | Heber Silver | Parquet | Event-driven joins |
| **News body** (full text) | Document Store | JSON/Text | Full-text search |
| **SEC filings** (full text) | Document Store | JSON/Text | Full-text search, RAG |
| **SEC metadata** | Heber Silver | Parquet | Point-in-time lookups |

---

## 56) Structured vs Unstructured Boundary

### 56.1 Architecture

```
Data-Gateway
     │
     ├─────────────────────────┬──────────────────────────┐
     ▼                         ▼                          ▼
 Heber (Parquet)         Document Store           Vector DB (future)
 ┌─────────────────┐    ┌─────────────────┐     ┌─────────────────┐
 │ bars            │    │ news_articles   │     │ news_embeddings │
 │ quotes          │    │ sec_filings     │     │ filing_chunks   │
 │ trades          │    │ press_releases  │     └─────────────────┘
 │ options         │    └─────────────────┘
 │ flow_alerts     │
 │ darkpool        │
 │ fundamentals    │
 │ economic        │
 │ forex           │
 │ crypto          │
 │ news_events     │ ← metadata only, links to doc store
 │ filing_events   │ ← metadata only, links to doc store
 └─────────────────┘
```

### 56.2 Design Principles

1. **Heber stores structured, columnar data** — optimized for analytics
2. **Document Store stores text/unstructured** — optimized for search
3. **Cross-reference via ID** — Heber metadata contains `doc_store_id`
4. **Same `ts_available` semantics** — leakage rules apply to metadata

### 56.3 Document Store (Out of Scope)

Document storage is **not part of Heber**. Recommended options:

- **Elasticsearch** — full-text search, aggregations
- **MongoDB** — flexible document storage
- **S3 + Athena** — simple JSON storage with SQL queries
- **Vector DB** (Pinecone, Qdrant) — for embedding-based retrieval

---

## 57) Heber Silver Datasets (Complete Inventory)

### 57.1 Market Data (Alpaca, Finnhub, yFinance)

| Dataset | Source | Partitioning | Key Columns |
|---------|--------|--------------|-------------|
| `bars` | Alpaca, yFinance | `dt`, `symbol` | open, high, low, close, volume, vwap |
| `quotes` | Alpaca | `dt`, `hour`, `symbol` | bid, ask, bid_size, ask_size |
| `trades` | Alpaca | `dt`, `hour`, `symbol` | price, size, exchange |
| `bars_daily` | Alpaca, yFinance | `dt`, `symbol` | OHLCV, adjusted |

### 57.2 Options (Alpaca)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `option_contracts` | — | underlying, strike, expiry, type |
| `option_quotes` | `dt`, `underlying` | bid, ask, delta, gamma, theta, vega, iv |
| `option_trades` | `dt`, `underlying` | price, size, exchange |

### 57.3 Alternative Data (Unusual Whales)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `flow_alerts` | `dt` | symbol, strike, expiry, premium, sentiment |
| `darkpool_trades` | `dt` | symbol, price, size, exchange |
| `congress_trades` | `dt` | politician, symbol, tx_type, amount |
| `lobbying` | `dt` | company, issue, amount |

### 57.4 Fundamentals (SEC, Alpha Vantage)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `company_info` | — | symbol, name, sector, industry, cik |
| `income_statement` | `fiscal_year` | revenue, net_income, eps, shares |
| `balance_sheet` | `fiscal_year` | assets, liabilities, equity |
| `cash_flow` | `fiscal_year` | operating, investing, financing |
| `ratios` | `dt` | pe, pb, ps, roe, roa |

### 57.5 Economic Indicators (Alpha Vantage)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `gdp` | — | value, date, frequency |
| `cpi` | — | value, date |
| `unemployment` | — | value, date |
| `interest_rate` | — | rate, date, type |
| `treasury_yield` | — | maturity, yield, date |

### 57.6 Forex & Crypto (Alpaca, Alpha Vantage)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `forex_rates` | `dt` | pair, open, high, low, close |
| `crypto_bars` | `dt`, `symbol` | open, high, low, close, volume |
| `crypto_quotes` | `dt`, `symbol` | bid, ask |

### 57.7 News & Filings (Metadata Only)

| Dataset | Partitioning | Key Columns |
|---------|--------------|-------------|
| `news_events` | `dt` | headline, symbols, source, sentiment, doc_store_id |
| `filing_events` | `dt` | cik, form_type, filed_date, accepted_date, doc_store_id |

---

## 58) News Events Schema

### 58.1 Parquet Schema (Heber Silver)

```python
news_events_schema = pa.schema([
    ("event_id", pa.string()),
    ("ts_event", pa.timestamp("us", tz="UTC")),      # When news was published
    ("ts_available", pa.timestamp("us", tz="UTC")), # When Heber received it

    # Content metadata
    ("headline", pa.string()),
    ("summary", pa.string()),                        # First 500 chars
    ("source", pa.string()),                         # Reuters, Bloomberg, etc.
    ("url", pa.string()),

    # Structured fields
    ("symbols", pa.list_(pa.string())),              # Mentioned tickers
    ("categories", pa.list_(pa.string())),           # earnings, merger, etc.

    # Enrichment
    ("sentiment_score", pa.float32()),               # -1 to +1
    ("sentiment_label", pa.string()),                # positive, negative, neutral
    ("relevance_score", pa.float32()),               # 0 to 1

    # Cross-reference
    ("doc_store_id", pa.string()),                   # ID in document store
    ("doc_store_type", pa.string()),                 # elasticsearch, mongodb, s3
])
```

### 58.2 Usage Pattern

```python
# 1. Query news metadata from Heber
news = client.read_asof(
    dataset="news_events",
    asof_time="2025-01-15T16:00:00Z",
    filters={"symbols": ["AAPL"], "sentiment_label": "negative"}
)

# 2. Fetch full text from document store (external)
for row in news.itertuples():
    article = doc_store.get(row.doc_store_type, row.doc_store_id)
    print(article["body"])
```

---

## 59) Filing Events Schema (SEC)

### 59.1 Parquet Schema (Heber Silver)

```python
filing_events_schema = pa.schema([
    ("filing_id", pa.string()),
    ("ts_filed", pa.timestamp("us", tz="UTC")),      # SEC filing date
    ("ts_accepted", pa.timestamp("us", tz="UTC")),   # SEC acceptance date
    ("ts_available", pa.timestamp("us", tz="UTC")), # When Heber received it

    # Company info
    ("cik", pa.string()),
    ("company_name", pa.string()),
    ("symbol", pa.string()),                         # If mapped

    # Filing details
    ("form_type", pa.string()),                      # 10-K, 10-Q, 8-K, 13F, etc.
    ("accession_number", pa.string()),
    ("file_number", pa.string()),

    # Period
    ("period_of_report", pa.date32()),               # Fiscal period end
    ("fiscal_year", pa.int32()),
    ("fiscal_quarter", pa.int32()),

    # Flags
    ("is_amendment", pa.bool_()),
    ("is_annual", pa.bool_()),
    ("is_quarterly", pa.bool_()),

    # Extracted structured data (for common filings)
    ("exhibits", pa.list_(pa.string())),
    ("items_reported", pa.list_(pa.string())),       # For 8-K: Item 2.02, etc.

    # Cross-reference
    ("doc_store_id", pa.string()),
    ("sec_url", pa.string()),
])
```

### 59.2 Extracted Financials

For 10-K and 10-Q filings, we extract structured financials into separate datasets:

```python
# Heber: Structured extraction (separate dataset)
income = client.read_asof(
    dataset="income_statement",
    asof_time="2025-01-15",  # Uses ts_available from filing
    filters={"symbol": "AAPL", "fiscal_year": 2024}
)
# → Returns: revenue, net_income, eps (structured, no leakage)

# Full filing text (external)
filing = doc_store.get("sec", filing_id)
# → Returns: full 10-K HTML/text
```

### 59.3 ts_available for Filings

**Critical:** SEC filings have specific availability semantics:

| Timestamp | Meaning |
|-----------|---------|
| `ts_filed` | When company submitted to SEC |
| `ts_accepted` | When SEC accepted the filing (public) |
| `ts_available` | `ts_accepted` — this is when it became public knowledge |

**Anti-leakage:** `ts_available = ts_accepted`, not `ts_filed`. Filings are not public until accepted.

---

## 60) Event Bus Streams (Complete)

### 60.1 Stream Inventory

| Stream | Source | Target Dataset |
|--------|--------|----------------|
| `stream:market.bars` | Alpaca | `bars` |
| `stream:market.quotes` | Alpaca | `quotes` |
| `stream:market.trades` | Alpaca | `trades` |
| `stream:market.bars_daily` | Alpaca, yFinance | `bars_daily` |
| `stream:options.quotes` | Alpaca | `option_quotes` |
| `stream:options.trades` | Alpaca | `option_trades` |
| `stream:intel.flow_alerts` | Unusual Whales | `flow_alerts` |
| `stream:intel.darkpool` | Unusual Whales | `darkpool_trades` |
| `stream:intel.congress` | Unusual Whales | `congress_trades` |
| `stream:news.articles` | News API, Finnhub | `news_events` |
| `stream:sec.filings` | SEC Edgar | `filing_events` |
| `stream:fundamentals.financials` | SEC, Alpha Vantage | `income_statement`, `balance_sheet`, etc. |
| `stream:economic.indicators` | Alpha Vantage | `gdp`, `cpi`, etc. |
| `stream:forex.rates` | Alpha Vantage | `forex_rates` |
| `stream:crypto.bars` | Alpaca | `crypto_bars` |

### 60.2 Consumer Group Mapping

| Consumer Group | Streams |
|----------------|---------|
| `heber-market` | `stream:market.*`, `stream:crypto.*` |
| `heber-options` | `stream:options.*` |
| `heber-intel` | `stream:intel.*` |
| `heber-fundamentals` | `stream:fundamentals.*`, `stream:economic.*`, `stream:forex.*` |
| `heber-events` | `stream:news.*`, `stream:sec.*` |

---

## 61) Implementation Slices (Updated)

### 61.1 Revised Slice Plan

| Slice | Scope | Datasets |
|-------|-------|----------|
| **1** | Core market data | bars, quotes, trades (Alpaca) |
| **2** | Options chain | option_contracts, option_quotes, option_trades |
| **3** | Alternative data | flow_alerts, darkpool_trades, congress_trades |
| **4** | News & filings | news_events, filing_events (metadata only) |
| **5** | Fundamentals | income_statement, balance_sheet, cash_flow, ratios |
| **6** | Economic & FX | gdp, cpi, forex_rates, crypto_bars |
| **7** | Gold layer | SDK primitives, feature pipelines, leakage validation |
| **8** | Hot Store | ClickHouse integration for real-time queries |

---

## 62) Summary: Data Source Additions

| Addition | Section | Description |
|----------|---------|-------------|
| Provider inventory | 55 | All 7 Data-Gateway providers catalogued |
| Storage boundary | 56 | Structured (Heber) vs unstructured (Doc Store) |
| Dataset inventory | 57 | 20+ Silver datasets across all data types |
| News schema | 58 | Parquet schema with doc_store cross-reference |
| Filing schema | 59 | SEC metadata with ts_available = ts_accepted |
| Stream inventory | 60 | 15 event bus streams with consumer groups |
| Updated slices | 61 | 8-slice implementation plan |
