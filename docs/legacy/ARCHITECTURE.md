> **Legacy doc — superseded by [`../ARCHITECTURE.md`](../ARCHITECTURE.md). Kept for historical reference only.**

# Architecture

Related docs:

- `/Users/jacobmcmillan/Empire/Heber/PRD.md`
- `/Users/jacobmcmillan/Empire/Heber/docs/DATA_CONTRACTS.md`
- `/Users/jacobmcmillan/Empire/Heber/docs/RUNBOOK.md`

## Overview

Heber is a lakehouse for market and intelligence data with a strict **zero-leakage** contract. The system ingests events from Data Gateway, writes raw and normalized lake layers, registers datasets in a catalog, and exposes read access through the `HeberReader` and CLI.

## Data Flow

```text
Data Gateway -> Redis Streams -> heber-consumer -> Bronze (JSONL.gz) + Silver (Parquet)
                                            v
                                    heber-catalog (Postgres)
                                            v
                                  HeberReader + CLI
                                            v
                                 Hot Store (ClickHouse)
```

## Data Layers

- **Bronze**: Raw provider payloads, immutable, gzipped JSONL. Partitioned by `provider/feed/dt/hour`.
- **Silver**: Normalized events for queries, Parquet. Partitioned by `feed/instrument_type/dt` (and `hour` for high-volume feeds).
- **Gold**: Features/labels, Parquet. Partitioned by `dataset/project/version/dt`.

## Core Services

- **heber-consumer** (`heber/writer/consumer.py`)
  - Redis Streams consumer with Bronze-first flow: parse envelope, set `ts_available`, write Bronze, gate by contracted raw feed, normalize for Silver, then write Silver or DLQ.
  - Uncontracted feed behavior is explicit: Bronze persists, Silver is skipped, DLQ event reason is `uncontracted_feed`.
  - Contracted-but-unmapped feed behavior is explicit: Bronze persists, Silver is skipped, DLQ event reason is `unmapped_feed`.
- **heber-compactor** (`heber/writer/compactor.py`)
  - Periodic Parquet compaction for lake partitions.
- **heber-catalog** (`heber/catalog/api.py`)
  - FastAPI service with dataset, instrument, feed mapping, and backfill endpoints.
- **heber-watch** (`heber/watch/`)
  - Tracks flow alert outcomes via triple-barrier labeling for ML Gold layer; polls option quotes from Data Gateway.
- **hot store helpers** (`heber/hotstore/`)
  - ClickHouse tables + query client; sync helpers are provided but not deployed in `docker-compose.yml`.

## Event Contract (EventEnvelope)

The canonical event format (see `heber/models/envelope.py`) includes:

- **Identifiers**: `event_id`, `provider`, `feed`, `source`
- **Instrument**: `instrument_type`, `instrument_key`, `symbol`
- **Timestamps**: `ts_event`, `ts_ingest`, `ts_available`
- **Payload**: normalized `payload` + optional `raw` (Bronze fidelity)

Zero-leakage is enforced via `ts_available` and `read_asof()` semantics.

## Bronze->Silver Normalization

Normalization contracts are centralized so live and backfill use identical rules:

- `heber/writer/ingest_contracts.py`
  - contracted raw feed allowlist for Silver routing
  - feed aliases (`ftds -> ftd`, `short_interest/short_volume -> short_data`, `ticker_flow -> flow_alerts`)
  - payload field mappings and per-feed normalization rules
- `heber/writer/key_normalization.py`
  - strict deterministic symbol/instrument key synthesis
- `heber/writer/normalizer.py`
  - shared row coercion from normalized envelope to Silver schema

This prevents divergence between `SilverWriter` and `BronzeToSilverTransformer`.

## Catalog Schema (Postgres)

The catalog tracks:

- Datasets + schema versions (`datasets`, `dataset_versions`)
- Instruments and provider mappings (`instrument_registry`, `instrument_provider_map`)
- Feed mappings (`feed_mappings`)
- Coverage metadata (`data_coverage`)

See `heber/catalog/db.py` for the canonical schema.

## Hot Store (ClickHouse)

Low-latency access to recent quotes/trades/bars for dashboards and signals. The `HeberReader` uses lake data for backtests and research.

## Zero-Leakage Enforcement

Point-in-time correctness is enforced directly in `HeberReader.read_asof()` via predicate pushdown. The `ts_available <= asof_time` filter is pushed into the pyarrow dataset scan before `to_table()`, preventing:

1. **Transport Leakage**: Using data before it physically arrived (`ts_available`).
2. **Revision Leakage**: Using corrected data that wasn't available at the query time.
3. **Lookahead**: Enforced via `asof_join` and `read_asof` primitives.

## Universe Management (`heber.universe`)

Handles instrument lifecycles to prevent **survivor bias**:

- Tracks listing and delisting events.
- Provides point-in-time universe snapshots (e.g., "S&P 500 constituents as of 2023-01-01").
- Filters out-of-universe instruments from backtests.

## Backtesting (`heber.backtest`)

Provides reproducible experiment tracking and data loading:

- **ExperimentConfig**: Captures all parameters, Git SHA, and universe definition.
- **ExperimentTracker**: Logs metrics and artifacts (plots, internal states).
- **BacktestDataLoader**: Loads data via the Firewall to guarantee leakage-free inputs.

## OSS Migration Components

These modules are present but not yet wired into `HeberReader`:

- **Iceberg**: `heber/storage/iceberg_catalog.py`, `heber/storage/iceberg_writer.py`
- **Schema Registry**: `heber/schema/registry_client.py` (Confluent-compatible API)
- **Versioning**: lakeFS integration (not yet implemented; Gold versions use filesystem discovery)

## Repository Map (Top-Level)

- `heber/` - core services, reader, and storage logic
  - `backfill/` - historical data backfilling service
  - `backtest/` - ML experiment tracking and data loading
  - `bus/` - Redis streams event bus utilities
  - `calendar/` - trading calendar integration
  - `catalog/` - metadata and dataset registry (Postgres)
  - `feast/` - feature store definitions
  - `features/` - feature view definitions
  - (firewall enforcement is built into `HeberReader.read_asof()` via predicate pushdown)
  - `gold/` - ML feature/label generation logic
  - `hotstore/` - ClickHouse writer and query helpers
  - `ml/` - meta-labeling and model training
  - `models/` - Pydantic data models (`EventEnvelope`)
  - `ops/` - observability, logging, and metrics
  - `quality/` - data quality checks (Soda)
  - `retention/` - data retention policy enforcement
  - `schema/` - schema registry client
  - `reader/` - canonical thin filesystem reader (`HeberReader`)
  - `sre/` - site reliability engineering scripts
  - `storage/` - Iceberg/lakeFS storage adapters
  - `universe/` - instrument universe management and survivor bias handling
  - `watch/` - real-time flow alert tracking
  - `writer/` - Bronze/Silver lake writers
- `docs/` - reader docs, ops runbooks, and provider endpoints
- `k8s/`, `infrastructure/` - deployment manifests and Terraform
- `scripts/` - volume init, docker build/push, backups
