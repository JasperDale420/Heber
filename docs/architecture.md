# Architecture

## Overview

Heber is a lakehouse for market and intelligence data with a strict **zero-leakage** contract. The system ingests events from Data Gateway, writes raw and normalized lake layers, registers datasets in a catalog, and exposes read access through the SDK and API.

## Data Flow

```
Data Gateway -> Redis Streams -> heber-consumer -> Bronze (JSONL.gz) + Silver (Parquet)
                                            v
                                    heber-catalog (Postgres)
                                            v
                                     SDK + CLI
                                            v
                                 Hot Store (ClickHouse)
```

## Data Layers

- **Bronze**: Raw provider payloads, immutable, gzipped JSONL. Partitioned by `provider/feed/dt/hour`.
- **Silver**: Normalized events for queries, Parquet. Partitioned by `feed/instrument_type/dt` (and `hour` for high-volume feeds).
- **Gold**: Features/labels, Parquet. Partitioned by `dataset/project/version/dt`.

## Core Services

- **heber-consumer** (`heber/writer/consumer.py`)
  - Redis Streams consumer; validates `EventEnvelope`, sets `ts_available`, writes Bronze + Silver.
- **heber-compactor** (`heber/writer/compactor.py`)
  - Periodic Parquet compaction for lake partitions.
- **heber-catalog** (`heber/catalog/api.py`)
  - FastAPI service with dataset, instrument, feed mapping, and backfill endpoints.
- **hot store helpers** (`heber/hotstore/`)
  - ClickHouse tables + query client; sync helpers are provided but not deployed in `docker-compose.yml`.

## Event Contract (EventEnvelope)

The canonical event format (see `heber/models/envelope.py`) includes:

- **Identifiers**: `event_id`, `provider`, `feed`, `source`
- **Instrument**: `instrument_type`, `instrument_key`, `symbol`
- **Timestamps**: `ts_event`, `ts_ingest`, `ts_available`
- **Payload**: normalized `payload` + optional `raw` (Bronze fidelity)

Zero-leakage is enforced via `ts_available` and `read_asof()` semantics.

## Catalog Schema (Postgres)

The catalog tracks:

- Datasets + schema versions (`datasets`, `dataset_versions`)
- Instruments and provider mappings (`instrument_registry`, `instrument_provider_map`)
- Feed mappings (`feed_mappings`)
- Coverage metadata (`data_coverage`)

See `heber/catalog/db.py` for the canonical schema.

## Hot Store (ClickHouse)

Low-latency access to recent quotes/trades/bars for dashboards and signals. The SDK uses lake data for backtests and research.

## OSS Migration Components

These modules are present but not yet wired into `HeberClient`:

- **Iceberg**: `heber/storage/iceberg_catalog.py`, `heber/storage/iceberg_writer.py`
- **Schema Registry**: `heber/schema/registry_client.py` (Confluent-compatible API)
- **Versioning**: `heber/versioning/` (lakeFS client; used by SDK for Gold version tags)

## Repository Map (Top-Level)

- `heber/` - core services, SDK, and storage logic
- `docs/` - SDK docs, ops runbooks, and provider endpoints
- `k8s/`, `infrastructure/` - deployment manifests and Terraform
- `scripts/` - volume init, docker build/push, backups
