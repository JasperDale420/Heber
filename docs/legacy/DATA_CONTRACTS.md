# DATA_CONTRACTS

## Overview

Heber consumes `EventEnvelope` records and stores data across Bronze, Silver, and Gold with strict normalization and point-in-time semantics.

## Schemas

### EventEnvelope

**Producer**: Data Gateway
**Consumers**: Heber consumer, watch services, downstream trading systems
**Format**: JSON envelope in Redis streams

Core required fields:

- `event_id`, `provider`, `feed`, `source`
- `instrument_type`, `instrument_key`, `symbol`
- `ts_event`, `ts_ingest`
- `payload`

Optional fields include `raw`, `quality_flags`, `schema_version`, and `ts_available`.

### Silver Datasets

Silver datasets are normalized Parquet tables keyed by canonical feed mappings.

Examples:

- `bars`
- `quotes`
- `trades`
- `flow_alerts`
- `market_tide`
- `sector_tide`

## Versioning

- Envelope schema and feed mappings are versioned in code.
- Dataset versions are tracked in the catalog.
- Gold datasets can be versioned via path conventions and lakeFS tags.

## Validation

- Ingestion validates envelope shape and key fields.
- Bronze-first policy preserves valid raw envelopes.
- Silver writes are contract-driven and skip/unmap invalid feeds with explicit DLQ reasons.

## Reference

Canonical detailed contract: `/Users/jacobmcmillan/Empire/Heber/docs/data_contract.md`.
