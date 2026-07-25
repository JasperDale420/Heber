# Project Overview & Product Definition (Heber)

Heber is the **data lakehouse** for the Empire monorepo. It receives normalized market and intelligence events from Data-Gateway, persists them through Bronze / Silver / Gold tiers on `/Volumes/heber/data`, and exposes both a catalog REST API and a filesystem reader for every downstream trading system (Cerberus, Kairos, Orbit, Orion, 3Roses, Athena, ...).

> Heber is the *only* sanctioned read path for historical lake data. Trading systems must not reach into raw provider APIs or scan provider Bronze files directly — they consume Silver/Gold through `HeberReader`.

## Mission

Provide point-in-time-correct, contract-validated, reproducible market data with a **strict zero-leakage guarantee**, so backtests and ML training cannot accidentally see information that wasn't available at the simulated decision time.

## Scope

In scope:

- Ingest `EventEnvelope` records from Data-Gateway via Redis Streams (`heber:events`).
- Persist raw provider payloads (Bronze) and typed, normalized rows (Silver).
- Compute and store features / labels / enriched datasets (Gold).
- Maintain a Postgres catalog of datasets, instruments, feed mappings, and coverage.
- Expose `HeberReader` (filesystem, predicate-pushdown) and the Catalog REST API (port `8085`).
- Run real-time flow-alert outcome tracking (`heber.watch`) for ML meta-labeling.
- Run scheduled end-of-day Gold feature pipelines (`heber.gold_poller`).
- Emit operational metrics + JSON health reports (dataflow, daily).

Out of scope:

- Generating trading signals or executing orders (that's Cerberus, Kairos, 3Roses, etc.).
- Hosting model inference services for non-watch use cases.
- Providing a high-latency HTTP read API for lake data — reads go through `HeberReader` (filesystem), not REST.
- Direct provider API integration (that's [Data-Gateway](../../Data-Gateway/README.md)).

## Architectural Pillars

| Pillar | Implementation |
|--------|----------------|
| **Zero leakage** | Every record carries `ts_available`. Reads filter `ts_available <= asof_time` via pyarrow predicate pushdown (not post-filter). See [system architecture](./ARCHITECTURE.md#zero-leakage). |
| **Contract-driven Silver** | `heber/writer/ingest_contracts.py` defines contracted feeds, aliases, and field mappings. Uncontracted feeds go to DLQ with explicit reason. |
| **Bronze-first immutability** | Bronze persists every validated envelope as gzipped JSONL before Silver normalization. Silver/Gold can always be rebuilt from Bronze. |
| **Filesystem reader** | `HeberReader` is a thin pyarrow.dataset wrapper. No HTTP, no Catalog, no lakeFS required to read data. Lightweight `pip install heber[reader]` for external consumers. |
| **Append-only layout** | Hive-partitioned Parquet per feed/dt(/hour). Compactor merges small files. No in-place updates. |

## Storage Layers

```
Bronze   raw JSONL.gz, immutable, append-only
         bronze/provider={}/feed={}/dt={}/hour={}/

Silver   normalized Parquet (rename + type coerce only — no derived fields)
         silver/feed={}/instrument_type={}/dt={}/[hour={}]/

Gold     ML features, labels, enriched datasets
         gold/dataset={}/project={}/version={}/dt={}/
```

Full path layout, partition keys, and rebuild semantics: [system architecture](./ARCHITECTURE.md#storage-layers).

## Services

| Service | Entry point | Port | Purpose |
|---------|-------------|------|---------|
| `heber-consumer` | `python -m heber.writer.consumer` | metrics `9090` | Redis Streams → Bronze + Silver writer |
| `heber-catalog` | `python -m heber.catalog.api` | API `8085`, metrics `9090` | Dataset / instrument / coverage / feed-mapping REST API |
| `heber-watch` | `python -m heber.watch` | metrics `9091` | flow_alerts → watch creation → option-quote polling → Gold feature enrichment |
| `heber-compactor` | `python -m heber.writer.compactor` | — | Periodic Parquet compaction (Silver/Gold) |
| `heber-gold-poller` | `python -m heber.gold_poller` | — | Scheduled EOD Gold feature pipelines (16:35 ET default) |
| `heber-dataflow-health` | `heber health-dataflow` | — | Scheduled Gateway → Ingest → Storage proof-of-flow |
| `health-daily` | `heber health-daily` | — | End-of-day 7-check report (partitions, cross-feed, Soda, fill rate, zero-leakage, DLQ, Gold) |

## Stakeholders & Consumers

- **Cerberus / 3Roses / Kairos / Orbit / Orion** — read Silver/Gold via `HeberReader` for strategies and ML training.
- **Athena** — reads Silver/Gold and event logs for post-trade analysis.
- **EmpireUI** — surfaces catalog metadata and health reports on the dashboard.
- **Data-Gateway** — sole upstream producer; publishes to the `heber:events` Redis Stream.

## Key Documents

- [System architecture](./ARCHITECTURE.md) — Mermaid diagrams, layer flow, zero-leakage mechanics.
- [Codebase summary](./codebase-summary.md) — package-by-package map.
- [Code standards](./code-standards.md) — Empire-wide and Heber-specific conventions.
- [Configuration guide](./configuration-guide.md) — every `HEBER_*` env var.
- [Deployment guide](./deployment-guide.md) — Docker, launchd, rollback.
- [API reference](./API_REFERENCE.md) — Catalog REST + `HeberReader` Python API.
- [Testing guide](./testing-guide.md) — markers, layout, quality gate.
- Root-level: [PRD.md](../PRD.md), [CHANGELOG.md](../CHANGELOG.md), [AGENTS.md](../AGENTS.md), [CLAUDE.md](../CLAUDE.md).

## Non-Goals (Explicitly)

- Heber **does not** call provider APIs. All upstream ingestion routes through Data-Gateway.
- Heber **does not** decide trade entries/exits. It supplies data and labels.
- Heber **does not** mutate Bronze. Schema fixes require re-deriving Silver/Gold from Bronze; Bronze itself is immutable.
- Heber **does not** silently store malformed events — bad rows are DLQ'd with an explicit reason, never corrupted-but-kept.
