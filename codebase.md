# Heber Codebase

*Generated: 2026-01-19T20:32:23*

---

## Summary

Directory: Users/jacobmcmillan/Empire/Heber
Files analyzed: 41

Estimated tokens: 95.8k

---

## File Structure

```
Directory structure:
└── Heber/
    ├── README.md
    ├── docker-compose.yml
    ├── Dockerfile
    ├── implementation.md
    ├── prd.md
    ├── pyproject.toml
    ├── .env.example
    ├── features/
    │   ├── entities.py
    │   ├── feature_store.yaml
    │   └── feature_views/
    │       ├── __init__.py
    │       └── momentum.py
    ├── heber/
    │   ├── __init__.py
    │   ├── config.py
    │   ├── catalog/
    │   │   ├── __init__.py
    │   │   ├── api.py
    │   │   ├── db.py
    │   │   ├── service.py
    │   │   └── urn.py
    │   ├── firewall/
    │   │   ├── __init__.py
    │   │   ├── asof.py
    │   │   ├── scd.py
    │   │   ├── tests.py
    │   │   └── validation.py
    │   ├── hotstore/
    │   │   ├── __init__.py
    │   │   ├── client.py
    │   │   ├── sync.py
    │   │   └── tables.py
    │   ├── models/
    │   │   ├── __init__.py
    │   │   ├── envelope.py
    │   │   └── silver.py
    │   ├── ops/
    │   │   ├── __init__.py
    │   │   ├── logging.py
    │   │   └── reliability.py
    │   ├── sdk/
    │   │   ├── __init__.py
    │   │   └── client.py
    │   └── writer/
    │       ├── __init__.py
    │       ├── bronze.py
    │       ├── compactor.py
    │       ├── consumer.py
    │       └── silver.py
    └── scripts/
        └── init_volume.sh

```

---

## Source Code

================================================
FILE: README.md
================================================
# Heber Data Lakehouse

Centralized storage for market and intelligence data across all trading projects.

## Quick Start

```bash
# Initialize external volume directories
./scripts/init_volume.sh

# Start infrastructure
docker-compose up -d

# Run tests
uv run pytest tests/ -v
```

## Architecture

```
Data Gateway → Redis Streams → Heber Writer → Bronze/Silver/Gold (Parquet)
                                    ↓
                              Heber Catalog (Postgres)
                                    ↓
                              Hot Store (ClickHouse)
```

## Storage

All data is stored on the external volume `/Volumes/heber`:

- `data/` - Bronze/Silver/Gold Parquet files
- `postgres/` - Catalog database
- `clickhouse/` - Hot Store
- `redis/` - Event bus streams

## Services

- **heber-catalog**: REST API for dataset/instrument discovery
- **heber-consumer**: Redis → Lake writer
- **heber-compactor**: Periodic file compaction

## SDK Usage

```python
from heber.sdk import HeberClient

client = HeberClient()

# Read Silver data (point-in-time correct)
bars = client.read_asof("bars", asof_time="2025-01-15", instrument_keys=["equity:AAPL"])

# Write Gold features
client.write_gold("momentum_features", df=features, project="kairos", version="v1")
```



================================================
FILE: docker-compose.yml
================================================
services:
  # Infrastructure
  postgres:
    image: postgres:16-alpine
    container_name: heber-postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-heber}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-heber_dev_password}
      POSTGRES_DB: ${POSTGRES_DB:-heber_catalog}
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/postgres/data:/var/lib/postgresql/data
    ports:
      - "5433:5432"
    healthcheck:
      test: [ "CMD-SHELL", "pg_isready -U heber -d heber_catalog" ]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    container_name: heber-redis
    command: redis-server --appendonly yes
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/redis/data:/data
    ports:
      - "6380:6379"
    healthcheck:
      test: [ "CMD", "redis-cli", "ping" ]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  clickhouse:
    image: clickhouse/clickhouse-server:24.1
    container_name: heber-clickhouse
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/clickhouse/data:/var/lib/clickhouse
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/clickhouse/logs:/var/log/clickhouse-server
    ports:
      - "8124:8123" # HTTP
      - "9002:9000" # Native
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    healthcheck:
      test: [ "CMD", "clickhouse-client", "--query", "SELECT 1" ]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  # Heber Services
  heber-catalog:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: heber-catalog
    command: [ "python", "-m", "uvicorn", "heber.catalog.api:app", "--host", "0.0.0.0", "--port", "8080" ]
    environment:
      - HEBER_DATA_ROOT=/data
      - HEBER_POSTGRES_URL=postgresql+asyncpg://heber:heber_dev_password@postgres:5432/heber_catalog
      - HEBER_REDIS_URL=redis://redis:6379
      - HEBER_CLICKHOUSE_HOST=clickhouse
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/data:/data
    ports:
      - "8085:8080"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:8080/health" ]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped

  heber-consumer:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: heber-consumer
    command: [ "python", "-m", "heber.writer.consumer" ]
    environment:
      - HEBER_DATA_ROOT=/data
      - HEBER_POSTGRES_URL=postgresql+asyncpg://heber:heber_dev_password@postgres:5432/heber_catalog
      - HEBER_REDIS_URL=redis://redis:6379
      - HEBER_CLICKHOUSE_HOST=clickhouse
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/data:/data
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      heber-catalog:
        condition: service_healthy
    restart: unless-stopped

  heber-compactor:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: heber-compactor
    command: [ "python", "-m", "heber.writer.compactor" ]
    environment:
      - HEBER_DATA_ROOT=/data
      - HEBER_POSTGRES_URL=postgresql+asyncpg://heber:heber_dev_password@postgres:5432/heber_catalog
    volumes:
      - ${HEBER_VOLUME_ROOT:-/Volumes/heber}/data:/data
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  heber-data:
    driver: local



================================================
FILE: Dockerfile
================================================
FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY heber/ ./heber/
COPY features/ ./features/

# Install dependencies
RUN uv pip install --system -e .

# Create non-root user
RUN useradd -m -u 1000 heber && chown -R heber:heber /app
USER heber

# Default command (overridden in docker-compose)
CMD ["python", "-m", "uvicorn", "heber.catalog.api:app", "--host", "0.0.0.0", "--port", "8080"]



================================================
FILE: implementation.md
================================================
# Heber Implementation Roadmap

> Complete task breakdown from PRD (**62 sections**). Each task is sized for ~1 agent session.
> ✅ = Done | ⏳ = In Progress | ⬜ = Not Started

---

## PRD Coverage Summary (By Domain)

| Domain | PRD Sections | Status |
|--------|--------------|--------|
| **Core** | §1-6 | ✅ Done |
| **Storage** | §7 | ⏳ Partial |
| **Datasets** | §8-9 | ⏳ Partial |
| **Zero-Leakage** | §10 | ⏳ Partial |
| **Catalog & SDK** | §11 | ⏳ Partial |
| **Operational** | §12 | ⬜ |
| **Backfill** | §13 | ⬜ |
| **Schema Evolution** | §14 | ⬜ |
| **Retention** | §15 | ⬜ |
| **Compaction** | §16 | ⏳ Partial |
| **Configuration** | §18 | ✅ Done |
| **Infrastructure** | §19-27 | ⬜ |
| **ML/Research** | §28-36 | ⬜ |
| **Reliability** | §37-44 | ⬜ |
| **Testing** | §45-54 | ⬜ |
| **Data Sources** | §55-62 | ⬜ |

---

# Part I: Core Foundation

## Phase 1: Project Setup (§4, §18) ✅

- [x] `pyproject.toml` with dependencies
- [x] `Dockerfile` for services
- [x] `docker-compose.yml` (postgres, redis, clickhouse)
- [x] `.env.example` with all env vars
- [x] `heber/config.py` - Pydantic Settings
- [x] `scripts/init_volume.sh`
- [x] Initialize `/Volumes/heber` directories
- [x] Start infrastructure containers

---

## Phase 2: EventEnvelope (§6) ✅

### 2.1 EventEnvelope Model (§6.1-6.7)

- [x] All required fields
- [x] Lineage model with correlation
- [x] `with_ts_available()` helper
- [x] `raw` field for Bronze fidelity
- [x] `source` field (websocket|rest)

### 2.2 instrument_key Standard (§6.2)

- [x] Format: `{type}:{canonical}` or `option:OCC:{symbol}`
- [x] Validation helper function (`validate_instrument_key()`)

### 2.3 ts_available Contract (§6.4)

- [x] Auto-set if not provided
- [x] `ts_effective` property (ts_available + processing_delay_ms)

---

## Phase 3: Storage Model (§7) ✅

### 3.1 Bronze (§7.1)

- [x] JSONL + gzip format
- [x] Partition: provider/feed/dt/hour
- [x] Store `raw` if present (full envelope dumped)

### 3.2 Silver (§7.2)

- [x] Parquet format
- [x] Partition: feed/instrument_type/dt (+hour for high-vol)
- [x] Snappy compression

### 3.3 Gold (§7.3)

- [x] Partition: dataset/project/version/dt
- [x] SDK write_gold()

### 3.4 File Sizing (§7.5)

- [x] Target 128-512 MB files (config: silver_target_file_size_mb)
- [x] Row group 64-256 MB (config: silver_row_group_size_mb)
- [x] Flush by max_rows, max_bytes, max_time

### 3.5 Hot Store Routing (§7.6, §12.10)

- [x] ClickHouse tables: quotes_hot, trades_hot, bars_hot
- [x] TTL retention: 7 days (quotes/trades), 30 days (bars)
- [x] "Latest" materialized views
- [x] HotStoreClient with query methods
- [x] HotStoreSync service for event routing
- [x] Sync lag monitoring (≤5 min SLA)

---

# Part II: Dataset Schemas

## Phase 4: Silver Schemas v1 (§8.7) ✅

### 4.1 Shared Base Columns

- [x] 12 required columns defined (SilverBase)

### 4.2 Market Data

- [x] `bars` schema (BarRecord)
- [x] `quotes` schema (QuoteRecord)
- [x] `trades` schema (TradeRecord)

### 4.3 Alternative Data

- [x] `flow_alerts` schema (FlowAlertRecord)
- [x] `darkpool_trades` schema (DarkpoolTradeRecord)

### 4.4 Reference Data

- [x] `option_contracts` schema (OptionContractRecord)

---

## Phase 5: Silver Schemas v1.5 (§8.3, §8.7.8) ✅

- [x] `greeks` schema (GreeksRecord) - iv, delta, gamma, theta, vega, rho
- [x] `option_chain_snapshots` schema (ChainSnapshotRecord) - one row per contract per snapshot
- [x] `market_tide` schema (MarketTideRecord) - UW periodic flow snapshot

---

## Phase 6: News Handling (§9, §58) ✅

### 6.1 news_articles Schema (§9.1)

- [x] news_id, provider, ts_published, ts_ingest, ts_available
- [x] headline, summary, body, url, source_name

### 6.2 news_entities Schema (§9.1)

- [x] news_id, instrument_key, entity_type, confidence, match_method

### 6.3 Revisions (§9.2)

- [x] valid_from, valid_to, revision_id

### 6.4 News Events Schema (§58)

- [x] sentiment_score, sentiment_label, relevance_score
- [x] doc_store_id cross-reference

---

## Phase 7: Filing Events (§59) ✅

- [x] FilingEventRecord schema with SEC metadata
- [x] ts_accepted for anti-leakage
- [x] doc_store_id cross-reference

---

# Part III: Zero-Leakage Firewall

## Phase 8: Zero-Leakage (§10) ✅

### 8.1 Timestamp Semantics (§10.1-10.2)

- [x] Three timestamps (ts_event, ts_ingest, ts_available)
- [x] Auto-set ts_available

### 8.2 As-Of Query (§10.3)

- [x] `read_asof()` filters ts_available <= T
- [x] `read_asof_range()` for time range queries
- [x] Error on unguarded read in training context

### 8.3 As-Of Joins (§10.4)

- [x] `asof_join()` implementation
- [x] Uses safe time = max(ts_event, ts_available)
- [x] Backward strategy for most recent prior row

### 8.4 Derived Time Keys (§10.5)

- [x] Documented in asof.py
- [x] Bars: use bar_start_ts, Trades/Quotes: use ts_event

### 8.5 Reference Tables (§10.6)

- [x] `read_reference_asof()` with validity windows
- [x] `join_with_reference_asof()` for SCD joins
- [x] SCD fields: valid_from, valid_to, revision_id

### 8.6 Late-Arriving Data (§10.7)

- [x] quality_flags support in SDK (existing)
- [x] Bloom filter dedup (in existing compactor)

### 8.7 Gold Build Gates (§10.9)

- [x] `GoldBuildMetadata` dataclass
- [x] `validate_gold_build()` with hard gates
- [x] LeakageError on violations

### 8.8 Train/Test Split (§10.10)

- [x] `validate_train_test_split()` with purge/embargo

### 8.9 Automated Leakage Tests (§10.12)

- [x] `validate_asof_read()` for runtime checks
- [x] CI tests in firewall/tests.py:
  - test_asof_read_filters_future_data
  - test_asof_join_no_future_lookups
  - test_training_context_requires_asof
- [x] Runtime monitors: monitor_availability_lag, monitor_late_arrivals
- [x] `run_all_leakage_tests()` for CI integration

---

# Part IV: Catalog & SDK

## Phase 9: Catalog DB (§11.2) ✅

### 9.1 Required Tables

- [x] datasets
- [x] dataset_versions
- [x] feed_mappings
- [x] instrument_registry
- [x] instrument_provider_map
- [x] data_coverage
- [x] projects

### 9.2 Optional Tables

- [x] requests
- [x] subscriptions

### 9.3 Database Indexes (§11.3)

- [x] `datasets(dataset_name)` unique
- [x] `dataset_versions(dataset_name, schema_version)` unique
- [x] `feed_mappings(provider, gateway_feed)` unique
- [x] `instrument_registry(instrument_key)` pk
- [x] `instrument_provider_map(provider, provider_symbol)`
- [x] `data_coverage(dataset_name, instrument_key)`
- [x] `subscriptions(project_name, provider, feed)`
- [x] `requests(project_name, provider, feed, created_at)`

### 9.4 Dataset URNs (§11.4) ✅

- [x] URN format: `heber://silver/bars@v1`
- [x] DatasetURN.parse() and **str**()
- [x] Path templates per layer
- [x] resolve_path() for filesystem resolution

### 9.5 Discovery Patterns (§11.5) ✅

- [x] Pattern A: Query by instrument + time
- [x] Pattern B: Query by symbol + date range
- [x] Pattern C: Trace by request_id (stub)

---

## Phase 10: Catalog API (§11.7) ✅

### 10.1 Dataset Endpoints

- [x] GET /datasets
- [x] GET /datasets/{name}
- [x] GET /datasets/{name}/versions
- [x] GET /datasets/{name}/coverage
- [x] GET /datasets/{name}/versions/{version}
- [x] POST /datasets

### 10.2 Instrument Endpoints

- [x] GET /instruments/{key}
- [x] POST /instruments/lookup
- [x] GET /instruments/search
- [x] PUT /instruments/{key}

### 10.3 Feed Endpoints

- [x] GET /feeds
- [x] GET /feeds/resolve

### 10.4 Backfill Endpoints (§11.7.3)

- [x] POST /backfill
- [x] GET /backfill/{id}
- [x] GET /backfill

### 10.5 Response Format (§11.7.4)

- [x] JSON envelope with data + meta
- [x] Error codes (§11.7.5)

### 10.6 Rate Limits (§11.7.6)

- [x] 1000 req/min read, 100 req/min write

### 10.7 Authentication (§11.7.2)

- [x] API key middleware (verify_api_key)

---

## Phase 11: SDK (§11.6, §11.8) ✅

### 11.1 Core Functions

- [x] list_datasets()
- [x] resolve_instrument()
- [x] read_asof()
- [x] write_gold()
- [x] discover() - paths + schema + partitions
- [x] asof_join() - anti-leakage join

### 11.2 SDK Distribution (§11.8)

- [ ] Package as heber-sdk
- [ ] Version semantics (MAJOR.MINOR.PATCH)
- [ ] Deprecation policy

---

# Part V: Operational Requirements

## Phase 12: Reliability & Logging (§12.1-12.4) ✅

- [x] Events don't block Gateway (async processing)
- [x] Idempotency via event_id (BloomFilter, EventDeduplicator)
- [x] Structured JSON logs (§12.5.5)
- [x] Error handling: DLQ (DeadLetterQueue), quarantine, backoff (retry_with_backoff)

---

## Phase 13: Observability (§12.5)

### 13.1 Metrics Stack (§12.5.1-12.5.2)

- [ ] Prometheus `/metrics` endpoint on port 9100
- [ ] Naming: `heber_<service>_<metric>{<labels>}`

### 13.2 Consumer Metrics

- [ ] `heber_consumer_events_received_total{feed,provider}`
- [ ] `heber_consumer_events_processed_total{feed,provider,status}`
- [ ] `heber_consumer_batch_size{feed}`
- [ ] `heber_consumer_lag_seconds{stream}`
- [ ] `heber_consumer_dedupe_drops_total{feed}`

### 13.3 Writer Metrics

- [ ] `heber_writer_rows_written_total{layer,dataset}`
- [ ] `heber_writer_bytes_written_total{layer,dataset}`
- [ ] `heber_writer_files_written_total{layer,dataset}`
- [ ] `heber_writer_flush_duration_seconds{layer}`
- [ ] `heber_writer_errors_total{layer,error_type}`

### 13.4 Compactor Metrics

- [ ] `heber_compactor_runs_total{dataset,status}`
- [ ] `heber_compactor_files_merged_total{dataset}`
- [ ] `heber_compactor_bytes_reclaimed_total{dataset}`

### 13.5 Catalog Metrics

- [ ] `heber_catalog_requests_total{endpoint,status_code}`
- [ ] `heber_catalog_request_duration_seconds{endpoint}`

### 13.6 Anti-Leakage Latency Metrics (§12.5.3)

- [ ] `heber_ingest_lag_seconds` (ts_ingest - ts_event)
- [ ] `heber_availability_lag_seconds` (ts_available - ts_event)
- [ ] `heber_commit_lag_seconds` (ts_commit - ts_ingest)

### 13.7 Alerting Rules (§12.5.4)

- [ ] HeberConsumerLagHigh (>60s for 5m)
- [ ] HeberConsumerLagCritical (>300s for 5m)
- [ ] HeberWriteErrorRateHigh (>1% for 5m)
- [ ] HeberHotStoreLagHigh (>300s for 5m)
- [ ] HeberAvailabilityLagSpike (p99 >30s)
- [ ] HeberDLQGrowing
- [ ] HeberCatalogDown
- [ ] HeberCompactionFailed

### 13.8 Dashboards (§12.5.7)

- [ ] Heber Overview Dashboard
- [ ] Heber Latency Dashboard
- [ ] Heber Health Dashboard

---

## Phase 14: Tracing (§12.5.6)

- [ ] OpenTelemetry OTLP integration
- [ ] Trace ID from Gateway via `lineage.trace_id`
- [ ] Spans: `process_batch`, `dedupe_check`, `write_bronze`, `write_silver`, `api_request`
- [ ] Sampling: 1% prod, 100% dev

---

## Phase 15: Health Checks (§12.12)

- [x] GET /health basic
- [ ] GET /livez (liveness)
- [ ] GET /readyz (readiness)
- [ ] GET /startup (startup probe)
- [ ] Dependency health checks

---

## Phase 16: Circuit Breakers (§12.13)

- [ ] Hard vs soft dependency classification
- [ ] Degradation matrix per service
- [ ] Settings: 5 failures, 30s open, 3 half-open probes
- [ ] Degraded mode metric: `heber_degraded_mode{dependency}`

---

## Phase 17: Rolling Upgrades (§12.14)

- [ ] Graceful shutdown sequence (SIGTERM)
- [ ] Readiness = false on shutdown
- [ ] Drain in-flight + flush buffers
- [ ] 30s shutdown timeout
- [ ] Consumer group rebalancing
- [ ] Canary deployment monitor metrics

---

## Phase 18: Event Bus Config (§12.7) *NEW*

### 18.1 Stream Topology (Pattern A)

- [ ] `stream:market.bars`
- [ ] `stream:market.quotes`
- [ ] `stream:market.trades`
- [ ] `stream:intel.flow_alerts`
- [ ] `stream:intel.darkpool_trades`

### 18.2 Consumer Groups

- [ ] Consumer group per stream
- [ ] Ack after successful write + Catalog update
- [ ] Unacked messages replay on restart

### 18.3 Ordering

- [ ] Preserve per-message timestamps as truth
- [ ] No total order assumption

---

## Phase 19: Backpressure & DLQ (§12.8) *NEW*

### 19.1 Backpressure

- [ ] Consumer lag grows (metric)
- [ ] Never drop data
- [ ] Scale consumers or widen batches

### 19.2 Retry Policy

- [ ] Max retries: 10
- [ ] Backoff: exponential + jitter (100ms → 30s)
- [ ] Retryable: transient storage/DB failures
- [ ] Non-retryable: schema mismatch, malformed envelope

### 19.3 Dead Letter Queue

- [ ] `stream:heber.dlq`
- [ ] `quarantine/` storage path
- [ ] DLQ payload: envelope, error_type, message, stack_trace, first_seen_ts, retry_count

---

## Phase 20: Dedupe Strategy (§12.11) *NEW*

### 20.1 Dedupe Layers

- [ ] Consumer: In-memory bloom filter (fast approx)
- [ ] Writer: Append-only per batch
- [ ] Compactor: Exact dedupe on merge

### 20.2 Bloom Filter Spec

- [ ] Expected items: 10M per hour window
- [ ] False positive rate: 1%
- [ ] Rotate hourly

---

## Phase 21: Compaction Schedule (§12.9)

- [ ] Compact hourly partitions after close (18:10-18:30 for hour=18)
- [ ] Preserve event_id uniqueness
- [ ] Preserve ts_available
- [ ] Atomic writes (temp → rename)

---

## Phase 22: Hot Store Sync (§12.10)

### 22.1 Sync Config

- [ ] Source: event bus or Silver
- [ ] Window: rolling last N days

### 22.2 ClickHouse Tables

- [ ] `quotes_hot`, `trades_hot`, `bars_hot`
- [ ] Partitioned by date
- [ ] TTL: 7 days quotes/trades, 30 days bars

### 22.3 Consistency

- [ ] Lag ≤5 minutes SLA
- [ ] Silver is source of truth (fallback)

---

## Phase 23: Backfill Pipeline (§13)

- [ ] REST backfill patterns
- [ ] Gap detection
- [ ] ts_available = ts_commit for historical
- [ ] Backfill job API (POST /backfill)
- [ ] Progress tracking
- [ ] heber-backfill service

---

## Phase 19: Schema Evolution (§14)

- [ ] Schema registry versioning
- [ ] Backwards/forwards compatibility
- [ ] Migration utilities
- [ ] Reader/writer version checks

---

## Phase 20: Retention & Lifecycle (§15)

- [ ] TTL policies per dataset
- [ ] Partition cleanup automation
- [ ] Archive to cold storage
- [ ] Retention metadata in Catalog

---

## Phase 21: Compaction Protocol (§16)

- [x] Basic compactor
- [ ] Atomicity via manifest
- [ ] Concurrent safety
- [ ] Compaction metrics

---

# Part VI: Infrastructure

## Phase 22: Container Build (§19)

- [x] Dockerfile
- [ ] Multi-stage optimization
- [ ] Image registry (ghcr.io)
- [ ] Version tagging
- [ ] Vulnerability scanning

---

## Phase 23: Kubernetes (§20)

- [ ] Deployment manifests
- [ ] Services
- [ ] ConfigMaps / Secrets
- [ ] Resource limits
- [ ] HPA autoscaling
- [ ] PodDisruptionBudgets

---

## Phase 24: Secrets (§21)

- [ ] External Secrets Operator
- [ ] Vault (if needed)
- [ ] Rotation policy

---

## Phase 25: IaC (§22)

- [ ] Terraform/Pulumi modules
- [ ] GCS/S3 buckets
- [ ] Database provisioning

---

## Phase 26: CI/CD (§23)

- [ ] GitHub Actions workflow
- [ ] Lint + type check
- [ ] Unit/integration tests
- [ ] Docker build & push
- [ ] Deploy to staging/prod

---

## Phase 27: Backup & DR (§24)

- [ ] Postgres backup
- [ ] Parquet backup
- [ ] Recovery procedures
- [ ] RTO/RPO

---

## Phase 28: Network (§25)

- [ ] VPC design
- [ ] Firewall rules
- [ ] Service mesh (optional)

---

## Phase 29: Cost Estimates (§26) *NEW*

- [ ] Document: Monthly production costs
- [ ] Compute estimates (CPU/RAM)
- [ ] Storage estimates (Parquet, Postgres, ClickHouse)
- [ ] Network egress estimates

---

# Part VII: ML/Research Features

## Phase 29: Gold Versioning (§28)

- [ ] Version numbering
- [ ] Manifest format
- [ ] Lineage tracking
- [ ] Reproducibility metadata

---

## Phase 30: Label Management (§29)

- [ ] Label dataset patterns
- [ ] Forward-looking ts_available
- [ ] SDK label helpers

---

## Phase 31: Train/Test Split (§30)

- [ ] Time-series split utilities
- [ ] Purge window calculation
- [ ] Embargo enforcement

---

## Phase 32: Feast Integration (§31)

### 32.1 Configuration

- [x] feature_store.yaml
- [x] entities.py
- [ ] Offline store → Gold Parquet
- [ ] Online store → ClickHouse

### 32.2 Feature Views

- [x] Momentum template
- [ ] Volatility, flow, microstructure views

### 32.3 Materialization & Serving

- [ ] Materialization pipeline
- [ ] Feast Feature Server
- [ ] SDK wrappers

---

## Phase 33: Feature Templates (§32)

- [ ] Implement all templates from PRD §32
- [ ] Registration helpers

---

## Phase 34: Data Quality (§33)

- [ ] Null rate thresholds
- [ ] Value range checks
- [ ] Freshness SLOs
- [ ] Quality dashboard

---

## Phase 35: Backtest Integration (§34)

- [ ] Data loading helpers
- [ ] Point-in-time fetching
- [ ] Result storage

---

## Phase 36: Survivor Bias (§35)

- [ ] Delisting tracking
- [ ] Universe snapshots
- [ ] Historical constituents

---

# Part VIII: Reliability Engineering

## Phase 37: SLO Framework (§37)

- [ ] SLI definitions
- [ ] SLO targets
- [ ] Burn rate alerts

---

## Phase 38: Error Budget (§38)

- [ ] Budget calculation
- [ ] Consumption tracking
- [ ] Policy enforcement

---

## Phase 39: Runbooks (§39)

- [ ] Consumer lag runbook
- [ ] Catalog unavailable runbook
- [ ] Data corruption runbook
- [ ] Disk full runbook

---

## Phase 40: On-Call (§40)

- [ ] Escalation matrix
- [ ] PagerDuty integration

---

## Phase 41: Chaos Engineering (§41)

- [ ] Failure injection tests
- [ ] Weekly chaos runs

---

## Phase 42: Capacity Planning (§42)

- [ ] Growth projections
- [ ] Resource forecasting

---

# Part IX: Testing

## Phase 43: Unit Tests (§46)

- [ ] EventEnvelope tests
- [ ] Bronze/Silver writer tests
- [ ] Catalog service tests
- [ ] SDK tests
- [ ] Bloom filter tests

---

## Phase 44: Integration Tests (§47)

- [ ] Consumer integration
- [ ] Writer integration
- [ ] Catalog integration
- [ ] SDK integration
- [ ] Hot Store integration

---

## Phase 45: E2E Tests (§48)

- [ ] Happy path: Event → Bronze → Silver → SDK
- [ ] Malformed event → DLQ
- [ ] Duplicate event → dedup
- [ ] Backfill flow

---

## Phase 46: Leakage Tests (§49)

- [ ] LK-001: No future data returned
- [ ] LK-002: asof_join correctness
- [ ] LK-003: Backfill ts_available
- [ ] LK-004: Gold build validation
- [ ] LK-005 through LK-007

---

## Phase 47: Performance Tests (§51)

- [ ] Write throughput benchmarks
- [ ] Query latency benchmarks
- [ ] Regression detection

---

## Phase 48: CI Gates (§53)

- [ ] PR merge gates (lint, unit, leakage)
- [ ] Main merge gates (E2E)
- [ ] Deploy gates (staging, prod)
- [ ] Flaky test policy (>5% = quarantine)

---

## Phase 49: Test Data Management (§50) *NEW*

### 49.1 Synthetic Data

- [ ] Data generator for each dataset type
- [ ] Configurable date ranges and symbols

### 49.2 Golden Datasets

- [ ] Curated test fixtures with known values
- [ ] Version controlled test data

### 49.3 Edge Case Library

- [ ] Clock skew scenarios
- [ ] Missing timestamps
- [ ] Schema mismatches
- [ ] Late-arriving data

---

## Phase 50: Test Environments (§52) *NEW*

- [ ] Local: Docker Compose (MinIO, Postgres, Redis)
- [ ] CI: GitHub Actions with testcontainers
- [ ] Staging: Kubernetes with real infra

---

# Part X: Data Sources

## Phase 51: Provider Inventory (§55)

- [ ] Document Alpaca capabilities
- [ ] Document Unusual Whales capabilities
- [ ] Document Finnhub, Alpha Vantage, yFinance, News API, SEC Edgar

---

## Phase 50: Structured vs Unstructured (§56)

- [ ] Define Heber boundary (structured)
- [ ] Define Document Store boundary (unstructured)
- [ ] Cross-reference via doc_store_id

---

## Phase 51: Additional Datasets (§57)

### 51.1 Market Data

- [x] bars, quotes, trades
- [ ] bars_daily

### 51.2 Options

- [ ] option_quotes
- [ ] option_trades

### 51.3 Alternative

- [ ] congress_trades
- [ ] lobbying

### 51.4 Fundamentals

- [ ] company_info
- [ ] income_statement
- [ ] balance_sheet
- [ ] cash_flow
- [ ] ratios

### 51.5 Economic

- [ ] gdp, cpi, unemployment
- [ ] interest_rate, treasury_yield

### 51.6 Forex & Crypto

- [ ] forex_rates
- [ ] crypto_bars, crypto_quotes

---

## Phase 52: Event Bus Streams (§60)

- [ ] Configure 15 streams per inventory
- [ ] Consumer group mapping

---

## Phase 53: Implementation Slices (§61)

Implement in order:

- [ ] Slice 1: Core market data
- [ ] Slice 2: Options chain
- [ ] Slice 3: Alternative data
- [ ] Slice 4: News & filings
- [ ] Slice 5: Fundamentals
- [ ] Slice 6: Economic & FX
- [ ] Slice 7: Gold layer
- [ ] Slice 8: Hot Store

---

## Phase 56: Access Control (§11.9) *FUTURE*

- [ ] Restrict Gold datasets per project
- [ ] Shared Silver datasets
- [ ] SDK token enforcement

---

## Phase 57: Gap Resolution Summaries

### 57.1 Summary §17

- [ ] Document: Data model decisions resolved

### 57.2 Summary §27

- [ ] Document: Infrastructure decisions resolved

### 57.3 Summary §36

- [ ] Document: ML/Quant decisions resolved

### 57.4 Summary §44

- [ ] Document: Reliability decisions resolved

### 57.5 Summary §54

- [ ] Document: QA/Testing decisions resolved

### 57.6 Summary §62

- [ ] Document: Data source decisions resolved

---

# Progress Summary

| Phase Range | Description | Estimated Tasks | Done |
|-------------|-------------|-----------------|------|
| 1-3 | Core Foundation | 30 | 24 |
| 4-7 | Datasets | 25 | 5 |
| 8 | Zero-Leakage | 20 | 5 |
| 9-11 | Catalog & SDK | 50 | 18 |
| 12-22 | Operational | 80 | 3 |
| 23-30 | Infrastructure | 35 | 1 |
| 31-38 | ML/Research | 30 | 3 |
| 39-44 | Reliability | 15 | 0 |
| 45-50 | Testing | 55 | 0 |
| 51-57 | Data Sources & Summaries | 45 | 3 |
| **TOTAL** | | **~385** | **~62** |

**Overall Progress: ~16%**

---

## Next Actions

1. **Complete Phase 4** - Add remaining Silver schemas (darkpool, option_contracts)
2. **Complete Phase 8** - Zero-leakage enforcement (asof_join, build gates)
3. **Complete Phase 9** - Add database indexes
4. **Start Phase 45** - Unit tests for validation



================================================
FILE: prd.md
================================================
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
from heber_sdk import HeberClient

client = HeberClient(
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
    
    # ts_available = ts_label + forward_window + market_close_delay
    # E.g., for T=2025-01-10, forward_window=5d:
    #   Label becomes available at 2025-01-15 16:05 (after market close)
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
from heber_sdk import HeberClient
import pandas as pd

def compute_momentum_features():
    """Daily job to compute momentum features."""
    client = HeberClient()
    
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
from heber_sdk import HeberClient
from feast import FeatureStore

# Heber client for Silver/Gold data
heber = HeberClient(
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
from heber_sdk import HeberClient
import mlflow  # or W&B, custom tracker

client = HeberClient(...)

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



================================================
FILE: pyproject.toml
================================================
[project]
name = "heber"
version = "0.1.0"
description = "Heber Data Lakehouse - Centralized storage for market and intelligence data"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    # Core
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    
    # API
    "fastapi>=0.109",
    "uvicorn[standard]>=0.27",
    
    # Database
    "sqlalchemy>=2.0",
    "asyncpg>=0.29",
    "alembic>=1.13",
    
    # Data processing
    "pandas>=2.0",
    "pyarrow>=15.0",
    "polars>=0.20",
    
    # Event bus
    "redis>=5.0",
    
    # Hot Store
    "clickhouse-connect>=0.7",
    
    # Feature Store
    "feast>=0.38",
    
    # Observability
    "structlog>=24.0",
    "prometheus-client>=0.19",
    
    # Utils
    "httpx>=0.26",
    "tenacity>=8.2",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.1",
    "ruff>=0.2",
    "mypy>=1.8",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.mypy]
python_version = "3.11"
strict = true



================================================
FILE: .env.example
================================================
# Environment Configuration
# Copy to .env and fill in values

# External Volume (where all data is stored)
HEBER_DATA_ROOT=/Volumes/heber/data
HEBER_VOLUME_ROOT=/Volumes/heber

# Postgres (Catalog DB)
POSTGRES_USER=heber
POSTGRES_PASSWORD=heber_dev_password
POSTGRES_DB=heber_catalog
HEBER_POSTGRES_URL=postgresql+asyncpg://heber:heber_dev_password@postgres:5432/heber_catalog

# Redis (Event Bus)
HEBER_REDIS_URL=redis://redis:6379

# ClickHouse (Hot Store)
HEBER_CLICKHOUSE_HOST=clickhouse
HEBER_CLICKHOUSE_PORT=9000
HEBER_CLICKHOUSE_USER=default
HEBER_CLICKHOUSE_PASSWORD=

# Catalog API
HEBER_API_HOST=0.0.0.0
HEBER_API_PORT=8080

# Feature Store (Feast)
FEAST_REPO_PATH=/app/features



================================================
FILE: features/entities.py
================================================
"""Entity definitions for Feast feature store."""

from feast import Entity

# Primary entity: what we compute features for
equity = Entity(
    name="instrument_key",
    description="Canonical instrument identifier (e.g., equity:AAPL, option:AAPL250117C00150000)",
    join_keys=["instrument_key"],
)



================================================
FILE: features/feature_store.yaml
================================================
# Feast Feature Store Configuration for Heber
project: heber
provider: local

# Registry: where feature metadata is stored
# In production, this would be the Heber Catalog (Postgres)
registry: 
  registry_type: file
  path: /data/feast/registry.pb

# Offline Store: historical feature storage (Gold Parquet)
offline_store:
  type: file

# Online Store: real-time feature serving (ClickHouse)
# Note: Using SQLite for local dev, switch to ClickHouse for production
online_store:
  type: sqlite
  path: /data/feast/online.db

# For production, use:
# online_store:
#   type: clickhouse
#   host: clickhouse
#   port: 9000
#   database: feast_online
#   user: default

# Entity key TTL (time to live)
entity_key_serialization_version: 2



================================================
FILE: features/feature_views/__init__.py
================================================
"""Feature views package."""



================================================
FILE: features/feature_views/momentum.py
================================================
"""Momentum feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity

# Source: Gold Parquet files
momentum_source = FileSource(
    name="momentum_source",
    path="/data/gold/dataset=momentum_features/",
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: momentum indicators
momentum_features = FeatureView(
    name="momentum_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="momentum_1d", dtype=Float32),
        Field(name="momentum_5d", dtype=Float32),
        Field(name="momentum_10d", dtype=Float32),
        Field(name="momentum_20d", dtype=Float32),
        Field(name="momentum_60d", dtype=Float32),
        Field(name="rsi_14", dtype=Float32),
        Field(name="rsi_28", dtype=Float32),
        Field(name="macd", dtype=Float32),
        Field(name="macd_signal", dtype=Float32),
    ],
    source=momentum_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "technical",
    },
)



================================================
FILE: heber/__init__.py
================================================
"""Heber Data Lakehouse - Core package."""

__version__ = "0.1.0"



================================================
FILE: heber/config.py
================================================
"""Heber configuration using Pydantic Settings."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Heber application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_prefix="HEBER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Storage
    data_root: Path = Field(
        default=Path("/Volumes/heber/data"),
        description="Root path for Bronze/Silver/Gold data",
    )
    volume_root: Path = Field(
        default=Path("/Volumes/heber"),
        description="Root path for external volume",
    )

    # Postgres (Catalog)
    postgres_url: str = Field(
        default="postgresql+asyncpg://heber:heber_dev_password@localhost:5432/heber_catalog",
        description="PostgreSQL connection URL for Catalog DB",
    )

    # Redis (Event Bus)
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL for event streams",
    )
    redis_stream_name: str = Field(
        default="heber:events",
        description="Redis stream name for incoming events",
    )
    redis_consumer_group: str = Field(
        default="heber-writers",
        description="Redis consumer group name",
    )

    # ClickHouse (Hot Store)
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=9000)
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")
    clickhouse_database: str = Field(default="heber")

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8080)

    # Writer settings (PRD §7.5 - File sizing, batching, compaction)
    bronze_flush_interval_seconds: int = Field(default=30, description="Max time before flushing Bronze")
    bronze_max_batch_size: int = Field(default=10000, description="Max events per Bronze file")
    
    # Silver file sizing targets (PRD §7.5)
    silver_target_file_size_mb: int = Field(default=256, description="Target Parquet file size (128-512 MB)")
    silver_max_rows_per_file: int = Field(default=1_000_000, description="Max rows per file (250k-2M)")
    silver_max_flush_time_seconds: int = Field(default=30, description="Max seconds before flush (5-30s)")
    silver_row_group_size_mb: int = Field(default=128, description="Parquet row group size (64-256 MB)")

    # Environment
    environment: Literal["dev", "staging", "prod"] = Field(default="dev")

    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_root / "gold"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience alias
settings = get_settings()



================================================
FILE: heber/catalog/__init__.py
================================================
"""Heber Catalog - Dataset and instrument registry."""



================================================
FILE: heber/catalog/api.py
================================================
"""Catalog REST API - FastAPI routes.

See PRD Section 11.7 for API contract.
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from heber.catalog.db import Base
from heber.catalog.service import CatalogService
from heber.config import settings

# Database setup
engine = create_async_engine(settings.postgres_url, echo=settings.environment == "dev")
async_session = async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler - create tables on startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(
    title="Heber Catalog API",
    description="Dataset and instrument registry for Heber Data Lakehouse",
    version="0.1.0",
    lifespan=lifespan,
)


# Dependency for database session
async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session


async def get_service(session: AsyncSession = Depends(get_session)) -> CatalogService:
    return CatalogService(session)


# Response models
class MetaResponse(BaseModel):
    request_id: str | None = None
    ts: datetime


class DatasetResponse(BaseModel):
    dataset_name: str
    layer: str
    owner: str
    description: str | None
    storage_root: str
    path_template: str | None
    partition_cols: list | None
    is_active: bool


class DatasetListResponse(BaseModel):
    data: list[DatasetResponse]
    meta: MetaResponse


class DatasetDetailResponse(BaseModel):
    data: DatasetResponse
    meta: MetaResponse


class InstrumentResponse(BaseModel):
    instrument_key: str
    instrument_type: str
    canonical_symbol: str
    underlying_key: str | None
    occ_symbol: str | None
    expiry: datetime | None
    strike: float | None
    put_call: str | None


class InstrumentLookupRequest(BaseModel):
    symbols: list[str]


class FeedMappingResponse(BaseModel):
    provider: str
    gateway_feed: str
    silver_dataset_name: str


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None


class ErrorEnvelope(BaseModel):
    error: ErrorResponse
    meta: MetaResponse


# Health check
@app.get("/health")
async def health():
    return {"status": "healthy", "service": "heber-catalog"}


# Dataset endpoints
@app.get("/api/v1/datasets", response_model=DatasetListResponse)
async def list_datasets(
    layer: str | None = Query(None, description="Filter by layer (bronze|silver|gold)"),
    service: CatalogService = Depends(get_service),
):
    datasets = await service.list_datasets(layer=layer)
    return DatasetListResponse(
        data=[
            DatasetResponse(
                dataset_name=d.dataset_name,
                layer=d.layer,
                owner=d.owner,
                description=d.description,
                storage_root=d.storage_root,
                path_template=d.path_template,
                partition_cols=d.partition_cols,
                is_active=d.is_active,
            )
            for d in datasets
        ],
        meta=MetaResponse(ts=datetime.utcnow()),
    )


@app.get("/api/v1/datasets/{name}", response_model=DatasetDetailResponse)
async def get_dataset(name: str, service: CatalogService = Depends(get_service)):
    dataset = await service.get_dataset(name)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    return DatasetDetailResponse(
        data=DatasetResponse(
            dataset_name=dataset.dataset_name,
            layer=dataset.layer,
            owner=dataset.owner,
            description=dataset.description,
            storage_root=dataset.storage_root,
            path_template=dataset.path_template,
            partition_cols=dataset.partition_cols,
            is_active=dataset.is_active,
        ),
        meta=MetaResponse(ts=datetime.utcnow()),
    )


@app.get("/api/v1/datasets/{name}/versions")
async def get_dataset_versions(name: str, service: CatalogService = Depends(get_service)):
    versions = await service.get_dataset_versions(name)
    return {
        "data": [
            {
                "schema_version": v.schema_version,
                "schema_json": v.schema_json,
                "is_current": v.is_current,
                "created_at": v.created_at,
            }
            for v in versions
        ],
        "meta": {"ts": datetime.utcnow()},
    }


@app.get("/api/v1/datasets/{name}/coverage")
async def get_dataset_coverage(name: str, service: CatalogService = Depends(get_service)):
    coverage = await service.get_coverage(name)
    return {
        "data": [
            {
                "instrument_key": c.instrument_key,
                "dt_min": c.dt_min,
                "dt_max": c.dt_max,
                "approx_row_count": c.approx_row_count,
            }
            for c in coverage
        ],
        "meta": {"ts": datetime.utcnow()},
    }


# Instrument endpoints
@app.get("/api/v1/instruments/{key}")
async def get_instrument(key: str, service: CatalogService = Depends(get_service)):
    instrument = await service.get_instrument(key)
    if not instrument:
        raise HTTPException(status_code=404, detail=f"Instrument '{key}' not found")
    return {
        "data": InstrumentResponse(
            instrument_key=instrument.instrument_key,
            instrument_type=instrument.instrument_type,
            canonical_symbol=instrument.canonical_symbol,
            underlying_key=instrument.underlying_key,
            occ_symbol=instrument.occ_symbol,
            expiry=instrument.expiry,
            strike=instrument.strike,
            put_call=instrument.put_call,
        ),
        "meta": {"ts": datetime.utcnow()},
    }


@app.post("/api/v1/instruments/lookup")
async def lookup_instruments(
    request: InstrumentLookupRequest,
    service: CatalogService = Depends(get_service),
):
    instruments = await service.lookup_instruments(request.symbols)
    return {
        "data": [
            InstrumentResponse(
                instrument_key=i.instrument_key,
                instrument_type=i.instrument_type,
                canonical_symbol=i.canonical_symbol,
                underlying_key=i.underlying_key,
                occ_symbol=i.occ_symbol,
                expiry=i.expiry,
                strike=i.strike,
                put_call=i.put_call,
            )
            for i in instruments
        ],
        "meta": {"ts": datetime.utcnow()},
    }


@app.get("/api/v1/instruments/search")
async def search_instruments(
    instrument_type: str | None = Query(None),
    symbol_prefix: str | None = Query(None),
    limit: int = Query(100, le=1000),
    service: CatalogService = Depends(get_service),
):
    instruments = await service.search_instruments(
        instrument_type=instrument_type,
        symbol_prefix=symbol_prefix,
        limit=limit,
    )
    return {
        "data": [
            InstrumentResponse(
                instrument_key=i.instrument_key,
                instrument_type=i.instrument_type,
                canonical_symbol=i.canonical_symbol,
                underlying_key=i.underlying_key,
                occ_symbol=i.occ_symbol,
                expiry=i.expiry,
                strike=i.strike,
                put_call=i.put_call,
            )
            for i in instruments
        ],
        "meta": {"ts": datetime.utcnow()},
    }


# Feed mapping endpoints
@app.get("/api/v1/feeds")
async def list_feeds(service: CatalogService = Depends(get_service)):
    mappings = await service.list_feed_mappings()
    return {
        "data": [
            FeedMappingResponse(
                provider=m.provider,
                gateway_feed=m.gateway_feed,
                silver_dataset_name=m.silver_dataset_name,
            )
            for m in mappings
        ],
        "meta": {"ts": datetime.utcnow()},
    }


@app.get("/api/v1/feeds/resolve")
async def resolve_feed(
    provider: str = Query(...),
    feed: str = Query(...),
    service: CatalogService = Depends(get_service),
):
    silver_dataset = await service.resolve_feed(provider, feed)
    if not silver_dataset:
        raise HTTPException(
            status_code=404,
            detail=f"No mapping found for provider='{provider}', feed='{feed}'",
        )
    return {
        "data": {"silver_dataset_name": silver_dataset},
        "meta": {"ts": datetime.utcnow()},
    }


# Additional Dataset endpoints (PRD §11.7.3)
@app.get("/api/v1/datasets/{name}/versions/{version}")
async def get_dataset_version(
    name: str,
    version: str,
    service: CatalogService = Depends(get_service),
):
    """Get specific schema version for a dataset."""
    versions = await service.get_dataset_versions(name)
    target = next((v for v in versions if v.schema_version == version), None)
    if not target:
        raise HTTPException(
            status_code=404,
            detail=f"Version '{version}' not found for dataset '{name}'",
        )
    return {
        "data": {
            "schema_version": target.schema_version,
            "schema_json": target.schema_json,
            "is_current": target.is_current,
            "created_at": target.created_at,
        },
        "meta": {"ts": datetime.utcnow()},
    }


class DatasetCreateRequest(BaseModel):
    """Request to create a new dataset."""
    dataset_name: str
    layer: str
    owner: str = "shared"
    description: str | None = None
    storage_root: str
    path_template: str | None = None
    partition_cols: list | None = None
    primary_keys: list | None = None


@app.post("/api/v1/datasets", status_code=201)
async def create_dataset(
    request: DatasetCreateRequest,
    service: CatalogService = Depends(get_service),
):
    """Create a new dataset in the catalog."""
    dataset = await service.create_dataset(
        dataset_name=request.dataset_name,
        layer=request.layer,
        owner=request.owner,
        description=request.description,
        storage_root=request.storage_root,
        path_template=request.path_template,
        partition_cols=request.partition_cols,
        primary_keys=request.primary_keys,
    )
    return {
        "data": {"dataset_name": dataset.dataset_name},
        "meta": {"ts": datetime.utcnow()},
    }


class InstrumentUpsertRequest(BaseModel):
    """Request to upsert an instrument."""
    instrument_key: str
    instrument_type: str
    canonical_symbol: str
    underlying_key: str | None = None
    occ_symbol: str | None = None
    expiry: datetime | None = None
    strike: float | None = None
    put_call: str | None = None


@app.put("/api/v1/instruments/{key}")
async def upsert_instrument(
    key: str,
    request: InstrumentUpsertRequest,
    service: CatalogService = Depends(get_service),
):
    """Upsert an instrument in the registry."""
    instrument = await service.upsert_instrument(
        instrument_key=key,
        instrument_type=request.instrument_type,
        canonical_symbol=request.canonical_symbol,
        underlying_key=request.underlying_key,
        occ_symbol=request.occ_symbol,
        expiry=request.expiry,
        strike=request.strike,
        put_call=request.put_call,
    )
    return {
        "data": {"instrument_key": instrument.instrument_key},
        "meta": {"ts": datetime.utcnow()},
    }


# Backfill endpoints (PRD §11.7.3)
class BackfillRequest(BaseModel):
    """Request to create a backfill job."""
    provider: str
    feed: str
    instrument_keys: list[str]
    start_date: str
    end_date: str
    project: str | None = None


# In-memory backfill jobs (use Redis/DB in production)
_backfill_jobs: dict = {}


@app.post("/api/v1/backfill", status_code=201)
async def create_backfill(request: BackfillRequest):
    """Create a new backfill job."""
    from uuid import uuid4
    job_id = str(uuid4())
    _backfill_jobs[job_id] = {
        "id": job_id,
        "provider": request.provider,
        "feed": request.feed,
        "instrument_keys": request.instrument_keys,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "project": request.project,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
    }
    return {
        "data": {"backfill_id": job_id, "status": "pending"},
        "meta": {"ts": datetime.utcnow()},
    }


@app.get("/api/v1/backfill/{id}")
async def get_backfill(id: str):
    """Get backfill job status."""
    job = _backfill_jobs.get(id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Backfill job '{id}' not found")
    return {
        "data": job,
        "meta": {"ts": datetime.utcnow()},
    }


@app.get("/api/v1/backfill")
async def list_backfills(
    status: str | None = Query(None),
    limit: int = Query(50, le=100),
):
    """List backfill jobs."""
    jobs = list(_backfill_jobs.values())
    if status:
        jobs = [j for j in jobs if j["status"] == status]
    return {
        "data": jobs[:limit],
        "meta": {"ts": datetime.utcnow(), "count": len(jobs)},
    }


# Error codes (PRD §11.7.5)
ERROR_CODES = {
    400: "INVALID_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    409: "CONFLICT",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
}


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    """Convert HTTP exceptions to PRD-compliant error format."""
    from fastapi.responses import JSONResponse
    code = ERROR_CODES.get(exc.status_code, "UNKNOWN_ERROR")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": code,
                "message": str(exc.detail),
            },
            "meta": {"ts": datetime.utcnow().isoformat()},
        },
    )


# Rate limiting (PRD §11.7.6)
# Note: Production should use Redis-backed rate limiter
from collections import defaultdict
import time

_rate_limit_store: dict = defaultdict(list)
RATE_LIMITS = {
    "read": 1000,   # 1000 req/min
    "write": 100,   # 100 req/min
}


async def check_rate_limit(api_key: str, endpoint_type: str = "read"):
    """Simple in-memory rate limiter (use Redis in production)."""
    now = time.time()
    window = 60  # 1 minute
    
    key = f"{api_key}:{endpoint_type}"
    _rate_limit_store[key] = [t for t in _rate_limit_store[key] if now - t < window]
    
    limit = RATE_LIMITS.get(endpoint_type, 1000)
    if len(_rate_limit_store[key]) >= limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    _rate_limit_store[key].append(now)


# Authentication middleware (PRD §11.7.2)
from fastapi import Header


async def verify_api_key(authorization: str | None = Header(None)):
    """Simple API key verification (MVP)."""
    if settings.environment == "dev":
        return "dev-user"
    
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid Authorization format")
    
    token = authorization[7:]
    # In production, validate against a key store
    if not token or len(token) < 10:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return token




================================================
FILE: heber/catalog/db.py
================================================
"""SQLAlchemy models for Heber Catalog.

See PRD Section 11.2 for table specifications.
"""

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Dataset(Base):
    """Dataset registry - what datasets exist in the lake."""

    __tablename__ = "datasets"

    dataset_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_name: str = Column(String(255), unique=True, nullable=False, index=True)
    layer: str = Column(String(50), nullable=False)  # bronze | silver | gold
    owner: str = Column(String(100), nullable=False)  # shared | project name
    description: str = Column(Text, nullable=True)
    storage_root: str = Column(String(500), nullable=False)
    path_template: str = Column(String(500), nullable=True)
    partition_cols: list = Column(JSONB, nullable=True)
    primary_keys: list = Column(JSONB, nullable=True)
    retention_policy: dict = Column(JSONB, nullable=True)
    is_active: bool = Column(Boolean, default=True, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    versions = relationship("DatasetVersion", back_populates="dataset", lazy="dynamic")


class DatasetVersion(Base):
    """Schema versions for datasets."""

    __tablename__ = "dataset_versions"

    dataset_version_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_name: str = Column(
        String(255), ForeignKey("datasets.dataset_name"), nullable=False, index=True
    )
    schema_version: str = Column(String(50), nullable=False)
    schema_json: dict = Column(JSONB, nullable=False)
    writer_min_version: str = Column(String(50), nullable=True)
    reader_min_version: str = Column(String(50), nullable=True)
    is_current: bool = Column(Boolean, default=True, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    dataset = relationship("Dataset", back_populates="versions")

    __table_args__ = (
        UniqueConstraint("dataset_name", "schema_version", name="uq_dataset_schema_version"),
    )


class FeedMapping(Base):
    """Maps provider feed names to canonical Silver dataset names."""

    __tablename__ = "feed_mappings"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    provider: str = Column(String(100), nullable=False, index=True)
    gateway_feed: str = Column(String(100), nullable=False)
    silver_dataset_name: str = Column(String(255), nullable=False)
    notes: str = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("provider", "gateway_feed", name="uq_provider_feed"),
    )


class InstrumentRegistry(Base):
    """Canonical instrument registry."""

    __tablename__ = "instrument_registry"

    instrument_key: str = Column(String(255), primary_key=True)
    instrument_type: str = Column(String(50), nullable=False, index=True)
    canonical_symbol: str = Column(String(50), nullable=False, index=True)
    underlying_key: str = Column(String(255), nullable=True)  # For options
    occ_symbol: str = Column(String(50), nullable=True)
    expiry: datetime = Column(Date, nullable=True)
    strike: float = Column(Float, nullable=True)
    put_call: str = Column(String(1), nullable=True)  # P or C
    multiplier: int = Column(Integer, nullable=True, default=100)
    currency: str = Column(String(10), nullable=True, default="USD")
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    provider_mappings = relationship(
        "InstrumentProviderMap", back_populates="instrument", lazy="dynamic"
    )


class InstrumentProviderMap(Base):
    """Maps provider-specific symbols to canonical instrument keys."""

    __tablename__ = "instrument_provider_map"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key: str = Column(
        String(255), ForeignKey("instrument_registry.instrument_key"), nullable=False, index=True
    )
    provider: str = Column(String(100), nullable=False, index=True)
    provider_symbol: str = Column(String(100), nullable=False, index=True)
    provider_id: str = Column(String(255), nullable=True)
    is_primary: bool = Column(Boolean, default=False, nullable=False)

    # Relationships
    instrument = relationship("InstrumentRegistry", back_populates="provider_mappings")

    __table_args__ = (
        UniqueConstraint("provider", "provider_symbol", name="uq_provider_symbol"),
    )


class DataCoverage(Base):
    """Fast lookup for what data exists per instrument."""

    __tablename__ = "data_coverage"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    dataset_name: str = Column(String(255), nullable=False, index=True)
    instrument_key: str = Column(String(255), nullable=False, index=True)
    dt_min: datetime = Column(Date, nullable=False)
    dt_max: datetime = Column(Date, nullable=False)
    last_updated_ts: datetime = Column(DateTime(timezone=True), server_default=func.now())
    approx_row_count: int = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("dataset_name", "instrument_key", name="uq_dataset_instrument"),
    )


class Project(Base):
    """Project registry for tracking consumers."""

    __tablename__ = "projects"

    project_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    project_name: str = Column(String(100), unique=True, nullable=False, index=True)
    description: str = Column(Text, nullable=True)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())


class Request(Base):
    """Request tracking for REST pulls (PRD §11.2.2)."""

    __tablename__ = "requests"

    request_id: str = Column(String(255), primary_key=True)
    project_name: str = Column(String(100), ForeignKey("projects.project_name"), nullable=True, index=True)
    provider: str = Column(String(100), nullable=False, index=True)
    feed: str = Column(String(100), nullable=False, index=True)
    params_json: dict = Column(JSONB, nullable=True)
    status: str = Column(String(50), default="pending")  # pending, completed, failed
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (
        # Index for audit queries (PRD §11.3)
        # CREATE INDEX idx_requests_project_provider_feed ON requests(project_name, provider, feed, created_at)
    )


class Subscription(Base):
    """Subscription tracking for WebSocket streams (PRD §11.2.2)."""

    __tablename__ = "subscriptions"

    subscription_id: str = Column(String(255), primary_key=True)
    project_name: str = Column(String(100), ForeignKey("projects.project_name"), nullable=True, index=True)
    provider: str = Column(String(100), nullable=False, index=True)
    feed: str = Column(String(100), nullable=False, index=True)
    instrument_keys: list = Column(JSONB, nullable=True)  # List of instrument_keys
    started_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    ended_at: datetime = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Index for audit queries (PRD §11.3)
        # CREATE INDEX idx_subscriptions_project ON subscriptions(project_name, provider, feed)
    )



================================================
FILE: heber/catalog/service.py
================================================
"""Catalog service business logic."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from heber.catalog.db import (
    DataCoverage,
    Dataset,
    DatasetVersion,
    FeedMapping,
    InstrumentProviderMap,
    InstrumentRegistry,
    Project,
)


class CatalogService:
    """Business logic for Catalog operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    # Dataset operations
    async def list_datasets(self, layer: str | None = None) -> list[Dataset]:
        """List all datasets, optionally filtered by layer."""
        query = select(Dataset).where(Dataset.is_active == True)
        if layer:
            query = query.where(Dataset.layer == layer)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_dataset(self, name: str) -> Dataset | None:
        """Get dataset by name."""
        result = await self.session.execute(
            select(Dataset).where(Dataset.dataset_name == name)
        )
        return result.scalar_one_or_none()

    async def get_dataset_versions(self, dataset_name: str) -> list[DatasetVersion]:
        """Get all schema versions for a dataset."""
        result = await self.session.execute(
            select(DatasetVersion)
            .where(DatasetVersion.dataset_name == dataset_name)
            .order_by(DatasetVersion.created_at.desc())
        )
        return list(result.scalars().all())

    async def get_current_schema(self, dataset_name: str) -> DatasetVersion | None:
        """Get current schema version for a dataset."""
        result = await self.session.execute(
            select(DatasetVersion)
            .where(DatasetVersion.dataset_name == dataset_name)
            .where(DatasetVersion.is_current == True)
        )
        return result.scalar_one_or_none()

    async def create_dataset(
        self,
        name: str,
        layer: str,
        owner: str,
        storage_root: str,
        description: str | None = None,
        path_template: str | None = None,
        partition_cols: list | None = None,
        primary_keys: list | None = None,
    ) -> Dataset:
        """Create a new dataset."""
        dataset = Dataset(
            dataset_name=name,
            layer=layer,
            owner=owner,
            storage_root=storage_root,
            description=description,
            path_template=path_template,
            partition_cols=partition_cols,
            primary_keys=primary_keys,
        )
        self.session.add(dataset)
        await self.session.commit()
        await self.session.refresh(dataset)
        return dataset

    # Instrument operations
    async def get_instrument(self, key: str) -> InstrumentRegistry | None:
        """Get instrument by key."""
        result = await self.session.execute(
            select(InstrumentRegistry).where(InstrumentRegistry.instrument_key == key)
        )
        return result.scalar_one_or_none()

    async def lookup_instruments(self, symbols: list[str]) -> list[InstrumentRegistry]:
        """Batch lookup instruments by symbol."""
        result = await self.session.execute(
            select(InstrumentRegistry).where(
                InstrumentRegistry.canonical_symbol.in_(symbols)
            )
        )
        return list(result.scalars().all())

    async def search_instruments(
        self,
        instrument_type: str | None = None,
        symbol_prefix: str | None = None,
        limit: int = 100,
    ) -> list[InstrumentRegistry]:
        """Search instruments with filters."""
        query = select(InstrumentRegistry)
        if instrument_type:
            query = query.where(InstrumentRegistry.instrument_type == instrument_type)
        if symbol_prefix:
            query = query.where(
                InstrumentRegistry.canonical_symbol.ilike(f"{symbol_prefix}%")
            )
        query = query.limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def upsert_instrument(
        self,
        instrument_key: str,
        instrument_type: str,
        canonical_symbol: str,
        **kwargs,
    ) -> InstrumentRegistry:
        """Create or update instrument."""
        existing = await self.get_instrument(instrument_key)
        if existing:
            for key, value in kwargs.items():
                if hasattr(existing, key) and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.utcnow()
        else:
            existing = InstrumentRegistry(
                instrument_key=instrument_key,
                instrument_type=instrument_type,
                canonical_symbol=canonical_symbol,
                **kwargs,
            )
            self.session.add(existing)
        await self.session.commit()
        await self.session.refresh(existing)
        return existing

    # Feed mapping operations
    async def resolve_feed(self, provider: str, gateway_feed: str) -> str | None:
        """Resolve gateway feed to Silver dataset name."""
        result = await self.session.execute(
            select(FeedMapping)
            .where(FeedMapping.provider == provider)
            .where(FeedMapping.gateway_feed == gateway_feed)
        )
        mapping = result.scalar_one_or_none()
        return mapping.silver_dataset_name if mapping else None

    async def list_feed_mappings(self) -> list[FeedMapping]:
        """List all feed mappings."""
        result = await self.session.execute(select(FeedMapping))
        return list(result.scalars().all())

    # Coverage operations
    async def get_coverage(
        self, dataset_name: str, instrument_key: str | None = None
    ) -> list[DataCoverage]:
        """Get data coverage for a dataset."""
        query = select(DataCoverage).where(DataCoverage.dataset_name == dataset_name)
        if instrument_key:
            query = query.where(DataCoverage.instrument_key == instrument_key)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def update_coverage(
        self,
        dataset_name: str,
        instrument_key: str,
        dt_min: datetime,
        dt_max: datetime,
        approx_row_count: int | None = None,
    ) -> DataCoverage:
        """Update data coverage for an instrument in a dataset."""
        result = await self.session.execute(
            select(DataCoverage)
            .where(DataCoverage.dataset_name == dataset_name)
            .where(DataCoverage.instrument_key == instrument_key)
        )
        coverage = result.scalar_one_or_none()
        
        if coverage:
            coverage.dt_min = min(coverage.dt_min, dt_min)
            coverage.dt_max = max(coverage.dt_max, dt_max)
            if approx_row_count:
                coverage.approx_row_count = (coverage.approx_row_count or 0) + approx_row_count
            coverage.last_updated_ts = datetime.utcnow()
        else:
            coverage = DataCoverage(
                dataset_name=dataset_name,
                instrument_key=instrument_key,
                dt_min=dt_min,
                dt_max=dt_max,
                approx_row_count=approx_row_count,
            )
            self.session.add(coverage)
        
        await self.session.commit()
        await self.session.refresh(coverage)
        return coverage



================================================
FILE: heber/catalog/urn.py
================================================
"""Dataset URN and path utilities per PRD §11.4.

URNs provide a stable identifier for datasets:
- heber://silver/bars@v1
- heber://silver/quotes@v1
- heber://gold/{project}/features@v1

Path templates translate URNs to actual file system paths.
"""

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)


@dataclass
class DatasetURN:
    """Parsed dataset URN (PRD §11.4).
    
    Format: heber://{layer}/{dataset}@{version}
    
    Examples:
        - heber://silver/bars@v1
        - heber://gold/kairos/features@v1
    """
    layer: str  # bronze, silver, gold
    dataset: str  # bars, quotes, etc.
    version: str = "v1"
    project: str | None = None  # For gold datasets
    
    @classmethod
    def parse(cls, urn: str) -> "DatasetURN":
        """Parse a URN string into components.
        
        Args:
            urn: URN string like "heber://silver/bars@v1"
            
        Returns:
            DatasetURN instance
            
        Raises:
            ValueError: If URN format is invalid
        """
        pattern = r"^heber://(\w+)/(.+?)(?:@(\w+))?$"
        match = re.match(pattern, urn)
        
        if not match:
            raise ValueError(f"Invalid URN format: {urn}")
        
        layer = match.group(1)
        dataset_part = match.group(2)
        version = match.group(3) or "v1"
        
        # Check for gold project prefix (e.g., "kairos/features")
        project = None
        dataset = dataset_part
        if layer == "gold" and "/" in dataset_part:
            parts = dataset_part.split("/", 1)
            project = parts[0]
            dataset = parts[1]
        
        return cls(layer=layer, dataset=dataset, version=version, project=project)
    
    def __str__(self) -> str:
        """Convert to URN string."""
        if self.project:
            return f"heber://{self.layer}/{self.project}/{self.dataset}@{self.version}"
        return f"heber://{self.layer}/{self.dataset}@{self.version}"


# Path templates per layer (PRD §11.4)
PATH_TEMPLATES = {
    "bronze": "{layer}/provider={provider}/feed={feed}/dt={dt}/hour={hour}/",
    "silver": "{layer}/feed={feed}/instrument_type={instrument_type}/dt={dt}/",
    "silver_hourly": "{layer}/feed={feed}/instrument_type={instrument_type}/dt={dt}/hour={hour}/",
    "gold": "{layer}/dataset={dataset}/project={project}/version={version}/dt={dt}/",
}


def get_path_template(layer: str, feed: str | None = None) -> str:
    """Get the path template for a layer/feed combination.
    
    Args:
        layer: bronze, silver, gold
        feed: Feed name (used to determine if hourly partitioning)
        
    Returns:
        Path template string
    """
    if layer == "silver" and feed in ("quotes", "trades"):
        return PATH_TEMPLATES["silver_hourly"]
    return PATH_TEMPLATES.get(layer, PATH_TEMPLATES["silver"])


def resolve_path(
    urn: str | DatasetURN,
    dt: date | None = None,
    hour: int | None = None,
    instrument_type: str = "equity",
    provider: str | None = None,
    base_path: str | Path | None = None,
) -> Path:
    """Resolve a URN to an actual file system path.
    
    Args:
        urn: Dataset URN (string or parsed)
        dt: Date partition value
        hour: Hour partition value (for bronze/high-vol silver)
        instrument_type: Instrument type for silver partitioning
        provider: Provider for bronze partitioning
        base_path: Base storage path (defaults to settings.storage_base_path)
        
    Returns:
        Resolved Path object
    """
    if isinstance(urn, str):
        urn = DatasetURN.parse(urn)
    
    if base_path is None:
        base_path = Path(settings.storage_base_path)
    else:
        base_path = Path(base_path)
    
    template = get_path_template(urn.layer, urn.dataset)
    
    # Build partition values
    dt_str = dt.isoformat() if dt else "*"
    hour_str = f"{hour:02d}" if hour is not None else "*"
    
    path_str = template.format(
        layer=urn.layer,
        feed=urn.dataset,
        dataset=urn.dataset,
        instrument_type=instrument_type,
        dt=dt_str,
        hour=hour_str,
        provider=provider or "*",
        project=urn.project or "shared",
        version=urn.version,
    )
    
    return base_path / path_str


def list_partitions(
    urn: str | DatasetURN,
    base_path: str | Path | None = None,
) -> list[dict[str, str]]:
    """List available partitions for a dataset (PRD §11.5 Pattern A).
    
    Args:
        urn: Dataset URN
        base_path: Base storage path
        
    Returns:
        List of partition dictionaries with keys like {dt, hour, instrument_type}
    """
    if isinstance(urn, str):
        urn = DatasetURN.parse(urn)
    
    path = resolve_path(urn, base_path=base_path)
    
    # Find all matching partitions
    # This is a simplified implementation - real version would glob the fs
    partitions = []
    
    # For now, return empty list as placeholder
    # In production, this would scan the filesystem
    logger.debug("list_partitions", urn=str(urn), path=str(path))
    
    return partitions


# Discovery pattern helpers (PRD §11.5)

def discover_by_instrument(
    instrument_key: str,
    dt_start: date | None = None,
    dt_end: date | None = None,
) -> list[dict]:
    """Pattern A: Query by instrument + time range.
    
    Returns list of datasets/partitions containing data for this instrument.
    """
    # This would query data_coverage table
    return []


def discover_by_symbol(
    symbol: str,
    dt_start: date | None = None,
    dt_end: date | None = None,
) -> list[dict]:
    """Pattern B: Query by symbol + date range.
    
    First resolves symbol to instrument_key, then queries coverage.
    """
    # This would:
    # 1. Query instrument_registry for canonical_symbol = symbol
    # 2. Call discover_by_instrument with result
    return []


def trace_by_request(request_id: str) -> dict:
    """Pattern C: Trace by request_id.
    
    Returns the request metadata and any data it produced.
    """
    # This would query requests table
    return {}



================================================
FILE: heber/firewall/__init__.py
================================================
"""Zero-Leakage Firewall - Core utilities for point-in-time correct queries.

Per PRD §10, this module provides the fundamental building blocks to prevent:
1. Transport/arrival leakage - Using data before it was available
2. Revision leakage - Using corrected data that wasn't available at the time
3. Enrichment leakage - Mixing outcomes into features
4. Label/target leakage - Future data in training features
5. Split leakage - Information bleed across train/test

CRITICAL RULE: All reads for research/backtest/ML must use ts_available <= T
"""

from heber.firewall.asof import read_asof, asof_join, read_asof_range
from heber.firewall.validation import (
    validate_asof_read,
    validate_gold_build,
    validate_train_test_split,
    LeakageError,
    GoldBuildMetadata,
)
from heber.firewall.scd import read_reference_asof, join_with_reference_asof
from heber.firewall.tests import run_all_leakage_tests, monitor_availability_lag

__all__ = [
    "read_asof",
    "asof_join",
    "read_asof_range",
    "validate_asof_read",
    "validate_gold_build",
    "validate_train_test_split",
    "read_reference_asof",
    "join_with_reference_asof",
    "LeakageError",
    "GoldBuildMetadata",
    "run_all_leakage_tests",
    "monitor_availability_lag",
]



================================================
FILE: heber/firewall/asof.py
================================================
"""As-Of Query and Join utilities per PRD §10.3-10.4.

CRITICAL: All reads for research/backtest/ML must use ts_available <= T
"""

from datetime import datetime
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


def read_asof(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    filters: dict[str, Any] | None = None,
    time_col: str = "ts_event",
    available_col: str = "ts_available",
) -> pl.LazyFrame:
    """Read data as-of a specific time (PRD §10.3).
    
    This is the fundamental anti-leakage primitive. All training/backtest
    reads MUST use this function to ensure point-in-time correctness.
    
    Args:
        df: Source data (LazyFrame or DataFrame)
        asof_time: The point-in-time cutoff - only rows available at this time are returned
        filters: Optional additional filters (column: value dict)
        time_col: Column to use for time range filtering (default: ts_event)
        available_col: Column containing availability timestamp (default: ts_available)
        
    Returns:
        Filtered LazyFrame with ts_available <= asof_time
        
    Example:
        >>> quotes = read_asof(quotes_df, asof_time=datetime(2026, 1, 15, 10, 30))
        # Only returns quotes that were known at 10:30 on Jan 15
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    # Apply the critical anti-leakage filter
    result = df.filter(pl.col(available_col) <= asof_time)
    
    # Apply any additional filters
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    logger.debug(
        "read_asof",
        asof_time=asof_time.isoformat(),
        filters=filters,
    )
    
    return result


def asof_join(
    left: pl.LazyFrame | pl.DataFrame,
    right: pl.LazyFrame | pl.DataFrame,
    left_on: str,
    right_on: str,
    by: str | list[str],
    left_time_col: str = "ts_event",
    right_time_col: str = "ts_event",
    right_available_col: str = "ts_available",
    tolerance: str | None = None,
    suffix: str = "_right",
) -> pl.LazyFrame:
    """As-of join with anti-leakage protection (PRD §10.4).
    
    Joins left to the most recent prior row from right such that:
    - ts_event_right <= left_time
    - ts_available_right <= left_time
    
    This ensures we never join data that wasn't available at the time.
    
    Args:
        left: Left dataframe (driving table)
        right: Right dataframe (lookup table)
        left_on: Left time column for the join
        right_on: Right time column for the join
        by: Column(s) to join on (e.g., instrument_key)
        left_time_col: Time column in left for filtering
        right_time_col: Time column in right for join
        right_available_col: Availability column in right
        tolerance: Optional max time difference (e.g., "1h", "30m")
        suffix: Suffix for right columns in result
        
    Returns:
        Joined LazyFrame with anti-leakage guarantee
        
    Example:
        >>> # Join trades with most recent quote at time of trade
        >>> result = asof_join(
        ...     trades, quotes,
        ...     left_on="ts_event", right_on="ts_event",
        ...     by="instrument_key"
        ... )
    """
    if isinstance(left, pl.DataFrame):
        left = left.lazy()
    if isinstance(right, pl.DataFrame):
        right = right.lazy()
    
    # Ensure by is a list
    by_cols = [by] if isinstance(by, str) else list(by)
    
    # Filter right to only include rows where data was available
    # This is the anti-leakage protection - we can't see data
    # before it was available
    right_filtered = right.with_columns([
        # Create a "join-safe time" that is the max of event time and available time
        # This ensures we never join data that wasn't available yet
        pl.when(pl.col(right_available_col) > pl.col(right_time_col))
        .then(pl.col(right_available_col))
        .otherwise(pl.col(right_time_col))
        .alias("_asof_safe_time")
    ])
    
    # Perform the as-of join using the safe time
    result = left.join_asof(
        right_filtered,
        left_on=left_on,
        right_on="_asof_safe_time",
        by=by_cols,
        tolerance=tolerance,
        suffix=suffix,
        strategy="backward",  # Use most recent prior row
    )
    
    # Drop the helper column
    result = result.drop("_asof_safe_time" + suffix)
    
    logger.debug(
        "asof_join",
        left_on=left_on,
        right_on=right_on,
        by=by_cols,
    )
    
    return result


def read_asof_range(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    start_time: datetime,
    end_time: datetime,
    filters: dict[str, Any] | None = None,
    time_col: str = "ts_event",
    available_col: str = "ts_available",
) -> pl.LazyFrame:
    """Read data in a time range, as-of a specific time (PRD §10.3).
    
    This combines time range filtering with as-of cutoff.
    
    Args:
        df: Source data
        asof_time: Point-in-time cutoff for availability
        start_time: Start of the time range (on time_col)
        end_time: End of the time range (on time_col)
        filters: Optional additional filters
        time_col: Column for time range filtering
        available_col: Column for availability filtering
        
    Returns:
        Filtered LazyFrame
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    result = df.filter(
        (pl.col(time_col) >= start_time) &
        (pl.col(time_col) <= end_time) &
        (pl.col(available_col) <= asof_time)
    )
    
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    return result



================================================
FILE: heber/firewall/scd.py
================================================
"""Slowly Changing Dimension (SCD) utilities for reference tables per PRD §10.6.

Reference tables with validity windows require special query logic.
"""

from datetime import datetime
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


def read_reference_asof(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    key_col: str | list[str],
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    filters: dict[str, Any] | None = None,
) -> pl.LazyFrame:
    """Read reference table as-of a specific time with validity windows (PRD §10.6).
    
    For slowly changing dimensions, selects rows where:
        valid_from <= T AND (valid_to IS NULL OR valid_to > T)
    
    This ensures we get the version of the reference data that was
    valid at the specified time.
    
    Args:
        df: Source reference table
        asof_time: Point-in-time to query
        key_col: Primary key column(s) for the reference table
        valid_from_col: Column containing validity start
        valid_to_col: Column containing validity end (nullable)
        filters: Optional additional filters
        
    Returns:
        LazyFrame with valid reference rows at asof_time
        
    Example:
        >>> # Get option contracts that were valid on Jan 15, 2026
        >>> contracts = read_reference_asof(
        ...     option_contracts_df,
        ...     asof_time=datetime(2026, 1, 15, 10, 0),
        ...     key_col="occ_symbol"
        ... )
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    # Apply validity window filter per PRD §10.6
    result = df.filter(
        (pl.col(valid_from_col) <= asof_time) &
        (
            pl.col(valid_to_col).is_null() |
            (pl.col(valid_to_col) > asof_time)
        )
    )
    
    # Apply additional filters
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    logger.debug(
        "read_reference_asof",
        asof_time=asof_time.isoformat(),
        key_col=key_col,
    )
    
    return result


def join_with_reference_asof(
    left: pl.LazyFrame | pl.DataFrame,
    reference: pl.LazyFrame | pl.DataFrame,
    left_key: str | list[str],
    ref_key: str | list[str],
    left_time_col: str = "ts_event",
    ref_valid_from: str = "valid_from",
    ref_valid_to: str = "valid_to",
    suffix: str = "_ref",
) -> pl.LazyFrame:
    """Join fact table with reference table using validity windows (PRD §10.6).
    
    For each row in left, joins to the reference row that was valid at
    that row's time. This handles slowly changing dimensions correctly.
    
    Args:
        left: Fact table (e.g., trades)
        reference: Reference table with validity windows (e.g., option_contracts)
        left_key: Key column(s) in left table
        ref_key: Key column(s) in reference table
        left_time_col: Time column in left for validity check
        ref_valid_from: Validity start column in reference
        ref_valid_to: Validity end column in reference
        suffix: Suffix for reference columns
        
    Returns:
        Joined LazyFrame with point-in-time correct reference data
        
    Example:
        >>> # Join trades with option contract details valid at trade time
        >>> result = join_with_reference_asof(
        ...     trades,
        ...     option_contracts,
        ...     left_key="instrument_key",
        ...     ref_key="instrument_key"
        ... )
    """
    if isinstance(left, pl.DataFrame):
        left = left.lazy()
    if isinstance(reference, pl.DataFrame):
        reference = reference.lazy()
    
    # Ensure keys are lists
    left_keys = [left_key] if isinstance(left_key, str) else list(left_key)
    ref_keys = [ref_key] if isinstance(ref_key, str) else list(ref_key)
    
    # Join and filter by validity
    result = left.join(
        reference,
        left_on=left_keys,
        right_on=ref_keys,
        how="left",
        suffix=suffix,
    ).filter(
        # Keep only rows where reference was valid at left's time
        (pl.col(ref_valid_from + suffix) <= pl.col(left_time_col)) &
        (
            pl.col(ref_valid_to + suffix).is_null() |
            (pl.col(ref_valid_to + suffix) > pl.col(left_time_col))
        )
    )
    
    logger.debug(
        "join_with_reference_asof",
        left_key=left_keys,
        ref_key=ref_keys,
    )
    
    return result



================================================
FILE: heber/firewall/tests.py
================================================
"""Automated Leakage Tests for CI per PRD §10.12.

Provides test fixtures and utilities for validating anti-leakage behavior.
These should be run as part of CI to ensure leakage cannot creep in quietly.
"""

from datetime import datetime, timedelta, UTC
from typing import Callable

import polars as pl
import structlog

from heber.firewall.asof import read_asof, asof_join
from heber.firewall.validation import LeakageError, validate_asof_read

logger = structlog.get_logger(__name__)


def create_test_dataframe(
    n_rows: int = 100,
    start_time: datetime | None = None,
    availability_lag_seconds: int = 5,
) -> pl.DataFrame:
    """Create a test dataframe with realistic timestamp patterns.
    
    Args:
        n_rows: Number of rows to generate
        start_time: Start time for data (defaults to now - 1 hour)
        availability_lag_seconds: Simulated lag between ts_event and ts_available
        
    Returns:
        DataFrame with ts_event, ts_available, and sample data
    """
    if start_time is None:
        start_time = datetime.now(UTC) - timedelta(hours=1)
    
    data = {
        "event_id": [f"evt_{i:04d}" for i in range(n_rows)],
        "instrument_key": ["equity:AAPL"] * n_rows,
        "ts_event": [start_time + timedelta(seconds=i) for i in range(n_rows)],
        "ts_available": [
            start_time + timedelta(seconds=i + availability_lag_seconds)
            for i in range(n_rows)
        ],
        "value": list(range(n_rows)),
    }
    
    return pl.DataFrame(data)


def test_asof_read_filters_future_data() -> bool:
    """Test that read_asof correctly filters out future data.
    
    This is a CRITICAL test - if this fails, we have leakage.
    
    Returns:
        True if test passes
        
    Raises:
        AssertionError: If leakage is detected
    """
    # Create data where some rows have ts_available in the future
    df = create_test_dataframe(n_rows=100, availability_lag_seconds=10)
    
    # Query as-of a time in the middle of the data
    # Should only get rows where ts_available <= asof_time
    asof_time = df["ts_available"][50]
    
    result = read_asof(df, asof_time).collect()
    
    # Verify: all returned rows must have ts_available <= asof_time
    max_available = result["ts_available"].max()
    assert max_available <= asof_time, (
        f"LEAKAGE: read_asof returned future data! "
        f"max_ts_available={max_available}, asof_time={asof_time}"
    )
    
    # Verify: we should have approximately half the rows
    assert len(result) > 0, "read_asof returned no data when it should have"
    assert len(result) <= 51, f"Too many rows returned: {len(result)}"
    
    logger.info("test_asof_read_filters_future_data PASSED")
    return True


def test_asof_join_no_future_lookups() -> bool:
    """Test that asof_join never joins future data.
    
    This validates that the right table's availability is respected.
    
    Returns:
        True if test passes
        
    Raises:
        AssertionError: If leakage is detected
    """
    # Create two tables: trades (left) and quotes (right)
    start = datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC)
    
    trades = pl.DataFrame({
        "event_id": ["t1", "t2", "t3"],
        "instrument_key": ["equity:AAPL"] * 3,
        "ts_event": [
            start + timedelta(seconds=30),
            start + timedelta(seconds=60),
            start + timedelta(seconds=90),
        ],
        "ts_available": [
            start + timedelta(seconds=31),
            start + timedelta(seconds=61),
            start + timedelta(seconds=91),
        ],
        "price": [150.0, 151.0, 152.0],
    })
    
    # Quotes with different availability lags
    quotes = pl.DataFrame({
        "event_id": ["q1", "q2", "q3", "q4"],
        "instrument_key": ["equity:AAPL"] * 4,
        "ts_event": [
            start + timedelta(seconds=10),
            start + timedelta(seconds=40),
            start + timedelta(seconds=70),
            start + timedelta(seconds=100),
        ],
        "ts_available": [
            start + timedelta(seconds=15),  # Available before t1
            start + timedelta(seconds=55),  # Available before t2 but after its event
            start + timedelta(seconds=85),  # Available before t3
            start + timedelta(seconds=110), # Available after all trades
        ],
        "bid_px": [149.5, 150.5, 151.5, 152.5],
    })
    
    # Perform as-of join
    result = asof_join(
        trades, quotes,
        left_on="ts_event",
        right_on="ts_event",
        by="instrument_key",
    ).collect()
    
    # For each trade, verify the joined quote was available at trade time
    for row in result.iter_rows(named=True):
        trade_time = row["ts_event"]
        quote_available = row.get("ts_available_right")
        
        if quote_available is not None:
            assert quote_available <= trade_time, (
                f"LEAKAGE: Joined quote not available at trade time! "
                f"trade_time={trade_time}, quote_ts_available={quote_available}"
            )
    
    logger.info("test_asof_join_no_future_lookups PASSED")
    return True


def test_training_context_requires_asof() -> bool:
    """Test that training context requires as-of time.
    
    Per PRD §10.11, reading without asof_time in training should error.
    
    Returns:
        True if test passes
        
    Raises:
        AssertionError: If validation is not enforced
    """
    try:
        validate_asof_read(
            df_has_ts_available=True,
            asof_time=None,
            context="training"
        )
        # If we get here, validation failed to catch the error
        raise AssertionError(
            "LEAKAGE RISK: Training read without asof_time was allowed!"
        )
    except LeakageError:
        # This is the expected behavior
        pass
    
    logger.info("test_training_context_requires_asof PASSED")
    return True


def run_all_leakage_tests() -> dict[str, bool]:
    """Run all automated leakage tests.
    
    This should be called from CI.
    
    Returns:
        Dict mapping test name to pass/fail
    """
    tests = [
        ("asof_read_filters_future", test_asof_read_filters_future_data),
        ("asof_join_no_future_lookups", test_asof_join_no_future_lookups),
        ("training_requires_asof", test_training_context_requires_asof),
    ]
    
    results = {}
    for name, test_fn in tests:
        try:
            results[name] = test_fn()
        except Exception as e:
            logger.error(f"Test {name} FAILED", error=str(e))
            results[name] = False
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"Leakage tests: {passed}/{total} passed")
    
    return results


# Runtime monitors (PRD §10.12)

def monitor_availability_lag(
    df: pl.DataFrame,
    event_col: str = "ts_event",
    available_col: str = "ts_available",
) -> dict[str, float]:
    """Monitor the distribution of (ts_available - ts_event) lag.
    
    Args:
        df: DataFrame with timestamp columns
        event_col: Event time column
        available_col: Availability time column
        
    Returns:
        Statistics about availability lag
    """
    lag_seconds = (
        df.select([
            ((pl.col(available_col) - pl.col(event_col)).dt.total_seconds())
            .alias("lag_seconds")
        ])
        .get_column("lag_seconds")
    )
    
    stats = {
        "mean_lag_seconds": lag_seconds.mean(),
        "median_lag_seconds": lag_seconds.median(),
        "p95_lag_seconds": lag_seconds.quantile(0.95),
        "max_lag_seconds": lag_seconds.max(),
        "min_lag_seconds": lag_seconds.min(),
    }
    
    return stats


def monitor_late_arrivals(
    df: pl.DataFrame,
    late_threshold_seconds: float = 60.0,
    available_col: str = "ts_available",
    ingest_col: str = "ts_ingest",
) -> dict[str, float]:
    """Monitor percent of late-arriving events.
    
    Args:
        df: DataFrame with timestamp columns
        late_threshold_seconds: Threshold for considering data "late"
        available_col: Availability time column
        ingest_col: Ingest time column
        
    Returns:
        Statistics about late arrivals
    """
    total = len(df)
    if total == 0:
        return {"late_percent": 0.0, "late_count": 0, "total_count": 0}
    
    late_mask = (
        (pl.col(available_col) - pl.col(ingest_col)).dt.total_seconds()
        > late_threshold_seconds
    )
    
    late_count = df.filter(late_mask).height
    
    return {
        "late_percent": (late_count / total) * 100,
        "late_count": late_count,
        "total_count": total,
    }



================================================
FILE: heber/firewall/validation.py
================================================
"""Validation utilities for Zero-Leakage enforcement per PRD §10.9-10.12.

Provides:
- LeakageError exception for leakage violations
- Validation functions for as-of reads
- Gold build gates that fail loudly on violations
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class LeakageError(Exception):
    """Raised when a potential data leakage is detected.
    
    This is a HARD FAILURE that must be fixed before proceeding.
    """
    pass


@dataclass
class GoldBuildMetadata:
    """Metadata required for every Gold dataset build (PRD §10.9).
    
    Every Gold build must record this information for audit trail.
    """
    feature_time: datetime
    max_ts_event_used: datetime
    max_ts_available_used: datetime
    dataset_version: str
    code_version: str  # git SHA
    input_datasets: list[str]  # names + schema versions
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_time": self.feature_time.isoformat(),
            "max_ts_event_used": self.max_ts_event_used.isoformat(),
            "max_ts_available_used": self.max_ts_available_used.isoformat(),
            "dataset_version": self.dataset_version,
            "code_version": self.code_version,
            "input_datasets": self.input_datasets,
        }


def validate_asof_read(
    df_has_ts_available: bool,
    asof_time: datetime | None,
    context: str = "training",
) -> None:
    """Validate that an as-of read is being performed correctly (PRD §10.11).
    
    In training/backtest context, reads without asof_time should fail.
    
    Args:
        df_has_ts_available: Whether the dataframe has ts_available column
        asof_time: The as-of time provided (None if not provided)
        context: "training", "backtest", "research", or "production"
        
    Raises:
        LeakageError: If read violates anti-leakage rules
    """
    # In training/backtest/research context, asof_time is required
    training_contexts = {"training", "backtest", "research"}
    
    if context in training_contexts:
        if asof_time is None:
            raise LeakageError(
                f"As-of time is required for {context} reads. "
                "Provide asof_time parameter to ensure point-in-time correctness."
            )
        
        if not df_has_ts_available:
            raise LeakageError(
                "Dataset is missing ts_available column. "
                "Cannot perform point-in-time correct read."
            )
    
    logger.debug("validate_asof_read", context=context, has_asof=asof_time is not None)


def validate_gold_build(
    metadata: GoldBuildMetadata,
    strict: bool = True,
) -> list[str]:
    """Validate Gold dataset build gates (PRD §10.9).
    
    These are HARD GATES that should fail the build.
    
    Args:
        metadata: Build metadata to validate
        strict: If True, raise LeakageError on violations
        
    Returns:
        List of warning messages (empty if valid)
        
    Raises:
        LeakageError: If strict=True and any gate fails
    """
    violations = []
    
    # Gate 1: max_ts_available_used must not exceed feature_time
    if metadata.max_ts_available_used > metadata.feature_time:
        msg = (
            f"LEAKAGE: max_ts_available_used ({metadata.max_ts_available_used}) "
            f"> feature_time ({metadata.feature_time}). "
            "This means future data is being used."
        )
        violations.append(msg)
        logger.error("gold_build_leakage", violation="future_data", **metadata.to_dict())
    
    # Gate 2: max_ts_event_used should typically not exceed max_ts_available
    # (This is a warning, not a hard failure)
    if metadata.max_ts_event_used > metadata.max_ts_available_used:
        msg = (
            f"WARNING: max_ts_event_used ({metadata.max_ts_event_used}) "
            f"> max_ts_available_used ({metadata.max_ts_available_used}). "
            "Check timestamp semantics."
        )
        violations.append(msg)
        logger.warning("gold_build_warning", **metadata.to_dict())
    
    # Log successful validation
    if not violations:
        logger.info("gold_build_validated", **metadata.to_dict())
    
    # Raise on violations in strict mode
    if strict and violations:
        raise LeakageError("\n".join(violations))
    
    return violations


def validate_train_test_split(
    train_end: datetime,
    test_start: datetime,
    purge_window: int,
    embargo_window: int,
) -> None:
    """Validate train/test split configuration (PRD §10.10).
    
    Args:
        train_end: End of training period
        test_start: Start of test period
        purge_window: Purge period in seconds (remove overlapping windows)
        embargo_window: Embargo period in seconds (hold out after split)
        
    Raises:
        LeakageError: If split configuration is invalid
    """
    gap_seconds = (test_start - train_end).total_seconds()
    required_gap = purge_window + embargo_window
    
    if gap_seconds < required_gap:
        raise LeakageError(
            f"Train/test split too close. "
            f"Gap: {gap_seconds}s, Required: {required_gap}s "
            f"(purge: {purge_window}s + embargo: {embargo_window}s)"
        )
    
    logger.debug(
        "validate_train_test_split",
        train_end=train_end.isoformat(),
        test_start=test_start.isoformat(),
        gap_seconds=gap_seconds,
    )



================================================
FILE: heber/hotstore/__init__.py
================================================
"""Hot Store module - ClickHouse integration for real-time queries.

Per PRD §7.6 and §12.10, Hot Store provides:
- Sub-second query latency for recent data
- Rolling window cache (7 days quotes/trades, 30 days bars)
- ClickHouse manages TTL eviction

Silver is always the source of truth. Hot Store is read-only for queries.
"""

from heber.hotstore.client import HotStoreClient, get_hotstore_client
from heber.hotstore.sync import HotStoreSync
from heber.hotstore.tables import (
    QUOTES_HOT_DDL,
    TRADES_HOT_DDL,
    BARS_HOT_DDL,
    create_all_tables,
)

__all__ = [
    "HotStoreClient",
    "get_hotstore_client",
    "HotStoreSync",
    "QUOTES_HOT_DDL",
    "TRADES_HOT_DDL",
    "BARS_HOT_DDL",
    "create_all_tables",
]



================================================
FILE: heber/hotstore/client.py
================================================
"""ClickHouse client for Hot Store queries per PRD §7.6 and §12.10.

Provides:
- Connection management
- Query methods with fallback to Silver
- Metrics for sync lag monitoring
"""

from datetime import datetime, timedelta
from typing import Any

import structlog
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

from heber.config import settings

logger = structlog.get_logger(__name__)


class HotStoreClient:
    """ClickHouse client for Hot Store queries.
    
    Per PRD §12.10.1, Hot Store supports different query modes:
    - Real-time dashboard: Hot Store only (accepts staleness)
    - Strategy signals: Hot Store with Silver fallback
    - Backtest/research: Silver only (never Hot Store)
    """

    def __init__(self, client: Client | None = None):
        self._client = client
        
    @property
    def client(self) -> Client:
        """Lazy-initialize ClickHouse client."""
        if self._client is None:
            self._client = get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
            )
        return self._client

    async def get_latest_quote(self, instrument_key: str) -> dict[str, Any] | None:
        """Get latest quote for an instrument from Hot Store.
        
        Args:
            instrument_key: Canonical instrument key (e.g., equity:AAPL)
            
        Returns:
            Latest quote or None if not found
        """
        query = """
        SELECT *
        FROM quotes_hot
        WHERE instrument_key = %(key)s
        ORDER BY ts_event DESC
        LIMIT 1
        """
        result = self.client.query(query, parameters={"key": instrument_key})
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return None

    async def get_latest_bar(
        self, 
        instrument_key: str, 
        timeframe: str = "1Min"
    ) -> dict[str, Any] | None:
        """Get latest bar for an instrument from Hot Store.
        
        Args:
            instrument_key: Canonical instrument key
            timeframe: Bar timeframe (1Min, 5Min, 1Hour, etc)
            
        Returns:
            Latest bar or None if not found
        """
        query = """
        SELECT *
        FROM bars_hot
        WHERE instrument_key = %(key)s
          AND timeframe = %(tf)s
        ORDER BY bar_start_ts DESC
        LIMIT 1
        """
        result = self.client.query(
            query, 
            parameters={"key": instrument_key, "tf": timeframe}
        )
        if result.result_rows:
            return dict(zip(result.column_names, result.result_rows[0]))
        return None

    async def get_quotes_range(
        self,
        instrument_key: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Get quotes for a time range.
        
        Args:
            instrument_key: Canonical instrument key
            start: Start timestamp
            end: End timestamp
            
        Returns:
            List of quote records
        """
        query = """
        SELECT *
        FROM quotes_hot
        WHERE instrument_key = %(key)s
          AND ts_event >= %(start)s
          AND ts_event <= %(end)s
        ORDER BY ts_event
        """
        result = self.client.query(
            query,
            parameters={"key": instrument_key, "start": start, "end": end}
        )
        return [
            dict(zip(result.column_names, row))
            for row in result.result_rows
        ]

    async def get_trades_range(
        self,
        instrument_key: str,
        start: datetime,
        end: datetime,
    ) -> list[dict[str, Any]]:
        """Get trades for a time range.
        
        Args:
            instrument_key: Canonical instrument key
            start: Start timestamp
            end: End timestamp
            
        Returns:
            List of trade records
        """
        query = """
        SELECT *
        FROM trades_hot
        WHERE instrument_key = %(key)s
          AND ts_event >= %(start)s
          AND ts_event <= %(end)s
        ORDER BY ts_event
        """
        result = self.client.query(
            query,
            parameters={"key": instrument_key, "start": start, "end": end}
        )
        return [
            dict(zip(result.column_names, row))
            for row in result.result_rows
        ]

    async def get_sync_lag_seconds(self, dataset: str) -> float:
        """Get sync lag between Silver and Hot Store (PRD §12.10.1).
        
        Args:
            dataset: Dataset name (quotes, trades, bars)
            
        Returns:
            Lag in seconds (Hot Store should be ≤300s behind Silver)
        """
        table = f"{dataset}_hot"
        query = f"""
        SELECT 
            now() - max(ts_available) as lag_seconds
        FROM {table}
        WHERE ts_event > now() - INTERVAL 1 HOUR
        """
        result = self.client.query(query)
        if result.result_rows and result.result_rows[0][0]:
            return float(result.result_rows[0][0])
        return float("inf")  # No recent data

    async def get_row_count(self, dataset: str, days: int = 7) -> int:
        """Get row count for a dataset in the retention window.
        
        Args:
            dataset: Dataset name (quotes, trades, bars)
            days: Number of days to count
            
        Returns:
            Row count
        """
        table = f"{dataset}_hot"
        query = f"""
        SELECT count()
        FROM {table}
        WHERE dt >= today() - INTERVAL {days} DAY
        """
        result = self.client.query(query)
        return result.result_rows[0][0] if result.result_rows else 0


def get_hotstore_client() -> HotStoreClient:
    """Get a HotStoreClient instance."""
    return HotStoreClient()



================================================
FILE: heber/hotstore/sync.py
================================================
"""Hot Store sync service per PRD §12.10.

Syncs data from event bus or Silver to Hot Store (ClickHouse).
Maintains ≤5 minute lag SLA under normal operation.
"""

from datetime import datetime, UTC
from typing import Any

import structlog

from heber.config import settings
from heber.hotstore.client import HotStoreClient, get_hotstore_client

logger = structlog.get_logger(__name__)

# Metrics counters (will be replaced with Prometheus metrics)
_metrics = {
    "rows_synced_total": 0,
    "sync_failures_total": 0,
}


class HotStoreSync:
    """Syncs data from event bus to Hot Store.
    
    Per PRD §12.10:
    - Source: event bus (preferred) or recently written Silver partitions
    - Window: rolling last N days per dataset
    - Correctness: Hot Store is read-only for queries
    """

    def __init__(self, client: HotStoreClient | None = None):
        self.client = client or get_hotstore_client()
        self._last_sync: dict[str, datetime] = {}

    async def sync_quote(self, event: dict[str, Any]) -> None:
        """Sync a quote event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing quote data
        """
        try:
            insert_query = """
            INSERT INTO quotes_hot (
                event_id, provider, feed, instrument_type, instrument_key, symbol,
                ts_event, ts_ingest, ts_available, source, schema_version,
                bid_px, bid_sz, ask_px, ask_sz, bid_exchange, ask_exchange
            ) VALUES
            """
            
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("bid_px", 0),
                payload.get("bid_sz", 0),
                payload.get("ask_px", 0),
                payload.get("ask_sz", 0),
                payload.get("bid_exchange"),
                payload.get("ask_exchange"),
            )
            
            self.client.client.insert("quotes_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="quotes", error=str(e))
            raise

    async def sync_trade(self, event: dict[str, Any]) -> None:
        """Sync a trade event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing trade data
        """
        try:
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("price", 0),
                payload.get("size", 0),
                payload.get("trade_id"),
                payload.get("exchange"),
                payload.get("tape"),
            )
            
            self.client.client.insert("trades_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="trades", error=str(e))
            raise

    async def sync_bar(self, event: dict[str, Any]) -> None:
        """Sync a bar event to Hot Store.
        
        Args:
            event: EventEnvelope dict containing bar data
        """
        try:
            payload = event.get("payload", {})
            values = (
                event["event_id"],
                event["provider"],
                event["feed"],
                event["instrument_type"],
                event["instrument_key"],
                event["symbol"],
                event["ts_event"],
                event["ts_ingest"],
                event.get("ts_available") or datetime.now(UTC),
                event["source"],
                event.get("schema_version", "v1"),
                payload.get("timeframe", "1Min"),
                payload.get("bar_start_ts") or event["ts_event"],
                payload.get("open", 0),
                payload.get("high", 0),
                payload.get("low", 0),
                payload.get("close", 0),
                payload.get("volume", 0),
                payload.get("trade_count"),
                payload.get("vwap"),
            )
            
            self.client.client.insert("bars_hot", [values])
            _metrics["rows_synced_total"] += 1
            
        except Exception as e:
            _metrics["sync_failures_total"] += 1
            logger.error("hot_store_sync_failed", dataset="bars", error=str(e))
            raise

    async def sync_event(self, event: dict[str, Any]) -> None:
        """Route an event to the appropriate sync method.
        
        Args:
            event: EventEnvelope dict
        """
        feed = event.get("feed", "")
        
        if feed == "quotes":
            await self.sync_quote(event)
        elif feed == "trades":
            await self.sync_trade(event)
        elif feed == "bars":
            await self.sync_bar(event)
        else:
            # Flow alerts, darkpool, etc. stay lake-only per PRD §7.6
            logger.debug("hot_store_skip", feed=feed, reason="lake-only dataset")

    async def get_metrics(self) -> dict[str, Any]:
        """Get sync metrics per PRD §12.10.1.
        
        Returns:
            Dict with lag, row counts, and failure stats
        """
        return {
            "rows_synced_total": _metrics["rows_synced_total"],
            "sync_failures_total": _metrics["sync_failures_total"],
            "quotes_lag_seconds": await self.client.get_sync_lag_seconds("quotes"),
            "trades_lag_seconds": await self.client.get_sync_lag_seconds("trades"),
            "bars_lag_seconds": await self.client.get_sync_lag_seconds("bars"),
        }



================================================
FILE: heber/hotstore/tables.py
================================================
"""ClickHouse table DDL for Hot Store per PRD §12.10.

Tables:
- quotes_hot: Last 7 days of quote data
- trades_hot: Last 7 days of trade data  
- bars_hot: Last 30 days of bar data

Retention is managed by ClickHouse TTL, not Heber.
"""

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


async def create_all_tables(client) -> None:
    """Create all Hot Store tables and views.
    
    Args:
        client: ClickHouse client (clickhouse-connect or similar)
    """
    statements = [
        QUOTES_HOT_DDL,
        TRADES_HOT_DDL,
        BARS_HOT_DDL,
        LATEST_QUOTES_VIEW_DDL,
        LATEST_BARS_VIEW_DDL,
    ]
    
    for stmt in statements:
        await client.execute(stmt)



================================================
FILE: heber/models/__init__.py
================================================
"""Heber data models."""

from heber.models.envelope import EventEnvelope, Lineage, validate_instrument_key
from heber.models.silver import (
    SilverBase,
    BarRecord,
    QuoteRecord,
    TradeRecord,
    FlowAlertRecord,
    DarkpoolTradeRecord,
    OptionContractRecord,
    # V1.5 schemas
    GreeksRecord,
    ChainSnapshotRecord,
    MarketTideRecord,
    # V2 schemas - News and Filing
    NewsArticleRecord,
    NewsEntityRecord,
    NewsEventRecord,
    FilingEventRecord,
)

__all__ = [
    "EventEnvelope",
    "Lineage",
    "validate_instrument_key",
    "SilverBase",
    "BarRecord",
    "QuoteRecord",
    "TradeRecord",
    "FlowAlertRecord",
    "DarkpoolTradeRecord",
    "OptionContractRecord",
    "GreeksRecord",
    "ChainSnapshotRecord",
    "MarketTideRecord",
    "NewsArticleRecord",
    "NewsEntityRecord",
    "NewsEventRecord",
    "FilingEventRecord",
]



================================================
FILE: heber/models/envelope.py
================================================
"""EventEnvelope model - the canonical event contract from Data Gateway.

This module provides a compatible EventEnvelope model that accepts events
from Data-Gateway while adding Heber-specific extensions for zero-leakage.
"""

import re
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field


class Lineage(BaseModel):
    """Lineage metadata for tracing event origin.
    
    Data-Gateway sends lineage as a dict, so we keep it flexible.
    """

    client_id: str | None = None
    project: str | None = None
    request_id: str | None = None
    subscription_id: str | None = None
    sequence: int | None = None
    trace_id: str | None = None
    stream_type: str | None = None


# Instrument key patterns per PRD §6.2
INSTRUMENT_KEY_PATTERNS = {
    "equity": re.compile(r"^equity:[A-Z]{1,5}$"),
    "crypto": re.compile(r"^crypto:[A-Z]{2,10}-[A-Z]{2,10}$"),
    "forex": re.compile(r"^forex:[A-Z]{3}-[A-Z]{3}$"),
    "option": re.compile(r"^option:OCC:[A-Z]{1,6}\d{6}[CP]\d{8}$"),
}


def validate_instrument_key(instrument_key: str, instrument_type: str) -> bool:
    """Validate instrument_key format per PRD §6.2.
    
    Args:
        instrument_key: The instrument key to validate
        instrument_type: One of equity, crypto, forex, option
        
    Returns:
        True if valid, False otherwise
        
    Examples:
        >>> validate_instrument_key("equity:AAPL", "equity")
        True
        >>> validate_instrument_key("crypto:BTC-USD", "crypto")
        True
        >>> validate_instrument_key("option:OCC:AAPL260116C00200000", "option")
        True
    """
    pattern = INSTRUMENT_KEY_PATTERNS.get(instrument_type)
    if pattern is None:
        return False
    return bool(pattern.match(instrument_key))


class EventEnvelope(BaseModel):
    """Universal event envelope from Data Gateway.
    
    This model is COMPATIBLE with Data-Gateway's EventEnvelope (gateway/core/envelope.py).
    Fields match Data-Gateway's structure, with Heber-specific extensions marked.
    
    See PRD Section 6.1 for full specification.
    """

    # === Fields from Data-Gateway (required) ===
    event_id: str = Field(..., description="SHA256 idempotency hash (32 chars)")
    provider: str = Field(..., description="Data provider: alpaca, unusual_whales, etc")
    feed: str = Field(..., description="Feed type: bars, quotes, trades, flow, etc")
    source: str = Field(..., description="Delivery method: websocket, rest")
    instrument_type: str = Field(..., description="Asset class: equity, option, crypto, forex")
    instrument_key: str = Field(..., description="Canonical key: equity:AAPL, crypto:BTC-USD")
    symbol: str = Field(..., description="Human-readable symbol")
    ts_event: datetime = Field(..., description="Event time from provider")
    ts_ingest: datetime = Field(..., description="Gateway receive/process time")
    
    # === Fields from Data-Gateway (optional with defaults) ===
    schema_version: str = Field(default="v1", description="Envelope schema version")
    lineage: dict[str, Any] = Field(default_factory=dict, description="Sequence numbers, stream IDs")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, cached")
    payload: dict[str, Any] = Field(..., description="Normalized event data")

    # === Heber Extensions (not in Data-Gateway) ===
    ts_available: datetime | None = Field(
        default=None,
        description="First safe time this record is queryable (anti-leakage gate). Set by Heber on write.",
    )
    raw: dict[str, Any] | None = Field(
        default=None, description="Original provider message (for Bronze fidelity, PRD §6.7)"
    )
    processing_delay_ms: int = Field(
        default=0, description="Processing delay for ts_effective calculation (PRD §6.4)"
    )

    @property
    def ts_effective(self) -> datetime | None:
        """Calculate ts_effective = ts_available + processing_delay_ms (PRD §6.4).
        
        Returns the time at which this record can be realistically used,
        accounting for processing delays.
        """
        if self.ts_available is None:
            return None
        return self.ts_available + timedelta(milliseconds=self.processing_delay_ms)

    def with_ts_available(self, ts: datetime) -> "EventEnvelope":
        """Return a copy with ts_available set."""
        return self.model_copy(update={"ts_available": ts})

    def is_valid_instrument_key(self) -> bool:
        """Check if instrument_key matches expected format per PRD §6.2."""
        return validate_instrument_key(self.instrument_key, self.instrument_type)





================================================
FILE: heber/models/silver.py
================================================
"""Silver layer schema definitions per PRD §8.7.

All Silver datasets include the shared base columns plus dataset-specific fields.
These Pydantic models are used for validation and documentation.
"""

from datetime import date, datetime
from typing import Any

import pyarrow as pa
from pydantic import BaseModel, Field


# ==============================================================================
# Shared Base Columns (PRD §8.7.1)
# ==============================================================================

class SilverBase(BaseModel):
    """Base columns present in EVERY Silver dataset (PRD §8.7.1)."""

    event_id: str = Field(..., description="Deterministic idempotency key (SHA256)")
    provider: str = Field(..., description="alpaca, unusual_whales, etc")
    feed: str = Field(..., description="Canonical feed name")
    instrument_type: str = Field(..., description="equity|option|crypto|forex")
    instrument_key: str = Field(..., description="Stable canonical instrument key")
    symbol: str = Field(..., description="Human-friendly symbol")
    ts_event: datetime = Field(..., description="Provider event timestamp")
    ts_ingest: datetime = Field(..., description="Gateway receive timestamp")
    ts_available: datetime = Field(..., description="Earliest safe-use timestamp (anti-leakage)")
    source: str = Field(..., description="websocket|rest")
    schema_version: str = Field(default="v1", description="Dataset schema version")
    quality_flags: list[str] = Field(default_factory=list, description="validated, deduped, late")
    lineage: dict[str, Any] | None = Field(default=None, description="Correlation metadata")


# ==============================================================================
# Market Data Schemas (PRD §8.7.2-8.7.4)
# ==============================================================================

class BarRecord(SilverBase):
    """Silver bars schema (PRD §8.7.2).
    
    Primary key: (instrument_key, timeframe, bar_start_ts)
    """

    timeframe: str = Field(..., description="1Min, 5Min, 1Hour, etc")
    bar_start_ts: datetime = Field(..., description="Bar start/open time")
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: int | None = None
    vwap: float | None = None


class QuoteRecord(SilverBase):
    """Silver quotes schema (PRD §8.7.3).
    
    Primary key: (instrument_key, ts_event)
    """

    bid_px: float
    bid_sz: float
    ask_px: float
    ask_sz: float
    bid_exchange: str | None = None
    ask_exchange: str | None = None
    conditions: list[str] | None = None


class TradeRecord(SilverBase):
    """Silver trades schema (PRD §8.7.4).
    
    Primary key: (instrument_key, ts_event, trade_id)
    """

    price: float
    size: float
    trade_id: str | None = None
    exchange: str | None = None
    conditions: list[str] | None = None
    tape: str | None = None


# ==============================================================================
# Alternative Data Schemas (PRD §8.7.5-8.7.6)
# ==============================================================================

class FlowAlertRecord(SilverBase):
    """Silver flow_alerts schema (PRD §8.7.5, Unusual Whales).
    
    Primary key: event_id
    """

    underlying: str
    occ_symbol: str | None = None
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    premium: float
    volume: float
    open_interest: float | None = None
    spot_px: float | None = None
    contract_px: float | None = None
    alert_type: str = Field(..., description="SWEEP, BLOCK, etc")
    side: str | None = None
    aggressor: str | None = None
    tags: list[str] | None = None


class DarkpoolTradeRecord(SilverBase):
    """Silver darkpool_trades schema (PRD §8.7.6, Unusual Whales).
    
    Primary key: event_id
    """

    underlying: str
    price: float
    size: float
    notional: float | None = None
    venue: str | None = None
    print_id: str | None = None
    conditions: list[str] | None = None


# ==============================================================================
# Reference Data Schemas (PRD §8.7.7)
# ==============================================================================

class OptionContractRecord(SilverBase):
    """Silver option_contracts schema (PRD §8.7.7, reference table).
    
    Primary key: (occ_symbol) or (underlying, expiry, strike, put_call)
    This is a reference table for options consistency.
    """

    underlying: str
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    multiplier: int = Field(default=100)
    style: str | None = Field(default=None, description="american or european")
    exchange: str | None = None
    # SCD fields for validity windows (PRD §10.6)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    revision_id: str | None = None


# ==============================================================================
# V1.5 Schemas (PRD §8.7.8) - Near-term
# ==============================================================================

class GreeksRecord(SilverBase):
    """Silver greeks schema (PRD §8.7.8, time-series).
    
    Primary key: (instrument_key, ts_event)
    Time-series Greeks data per option contract.
    """

    # Option identification
    underlying: str
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    
    # Greeks values
    iv: float = Field(..., description="Implied volatility")
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float | None = None
    
    # Additional context
    underlying_price: float | None = Field(None, description="Spot price at calculation time")
    bid_iv: float | None = None
    ask_iv: float | None = None
    mid_iv: float | None = None


class ChainSnapshotRecord(SilverBase):
    """Silver option_chain_snapshots schema (PRD §8.7.8, snapshot stream).
    
    One row per contract per snapshot. Snapshot cadence is typically 5-15 minutes.
    Primary key: (snapshot_id, instrument_key) or (underlying, snapshot_id, occ_symbol)
    """

    # Snapshot identification
    snapshot_id: str = Field(..., description="Unique ID for this snapshot")
    underlying: str
    
    # Contract identification
    occ_symbol: str
    expiry: date
    strike: float
    put_call: str = Field(..., description="P or C")
    
    # Snapshot data
    bid_px: float | None = None
    ask_px: float | None = None
    mid_px: float | None = None
    last_px: float | None = None
    bid_sz: float | None = None
    ask_sz: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    
    # Greeks at snapshot time (optional)
    iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    
    # Underlying context
    underlying_price: float | None = None


class MarketTideRecord(SilverBase):
    """Silver market_tide schema (PRD §8.7.8, UW periodic snapshot).
    
    Primary key: (ts_event) or (snapshot_id)
    Periodic market sentiment/flow snapshot from Unusual Whales.
    """

    snapshot_id: str | None = Field(None, description="Snapshot identifier if provided")
    
    # Market-wide aggregates
    total_call_premium: float | None = None
    total_put_premium: float | None = None
    call_put_ratio: float | None = None
    
    # Sentiment indicators
    bullish_flow: float | None = None
    bearish_flow: float | None = None
    neutral_flow: float | None = None
    net_flow: float | None = None
    
    # Volume metrics
    total_volume: float | None = None
    unusual_volume_count: int | None = None
    
    # Sector/index data (if provided)
    sector_data: dict[str, Any] | None = None
    index_data: dict[str, Any] | None = None


# ==============================================================================
# V2 Schemas - News and Filing Data (PRD §9, §58, §59)
# ==============================================================================

class NewsArticleRecord(SilverBase):
    """Silver news_articles schema (PRD §9.1).
    
    Primary key: news_id
    """

    news_id: str = Field(..., description="Hash of URL + title + publish time")
    ts_published: datetime = Field(..., description="Original publish timestamp")
    headline: str
    summary: str | None = None
    body: str | None = Field(None, description="Full text, subject to licensing")
    url: str
    source_name: str | None = None
    
    # Revision fields (PRD §9.2)
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    revision_id: str | None = None


class NewsEntityRecord(SilverBase):
    """Silver news_entities schema (PRD §9.1).
    
    Links news articles to instruments. One row per (news_id, instrument_key) pair.
    """

    news_id: str = Field(..., description="References news_articles.news_id")
    entity_type: str = Field(..., description="company, sector, index, etc")
    confidence: float = Field(..., description="0.0-1.0 confidence score")
    match_method: str = Field(..., description="provider_tags | NER | keywords")


class NewsEventRecord(SilverBase):
    """Silver news_events schema (PRD §58).
    
    Structured news events with sentiment, for Silver-level analytics.
    Cross-references Document Store for full content.
    """

    news_id: str
    doc_store_id: str | None = Field(None, description="Cross-reference to Document Store")
    
    # Sentiment analysis
    sentiment_score: float | None = Field(None, description="-1.0 (bearish) to 1.0 (bullish)")
    sentiment_label: str | None = Field(None, description="bullish, bearish, neutral")
    relevance_score: float | None = Field(None, description="0.0-1.0 relevance to instrument")
    
    # Event classification
    event_type: str | None = Field(None, description="earnings, guidance, M&A, etc")
    magnitude: str | None = Field(None, description="low, medium, high impact")


class FilingEventRecord(SilverBase):
    """Silver filing_events schema (PRD §59).
    
    SEC filings with anti-leakage timestamp semantics.
    ts_available = ts_accepted (when SEC accepted the filing)
    """

    filing_id: str = Field(..., description="Unique filing identifier")
    accession_number: str = Field(..., description="SEC accession number")
    form_type: str = Field(..., description="10-K, 10-Q, 8-K, etc")
    
    # Timestamps (anti-leakage critical)
    ts_filed: datetime = Field(..., description="When company filed")
    ts_accepted: datetime = Field(..., description="When SEC accepted - use for ts_available")
    
    # Filing metadata
    company_name: str | None = None
    cik: str | None = Field(None, description="SEC Central Index Key")
    
    # Cross-reference
    doc_store_id: str | None = Field(None, description="Cross-reference to Document Store")
    
    # Extracted highlights (optional)
    summary: str | None = None
    key_items: list[str] | None = None


# ==============================================================================
# PyArrow Schema Helpers
# ==============================================================================

SILVER_BASE_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("provider", pa.string(), nullable=False),
    pa.field("feed", pa.string(), nullable=False),
    pa.field("instrument_type", pa.string(), nullable=False),
    pa.field("instrument_key", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("ts_event", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("ts_ingest", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("ts_available", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("quality_flags", pa.list_(pa.string())),
    pa.field("lineage", pa.string()),  # JSON serialized
])


def get_bars_schema() -> pa.Schema:
    """PyArrow schema for bars Silver dataset."""
    return pa.schema([
        *SILVER_BASE_SCHEMA,
        pa.field("timeframe", pa.string(), nullable=False),
        pa.field("bar_start_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("trade_count", pa.int64()),
        pa.field("vwap", pa.float64()),
    ])


def get_darkpool_trades_schema() -> pa.Schema:
    """PyArrow schema for darkpool_trades Silver dataset."""
    return pa.schema([
        *SILVER_BASE_SCHEMA,
        pa.field("underlying", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("size", pa.float64(), nullable=False),
        pa.field("notional", pa.float64()),
        pa.field("venue", pa.string()),
        pa.field("print_id", pa.string()),
        pa.field("conditions", pa.list_(pa.string())),
    ])


def get_option_contracts_schema() -> pa.Schema:
    """PyArrow schema for option_contracts Silver reference table."""
    return pa.schema([
        *SILVER_BASE_SCHEMA,
        pa.field("underlying", pa.string(), nullable=False),
        pa.field("occ_symbol", pa.string(), nullable=False),
        pa.field("expiry", pa.date32(), nullable=False),
        pa.field("strike", pa.float64(), nullable=False),
        pa.field("put_call", pa.string(), nullable=False),
        pa.field("multiplier", pa.int32(), nullable=False),
        pa.field("style", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("valid_from", pa.timestamp("us", tz="UTC")),
        pa.field("valid_to", pa.timestamp("us", tz="UTC")),
        pa.field("revision_id", pa.string()),
    ])



================================================
FILE: heber/ops/__init__.py
================================================
"""Ops module for Heber operational requirements.

Per PRD §12, this module provides reliability and observability utilities.
"""

from heber.ops.reliability import (
    BloomFilter,
    EventDeduplicator,
    DeadLetterQueue,
    DLQEvent,
    DeduplicationResult,
    retry_with_backoff,
    retry_with_backoff_async,
)
from heber.ops.logging import (
    configure_logging,
    get_logger,
    log_event_received,
    log_batch_written,
    log_error,
    log_dlq_event,
    log_retry,
)

__all__ = [
    # Reliability (§12.1-12.4)
    "BloomFilter",
    "EventDeduplicator",
    "DeadLetterQueue",
    "DLQEvent",
    "DeduplicationResult",
    "retry_with_backoff",
    "retry_with_backoff_async",
    # Logging (§12.3, §12.5.5)
    "configure_logging",
    "get_logger",
    "log_event_received",
    "log_batch_written",
    "log_error",
    "log_dlq_event",
    "log_retry",
]



================================================
FILE: heber/ops/logging.py
================================================
"""Structured logging configuration per PRD §12.3 and §12.5.5.

Provides:
- JSON structured logs with required fields
- Log levels: DEBUG, INFO, WARNING, ERROR
- Service/instance identification
- Trace context propagation
"""

import os
import sys
from datetime import datetime, UTC
from typing import Any

import structlog


def get_instance_id() -> str:
    """Get unique instance identifier."""
    return os.environ.get("INSTANCE_ID", os.environ.get("HOSTNAME", "unknown"))


def get_service_name() -> str:
    """Get service name from environment."""
    return os.environ.get("SERVICE_NAME", "heber")


def add_timestamp(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add ISO timestamp to log event."""
    event_dict["ts"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return event_dict


def add_service_context(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add service context per PRD §12.5.5."""
    event_dict["service"] = get_service_name()
    event_dict["instance_id"] = get_instance_id()
    return event_dict


def configure_logging(
    service_name: str | None = None,
    log_level: str = "INFO",
    json_output: bool = True,
) -> None:
    """Configure structured logging per PRD §12.3 and §12.5.5.
    
    Args:
        service_name: Override service name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        json_output: If True, output JSON; otherwise, dev-friendly format
    """
    if service_name:
        os.environ["SERVICE_NAME"] = service_name
    
    # Shared processors
    shared_processors = [
        structlog.stdlib.add_log_level,
        add_timestamp,
        add_service_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    if json_output:
        # Production: JSON output per PRD §12.5.5
        processors = shared_processors + [
            structlog.processors.JSONRenderer()
        ]
    else:
        # Development: Console-friendly output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer()
        ]
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance."""
    return structlog.get_logger(name)


# Log event helpers for common operations (PRD §12.3)

def log_event_received(
    logger: structlog.stdlib.BoundLogger,
    event_id: str,
    provider: str,
    feed: str,
    instrument_key: str,
    ts_event: datetime,
    ts_ingest: datetime,
    ts_available: datetime,
    schema_version: str = "v1",
    quality_flags: list[str] | None = None,
) -> None:
    """Log gateway event receipt per PRD §12.3."""
    logger.info(
        "event_received",
        event_id=event_id,
        provider=provider,
        feed=feed,
        instrument_key=instrument_key,
        ts_event=ts_event.isoformat() if ts_event else None,
        ts_ingest=ts_ingest.isoformat() if ts_ingest else None,
        ts_available=ts_available.isoformat() if ts_available else None,
        schema_version=schema_version,
        quality_flags=quality_flags or [],
    )


def log_batch_written(
    logger: structlog.stdlib.BoundLogger,
    feed: str,
    dt: str,
    file_count: int,
    rows_written: int,
    duration_ms: float,
    ingest_lag_ms: float | None = None,
) -> None:
    """Log batch write per PRD §12.3."""
    logger.info(
        "batch_written",
        feed=feed,
        dt=dt,
        file_count=file_count,
        rows_written=rows_written,
        duration_ms=duration_ms,
        ingest_lag_ms=ingest_lag_ms,
    )


def log_error(
    logger: structlog.stdlib.BoundLogger,
    error: Exception,
    operation: str,
    **context: Any,
) -> None:
    """Log error with context."""
    logger.error(
        "error",
        operation=operation,
        error_type=type(error).__name__,
        error_message=str(error),
        **context,
        exc_info=True,
    )


def log_dlq_event(
    logger: structlog.stdlib.BoundLogger,
    event_id: str,
    feed: str,
    provider: str,
    error_type: str,
    attempts: int,
) -> None:
    """Log dead-letter queue event."""
    logger.warning(
        "dlq_event",
        event_id=event_id,
        feed=feed,
        provider=provider,
        error_type=error_type,
        attempts=attempts,
    )


def log_retry(
    logger: structlog.stdlib.BoundLogger,
    operation: str,
    attempt: int,
    max_retries: int,
    delay_seconds: float,
    error: str,
) -> None:
    """Log retry attempt."""
    logger.warning(
        "retry",
        operation=operation,
        attempt=attempt,
        max_retries=max_retries,
        delay_seconds=delay_seconds,
        error=error,
    )



================================================
FILE: heber/ops/reliability.py
================================================
"""Reliability module for Heber per PRD §12.1-12.4.

Provides:
- Idempotency via event_id deduplication (§12.2)
- Dead-letter queue handling (§12.4)
- Retry with exponential backoff and jitter (§12.4)
"""

import hashlib
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# Default Bloom filter constants (per PRD §12.2)
BLOOM_FILTER_SIZE = 10_000_000  # 10M bits default
BLOOM_FILTER_HASHES = 7


class BloomFilter:
    """Simple Bloom filter for event_id deduplication (PRD §12.2).
    
    Used to quickly check if an event_id has been seen recently.
    False positives are possible but false negatives are not.
    """
    
    def __init__(self, size: int = BLOOM_FILTER_SIZE, num_hashes: int = BLOOM_FILTER_HASHES):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = bytearray((size + 7) // 8)
        self.count = 0
    
    def _hashes(self, event_id: str) -> list[int]:
        """Generate hash positions for an event_id."""
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.sha256(f"{event_id}:{i}".encode()).hexdigest()
            positions.append(int(h, 16) % self.size)
        return positions
    
    def add(self, event_id: str) -> None:
        """Add an event_id to the filter."""
        for pos in self._hashes(event_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bit_array[byte_idx] |= (1 << bit_idx)
        self.count += 1
    
    def contains(self, event_id: str) -> bool:
        """Check if an event_id might be in the filter."""
        for pos in self._hashes(event_id):
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True
    
    def add_if_new(self, event_id: str) -> bool:
        """Add event_id if not seen before. Returns True if new."""
        if self.contains(event_id):
            return False
        self.add(event_id)
        return True


@dataclass
class DeduplicationResult:
    """Result of deduplication check."""
    is_duplicate: bool
    event_id: str
    reason: str | None = None


class EventDeduplicator:
    """Event deduplication using Bloom filter + optional backing store (PRD §12.2).
    
    Two-tier approach:
    1. Bloom filter for fast in-memory checks
    2. Optional backing store (Redis/DB) for persistence across restarts
    """
    
    def __init__(
        self,
        bloom_size: int = BLOOM_FILTER_SIZE,
        backing_store: Any = None,
    ):
        self.bloom = BloomFilter(size=bloom_size)
        self.backing_store = backing_store  # Redis client or similar
        self._stats = {"checked": 0, "duplicates": 0}
    
    def check_and_register(self, event_id: str) -> DeduplicationResult:
        """Check if event is duplicate, register if new.
        
        Returns:
            DeduplicationResult with is_duplicate flag
        """
        self._stats["checked"] += 1
        
        # Fast path: Bloom filter check
        if self.bloom.contains(event_id):
            # Possible duplicate - verify with backing store if available
            if self.backing_store:
                if self._backing_contains(event_id):
                    self._stats["duplicates"] += 1
                    return DeduplicationResult(
                        is_duplicate=True,
                        event_id=event_id,
                        reason="backing_store_match",
                    )
            else:
                # No backing store, treat as duplicate (conservative)
                self._stats["duplicates"] += 1
                return DeduplicationResult(
                    is_duplicate=True,
                    event_id=event_id,
                    reason="bloom_filter_match",
                )
        
        # Not a duplicate - register it
        self.bloom.add(event_id)
        if self.backing_store:
            self._backing_add(event_id)
        
        return DeduplicationResult(is_duplicate=False, event_id=event_id)
    
    def _backing_contains(self, event_id: str) -> bool:
        """Check backing store for event_id."""
        # Override in subclass or use Redis client
        return False
    
    def _backing_add(self, event_id: str) -> None:
        """Add event_id to backing store."""
        # Override in subclass or use Redis client
        pass
    
    @property
    def stats(self) -> dict:
        return {
            **self._stats,
            "bloom_count": self.bloom.count,
        }


@dataclass
class DLQEvent:
    """Event in the Dead Letter Queue (PRD §12.4)."""
    event_id: str
    original_payload: dict
    error_type: str
    error_message: str
    feed: str
    provider: str
    attempts: int = 1
    first_failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_failed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "original_payload": self.original_payload,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "feed": self.feed,
            "provider": self.provider,
            "attempts": self.attempts,
            "first_failed_at": self.first_failed_at.isoformat(),
            "last_failed_at": self.last_failed_at.isoformat(),
        }


class DeadLetterQueue:
    """Dead Letter Queue for failed events (PRD §12.4).
    
    Events that fail processing are sent here for:
    - Manual inspection
    - Delayed retry
    - Alert generation
    """
    
    def __init__(self, max_size: int = 10000):
        self._queue: list[DLQEvent] = []
        self.max_size = max_size
        self._stats = {"added": 0, "reprocessed": 0, "dropped": 0}
    
    def add(
        self,
        event_id: str,
        payload: dict,
        error: Exception,
        feed: str,
        provider: str,
    ) -> None:
        """Add a failed event to the DLQ."""
        # Check if already in queue
        for existing in self._queue:
            if existing.event_id == event_id:
                existing.attempts += 1
                existing.last_failed_at = datetime.now(UTC)
                existing.error_message = str(error)
                logger.warning(
                    "dlq_retry_failed",
                    event_id=event_id,
                    attempts=existing.attempts,
                )
                return
        
        # Add new entry
        if len(self._queue) >= self.max_size:
            dropped = self._queue.pop(0)
            self._stats["dropped"] += 1
            logger.error("dlq_overflow", dropped_event_id=dropped.event_id)
        
        self._queue.append(DLQEvent(
            event_id=event_id,
            original_payload=payload,
            error_type=type(error).__name__,
            error_message=str(error),
            feed=feed,
            provider=provider,
        ))
        self._stats["added"] += 1
        
        logger.warning(
            "dlq_event_added",
            event_id=event_id,
            error_type=type(error).__name__,
            feed=feed,
            provider=provider,
        )
    
    def pop(self) -> DLQEvent | None:
        """Remove and return the oldest event from the queue."""
        if self._queue:
            event = self._queue.pop(0)
            self._stats["reprocessed"] += 1
            return event
        return None
    
    def peek(self, n: int = 10) -> list[DLQEvent]:
        """View the next n events without removing them."""
        return self._queue[:n]
    
    def __len__(self) -> int:
        return len(self._queue)
    
    @property
    def stats(self) -> dict:
        return {**self._stats, "current_size": len(self._queue)}


def retry_with_backoff(
    fn: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.5,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> Any:
    """Execute function with exponential backoff and jitter (PRD §12.4).
    
    Args:
        fn: Function to execute
        max_retries: Maximum retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap
        jitter: Random jitter factor (0-1)
        on_retry: Callback on each retry with (attempt, exception)
        
    Returns:
        Result of fn()
        
    Raises:
        Last exception if all retries exhausted
    """
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:
            last_exception = e
            
            if attempt == max_retries:
                logger.error(
                    "retry_exhausted",
                    attempts=attempt + 1,
                    error=str(e),
                )
                raise
            
            # Calculate backoff with jitter
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter_amount = delay * jitter * random.random()
            actual_delay = delay + jitter_amount
            
            logger.warning(
                "retry_attempt",
                attempt=attempt + 1,
                max_retries=max_retries,
                delay_seconds=actual_delay,
                error=str(e),
            )
            
            if on_retry:
                on_retry(attempt + 1, e)
            
            time.sleep(actual_delay)
    
    raise last_exception


async def retry_with_backoff_async(
    fn: Callable,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.5,
) -> Any:
    """Async version of retry_with_backoff."""
    import asyncio
    
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await fn()
        except Exception as e:
            last_exception = e
            
            if attempt == max_retries:
                raise
            
            delay = min(base_delay * (2 ** attempt), max_delay)
            jitter_amount = delay * jitter * random.random()
            
            await asyncio.sleep(delay + jitter_amount)
    
    raise last_exception



================================================
FILE: heber/sdk/__init__.py
================================================
"""Heber SDK - Client library for accessing the data lakehouse."""

from heber.sdk.client import HeberClient

__all__ = ["HeberClient"]



================================================
FILE: heber/sdk/client.py
================================================
"""HeberClient - Main SDK client for accessing Heber Data Lakehouse.

Provides safe, point-in-time correct access to Silver and Gold datasets.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import httpx
import pandas as pd
import pyarrow.parquet as pq
import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)


class HeberClient:
    """Client for reading and writing Heber datasets.
    
    Example:
        client = HeberClient()
        
        # Read Silver data (point-in-time correct)
        bars = client.read_asof(
            dataset="bars",
            asof_time=datetime(2025, 1, 15),
            instrument_keys=["equity:AAPL"],
        )
        
        # Write Gold features
        client.write_gold(
            dataset="momentum_features",
            df=features,
            project="kairos",
            version="v1",
        )
    """

    def __init__(
        self,
        catalog_url: str | None = None,
        data_root: Path | None = None,
        api_key: str | None = None,
    ):
        """Initialize HeberClient.
        
        Args:
            catalog_url: URL of the Catalog API. Defaults to settings.
            data_root: Root path for data. Defaults to settings.
            api_key: API key for authentication.
        """
        self.catalog_url = catalog_url or f"http://localhost:{settings.api_port}/api/v1"
        self.data_root = data_root or settings.data_root
        self.api_key = api_key
        self._http_client: httpx.Client | None = None

    @property
    def http_client(self) -> httpx.Client:
        """Lazy HTTP client initialization."""
        if self._http_client is None:
            headers = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._http_client = httpx.Client(
                base_url=self.catalog_url,
                headers=headers,
                timeout=30.0,
            )
        return self._http_client

    def close(self):
        """Close the HTTP client."""
        if self._http_client:
            self._http_client.close()
            self._http_client = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # Catalog operations
    def list_datasets(self, layer: str | None = None) -> list[dict]:
        """List all datasets, optionally filtered by layer."""
        params = {}
        if layer:
            params["layer"] = layer
        response = self.http_client.get("/datasets", params=params)
        response.raise_for_status()
        return response.json()["data"]

    def get_dataset(self, name: str) -> dict:
        """Get dataset metadata by name."""
        response = self.http_client.get(f"/datasets/{name}")
        response.raise_for_status()
        return response.json()["data"]

    def resolve_instrument(self, symbol: str) -> str | None:
        """Resolve symbol to canonical instrument_key."""
        response = self.http_client.post("/instruments/lookup", json={"symbols": [symbol]})
        response.raise_for_status()
        data = response.json()["data"]
        if data:
            return data[0]["instrument_key"]
        return None

    def discover(
        self,
        dataset_name: str,
        layer: str = "silver",
        schema_version: str = "latest",
    ) -> dict:
        """Discover dataset paths, schema, and partitions (PRD §11.6).
        
        Args:
            dataset_name: Name of dataset (e.g., "bars", "quotes")
            layer: Storage layer (bronze, silver, gold)
            schema_version: Schema version or "latest"
            
        Returns:
            dict with keys:
              - paths: list of partition paths
              - schema: schema definition
              - partitions: list of partition values
        """
        # Get dataset metadata
        dataset = self.get_dataset(dataset_name)
        
        # Get schema version
        response = self.http_client.get(f"/datasets/{dataset_name}/versions")
        response.raise_for_status()
        versions = response.json()["data"]
        
        if schema_version == "latest":
            schema = next((v for v in versions if v.get("is_current")), versions[0] if versions else None)
        else:
            schema = next((v for v in versions if v.get("schema_version") == schema_version), None)
        
        # Build base path
        base_path = self.data_root / layer / f"feed={dataset_name}"
        
        # Discover partitions
        partitions = []
        if base_path.exists():
            for partition_dir in base_path.glob("**/dt=*"):
                partition_parts = {}
                for part in str(partition_dir).split("/"):
                    if "=" in part:
                        key, value = part.split("=", 1)
                        partition_parts[key] = value
                partitions.append(partition_parts)
        
        return {
            "dataset": dataset,
            "layer": layer,
            "paths": [str(base_path)],
            "schema": schema,
            "partitions": partitions,
        }

    def asof_join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on_keys: list[str] = None,
        left_time: str = "ts_event",
        right_time: str = "ts_event",
        right_available: str = "ts_available",
        tolerance: str | None = None,
        suffix: str = "_right",
    ) -> pd.DataFrame:
        """Point-in-time correct as-of join (PRD §10.4, §11.6).
        
        Joins left to the most recent prior row from right where:
        - ts_event_right <= left_time
        - ts_available_right <= left_time
        
        Args:
            left: Left DataFrame (driving table)
            right: Right DataFrame (lookup table)
            on_keys: Join key columns (e.g., ["instrument_key"])
            left_time: Time column in left for join
            right_time: Time column in right for join
            right_available: Availability column in right
            tolerance: Max time difference (e.g., "1h", "30m")
            suffix: Suffix for right columns
            
        Returns:
            Joined DataFrame with anti-leakage guarantee
        """
        if on_keys is None:
            on_keys = ["instrument_key"]
        
        # Create a safe join time for right table
        # This is the max of ts_event and ts_available
        right = right.copy()
        right["_safe_time"] = right[[right_time, right_available]].max(axis=1)
        
        # Sort both tables
        left = left.sort_values(left_time)
        right = right.sort_values("_safe_time")
        
        # Perform pandas merge_asof
        result = pd.merge_asof(
            left,
            right,
            left_on=left_time,
            right_on="_safe_time",
            by=on_keys,
            tolerance=pd.Timedelta(tolerance) if tolerance else None,
            direction="backward",
            suffixes=("", suffix),
        )
        
        # Drop helper column
        if "_safe_time" + suffix in result.columns:
            result = result.drop(columns=["_safe_time" + suffix])
        elif "_safe_time" in result.columns:
            result = result.drop(columns=["_safe_time"])
        
        logger.debug(
            "asof_join complete",
            left_rows=len(left),
            right_rows=len(right),
            result_rows=len(result),
        )
        
        return result

    # Silver layer reads
    def read_silver(
        self,
        dataset: str,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        instrument_keys: list[str] | None = None,
        instrument_type: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read from Silver layer.
        
        Args:
            dataset: Dataset name (e.g., "bars", "quotes", "trades")
            time_range: (start, end) datetime range
            instrument_keys: Filter to specific instruments
            instrument_type: Filter by instrument type
            columns: Columns to read (None for all)
            
        Returns:
            DataFrame with Silver data
        """
        silver_path = self.data_root / "silver" / f"feed={dataset}"
        
        if not silver_path.exists():
            return pd.DataFrame()

        # Build filters
        filters = []
        if instrument_keys:
            filters.append(("instrument_key", "in", instrument_keys))
        if instrument_type:
            filters.append(("instrument_type", "==", instrument_type))

        try:
            table = pq.read_table(
                silver_path,
                columns=columns,
                filters=filters if filters else None,
            )
            df = table.to_pandas()

            # Apply time range filter
            if time_range and not df.empty:
                start, end = time_range
                if isinstance(start, str):
                    start = pd.Timestamp(start)
                if isinstance(end, str):
                    end = pd.Timestamp(end)
                df = df[(df["ts_event"] >= start) & (df["ts_event"] <= end)]

            return df

        except Exception as e:
            logger.error("Failed to read Silver", dataset=dataset, error=str(e))
            raise

    def read_asof(
        self,
        dataset: str,
        asof_time: datetime | str,
        instrument_keys: list[str] | None = None,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Read Silver data with point-in-time correctness.
        
        Only returns rows where ts_available <= asof_time.
        This is the primary read method for training and backtesting.
        
        Args:
            dataset: Dataset name
            asof_time: Point-in-time cutoff (only data available by this time)
            instrument_keys: Filter to specific instruments
            time_range: (start, end) for ts_event range
            columns: Columns to read
            
        Returns:
            DataFrame with point-in-time correct data
        """
        if isinstance(asof_time, str):
            asof_time = pd.Timestamp(asof_time)

        # Read base data
        df = self.read_silver(
            dataset=dataset,
            time_range=time_range,
            instrument_keys=instrument_keys,
            columns=columns,
        )

        if df.empty:
            return df

        # Apply point-in-time filter (the critical anti-leakage gate)
        df = df[df["ts_available"] <= asof_time]

        logger.debug(
            "read_asof complete",
            dataset=dataset,
            asof_time=asof_time,
            rows=len(df),
        )

        return df

    # Gold layer operations
    def read_gold(
        self,
        dataset: str,
        project: str | None = None,
        version: str | None = None,
        time_range: tuple[datetime | str, datetime | str] | None = None,
        instrument_keys: list[str] | None = None,
        asof_time: datetime | str | None = None,
    ) -> pd.DataFrame:
        """Read from Gold layer (features/labels).
        
        Args:
            dataset: Dataset name (e.g., "momentum_features")
            project: Filter by project
            version: Specific version (None for latest)
            time_range: (start, end) datetime range
            instrument_keys: Filter to specific instruments
            asof_time: Point-in-time cutoff (optional)
            
        Returns:
            DataFrame with Gold data
        """
        gold_path = self.data_root / "gold" / f"dataset={dataset}"
        
        if project:
            gold_path = gold_path / f"project={project}"
        if version:
            gold_path = gold_path / f"version={version}"

        if not gold_path.exists():
            return pd.DataFrame()

        # Build filters
        filters = []
        if instrument_keys:
            filters.append(("instrument_key", "in", instrument_keys))

        try:
            table = pq.read_table(
                gold_path,
                filters=filters if filters else None,
            )
            df = table.to_pandas()

            # Apply time range filter
            if time_range and not df.empty:
                start, end = time_range
                if isinstance(start, str):
                    start = pd.Timestamp(start)
                if isinstance(end, str):
                    end = pd.Timestamp(end)
                time_col = "ts_event" if "ts_event" in df.columns else "ts_label"
                df = df[(df[time_col] >= start) & (df[time_col] <= end)]

            # Apply point-in-time filter
            if asof_time and "ts_available" in df.columns:
                if isinstance(asof_time, str):
                    asof_time = pd.Timestamp(asof_time)
                df = df[df["ts_available"] <= asof_time]

            return df

        except Exception as e:
            logger.error("Failed to read Gold", dataset=dataset, error=str(e))
            raise

    def write_gold(
        self,
        dataset: str,
        df: pd.DataFrame,
        project: str,
        version: str,
        metadata: dict[str, Any] | None = None,
    ) -> Path:
        """Write features/labels to Gold layer.
        
        Args:
            dataset: Dataset name
            df: DataFrame to write (must include instrument_key, ts_event, ts_available)
            project: Project name
            version: Version string
            metadata: Additional metadata to log
            
        Returns:
            Path to written file
        """
        # Validate required columns
        required_cols = {"instrument_key", "ts_event", "ts_available"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Validate point-in-time correctness
        if (df["ts_available"] < df["ts_event"]).any():
            raise ValueError("ts_available cannot be before ts_event (leakage detected)")

        # Determine partition (by date)
        df = df.copy()
        df["dt"] = pd.to_datetime(df["ts_event"]).dt.date

        # Write partitioned by date
        total_rows = 0
        output_paths = []

        for dt, group_df in df.groupby("dt"):
            partition_path = (
                self.data_root
                / "gold"
                / f"dataset={dataset}"
                / f"project={project}"
                / f"version={version}"
                / f"dt={dt}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            file_path = partition_path / f"part-{ts}.parquet"

            # Drop temporary dt column
            write_df = group_df.drop(columns=["dt"])
            write_df.to_parquet(file_path, compression="snappy")

            total_rows += len(write_df)
            output_paths.append(file_path)

        logger.info(
            "Wrote Gold dataset",
            dataset=dataset,
            project=project,
            version=version,
            rows=total_rows,
            files=len(output_paths),
        )

        return output_paths[0] if output_paths else None



================================================
FILE: heber/writer/__init__.py
================================================
"""Heber LakeWriter - Bronze and Silver layer writers."""



================================================
FILE: heber/writer/bronze.py
================================================
"""Bronze layer writer - raw provider payloads.

Bronze is append-only, immutable storage of original events.
Format: JSONL + gzip
Path: bronze/provider={}/feed={}/dt={}/hour={}/
"""

import gzip
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope

logger = structlog.get_logger(__name__)


class BronzeWriter:
    """Writes events to Bronze layer as JSONL files."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.buffer_counts: dict[str, int] = defaultdict(int)
        self.last_flush: datetime = datetime.utcnow()

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        dt = envelope.ts_event.strftime("%Y-%m-%d")
        hour = envelope.ts_event.strftime("%H")
        return f"provider={envelope.provider}/feed={envelope.feed}/dt={dt}/hour={hour}"

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.bronze_path / partition_key
        base.mkdir(parents=True, exist_ok=True)
        
        # Use timestamp-based filename for uniqueness
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return base / f"events-{ts}.jsonl.gz"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)
        
        # Store the full envelope (including raw if present)
        event_dict = envelope.model_dump(mode="json")
        self.buffers[partition_key].append(event_dict)
        self.buffer_counts[partition_key] += 1

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.utcnow()
        elapsed = (now - self.last_flush).total_seconds()

        for partition_key, events in list(self.buffers.items()):
            should_flush = (
                len(events) >= settings.bronze_max_batch_size
                or elapsed >= settings.bronze_flush_interval_seconds
            )
            if should_flush and events:
                await self._flush_partition(partition_key, events)
                self.buffers[partition_key] = []

        self.last_flush = now

    async def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, events in list(self.buffers.items()):
            if events:
                await self._flush_partition(partition_key, events)
                self.buffers[partition_key] = []

    async def _flush_partition(self, partition_key: str, events: list[dict]) -> None:
        """Write events to a partition file."""
        file_path = self._get_file_path(partition_key)
        
        try:
            # Write as gzipped JSONL
            with gzip.open(file_path, "wt", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps(event, default=str) + "\n")

            logger.info(
                "Flushed Bronze partition",
                partition=partition_key,
                events=len(events),
                file=str(file_path),
            )
        except Exception as e:
            logger.error(
                "Failed to flush Bronze partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise



================================================
FILE: heber/writer/compactor.py
================================================
"""Parquet file compactor.

Merges small Parquet files into target-sized files.
Runs periodically to prevent "small file problem."
"""

import asyncio
import signal
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)

# Target file size in bytes (256 MB)
TARGET_FILE_SIZE = settings.silver_target_file_size_mb * 1024 * 1024


class Compactor:
    """Compacts small Parquet files into larger ones."""

    def __init__(self):
        self.running = False

    async def compact_partition(self, partition_path: Path) -> int:
        """Compact all small files in a partition.
        
        Returns number of files merged.
        """
        parquet_files = sorted(partition_path.glob("*.parquet"))
        
        if len(parquet_files) <= 1:
            return 0

        # Check total size
        total_size = sum(f.stat().st_size for f in parquet_files)
        
        # Only compact if we have multiple small files
        small_files = [f for f in parquet_files if f.stat().st_size < TARGET_FILE_SIZE]
        
        if len(small_files) <= 1:
            return 0

        logger.info(
            "Compacting partition",
            partition=str(partition_path),
            files=len(small_files),
            total_bytes=total_size,
        )

        try:
            # Read all small files
            tables = []
            for f in small_files:
                table = pq.read_table(f)
                tables.append(table)

            # Concatenate
            import pyarrow as pa
            merged_table = pa.concat_tables(tables)

            # Write merged file
            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            merged_path = partition_path / f"compacted-{ts}.parquet"
            
            pq.write_table(
                merged_table,
                merged_path,
                compression="snappy",
                row_group_size=250_000,
            )

            # Delete original small files
            for f in small_files:
                f.unlink()

            logger.info(
                "Compaction complete",
                partition=str(partition_path),
                merged_files=len(small_files),
                output_file=str(merged_path),
                rows=merged_table.num_rows,
            )

            return len(small_files)

        except Exception as e:
            logger.error(
                "Compaction failed",
                partition=str(partition_path),
                error=str(e),
                exc_info=True,
            )
            return 0

    async def scan_and_compact(self, layer: str = "silver") -> dict:
        """Scan layer for partitions that need compaction."""
        layer_path = settings.data_root / layer
        
        if not layer_path.exists():
            return {"partitions_scanned": 0, "files_merged": 0}

        partitions_scanned = 0
        files_merged = 0

        # Walk through all partitions (directories containing .parquet files)
        for partition_path in layer_path.rglob("*"):
            if partition_path.is_dir():
                parquet_files = list(partition_path.glob("*.parquet"))
                if parquet_files:
                    partitions_scanned += 1
                    merged = await self.compact_partition(partition_path)
                    files_merged += merged

        return {
            "partitions_scanned": partitions_scanned,
            "files_merged": files_merged,
        }

    async def run(self, interval_minutes: int = 60):
        """Run compactor on a schedule."""
        self.running = True
        
        logger.info("Starting compactor", interval_minutes=interval_minutes)

        while self.running:
            try:
                # Compact Silver layer
                result = await self.scan_and_compact("silver")
                logger.info("Compaction cycle complete", **result)

                # Wait for next cycle
                await asyncio.sleep(interval_minutes * 60)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Compactor error", error=str(e), exc_info=True)
                await asyncio.sleep(60)  # Back off on error

        logger.info("Compactor stopped")

    def stop(self):
        """Stop the compactor."""
        self.running = False


async def main():
    """Entry point for the compactor."""
    compactor = Compactor()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, compactor.stop)

    await compactor.run()


if __name__ == "__main__":
    asyncio.run(main())



================================================
FILE: heber/writer/consumer.py
================================================
"""Redis Streams consumer for incoming events.

Subscribes to the event stream from Data Gateway and routes to Bronze/Silver writers.
"""

import asyncio
import json
import signal
from datetime import datetime

import redis.asyncio as redis
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope
from heber.writer.bronze import BronzeWriter
from heber.writer.silver import SilverWriter

logger = structlog.get_logger(__name__)


class EventConsumer:
    """Consumes events from Redis Streams and writes to Lake layers."""

    def __init__(self):
        self.redis: redis.Redis | None = None
        self.bronze_writer = BronzeWriter()
        self.silver_writer = SilverWriter()
        self.running = False
        self.consumer_name = f"consumer-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    async def connect(self):
        """Connect to Redis."""
        self.redis = redis.from_url(settings.redis_url)
        logger.info("Connected to Redis", url=settings.redis_url)

        # Create consumer group if it doesn't exist
        try:
            await self.redis.xgroup_create(
                name=settings.redis_stream_name,
                groupname=settings.redis_consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                "Created consumer group",
                stream=settings.redis_stream_name,
                group=settings.redis_consumer_group,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug("Consumer group already exists")
            else:
                raise

    async def process_event(self, event_data: dict) -> bool:
        """Process a single event through Bronze and Silver layers.
        
        Returns True if successful, False otherwise.
        """
        try:
            # Parse envelope
            payload_str = event_data.get(b"payload") or event_data.get("payload")
            if isinstance(payload_str, bytes):
                payload_str = payload_str.decode("utf-8")
            
            event_dict = json.loads(payload_str)
            envelope = EventEnvelope.model_validate(event_dict)

            # Set ts_available if not present
            if envelope.ts_available is None:
                envelope = envelope.with_ts_available(datetime.utcnow())

            # Write to Bronze (always)
            await self.bronze_writer.write(envelope)

            # Write to Silver (normalized)
            await self.silver_writer.write(envelope)

            logger.debug(
                "Processed event",
                event_id=envelope.event_id,
                feed=envelope.feed,
                instrument_key=envelope.instrument_key,
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to process event",
                error=str(e),
                event_data=str(event_data)[:200],
                exc_info=True,
            )
            return False

    async def run(self):
        """Main consumer loop."""
        await self.connect()
        self.running = True

        logger.info(
            "Starting consumer",
            stream=settings.redis_stream_name,
            group=settings.redis_consumer_group,
            consumer=self.consumer_name,
        )

        while self.running:
            try:
                # Read from stream with consumer group
                messages = await self.redis.xreadgroup(
                    groupname=settings.redis_consumer_group,
                    consumername=self.consumer_name,
                    streams={settings.redis_stream_name: ">"},
                    count=100,
                    block=1000,  # Block for 1 second
                )

                if not messages:
                    continue

                for stream_name, stream_messages in messages:
                    for message_id, message_data in stream_messages:
                        success = await self.process_event(message_data)
                        
                        if success:
                            # Acknowledge message
                            await self.redis.xack(
                                settings.redis_stream_name,
                                settings.redis_consumer_group,
                                message_id,
                            )
                        else:
                            # TODO: Send to DLQ
                            logger.warning("Event failed, needs DLQ", message_id=message_id)

                # Flush writers periodically
                await self.bronze_writer.flush_if_needed()
                await self.silver_writer.flush_if_needed()

            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                break
            except Exception as e:
                logger.error("Consumer error", error=str(e), exc_info=True)
                await asyncio.sleep(1)  # Back off on error

        # Final flush
        await self.bronze_writer.flush()
        await self.silver_writer.flush()
        logger.info("Consumer stopped")

    async def stop(self):
        """Stop the consumer."""
        self.running = False
        if self.redis:
            await self.redis.close()


async def main():
    """Entry point for the consumer."""
    consumer = EventConsumer()

    # Handle signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(consumer.stop()))

    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())



================================================
FILE: heber/writer/silver.py
================================================
"""Silver layer writer - normalized Parquet datasets.

Silver is the canonical, normalized event layer optimized for querying.
Format: Parquet
Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
"""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.models.envelope import EventEnvelope

logger = structlog.get_logger(__name__)

# Dataset-specific schemas (per PRD Section 8.7)
SILVER_SCHEMAS = {
    "bars": pa.schema([
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        # Bars-specific
        ("timeframe", pa.string()),
        ("bar_start_ts", pa.timestamp("us", tz="UTC")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("trade_count", pa.int64()),
        ("vwap", pa.float64()),
    ]),
    "quotes": pa.schema([
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        # Quotes-specific
        ("bid_px", pa.float64()),
        ("bid_sz", pa.float64()),
        ("ask_px", pa.float64()),
        ("ask_sz", pa.float64()),
        ("bid_exchange", pa.string()),
        ("ask_exchange", pa.string()),
    ]),
    "trades": pa.schema([
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        # Trades-specific
        ("trade_id", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("exchange", pa.string()),
        ("tape", pa.string()),
    ]),
    "flow_alerts": pa.schema([
        ("event_id", pa.string()),
        ("provider", pa.string()),
        ("feed", pa.string()),
        ("instrument_type", pa.string()),
        ("instrument_key", pa.string()),
        ("symbol", pa.string()),
        ("ts_event", pa.timestamp("us", tz="UTC")),
        ("ts_ingest", pa.timestamp("us", tz="UTC")),
        ("ts_available", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("schema_version", pa.string()),
        ("quality_flags", pa.list_(pa.string())),
        # Flow-specific
        ("underlying", pa.string()),
        ("occ_symbol", pa.string()),
        ("expiry", pa.date32()),
        ("strike", pa.float64()),
        ("put_call", pa.string()),
        ("premium", pa.float64()),
        ("volume", pa.float64()),
        ("open_interest", pa.float64()),
        ("spot_px", pa.float64()),
        ("contract_px", pa.float64()),
        ("alert_type", pa.string()),
        ("side", pa.string()),
        ("aggressor", pa.string()),
    ]),
}

# Default schema for unknown feeds
DEFAULT_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("provider", pa.string()),
    ("feed", pa.string()),
    ("instrument_type", pa.string()),
    ("instrument_key", pa.string()),
    ("symbol", pa.string()),
    ("ts_event", pa.timestamp("us", tz="UTC")),
    ("ts_ingest", pa.timestamp("us", tz="UTC")),
    ("ts_available", pa.timestamp("us", tz="UTC")),
    ("source", pa.string()),
    ("schema_version", pa.string()),
    ("quality_flags", pa.list_(pa.string())),
    ("payload_json", pa.string()),  # Store payload as JSON string
])


class SilverWriter:
    """Writes normalized events to Silver layer as Parquet."""

    def __init__(self):
        self.buffers: dict[str, list[dict]] = defaultdict(list)
        self.last_flush: datetime = datetime.utcnow()

    def _get_partition_key(self, envelope: EventEnvelope) -> str:
        """Generate partition key for an event."""
        dt = envelope.ts_event.strftime("%Y-%m-%d")
        
        # High-volume feeds use hour partitioning
        if envelope.feed in ("quotes", "trades"):
            hour = envelope.ts_event.strftime("%H")
            return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}/hour={hour}"
        
        return f"feed={envelope.feed}/instrument_type={envelope.instrument_type}/dt={dt}"

    def _get_schema(self, feed: str) -> pa.Schema:
        """Get schema for a feed."""
        return SILVER_SCHEMAS.get(feed, DEFAULT_SCHEMA)

    def _envelope_to_row(self, envelope: EventEnvelope) -> dict[str, Any]:
        """Convert envelope to Silver row format."""
        # Base columns from envelope
        row = {
            "event_id": envelope.event_id,
            "provider": envelope.provider,
            "feed": envelope.feed,
            "instrument_type": envelope.instrument_type,
            "instrument_key": envelope.instrument_key,
            "symbol": envelope.symbol,
            "ts_event": envelope.ts_event,
            "ts_ingest": envelope.ts_ingest,
            "ts_available": envelope.ts_available,
            "source": envelope.source,
            "schema_version": envelope.schema_version,
            "quality_flags": envelope.quality_flags,
        }

        # Add payload fields
        payload = envelope.payload
        if envelope.feed in SILVER_SCHEMAS:
            # Map payload fields to schema columns
            for field in SILVER_SCHEMAS[envelope.feed]:
                if field.name not in row:
                    row[field.name] = payload.get(field.name)
        else:
            # Store payload as JSON for unknown feeds
            import json
            row["payload_json"] = json.dumps(payload, default=str)

        return row

    def _get_file_path(self, partition_key: str) -> Path:
        """Get file path for a partition."""
        base = settings.silver_path / partition_key
        base.mkdir(parents=True, exist_ok=True)
        
        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        return base / f"part-{ts}.parquet"

    async def write(self, envelope: EventEnvelope) -> None:
        """Buffer an event for writing."""
        partition_key = self._get_partition_key(envelope)
        row = self._envelope_to_row(envelope)
        self.buffers[partition_key].append(row)

    async def flush_if_needed(self) -> None:
        """Flush buffers if conditions are met."""
        now = datetime.utcnow()
        elapsed = (now - self.last_flush).total_seconds()

        for partition_key, rows in list(self.buffers.items()):
            should_flush = (
                len(rows) >= settings.silver_max_rows_per_file
                or elapsed >= settings.bronze_flush_interval_seconds
            )
            if should_flush and rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

        self.last_flush = now

    async def flush(self) -> None:
        """Flush all buffers immediately."""
        for partition_key, rows in list(self.buffers.items()):
            if rows:
                await self._flush_partition(partition_key, rows)
                self.buffers[partition_key] = []

    async def _flush_partition(self, partition_key: str, rows: list[dict]) -> None:
        """Write rows to a Parquet file."""
        if not rows:
            return

        # Determine feed from partition key
        feed = partition_key.split("/")[0].split("=")[1]
        schema = self._get_schema(feed)
        file_path = self._get_file_path(partition_key)

        try:
            # Create Arrow table
            table = pa.Table.from_pylist(rows, schema=schema)

            # Write Parquet with compression
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                row_group_size=100_000,
            )

            logger.info(
                "Flushed Silver partition",
                partition=partition_key,
                rows=len(rows),
                file=str(file_path),
            )
        except Exception as e:
            logger.error(
                "Failed to flush Silver partition",
                partition=partition_key,
                error=str(e),
                exc_info=True,
            )
            raise



================================================
FILE: scripts/init_volume.sh
================================================
#!/bin/bash
# Initialize directory structure on external volume
# Run this once before starting Docker Compose

set -euo pipefail

VOLUME_ROOT="${HEBER_VOLUME_ROOT:-/Volumes/heber}"

echo "Initializing Heber storage on $VOLUME_ROOT..."

# Create directory structure
directories=(
    "data/bronze"
    "data/silver"
    "data/gold"
    "postgres/data"
    "clickhouse/data"
    "clickhouse/logs"
    "redis/data"
    "logs"
)

for dir in "${directories[@]}"; do
    full_path="$VOLUME_ROOT/$dir"
    if [ ! -d "$full_path" ]; then
        echo "Creating $full_path"
        mkdir -p "$full_path"
    else
        echo "Already exists: $full_path"
    fi
done

# Set permissions (needed for Docker containers)
echo "Setting permissions..."
chmod -R 755 "$VOLUME_ROOT/data"
chmod -R 700 "$VOLUME_ROOT/postgres"
chmod -R 755 "$VOLUME_ROOT/clickhouse"
chmod -R 755 "$VOLUME_ROOT/redis"

echo ""
echo "✓ Heber storage initialized at $VOLUME_ROOT"
echo ""
echo "Directory structure:"
ls -la "$VOLUME_ROOT"
echo ""
echo "Data directories:"
ls -la "$VOLUME_ROOT/data"


