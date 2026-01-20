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

## Phase 13: Observability (§12.5) ✅

### 13.1 Metrics Stack (§12.5.1-12.5.2)

- [x] Prometheus `/metrics` endpoint on port 9100
- [x] Naming: `heber_<service>_<metric>{<labels>}`

### 13.2 Consumer Metrics

- [x] `heber_consumer_events_received_total{feed,provider}`
- [x] `heber_consumer_events_processed_total{feed,provider,status}`
- [x] `heber_consumer_batch_size{feed}`
- [x] `heber_consumer_lag_seconds{stream}`
- [x] `heber_consumer_dedupe_drops_total{feed}`

### 13.3 Writer Metrics

- [x] `heber_writer_rows_written_total{layer,dataset}`
- [x] `heber_writer_bytes_written_total{layer,dataset}`
- [x] `heber_writer_files_written_total{layer,dataset}`
- [x] `heber_writer_flush_duration_seconds{layer}`
- [x] `heber_writer_errors_total{layer,error_type}`

### 13.4 Compactor Metrics

- [x] `heber_compactor_runs_total{dataset,status}`
- [x] `heber_compactor_files_merged_total{dataset}`
- [x] `heber_compactor_bytes_reclaimed_total{dataset}`

### 13.5 Catalog Metrics

- [x] `heber_catalog_requests_total{endpoint,status_code}`
- [x] `heber_catalog_request_duration_seconds{endpoint}`

### 13.6 Anti-Leakage Latency Metrics (§12.5.3)

- [x] `heber_ingest_lag_seconds` (ts_ingest - ts_event)
- [x] `heber_availability_lag_seconds` (ts_available - ts_event)
- [x] `heber_commit_lag_seconds` (ts_commit - ts_ingest)

### 13.7 Alerting Rules (§12.5.4)

- [x] HeberConsumerLagHigh (>60s for 5m)
- [x] HeberConsumerLagCritical (>300s for 5m)
- [x] HeberWriteErrorRateHigh (>1% for 5m)
- [x] HeberHotStoreLagHigh (>300s for 5m)
- [x] HeberAvailabilityLagSpike (p99 >30s)
- [x] HeberDLQGrowing
- [x] HeberCatalogDown
- [x] HeberCompactionFailed

### 13.8 Dashboards (§12.5.7)

- [x] Heber Overview Dashboard
- [x] Heber Latency Dashboard
- [x] Heber Health Dashboard

---

## Phase 14: Tracing (§12.5.6) ✅

- [x] OpenTelemetry OTLP integration
- [x] Trace ID from Gateway via `lineage.trace_id`
- [x] Spans: `process_batch`, `dedupe_check`, `write_bronze`, `write_silver`, `api_request`
- [x] Sampling: 1% prod, 100% dev

---

## Phase 15: Health Checks (§12.12) ✅

- [x] GET /health basic
- [x] GET /livez (liveness)
- [x] GET /readyz (readiness)
- [x] GET /startup (startup probe)
- [x] Dependency health checks (Redis, Postgres, S3, HTTP)

---

## Phase 16: Circuit Breakers (§12.13) ✅

- [x] Hard vs soft dependency classification
- [x] Degradation matrix per service
- [x] Settings: 5 failures, 30s open, 3 half-open probes
- [x] Degraded mode metric: `heber_degraded_mode{dependency}`

---

## Phase 17: Rolling Upgrades (§12.14) ✅

- [x] Graceful shutdown sequence (SIGTERM)
- [x] Readiness = false on shutdown
- [x] Drain in-flight + flush buffers
- [x] 30s shutdown timeout (configurable via HEBER_SHUTDOWN_TIMEOUT_SECONDS)
- [x] Consumer group rebalancing
- [x] Canary deployment monitor metrics

---

## Phase 18: Event Bus Config (§12.7) ✅

### 18.1 Stream Topology (Pattern A)

- [x] `stream:market.bars`
- [x] `stream:market.quotes`
- [x] `stream:market.trades`
- [x] `stream:intel.flow_alerts`
- [x] `stream:intel.darkpool_trades`

### 18.2 Consumer Groups

- [x] Consumer group per stream
- [x] Ack after successful write + Catalog update
- [x] Unacked messages replay on restart

### 18.3 Ordering

- [x] Preserve per-message timestamps as truth
- [x] No total order assumption

---

## Phase 19: Backpressure & DLQ (§12.8) ✅

### 19.1 Backpressure

- [x] Consumer lag grows (metric: heber_consumer_lag_seconds)
- [x] Never drop data
- [x] Scale consumers or widen batches (BackpressureMonitor)

### 19.2 Retry Policy

- [x] Max retries: 10
- [x] Backoff: exponential + jitter (100ms → 30s)
- [x] Retryable: transient storage/DB failures
- [x] Non-retryable: schema mismatch, malformed envelope

### 19.3 Dead Letter Queue

- [x] `stream:heber.dlq`
- [x] `quarantine/` storage path
- [x] DLQ payload: envelope, error_type, message, stack_trace, first_seen_ts, retry_count

---

## Phase 20: Dedupe Strategy (§12.11) ✅

### 20.1 Dedupe Layers

- [x] Consumer: In-memory bloom filter (fast approx)
- [x] Writer: Append-only per batch (dedupe_batch_at_writer)
- [x] Compactor: Exact dedupe on merge (dedupe_at_compaction)

### 20.2 Bloom Filter Spec

- [x] Expected items: 10M per hour window
- [x] False positive rate: 1%
- [x] Rotate hourly (RotatingBloomFilter)

---

## Phase 21: Compaction Schedule (§12.9) ✅

- [x] Compact hourly partitions after close (18:10-18:30 for hour=18)
- [x] Preserve event_id uniqueness (via dedupe_at_compaction)
- [x] Preserve ts_available (immutable in compaction)
- [x] Atomic writes (temp → rename via AtomicWriter)

---

## Phase 22: Hot Store Sync (§12.10) ✅

### 22.1 Sync Config

- [x] Source: event bus or Silver (HotStoreSyncer)
- [x] Window: rolling last N days (configurable)

### 22.2 ClickHouse Tables

- [x] `quotes_hot`, `trades_hot`, `bars_hot` (HotStoreTable enum)
- [x] Partitioned by date (dt column with MATERIALIZED)
- [x] TTL: 7 days quotes/trades, 30 days bars (ClickHouse-managed)

### 22.3 Consistency

- [x] Lag ≤5 minutes SLA (hot_store_lag_seconds metric)
- [x] Silver is source of truth (HotStoreReader with fallback)

---

## Phase 23: Backfill Pipeline (§13) ✅

- [x] REST backfill patterns (BackfillCoordinator, data_fetcher)
- [x] Gap detection (GapDetector.detect_gaps, get_coverage_summary)
- [x] ts_available = ts_commit for historical (TsAvailablePolicy, BackfillWriter)
- [x] Backfill job API (POST /backfill, GET /backfill/{id}, GET /backfill)
- [x] Progress tracking (progress_dates_completed, backfill_progress_percent)
- [x] heber-backfill service (BackfillCoordinator.run_job)

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
