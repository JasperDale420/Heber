# Changelog

All notable changes to the Heber Data Lakehouse project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Part VII: ML/Research Features (PRD §28-35)

- **Gold Dataset Versioning** (Phase 30, PRD §28)
  - `GoldDatasetVersion` dataclass with semantic versioning support
  - `GoldVersionRegistry` for version persistence and lineage tracking
  - `read_gold()` with version pinning and compatibility checks
  - `check_compatibility()` for safe version upgrades
  - 11/11 tests passing

- **Label Management** (Phase 31, PRD §29)
  - `LabelMetadata` with forward_window, label_horizon, availability_lag
  - `compute_availability_time()` for point-in-time correct labels
  - `write_label()` and `read_label()` with ts_available filtering
  - Zero-leakage guarantee through availability alignment
  - 15/15 tests passing

- **Train/Test Split Utilities** (Phase 32, PRD §30)
  - `walk_forward_splits()` for rolling train/test windows
  - `expanding_window_splits()` for growing training data
  - `HoldoutSet` with `check_holdout_access()` warnings
  - `purge_window()` for label-aware data purging
  - Embargo enforcement between train/test periods
  - 19/19 tests passing

- **Feast Integration** (Phase 33, PRD §31)
  - Feature views: `volatility.py`, `flow.py`, `microstructure.py`, `labels.py`
  - `materialize_features()` for incremental/full materialization
  - `get_historical_features()` for point-in-time training data
  - `get_online_features()` for low-latency inference
  - `search_features()` for feature discovery by owner/category
  - 6/6 tests passing

- **Feature Template Library** (Phase 34, PRD §32)
  - `heber/features/templates/momentum.py` - RSI, MACD, ROC, momentum returns
  - `heber/features/templates/volatility.py` - ATR, Parkinson vol, Bollinger, z-scores
  - `heber/features/templates/flow.py` - Options premium aggregates, call/put ratio
  - `heber/features/templates/microstructure.py` - Spread, depth, imbalance metrics
  - `heber/features/templates/cross_asset.py` - Beta, alpha, relative strength, correlation
  - `heber/features/templates/labels.py` - Forward return labels, classification labels
  - 7/7 tests passing

- **Data Quality Contracts** (Phase 35, PRD §33)
  - `QualityContract` for defining validation rules per dataset
  - `QualityViolation` for tracking failures with affected symbols/dates
  - `QualityReport` with pass/fail status and metrics
  - `DataQualityValidator` with checks:
    - `fill_rate` - % of expected trading days with data
    - `non_null_rate` - % non-null values for OHLCV columns
    - `max_lag_hours` - Data freshness from market close
    - `max_gap_seconds` - Maximum gap between data points
  - Default contracts for bars, trades, quotes datasets
  - 12/12 tests passing

- **Backtest Integration** (Phase 36, PRD §34)
  - `ExperimentConfig` for reproducibility metadata capture
  - `BacktestDataLoader` with point-in-time correct loading
  - `ExperimentTracker` for fold logging and result persistence
  - `generate_reproducibility_checklist()` per PRD §34.4
  - 9/9 tests passing

- **Survivor Bias Handling** (Phase 37, PRD §35)
  - `InstrumentLifecycle` with list_date, delist_date, delist_reason
  - `DelistReason` enum: bankruptcy, merger, acquisition, voluntary, regulatory
  - `UniverseManager` for point-in-time universe snapshots
  - `filter_dataframe()` with:
    - `exclude_future_delistings` - Strict survivor bias prevention
    - `mark_delistings` - Flag upcoming delistings
  - `get_delisted_instruments()`, `get_newly_listed_instruments()`
  - 13/13 tests passing

#### Part VIII: Reliability Engineering (PRD §37-38)

- **SLO Framework** (Phase 38, PRD §37)
  - `SLI` dataclass for Service Level Indicators with PromQL queries
  - `SLO` with target percentage and error budget ratio calculation
  - `BurnRateAlert` with Prometheus rule generation
  - `SLOManager` for status calculation and rule generation
  - Default SLOs: Ingestion (99.9%), Write (99.95%), Catalog (99.9%)
  - Burn rate alerts: 14x/1h (critical), 6x/6h (warning), 3x/1d, 1x/3d

- **Error Budget Policy** (Phase 39, PRD §38)
  - `ErrorBudget` with allowed/remaining calculation
  - `BudgetState` enum: healthy (>50%), warning (25-50%), critical (<25%), exhausted
  - `BudgetPolicy` with deploy gates by state
  - `DeployRisk` levels: standard, high_risk, breaking_change, infrastructure
  - `ErrorBudgetManager` for policy enforcement and reporting
  - 20/20 tests passing

- **Incident Runbooks** (Phase 40, PRD §39)
  - `Runbook` with symptoms, triage steps, resolutions
  - `RunbookRegistry` with lookup by key or alert name
  - 6 default runbooks: consumer lag, DLQ, Hot Store, Catalog, compaction, leakage
  - Markdown export for documentation

- **On-Call & Escalation** (Phase 41, PRD §40)
  - `OnCallSchedule` with active time checking
  - `EscalationPolicy` with P1-P4 response/escalation times
  - `Incident` lifecycle: create, acknowledge, resolve
  - `OnCallManager` with escalation logic and channel routing
  - 18/18 tests passing

- **Chaos Engineering** (Phase 42, PRD §41)
  - `ChaosExperiment` with hypothesis, procedure, success criteria
  - `ChaosRegistry` with scheduling by frequency (weekly/monthly/quarterly)
  - 7 default experiments: kill pods, throttle S3, block Catalog, bad events, etc.
  - `ExperimentRun` lifecycle: start, complete, pass/fail tracking
  - Markdown runbook export

- **Capacity Planning** (Phase 43, PRD §42)
  - `BaselineMetric` with 5 defaults: events/day, peak rate, storage
  - `ScalingTrigger` with 7 thresholds: CPU, lag, memory, connections
  - `CapacityForecast` for Q1-Q4 2026 projections
  - `BottleneckAnalysis` for 5 components
  - `CapacityPlanner` with cost projection (volume multiplier)
  - 18/18 tests passing

#### Part VI: Final Infrastructure (PRD §21-29)

- **Backup & Disaster Recovery** (Phase 27, PRD §27)
  - Tiered backup strategy: Hot (1h), Warm (6h), Cold (24h)
  - Recovery procedures for Bronze/Silver/Gold/Catalog
  - RTO/RPO targets per data tier

- **Network Topology** (Phase 28, PRD §28)
  - Multi-tier VPC architecture
  - Security group configurations
  - Service mesh considerations

- **Cost Management** (Phase 29, PRD §29)
  - Monthly cost tracking per component
  - Optimization recommendations
  - Budget alerts

#### Part V: Production Infrastructure (PRD §17-20)

- **Secrets Management** (Phase 24, PRD §17)
  - External Secrets Operator integration
  - Secret rotation procedures

- **Infrastructure as Code** (Phase 25, PRD §18)
  - Terraform modules for AWS resources
  - State management configuration

- **CI/CD Pipeline** (Phase 26, PRD §19)
  - GitHub Actions workflows
  - Docker image builds with Trivy scanning
  - Automated testing gates

#### Part IV: Kubernetes Deployment (PRD §19-20)

- **Container Build** (Phase 22, PRD §19)
  - Multi-stage Dockerfile
  - Security hardening

- **Kubernetes Deployment** (Phase 23, PRD §20)
  - Helm charts for all services
  - HPA configurations
  - PDB policies

#### Part III: Data Lifecycle (PRD §14-16)

- **Compaction Protocol** (Phase 21, PRD §16)
  - Manifest-based commit protocol with crash recovery
  - Concurrent write prevention via distributed locks
  - Compaction scheduling with backoff

- **Retention & Lifecycle** (Phase 20, PRD §15)
  - Automated retention reaper
  - Tier-based retention policies

- **Schema Evolution** (Phase 19, PRD §14)
  - Backward/forward compatibility checks
  - Schema registry integration

## [0.1.0] - 2025-12-01

### Added

- Initial project structure
- Bronze layer ingestion from Redis Streams
- Silver layer canonical schema
- Gold layer feature datasets
- Hot Store integration (ClickHouse)
- Zero-Leakage Firewall
- SDK client library
- Catalog service (PostgreSQL)

---

_For earlier history, see git commit log._
