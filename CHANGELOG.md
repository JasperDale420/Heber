# Changelog

All notable changes to the Heber Data Lakehouse project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Documentation

- Added Catalog API reference (`docs/catalog_api.md`)
- Added data contract (`docs/data_contract.md`)
- Added schema registry usage (`docs/schema_registry.md`)
- Added Iceberg migration status (`docs/iceberg_migration.md`)
- Added Hot Store guide (`docs/hot_store.md`)
- Added architecture overview (`docs/architecture.md`)
- Added configuration guide updates and host port mapping (`docs/configuration.md`)
- Added technical debt audit (`docs/technical_debt_audit.md`)
- Expanded technical debt audit (pass 2 findings and scope)
- Expanded technical debt audit (pass 3: features/Feast review)
- Expanded technical debt audit (pass 4: ops review)
- Expanded technical debt audit (pass 5: firewall/models review)
- Expanded technical debt audit (pass 6: gold/retention review)
- Expanded technical debt audit (pass 7: feast review)
- Expanded technical debt audit (pass 8: scripts/docs review)
- Expanded technical debt audit (pass 9: versioning/calendar review)
- Expanded technical debt audit (pass 10: hotstore/schemas review)
- Expanded technical debt audit (pass 11: infra/k8s review)

#### Alert Watch Service (`heber/watch/`)

Real-time tracking of flow alert outcomes for ML labeling:

- **Watch Models** (`models.py`) - `AlertWatch`, `WatchSnapshot`, `WatchOutcome` Pydantic models with Redis key patterns
- **Watch Manager** (`manager.py`) - CRUD operations for active watches in Redis
- **Snapshot Poller** (`poller.py`) - Polls option quotes from Data Gateway every 5-15 min
- **Barrier Checker** (`checker.py`) - Detects TP/SL barrier hits and computes labels
- **Alert Consumer** (`consumer.py`) - Listens to `flow_alerts` Redis stream, auto-creates watches
- **Label Writer** (`writer.py`) - Writes completed outcomes to Gold layer
- **Service Orchestrator** (`WatchService`) - Runs all components concurrently

Polling strategy by horizon:

- Intraday (0-2 DTE): 5 min intervals, 4h max window
- Swing (3-21 DTE): 15 min intervals, 5 day max window
- LEAP (22+ DTE): 1 hour intervals, 30 day max window

#### Trading Calendar Integration (`heber/calendar/`)

Market-hours awareness for the watch service using `exchange-calendars`:

- **`MarketCalendar`** class wrapping NYSE calendar (XNYS)
- `is_market_open()` - Check if market is open for trading
- `add_trading_hours()` - Skip non-trading time in window calculations
- `trading_minutes_until()` - Count trading minutes between timestamps
- `seconds_until_open()` - Sleep until market opens

Integrated into watch service:

- **SnapshotPoller** - Skips polling when market closed, sleeps until open
- **WatchManager** - Window calculations use trading hours, not clock time
- **BarrierChecker** - Adds `trading_minutes_to_hit` metric to outcomes

#### Watch Service CLI & Docker

Full integration for standalone operation:

- **CLI entry point**: `python -m heber.watch [--redis URL] [--gateway URL] [--output PATH]`

#### Meta-Labeling Feature Capture (`heber/watch/features.py`)

Feature extraction for training meta-models that predict alert success:

- **`AlertFeatures` dataclass** - 30 features captured at alert time:
  - Contract info: strike, expiry, DTE, moneyness
  - Alert characteristics: premium, volume, OI ratio, alert type
  - Timing: hour, day of week, minutes since open/to close
  - Sentiment: bullish/bearish/sweep/block flags
- **`AlertFeatureExtractor`** - Extracts features from `FlowAlertRecord`
- **Market enrichment** - Fetches Alpaca bars via Data Gateway for returns/volatility
- **Greeks enrichment** - Fetches delta/gamma/theta/vega/IV from Alpaca option chain
- **IV rank enrichment** - Fetches IV rank from UW options endpoint
- **Redis storage** - Features stored with 7-day TTL for training
- Integrated into `AlertWatchConsumer` - auto-captures on alert arrival

#### ML Dataset Builder (`heber/ml/datasets.py`)

Training dataset construction for meta-models:

- **`MetaLabelDatasetBuilder`** - Joins features with outcomes
- **`DatasetConfig`** - Configurable paths, filters, split ratios
- **Temporal train/test split** - Purge/embargo to prevent leakage
- **`to_xy()` helper** - Converts to (X, y) for sklearn-compatible training
- Supports both Parquet files and Redis feature cache

#### ML Training Pipeline (`heber/ml/trainer.py`)

LightGBM-based meta-model training with MLflow integration:

- **`MetaModelTrainer`** - Trains binary classifier on meta-labels
- **`TrainingConfig`** - Hyperparameters and thresholds
- **MLflow logging** - Tracks experiments, params, metrics, models
- **`train_meta_model()`** - Convenience function for end-to-end training
- **Save/load** - Joblib serialization with config JSON

#### ML Inference Service (`heber/ml/inference.py`)

Real-time scoring of alerts with trained meta-model:

- **`MetaLabelScorer`** - Scores alerts with probability of TP hit
- **`AlertGate`** - Fail-open gate to filter low-probability alerts
- **`InferenceConfig`** - Thresholds and cache settings
- **Score caching** - Redis-backed for repeated lookups
- **Confidence classification** - "high" / "medium" / "low" buckets

- **Environment variables**: `HEBER_REDIS_URL`, `DATA_GATEWAY_URL`, `HEBER_GOLD_PATH`
- **Docker service**: `heber-watch` in `docker-compose.yml`

#### Contract-Based Barrier Labels

Enhanced `alert_labels.py` template with option contract labeling:

- **`ContractBarrierConfig`** - TP/SL thresholds for options (e.g., +25%/-15%)
- **`_compute_contract_barrier_outcome()`** - Barrier detection on option price path
- **Dual labeling** - Primary `contract_hit_tp_first` + secondary `hit_tp_first` (underlying)
- **Presets**: `aggressive()`, `moderate()`, `conservative()`

#### Alert Labels Pipeline Enhancement

Updated `heber/features/pipelines/alert_labels.py`:

- Fetches option bars from Data Gateway API
- Computes both underlying and contract barrier labels
- New CLI flags: `--no-contract`, `--gateway-url`

### Fixed

- **Silver Writer Type Coercion** (`heber/writer/silver.py`)
  - Added `_coerce_value()` method for automatic type conversion to Arrow types
  - Added field name mapping for UW flow_alerts: `price`→`contract_px`, `underlying_price`→`spot_px`, `option_chain`→`occ_symbol`, `alert_rule`→`alert_type`
  - Fixes `ArrowTypeError: object of type <class 'str'> cannot be converted to int` when processing UW flow alerts with string numeric values

\n\n#### SonarQube Code Quality Remediation\n\n- Replaced deprecated `datetime.utcnow()` with `datetime.now(UTC)` in `writer.py` and `writer/consumer.py`\n- Extracted constants for duplicate literals: `DEFAULT_GATEWAY_URL`, `DEFAULT_STORAGE_ROOT`\n- Refactored complex functions by extracting helpers in `consumer.py` and `alert_labels.py`\n- Removed async from functions without await in `hotstore/client.py`, `backfill`, `retention`\n- Removed unused parameters in `openmetadata_client.py` and `backfill/__init__.py`\n- Fixed asyncio.create_task GC issue in `backfill/__init__.py`\n\n### Added

#### Code Quality Pipeline

- **Pre-commit Hooks** (`.pre-commit-config.yaml`)
  - Ruff linter with auto-fix and formatting
  - Detect-secrets for secret leak prevention
  - Standard hooks: trailing whitespace, end-of-file, yaml, merge conflicts, debug statements
  - MyPy and Bandit documented for manual CI runs (deferred due to existing issues)

- **Security Scanning** (`pyproject.toml`)
  - Bandit configuration with test exclusions
  - Detect-secrets baseline generation

- **Dependency Management** (`.github/dependabot.yml`)
  - Weekly Python dependency updates
  - Weekly GitHub Actions updates
  - Weekly Docker dependency updates

- **SonarQube Integration** (`sonar-project.properties`)
  - Project configuration with Python 3.11 target
  - Coverage report integration
  - Source/test path configuration

- **Development Documentation** (`README.md`)
  - Prerequisites and setup instructions
  - Code quality tools usage guide
  - CI/CD pipeline overview

- **Test Infrastructure** (`tests/`)
  - Created tests directory with placeholder test
  - pytest configuration in pyproject.toml

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
  - `heber/features/templates/alert_labels.py` - Flow alert return labels for underlying and option contracts
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

#### Part IX: Testing Framework (PRD §45-50)

- **Synthetic Data Generators** (Phase 49, PRD §50)
  - `SyntheticDataGenerator` for bars, trades, quotes
  - Deterministic generation with seed support
  - `TestDataConfig` for date ranges and symbols
  - `TestFixture` and `FixtureRegistry` for curated test data

- **Leakage Validation Suite** (Phase 46, PRD §49)
  - `LeakageValidator` with LK-001 through LK-007 test cases
  - `validate_no_future_data()` for zero-leakage assertion
  - `validate_backfill_ts_available()` for backfill validation
  - `validate_gold_lineage()` for feature/label integrity
  - Report generation with pass/fail summary
  - 17/17 tests passing

- **Unit/Integration/E2E Framework** (Phases 43-45, PRD §46-48)
  - `UnitTestSpec` with 7 module test areas
  - `MockStrategy` for S3, Redis, Postgres, ClickHouse
  - `IntegrationTestHarness` with 6 component suites
  - `E2ETestSuite` with 7 test flows and schedule

- **Performance Testing** (Phase 47, PRD §51)
  - `PerformanceSLO` with 5 targets (throughput, latency)
  - `LoadTestScenario` with 5 load profiles
  - `RegressionDetection` for baseline comparison
  - `PerformanceTester` with SLO checking

- **CI Gates** (Phase 48, PRD §53)
  - `CoverageRequirement` with 6 component thresholds
  - `CIGate` for PR, main, staging, prod gates
  - `FlakyTestPolicy` with quarantine logic
  - `CIGateEnforcer` with gate checking and reporting
  - 18/18 tests passing

#### Part X: Data Sources (PRD §52, §55-57)

- **Test Environments** (Phase 50, PRD §52)
  - `EnvironmentConfig` for local, CI, staging, production
  - `DockerComposeService` with 4 services (Postgres, Redis, MinIO, ClickHouse)
  - `StagingConfig` with AWS resource specs
  - `EnvironmentManager` with Docker Compose generation

- **Data Source Inventory** (Phase 51, PRD §55-57)
  - `DataProvider` with 7 providers (Alpaca, UW, Finnhub, etc.)
  - `DatasetSpec` with 25 dataset definitions
  - `StorageBoundary` enum (Heber vs Document Store)
  - `ProviderRegistry` and `DatasetCatalog`
  - 17/17 tests passing

- **Additional Dataset Schemas** (Phase 51, PRD §57)
  - `DailyBar` for daily OHLCV with adjusted close, dividends, splits
  - `OptionQuote` and `OptionTrade` with Greeks (delta, gamma, theta, vega)
  - `CongressTrade` and `LobbyingDisclosure` for alternative data
  - `CompanyInfo`, `IncomeStatement`, `BalanceSheet`, `CashFlow`, `FinancialRatios`
  - `EconomicIndicator`, `InterestRate`, `TreasuryYield`
  - `ForexRate`, `CryptoBar`, `CryptoQuote`
  - 16 schemas total, 14/14 tests passing

- **Event Bus Streams** (Phase 52, PRD §60)
  - `StreamConfig` with 15 streams across priorities
  - `ConsumerGroupConfig` with 6 consumer groups
  - `StreamRegistry` for stream/group management

- **Implementation Slices** (Phase 53, PRD §61)
  - `ImplementationSlice` with 8 ordered slices
  - `SliceManager` with dependency tracking and status

- **Gap Resolution Summaries** (Phase 57, PRD §17-62)
  - `DecisionRecord` for design decisions
  - `GapResolutionRegistry` with 18 decisions across 6 categories
  - 17/17 tests passing

- **Access Control** (Phase 56, PRD §11.9)
  - `Project` for project-based access control
  - `DatasetPermission` with layer-based access levels
  - `SDKToken` with scopes, expiry, and validation
  - `AccessControlManager` for permission checking
  - Silver shared by default, Gold requires explicit permission
  - 17/17 tests passing

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
