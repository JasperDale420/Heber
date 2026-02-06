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
- Expanded technical debt audit (pass 12: backfill/backtest review)
- Expanded technical debt audit (pass 13: ops logging/reliability re-audit)
- Expanded technical debt audit (pass 14: versioning + k8s runtime conformance re-audit)
- Expanded technical debt audit (pass 15: backup/security scripts re-audit)
- Expanded technical debt audit (pass 16: tracing + init/docs drift re-audit)
- Expanded technical debt audit (pass 17: versioning/k8s runtime re-audit + worker entrypoint findings)
- Expanded technical debt audit (pass 18: ops logging/reliability + UW coverage-doc re-audit)
- Expanded technical debt audit (pass 19: backup/security scripts + labeling/data contract docs re-audit)
- Expanded technical debt audit (pass 20: backfill/hotloader runtime conformance re-audit)
- Expanded technical debt audit (pass 21: observability/runtime wiring + k8s metrics conformance re-audit)
- Expanded technical debt audit (pass 22: calendar/hotstore/schema conformance re-audit)
- Expanded technical debt audit (pass 23: MarketCalendar timezone hardening + regression-test re-audit)
- Expanded technical debt audit (pass 24: additional schema registry test hardening re-audit)
- Expanded technical debt audit (pass 25: include_extended behavior hardening re-audit)
- Expanded technical debt audit (pass 26: Hot Store base-column conformance re-audit)
- Expanded technical debt audit (pass 27: filesystem security scan gate hardening re-audit)
- Expanded technical debt audit (pass 28: catalog backup cleanup-trap hardening re-audit)
- Expanded technical debt audit (pass 29: clickhouse-backup destination-output alignment re-audit)
- Expanded technical debt audit (pass 30: labeling/data-contract docs alignment re-audit)
- Expanded technical debt audit (pass 31: UW endpoint summary reconciliation re-audit)
- Expanded technical debt audit (pass 32: log-level filtering remediation re-audit)
- Expanded technical debt audit (pass 33: dedupe Bloom-rotation remediation re-audit)
- Expanded technical debt audit (pass 34: lakeFS namespace configurability remediation re-audit)
- Expanded technical debt audit (pass 35: k8s HPA/probe conformance remediation re-audit)
- Expanded technical debt audit (pass 36: tracing optional-dependency safety remediation re-audit)
- Expanded technical debt audit (pass 37: cross-platform init-volume remediation re-audit)
- Expanded technical debt audit (pass 38: worker entrypoint runtime remediation re-audit)
- Expanded technical debt audit (pass 39: metrics-exporter wiring remediation re-audit)
- Expanded technical debt audit (pass 40: lakeFS operation-metrics coverage remediation re-audit)
- Added high-severity remediation plan (`docs/technical_debt_plan.md`)

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
- **Redis Event Bus Pending Claims** (`heber/bus/__init__.py`)
  - Claimed idle messages are now yielded to consumers instead of being dropped
  - Added regression test to ensure claimed messages are processed
- **Meta-Label Alignment** (`heber/watch/checker.py`, `heber/ml/datasets.py`)
  - Label rows now emit canonical outcome columns (`outcome`, `hit_tp_first`, `mfe`, `mae`, `bars_to_hit`)
  - Dataset builder normalizes legacy columns and uses correct outcome values
- **Feature Template Availability** (`heber/features/templates/*.py`)
  - `ts_available` is now derived from source data availability instead of wall-clock time
  - Flow rolling windows now use time-indexed aggregation for correctness
- **Feast Feature View Alignment** (`features/feature_views/*.py`, `features/feature_store.yaml`)
  - Feature view schemas now match produced template/pipeline columns for flow, microstructure, momentum, volatility, return labels, and alert labels
  - Gold offline source paths now follow `dataset/project/version/dt/*.parquet` layout with configurable roots/project/version globs
  - Feast local registry/online paths no longer hardcode `/data/feast`
  - Added regression coverage for schema/path alignment (`tests/test_feature_view_alignment.py`)
- **Pytest Discovery Expansion** (`pyproject.toml`)
  - Test discovery now includes both `tests/` and `heber/`
  - Added support for in-package test files named `tests.py` and `tests_*.py`
  - Default `pytest --collect-only` now sees in-package coverage that was previously skipped
- **Runtime Entrypoint Alignment** (`Dockerfile`, `k8s/base/deployments/*.yaml`)
  - Replaced stale module paths (`heber.bus.consumer`, `heber.writer.service`, `heber.writer.compaction`) with existing runtime modules
  - Docker consumer/writer now run `heber.writer.consumer`; compactor runs `heber.writer.compactor`
  - Kubernetes consumer/writer/compactor deployments now use matching module entrypoints
  - Added regression coverage for runtime module references (`tests/test_runtime_entrypoints.py`)
- **Terraform Module Availability** (`infrastructure/terraform/modules/*`)
  - Added local Terraform module scaffolds for `vpc`, `s3`, `rds`, `elasticache`, `ecr`, and `eks` so root module sources resolve
  - Preserved existing root module inputs/outputs wiring while unblocking initialization from missing-module failures
  - Added regression checks for module-source path resolution (`tests/test_terraform_module_sources.py`)
- **SDK Catalog URL Alignment** (`heber/config.py`, `heber/sdk/client.py`)
  - Added `HEBER_CATALOG_URL` defaulting to `http://localhost:8085/api/v1` for SDK clients
  - `HeberClient` now defaults to `settings.catalog_url` instead of deriving URL from API service bind port
  - Updated SDK/config docs and added regression checks (`tests/test_sdk_catalog_defaults.py`)
- **Hot Store Unification** (`heber/hotstore/*`, `heber/writer/hotstore.py`)
  - Consolidated Hot Store sync/write logic into `heber.hotstore.sync` using the existing `clickhouse-connect` client path
  - Replaced legacy duplicate `heber.writer.hotstore` implementation with a compatibility re-export facade
  - Fixed async/sync mismatch points by using sync-safe table creation (`create_all_tables`) plus optional async helper (`create_all_tables_async`)
  - Added regression coverage for unified table creation, batch writes, and metrics (`tests/test_hotstore_unification.py`)
- **Consumer DLQ + Pending Recovery** (`heber/writer/consumer.py`)
  - Added startup recovery for idle pending Redis stream entries via `XPENDING`/`XCLAIM`
  - Added per-message retry with configurable backoff before dead-lettering
  - Added Redis DLQ routing for unrecoverable messages (`HEBER_REDIS_DLQ_STREAM_NAME`)
  - Added regression coverage for pending recovery and DLQ behavior (`tests/test_writer_consumer_reliability.py`)
- **Silver Flush Timing Fix** (`heber/writer/silver.py`)
  - Silver flush checks now use `silver_max_flush_time_seconds` instead of Bronze flush interval settings
  - Added regression tests to ensure Silver timing is independent from Bronze config (`tests/test_silver_flush_config.py`)
- **UTC Time Handling Standardization** (`heber/writer/*.py`, `heber/catalog/*.py`, `heber/sdk/client.py`)
  - Replaced remaining naive `datetime.utcnow()` calls with timezone-aware `datetime.now(UTC)` across runtime modules
  - Updated Silver flush timing tests for aware UTC datetimes
  - Added regression guard to block new `datetime.utcnow()` usage in `heber/` sources (`tests/test_utcnow_regression.py`)
- **Compactor Atomic Merge Hardening** (`heber/writer/compactor.py`)
  - Switched compaction from all-in-memory concatenate to streamed writes via `ParquetWriter`
  - Compaction now writes to temp files and promotes merged output atomically before removing source files
  - Added per-partition lock-file handling and failure cleanup so failed compactions keep source files intact
  - Added regression tests for successful merge cleanup and failure safety (`tests/test_compactor_safety.py`)
- **Silver Schema Source Consolidation** (`heber/schemas/silver.py`, `heber/writer/silver.py`, `heber/writer/transformer.py`)
  - Moved canonical Silver Arrow schema definitions out of `heber.writer.silver` into shared `heber.schemas.silver`
  - Updated writer and Bronze-to-Silver transformer to consume the shared schema module
  - Added regression tests to enforce single-source schema ownership and block inline schema constant reintroduction (`tests/test_silver_schema_source.py`)
- **Hot Store Event Batching** (`heber/hotstore/sync.py`)
  - Added buffered quote/trade/bar event sync with configurable row and time flush thresholds
  - Replaced one-insert-per-event sync path with threshold-based batched inserts
  - Added best-effort buffer flush on sync loop exit and explicit stop shutdown
  - Added regression tests for threshold-triggered batch inserts and stop-time flush (`tests/test_hotstore_unification.py`)
- **Local Port Default Alignment** (`heber/config.py`, `README.md`, `docs/configuration.md`, `.env.example`)
  - Updated host runtime defaults to match docker-compose exposure (`Postgres: 5433`, `Redis: 6380`)
  - Synced configuration docs and environment template with the same host defaults
  - Extended settings regression coverage for Postgres/Redis defaults (`tests/test_sdk_catalog_defaults.py`)
- **Catalog Migration Baseline + Startup Guard** (`heber/catalog/api.py`, `alembic/*`)
  - Added Alembic migration scaffolding with an initial Catalog baseline revision
  - Catalog API lifespan now applies `Base.metadata.create_all` only in `dev` environment
  - Non-dev environments now skip runtime schema auto-create and are expected to run Alembic migrations
  - Added regression tests for dev/non-dev startup behavior and migration assets (`tests/test_catalog_migrations.py`)
- **Watch Async Redis Non-Blocking Refactor** (`heber/watch/*.py`)
  - Added async wrappers in `WatchManager` for Redis-backed CRUD/update operations used from async loops
  - Watch consumer stream read/ack and watch creation now offload sync Redis/manager calls via `asyncio.to_thread`
  - Snapshot poller now uses async manager wrappers for active-watch fetches, snapshot writes, and price updates
  - Check/write loop now offloads synchronous barrier checks from async context
  - Added regression tests to verify non-blocking async paths (`tests/test_watch_async_redis.py`)
- **Watch Consumer Retry + DLQ Reliability** (`heber/watch/consumer.py`)
  - Added bounded retry/backoff for flow-alert processing before terminal failure handling
  - Added Redis DLQ write path with message metadata for unrecoverable watch-consumer records
  - Updated ACK policy to acknowledge only on successful processing or successful DLQ write
  - Retains pending messages when DLQ write fails, avoiding silent drops
  - Added regression tests for retry count, DLQ routing, and ACK decision behavior (`tests/test_watch_consumer_reliability.py`)
- **Stream Naming Convention Unification** (`heber/bus/__init__.py`, `heber/bus/streams.py`, `heber/watch/consumer.py`)
  - Standardized event-bus stream naming to `heber:events:*` across stream enum values and registry helpers
  - Watch consumer now defaults to `settings.redis_stream_name` instead of hardcoded stream literals
  - Updated operations runbook/troubleshooting Redis commands to use aligned event and DLQ stream keys
  - Added regression coverage for stream naming consistency (`tests/test_stream_naming_conventions.py`)
- **Alert Label Pipeline Bar-Key + Intraday Wiring** (`heber/features/pipelines/alert_labels.py`, `heber/features/templates/alert_labels.py`)
  - Alert-label bar reads now canonicalize equity symbols (`equity:*`) and include legacy raw-key filters for compatibility
  - Intraday labeling now reads from `bars` and filters `timeframe` to 5-minute bars instead of querying stale `bars_5min`
  - Added fallback to daily bars when intraday data is unavailable or timeframe metadata is missing
  - Added regression tests for key normalization, intraday dataset selection, and fallback behavior (`tests/test_alert_labels_pipeline_keys.py`)
- **Intraday Label Window Unit Fix** (`heber/features/templates/alert_labels.py`)
  - Corrected intraday horizon window math to use 5-minute bar durations instead of day-based offsets
  - `ts_available` and SPY-relative return windows now share the same minute-based intraday horizon timing
  - Added regression tests for intraday/daily window duration behavior (`tests/test_alert_label_intraday_windows.py`)
- **Flow Feature Rolling Window Hardening** (`heber/features/templates/flow.py`)
  - Normalized flow `ts_event` values to UTC and dropped invalid timestamps before time-window rolling
  - Added regression checks that 24-hour aggregates are time-windowed (not row-count based)
  - Added regression checks for UTC normalization of string timestamps in flow feature outputs (`heber/features/templates/tests.py`)
- **Lifecycle Async Shutdown Wait Race Fix** (`heber/ops/lifecycle.py`)
  - `async_wait_for_shutdown` now returns immediately when shutdown is already signaled
  - Added race-safe async shutdown-event initialization to prevent hung waits
  - Added regression coverage for pre-signaled and late-signaled async shutdown waits (`tests/test_lifecycle_shutdown_wait.py`)
- **Lifecycle Shutdown Timeout Status Fix** (`heber/ops/lifecycle.py`)
  - Shutdown timeout paths now report `status="timeout"` instead of `status="success"` in lifecycle metrics
  - Sync/async shutdown methods now return `False` when drain timeout occurs and `True` only on successful drain
  - Added regression coverage for sync timeout, async timeout, and successful drain behavior (`tests/test_lifecycle_shutdown_timeout.py`)
- **Structured Logging Level Filtering** (`heber/ops/logging.py`)
  - `configure_logging(log_level=...)` now validates level names and fails fast on invalid values
  - Logging level now applies to both stdlib root logger configuration and structlog filtering wrappers
  - Added regression tests for INFO/DEBUG behavior in JSON and console render modes (`tests/test_logging_level_filtering.py`)
- **Dedupe Bloom Rotation Bounding** (`heber/ops/reliability.py`)
  - `EventDeduplicator` now rotates Bloom filters on a configured interval to bound long-lived false-positive buildup
  - Duplicate checks now include active and previous Bloom windows, so recent duplicates are still caught across a rotation boundary
  - Added regression tests covering in-window duplicate detection and post-rotation aging behavior (`tests/test_event_deduplicator_rotation.py`)
- **lakeFS Storage Namespace Configurability** (`heber/versioning/__init__.py`)
  - Added configurable storage namespace resolution via `LAKEFS_STORAGE_NAMESPACE_BASE` and `LAKEFS_STORAGE_NAMESPACE_TEMPLATE`
  - Repository creation now uses config-driven namespace resolution instead of hardcoded `s3://heber-lakehouse/{repo}`
  - Added regression tests for namespace resolution and repository create-path wiring (`tests/test_lakefs_namespace_config.py`)
- **lakeFS Operation Metrics Coverage** (`heber/versioning/__init__.py`)
  - Added success/error counter and duration histogram instrumentation for `create_tag`, `list_tags`, `merge`, and `diff`
  - Error paths now include repository-resolution and branch-resolution failures for these operations
  - Added regression tests for operation metrics coverage across success and error paths (`tests/test_lakefs_operation_metrics.py`)
- **Kubernetes HPA/Probe Runtime Conformance** (`k8s/base/hpa/*.yaml`, `k8s/base/deployments/*.yaml`)
  - Replaced stale custom HPA pod metrics with CPU/memory resource metrics for catalog/consumer/writer autoscalers
  - Replaced worker HTTP health probes with exec probes that verify expected runtime entrypoints
  - Added regression checks for HPA metric type and worker probe mode (`tests/test_k8s_hpa_probe_conformance.py`)
- **Tracing No-OTEL Decorator Safety** (`heber/ops/tracing.py`)
  - `traced()` now avoids unconditional `SpanKind` access when OpenTelemetry is unavailable
  - No-OpenTelemetry paths now pass `kind=None` to noop tracing context safely
  - Added regression coverage for `@traced` execution with `OTEL_AVAILABLE=False` (`tests/test_tracing_no_otel.py`)
- **Cross-Platform Volume Init Guarding** (`scripts/init_volume.sh`)
  - Added explicit OS/tool checks before invoking macOS-only `dot_clean`
  - Non-macOS and missing-tool paths now emit clear skip messages instead of implicit fallback
  - Added regression checks for explicit platform guards and removal of `dot_clean ... || true` behavior (`tests/test_init_volume_platform_guard.py`)
- **Worker Entrypoint Service Modes** (`heber/backfill/__main__.py`, `heber/writer/hotstore.py`)
  - Added executable `python -m heber.backfill` service entrypoint with backfill API and `/health`/`/ready` routes
  - Added real hotloader CLI runtime for `python -m heber.writer.hotstore` with continuous sync-loop mode and `--once` mode
  - Added regression coverage for entrypoint execution paths and runtime module availability (`tests/test_worker_entrypoint_services.py`, `tests/test_runtime_entrypoints.py`)
- **Metrics Exporter Wiring Alignment** (`heber/ops/metrics.py`, service entrypoints)
  - Added `start_metrics_server_from_env` helper and wired it into catalog, consumer/writer, compactor, hotloader, and backfill entrypoint paths
  - Kept deployment scrape annotations/ports aligned with runtime behavior by ensuring scraped entrypoints start metrics exporters
  - Added regression checks for deployment-to-entrypoint metrics alignment (`tests/test_metrics_exporter_alignment.py`)

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
