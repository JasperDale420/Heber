# Heber Technical Debt Audit

Date: 2026-02-05

This audit is based on static code inspection only. No services were run and no tests or linters were executed.

## Scope

Audit Pass 1 (2026-02-05, files reviewed directly):
- README.md
- CHANGELOG.md
- pyproject.toml
- docker-compose.yml
- Dockerfile
- CLAUDE.md
- codebase.md
- heber/config.py
- heber/models/envelope.py
- heber/sdk/client.py
- heber/writer/consumer.py
- heber/writer/bronze.py
- heber/writer/silver.py
- heber/writer/compactor.py
- heber/writer/hotstore.py
- heber/hotstore/client.py
- heber/hotstore/sync.py
- heber/firewall/asof.py
- heber/catalog/api.py
- heber/catalog/db.py
- tests/test_placeholder.py
- tests/test_edge_cases.py

Audit Pass 2 (2026-02-05, files reviewed directly):
- docs/architecture.md
- docs/operations/backup-dr-runbook.md
- docs/operations/cost-estimates.md
- docs/operations/deployment.md
- docs/operations/monitoring.md
- docs/operations/network-topology.md
- docs/operations/troubleshooting.md
- heber/watch/__init__.py
- heber/watch/__main__.py
- heber/watch/checker.py
- heber/watch/consumer.py
- heber/watch/features.py
- heber/watch/manager.py
- heber/watch/models.py
- heber/watch/poller.py
- heber/watch/writer.py
- heber/ml/datasets.py
- heber/ml/inference.py
- heber/ml/trainer.py
- heber/quality/contracts.py
- heber/quality/soda_scanner.py
- heber/quality/tests.py
- heber/sre/capacity.py
- heber/sre/chaos.py
- heber/sre/error_budget.py
- heber/sre/oncall.py
- heber/sre/runbooks.py
- heber/sre/slo.py
- heber/sre/tests.py
- heber/testing/ci_gates.py
- heber/testing/environments.py
- heber/testing/framework.py
- heber/testing/generators.py
- heber/testing/leakage.py
- heber/testing/performance.py
- heber/testing/tests.py
- heber/testing/tests_framework.py
- heber/storage/iceberg_catalog.py
- heber/storage/iceberg_writer.py
- heber/universe/survivor_bias.py
- heber/universe/tests.py
- heber/catalog/service.py
- heber/catalog/datasources.py
- heber/bus/__init__.py
- heber/bus/backpressure.py
- heber/bus/dedupe.py
- heber/bus/streams.py
- heber/schema/registry_client.py
- heber/schemas/additional.py

Audit Pass 3 (2026-02-05, files reviewed directly):
- features/entities.py
- features/feature_store.yaml
- features/feature_views/__init__.py
- features/feature_views/alert_labels.py
- features/feature_views/flow.py
- features/feature_views/labels.py
- features/feature_views/microstructure.py
- features/feature_views/momentum.py
- features/feature_views/volatility.py
- heber/features/__init__.py
- heber/features/pipelines/__init__.py
- heber/features/pipelines/alert_labels.py
- heber/features/templates/__init__.py
- heber/features/templates/alert_labels.py
- heber/features/templates/cross_asset.py
- heber/features/templates/flow.py
- heber/features/templates/labels.py
- heber/features/templates/microstructure.py
- heber/features/templates/momentum.py
- heber/features/templates/tests.py
- heber/features/templates/volatility.py

Audit Pass 4 (2026-02-05, files reviewed directly):
- heber/ops/__init__.py
- heber/ops/alerting.py
- heber/ops/circuit_breaker.py
- heber/ops/gap_resolutions.py
- heber/ops/health.py
- heber/ops/lifecycle.py
- heber/ops/logging.py
- heber/ops/metrics.py
- heber/ops/reliability.py
- heber/ops/slices.py
- heber/ops/tests_remaining.py
- heber/ops/tracing.py

Audit Pass 5 (2026-02-05, files reviewed directly):
- heber/firewall/__init__.py
- heber/firewall/scd.py
- heber/firewall/validation.py
- heber/firewall/tests.py
- heber/models/__init__.py
- heber/models/silver.py

Audit Pass 6 (2026-02-05, files reviewed directly):
- heber/gold/__init__.py
- heber/gold/label_tests.py
- heber/gold/labels.py
- heber/gold/split_tests.py
- heber/gold/splits.py
- heber/gold/tests.py
- heber/retention/__init__.py

Audit Pass 7 (2026-02-05, files reviewed directly):
- heber/feast/__init__.py
- heber/feast/materialization.py
- heber/feast/tests.py

Audit Pass 8 (2026-02-05, files reviewed directly):
- scripts/backup/clickhouse-backup.sh
- scripts/backup/validate-catalog-backup.sh
- scripts/docker-build.sh
- scripts/docker-push.sh
- scripts/init_volume.sh
- scripts/security-scan.sh
- docs/Alpaca_market_data_endpoints.md
- docs/Alpaca_trading_endpoints.md
- docs/UW_endpoints.md
- docs/catalog_api.md
- docs/configuration.md
- docs/data_contract.md
- docs/hot_store.md
- docs/iceberg_migration.md
- docs/labeling_strategy.md
- docs/schema_registry.md
- docs/schemaaudit.md
- docs/sdk.md

Audit Pass 9 (2026-02-05, files reviewed directly):
- heber/versioning/__init__.py
- heber/calendar/market.py

Audit Pass 10 (2026-02-05, files reviewed directly):
- heber/hotstore/tables.py
- heber/schemas/tests_additional.py

Audit Pass 11 (2026-02-05, files reviewed directly):
- infrastructure/terraform/main.tf
- infrastructure/terraform/environments/dev/main.tf
- infrastructure/terraform/environments/staging/main.tf
- infrastructure/terraform/environments/prod/main.tf
- k8s/base/configmap.yaml
- k8s/base/deployments/backfill.yaml
- k8s/base/deployments/catalog.yaml
- k8s/base/deployments/compactor.yaml
- k8s/base/deployments/consumer.yaml
- k8s/base/deployments/hotloader.yaml
- k8s/base/deployments/writer.yaml
- k8s/base/hpa/catalog.yaml
- k8s/base/hpa/consumer.yaml
- k8s/base/hpa/writer.yaml
- k8s/base/kustomization.yaml
- k8s/base/namespace.yaml
- k8s/base/pdb/catalog.yaml
- k8s/base/pdb/consumer.yaml
- k8s/base/pdb/hotloader.yaml
- k8s/base/pdb/writer.yaml
- k8s/base/secrets/cluster-secret-store.yaml
- k8s/base/secrets/external-secret.yaml
- k8s/base/secrets/secrets-local.yaml.example
- k8s/base/services/catalog.yaml
- k8s/base/services/consumer.yaml
- k8s/base/services/hotloader.yaml
- k8s/base/services/writer.yaml
- k8s/overlays/dev/kustomization.yaml
- k8s/overlays/staging/kustomization.yaml
- k8s/overlays/prod/kustomization.yaml

Audit Pass 12 (2026-02-05, files reviewed directly):
- heber/backfill/__init__.py
- heber/backtest/__init__.py
- heber/backtest/integration.py
- heber/backtest/tests.py

Not yet audited in this run (recommend a future pass):
None.

## Remediation Updates

Updated: 2026-02-05

- `TD-015` addressed via `T-01`: Redis pending claims are consumed instead of dropped.
- `TD-016` addressed via `T-02`: meta-label writer and dataset builder columns are aligned.
- `TD-034` addressed via `T-03`: template `ts_available` values are source-derived.
- `TD-033` addressed via `T-04`: feature view schemas and offline paths align with Gold outputs and include schema/path regression coverage.
- `TD-001` addressed via `T-05`: pytest discovery now includes in-package tests under `heber/` and legacy test file names (`tests.py`, `tests_*.py`).

## Executive Summary

The core architecture is clear, but several operational hazards and correctness gaps remain. The most urgent issues are test discovery (most in-package tests are not being executed), mismatched service ports (SDK defaults do not match docker-compose), invalid Dockerfile targets, inconsistent Hot Store implementations, a broken meta-label training pipeline (label columns and paths do not match), an event-bus claim path that can silently drop messages, and a Feast/feature pipeline mismatch (feature views do not align with Gold layout or computed columns). In ops, tracing is not safe to disable (decorators crash when OpenTelemetry is missing), async shutdown signaling can hang, and deduplication can permanently drop valid events due to unbounded Bloom false positives. In the firewall/models layer, SCD joins can reference missing columns, Gold build validation treats warnings as hard failures, and Silver schemas drift between Pydantic models and Arrow definitions (lineage types, schema versions, and date representations). In the Gold/retention layer, label reads can bypass ts_available if datasets are malformed, version selection is lexicographic, and retention scanning does not align to the Gold layout, so retention/version pruning is likely ineffective. In Feast integration, materialization hides row counts, the default repo path is hardcoded, and search behavior treats `tags` as keys rather than values. In lakeFS versioning and calendar logic, repository creation is hardcoded to a fixed S3 namespace and the calendar assumes tz-aware inputs, which can crash on naive datetimes. In infrastructure manifests, Terraform references missing modules and Kubernetes configs reference images/commands that do not exist in this repo, while HPAs and probes assume metrics/health endpoints that are not implemented. In backfill/backtest, APIs allow unbounded background tasks with no persistence or cancellation signaling, and backtest reproducibility does not capture data as-of cutoffs. Finally, Hot Store DDL and schema tests contain drift: tables omit some schema fields and async DDL creation assumes an async client while other modules use sync clients; schema tests are hardcoded to a count and can drift as schemas evolve. There are also multiple time-handling risks and data pipeline resiliency gaps that could lead to leakage or data loss.

## Findings Summary

Severity key: High, Medium, Low

| ID | Severity | Area | Summary |
| --- | --- | --- | --- |
| TD-001 | High | Testing | Pytest only discovers `tests/`, so in-package tests under `heber/` are not executed. |
| TD-002 | High | SDK/Config | SDK default catalog URL uses port 8080 but docker-compose exposes 8085. |
| TD-003 | High | Docker | Dockerfile stages reference non-existent modules. |
| TD-004 | High | Hot Store | Two divergent Hot Store implementations with inconsistent clients and async behavior. |
| TD-005 | Medium | Silver Writer | Flush interval uses Bronze setting instead of Silver setting. |
| TD-006 | Medium | Time Handling | Widespread use of naive `datetime.utcnow()` despite UTC expectations. |
| TD-007 | Medium | Compaction | Compactor loads all files into memory and deletes originals without atomic swap. |
| TD-008 | Medium | Ingestion | Redis consumer has no DLQ or pending-entries recovery. |
| TD-009 | Medium | Schemas | Schema definitions duplicated and hardcoded in `heber/writer/silver.py`. |
| TD-010 | Medium | Local Dev | Config defaults (Redis/Postgres) do not align with docker-compose host ports. |
| TD-011 | Medium | Hot Store | HotStoreSync inserts one row per event with no batching. |
| TD-012 | Medium | Catalog DB | No Alembic migrations; tables are created at runtime. |
| TD-013 | Low | Validation | Instrument key validation is defined but not enforced in ingestion. |
| TD-014 | Low | Observability | Metrics are mostly placeholders and not wired to a running exporter. |
| TD-015 | High | Event Bus | Claimed pending messages are not yielded to consumers, risking silent drops. |
| TD-016 | High | ML | Meta-label dataset builder expects columns not produced by the label writer. |
| TD-017 | Medium | Watch Service | Uses synchronous Redis calls inside async loops; can block the event loop. |
| TD-018 | Medium | Watch Service | Acks watch-stream messages even when processing fails; no DLQ. |
| TD-019 | Medium | Features | Time-of-day features are computed without timezone conversion (UTC vs ET). |
| TD-020 | Medium | Integration | Data Gateway endpoints are inconsistent across watch/feature codepaths. |
| TD-021 | Medium | ML | Default gold/features paths do not align with configured volume root. |
| TD-022 | Medium | ML | Feature persistence to Gold is not wired; builder expects Parquet that is never written. |
| TD-023 | Medium | ML | Feature ordering for inference is not tied to training feature order. |
| TD-024 | Medium | Data Quality | Soda scanner default Silver path misses `/data` segment. |
| TD-025 | Low | Data Quality | Non-null rate uses hard-coded 0.99 for per-column threshold. |
| TD-026 | Medium | Testing | `tests_framework.py` calls missing `E2ETestSuite.get_schedule()`. |
| TD-027 | Low | Environment | Testing environment defaults (ports/services) diverge from docker-compose. |
| TD-028 | Medium | Iceberg | `create_silver_table` uses a partition spec format that may not match PyIceberg API. |
| TD-029 | Low | Backpressure | Quarantine path reads provider/feed from `envelope.meta` which doesn't exist. |
| TD-030 | Medium | Streams | Stream naming diverges (`stream:*` vs `heber:events`) across code/docs. |
| TD-031 | Low | Watch Models | `created_at`/`updated_at` use naive `datetime.utcnow()` defaults. |
| TD-032 | Low | Watch Poller | Polling interval ignores per-horizon settings and over-polls long horizons. |
| TD-033 | High | Feast/Features | Feature views hardcode paths and schemas that do not match Gold layout or computed columns. |
| TD-034 | High | Features | Feature templates set `ts_available` to current time, breaking point-in-time correctness. |
| TD-035 | Medium | Label Pipeline | Alert label pipeline queries bars using raw symbols, not canonical instrument keys. |
| TD-036 | Medium | Label Pipeline | Pipeline references `bars_5min` dataset that is not defined in Silver schemas. |
| TD-037 | Medium | Label Pipeline | Intraday labeling uses day-based windows for availability and SPY returns. |
| TD-038 | Medium | Flow Features | Time-window rolling uses `on=ts_event` on Series and lacks datetime normalization. |
| TD-039 | Medium | Tracing | Tracing decorator crashes when OpenTelemetry is not installed, despite intended noop behavior. |
| TD-040 | Medium | Lifecycle | Async shutdown wait can hang if shutdown happens before the async event is created. |
| TD-041 | Low | Lifecycle | Shutdown timeouts are logged but still reported as success in metrics. |
| TD-042 | Low | Logging | `configure_logging()` accepts a log level but does not apply it. |
| TD-043 | Medium | Reliability | Bloom filter dedupe has no TTL/rotation, causing rising false positives and potential drops without a backing store. |
| TD-044 | Low | Reliability | In-memory DLQ is non-persistent; failures are lost on restart. |
| TD-045 | Medium | Firewall | SCD join expects suffixed validity columns that may not exist, causing runtime failures. |
| TD-046 | Low | Firewall | Gold build validation treats warning-level violations as hard failures in strict mode. |
| TD-047 | Medium | Models | `lineage` is a dict in Pydantic but a string in Arrow schema; serialization is inconsistent. |
| TD-048 | Low | Models | `schema_version` defaults to `v1` for all models, including v2/v3/v4 datasets. |
| TD-049 | Low | Models | Date fields are inconsistently typed (`date` vs `str`) across Silver schemas. |
| TD-050 | Low | Gold Labels | `read_label()` chooses the “latest” version lexicographically, which can pick the wrong semantic version. |
| TD-051 | Medium | Retention | Retention scanning assumes `dt=` partitions under `<layer>/<dataset>` and does not align with Gold layout/versioning, so Gold retention and version pruning are ineffective. |
| TD-052 | Low | Retention | Retention policies for Hot Store and DLQ are defined but never executed by the reaper. |
| TD-053 | Low | Retention | Retention defaults are hardcoded to `/data/heber` and ignore configured data roots. |
| TD-054 | Medium | Gold Labels | `read_label()` allows datasets without `ts_available` and returns all rows, bypassing point-in-time guards. |
| TD-055 | Low | Retention | Version pruning sorts versions lexicographically instead of semver or creation time. |
| TD-056 | Low | Feast | Default repo path is hardcoded to `features/`, ignoring configured locations. |
| TD-057 | Low | Feast | Materialization returns `-1` counts and does not report actual rows materialized. |
| TD-058 | Low | Feast | `search_features()` treats `tags` as keys and ignores tag values, leading to unexpected matches. |
| TD-059 | Low | Scripts | ClickHouse backup script logs S3 bucket/prefix but never applies them to `clickhouse-backup`. |
| TD-060 | Medium | Scripts | Catalog backup validation can leak the test DB instance when any step fails. |
| TD-061 | Low | Scripts | Volume init script assumes macOS (`dot_clean`) without platform checks. |
| TD-062 | Low | Docs | Labeling docs reference an outdated module path and function signature for split validation. |
| TD-063 | Low | Docs | Data contract claims a Gold layout that doesn’t match the label writer’s on-disk layout. |
| TD-064 | Low | Docs | UW endpoint coverage summary counts conflict with its own tables. |
| TD-065 | Low | Scripts | Security scan does not fail the build on filesystem secrets/misconfig findings. |
| TD-066 | Medium | Versioning | lakeFS repo creation hardcodes S3 namespace (`s3://heber-lakehouse/{repo}`) and ignores config. |
| TD-067 | Low | Versioning | lakeFS metrics are missing for tag/list/diff operations and error paths are not consistently instrumented. |
| TD-068 | Medium | Calendar | Market calendar assumes tz-aware datetimes; naive inputs will raise on `tz_convert`. |
| TD-069 | Low | Calendar | `include_extended` flag is unused; extended hours are never applied. |
| TD-070 | Low | Hot Store | Hot Store DDL omits columns present in Silver schemas (e.g., `quality_flags`, `lineage`). |
| TD-071 | Medium | Hot Store | `create_all_tables()` always awaits `client.execute`, but the primary ClickHouse client is sync. |
| TD-072 | Low | Testing | Additional schema tests assert a fixed schema count, which will break on new schemas. |
| TD-073 | High | Infra | Terraform root module references local modules (`./modules/*`) that are not present. |
| TD-074 | High | K8s | Deployments reference module paths that don’t exist (`heber.bus.consumer`, `heber.writer.service`, `heber.writer.compaction`). |
| TD-075 | Medium | K8s | HPA targets custom metrics that are not exported by current metrics definitions. |
| TD-076 | Medium | K8s | Liveness/readiness probes expect `/health` and `/ready` endpoints on metrics ports that are not implemented. |
| TD-077 | Medium | K8s | Images are referenced as `heber:<service>-latest`, but kustomize only rewrites `heber` image name. |
| TD-078 | Low | K8s | Namespace base is `heber` while overlays change namespace; secrets/configmap names may not be present in each namespace. |
| TD-079 | Low | Infra | Terraform backends and module configs are hardcoded to `us-east-1` without variable override. |
| TD-080 | Medium | Backfill | Backfill writes only Silver temp partitions and never updates Bronze, coverage, or catalog metadata. |
| TD-081 | Medium | Backfill | Backfill jobs are in-memory only; jobs and progress are lost on restart. |
| TD-082 | Medium | Backfill | `BackfillWriter._write_parquet()` silently drops data when `pyarrow` is missing. |
| TD-083 | Low | Backfill | Gap detection assumes `silver/{provider}_{feed}/dt=*` layout, which may not match actual partitions. |
| TD-084 | Low | Backtest | Label reads use `read_gold()` without a version parameter, which may load unintended versions. |
| TD-085 | Low | Backtest | Experiment results omit dataset as-of timestamps, weakening reproducibility. |

## Detailed Findings

**TD-001: Pytest only discovers `tests/`, so in-package tests are not executed.**
Evidence: `pyproject.toml` restricts `testpaths` to `tests`, while there are multiple test modules under `heber/` such as `heber/catalog/tests_access_control.py` and `heber/ops/tests_remaining.py`. This means those tests never run in CI or locally by default. This hides regressions in critical logic.
Recommendation: Expand `testpaths` to include `heber` or move internal tests into `tests/` with a consistent naming convention. Add a minimal CI check that confirms expected test modules are collected.

**TD-002: SDK default catalog URL uses port 8080 but docker-compose exposes 8085.**
Evidence: `heber/config.py` defaults `api_port` to 8080 and `heber/sdk/client.py` uses `settings.api_port`. `docker-compose.yml` maps catalog 8080 to host 8085 and `README.md` advertises 8085. This mismatch breaks the default SDK in a typical docker-compose local setup.
Recommendation: Add `HEBER_CATALOG_URL` and use it as the SDK default. Align README and `.env.example` with the new behavior.

**TD-003: Dockerfile stages reference non-existent modules.**
Evidence: `Dockerfile` stages reference `heber.bus.consumer`, `heber.writer.service`, and `heber.writer.compaction`, which are not present. This makes those build targets fail and creates confusion for deployment.
Recommendation: Fix stage commands to point to existing modules (`heber.writer.consumer`, `heber.writer.compactor`) or remove the stages entirely if unused.

**TD-004: Hot Store has two divergent implementations with inconsistent clients and async behavior.**
Evidence: `heber/hotstore/client.py` uses `clickhouse-connect` synchronously. `heber/writer/hotstore.py` uses `clickhouse_driver` and defines async methods that `await` a non-async client. `heber/hotstore/sync.py` uses a third approach with direct inserts. This creates runtime inconsistency and unclear ownership of the Hot Store pipeline.
Recommendation: Pick one Hot Store implementation, unify on a single ClickHouse client, and enforce a consistent async model. Deprecate or delete the unused path.

**TD-005: Silver writer flush interval uses Bronze setting.**
Evidence: `heber/writer/silver.py` uses `settings.bronze_flush_interval_seconds` inside `flush_if_needed`. This is likely a bug and creates unexpected flush timing for Silver files.
Recommendation: Replace with `settings.silver_max_flush_time_seconds` and add a regression test around flush timing.

**TD-006: Naive time handling risks incorrect UTC semantics.**
Evidence: `datetime.utcnow()` is used in `heber/writer/bronze.py`, `heber/writer/silver.py`, `heber/writer/compactor.py`, and `heber/catalog/api.py`. These values are naive (no timezone). Some schemas expect `timestamp('us', tz='UTC')` which will mismatch or silently coerce.
Recommendation: Replace with `datetime.now(UTC)` and enforce timezone-aware timestamps in `EventEnvelope` validation. Add a guard that rejects naive datetimes for `ts_event`, `ts_ingest`, and `ts_available`.

**TD-007: Compactor loads all small files into memory and deletes originals without atomic swap.**
Evidence: `heber/writer/compactor.py` reads all parquet files into memory, concatenates, writes a new file, and deletes originals immediately. A crash between write and delete will corrupt data or lose lineage.
Recommendation: Use streaming compaction or chunked merges, write to a temp file, and replace atomically. Consider using a manifest or lock file.

**TD-008: Redis consumer has no DLQ or pending-entry recovery.**
Evidence: `heber/writer/consumer.py` logs failed events but does not push to a dead-letter stream or attempt to claim pending entries on startup. Failed events can remain stuck in the group without visibility.
Recommendation: Add a DLQ stream, implement a retry/backoff policy, and add a startup sweep of `XPENDING`/`XCLAIM` for stale messages.

**TD-009: Schema definitions are duplicated and hardcoded in `heber/writer/silver.py`.**
Evidence: `SILVER_SCHEMAS` is a large in-code dictionary while schema information also exists in docs and likely in `heber/schemas/`. This will drift over time and complicates schema evolution.
Recommendation: Move schemas into a single source of truth (JSON/YAML or a schema registry client) and generate both Arrow schemas and documentation from it.

**TD-010: Local dev ports are inconsistent between config and docker-compose.**
Evidence: `docker-compose.yml` exposes Postgres on 5433 and Redis on 6380, but `heber/config.py` defaults to 5432 and 6379. This breaks local usage from host processes unless the user manually sets env vars.
Recommendation: Align defaults with compose host ports or explicitly document and populate `.env.example` with correct values for local dev.

**TD-011: HotStoreSync inserts one row per event without batching.**
Evidence: `heber/hotstore/sync.py` calls `client.insert` per event. This will be slow under load and could violate the 5-minute SLA.
Recommendation: Buffer and batch inserts in the sync path and add a flush policy similar to the Silver writer.

**TD-012: Catalog DB has no migration strategy.**
Evidence: `heber/catalog/api.py` calls `Base.metadata.create_all` on startup and there are no Alembic migrations in the repo. This is brittle for production schema changes.
Recommendation: Add Alembic migrations and remove auto-create for non-dev environments.

**TD-013: Instrument key validation is defined but not enforced.**
Evidence: `validate_instrument_key` exists in `heber/models/envelope.py`, but `EventConsumer` does not enforce it. Invalid instrument keys can silently flow into Silver.
Recommendation: Validate in the consumer and attach a quality flag or reject invalid events.

**TD-014: Observability is partially stubbed.**
Evidence: Several modules reference metrics counters but there is no wiring to a metrics server or standard export path. Some modules have metrics placeholders and log-only paths.
Recommendation: Add a consistent metrics export strategy, likely via Prometheus, and register metrics in service entrypoints.

**TD-015: Claimed pending messages are not yielded to consumers (possible silent drops).**
Evidence: `heber/bus/__init__.py` calls `_claim_idle_messages()` inside `RedisEventBus.consume()` but discards the returned messages. Claimed messages are never processed, yet are removed from other consumers.
Recommendation: Yield claimed messages in the consume loop or merge them into the next batch. Add an integration test that simulates XPENDING/XCLAIM behavior.

**TD-016: Meta-label dataset builder expects columns not produced by the label writer.**
Evidence: `heber/ml/datasets.py` expects `outcome` and `hit_tp_first`, but `heber/watch/checker.py`/`outcome_to_label_row()` writes `outcome_reason` and `contract_hit_tp_first`. Joins and labeling will be empty or incorrect.
Recommendation: Align the label writer output with the dataset builder expectations (or vice versa) and add a small end-to-end test from watch outcome → dataset build.

**TD-017: Watch service uses synchronous Redis calls inside async loops.**
Evidence: `heber/watch/consumer.py` and `heber/watch/manager.py` use sync `redis` clients inside `async def` loops. This can block the event loop and stall poller/checker tasks.
Recommendation: Switch to `redis.asyncio` for the watch service or run sync calls in a thread executor. Add backpressure metrics for watch processing latency.

**TD-018: Watch consumer acknowledges messages even when processing fails; no DLQ.**
Evidence: `heber/watch/consumer.py` unconditionally XACKs each message after `_process_alert`, even if parsing/processing failed. Failed alerts are silently dropped and no DLQ exists for the watch stream.
Recommendation: Only ack on success, and add a DLQ stream for watch failures with retry/backoff similar to `heber/bus/backpressure.py`.

**TD-019: Time-of-day features are computed without timezone conversion.**
Evidence: `heber/watch/features.py` uses `alert.ts_event.hour` and assumes ET market hours. If `ts_event` is UTC (likely), time features are wrong.
Recommendation: Normalize timestamps to a market timezone before feature extraction (e.g., convert UTC to US/Eastern) and document the assumption.

**TD-020: Data Gateway endpoint paths are inconsistent.**
Evidence: `SnapshotPoller` uses `/api/v1/alpaca/options/quotes`, while feature enrichment uses `/alpaca/...` and `/uw/...` paths. These may not exist on the same gateway.
Recommendation: Centralize Data Gateway base paths and versioning in config and align all callers.

**TD-021: Meta-label builder defaults to `/tmp/heber/gold` instead of configured data root.**
Evidence: `heber/ml/datasets.py` default paths point to `/tmp/heber/gold`, while the system’s data root is `/Volumes/heber/data` via `Settings`.
Recommendation: Use `HEBER_DATA_ROOT` or `settings.gold_path` and ensure the builder follows environment configuration.

**TD-022: Feature persistence to Gold is not wired; builder expects Parquet that is never written.**
Evidence: Features are stored in Redis (`heber/watch/features.py`) but no job writes features to Gold; `MetaLabelDatasetBuilder` expects Parquet under `meta_labels/features`.
Recommendation: Add a persistence step that writes features to Gold (or update the builder to read from Redis + outcomes).

**TD-023: Inference feature ordering is not tied to training feature order.**
Evidence: Training uses `MetaLabelDatasetBuilder.get_feature_columns()` (dataframe column order), while inference uses `AlertFeatures.numeric_feature_names()` (fixed order). These can diverge.
Recommendation: Persist the feature name order with the model (e.g., in model metadata) and enforce the same order at inference.

**TD-024: Soda scanner default Silver path is likely wrong.**
Evidence: `SodaConfig.silver_path` defaults to `/Volumes/heber/silver` but the data root is `/Volumes/heber/data/silver`.
Recommendation: Default to `settings.silver_path` or set `HEBER_SILVER_PATH` in `.env.example`.

**TD-025: Non-null rate uses a hard-coded 0.99 threshold for per-column reporting.**
Evidence: `DataQualityValidator.check_non_null_rate()` flags columns below 0.99 regardless of the contract threshold. This can produce false violations when contract thresholds differ.
Recommendation: Use the contract threshold when identifying columns below threshold.

**TD-026: `tests_framework.py` references a missing method.**
Evidence: `TestE2ETestSuite.test_get_schedule()` calls `E2ETestSuite.get_schedule()` but the method does not exist in `heber/testing/framework.py`.
Recommendation: Implement `get_schedule()` or remove the test to keep test suites consistent.

**TD-027: Testing environment defaults diverge from docker-compose.**
Evidence: `heber/testing/environments.py` defines local services on ports 5432/6379, while `docker-compose.yml` uses 5433/6380. This causes confusion in local testing guidance.
Recommendation: Align testing defaults with compose ports or document the difference.

**TD-028: Iceberg partition spec format may not match PyIceberg API.**
Evidence: `create_silver_table()` passes a list to `partition_spec`. PyIceberg typically expects a `PartitionSpec` object; this may break at runtime.
Recommendation: Use `PartitionSpec` builders from PyIceberg and add a smoke test that initializes tables.

**TD-029: Quarantine paths read provider/feed from `envelope.meta`.**
Evidence: `heber/bus/backpressure.py` expects `envelope["meta"]["provider"]`/`["feed"]`, but `EventEnvelope` stores `provider` and `feed` at the top level.
Recommendation: Use top-level fields or normalize envelope format before quarantine writes.

**TD-030: Stream naming diverges across modules and docs.**
Evidence: `heber/bus` uses `stream:*` names, writer/consumer uses `heber:events`, and ops docs reference `stream:market.bars`. This split-brain naming leads to non-wired components.
Recommendation: Standardize on one stream naming convention and update docs, bus config, and consumers together.

**TD-031: Watch model timestamps use naive `datetime.utcnow()` defaults.**
Evidence: `heber/watch/models.py` sets `created_at` and `updated_at` with `datetime.utcnow()` (naive), while other parts expect timezone-aware UTC.
Recommendation: Use `datetime.now(UTC)` or enforce timezone-aware defaults across models.

**TD-032: Watch poller ignores per-horizon intervals.**
Evidence: `SnapshotPoller.run()` uses the minimum interval from `POLL_CONFIG` for all watches, which over-polls swing/LEAP horizons.
Recommendation: Respect per-watch polling intervals or schedule per-horizon polling loops.

**TD-033: Feast feature views hardcode paths and schemas that do not match Gold layout or computed columns.**
Evidence: Feature views in `features/feature_views/*` point to `/data/gold/dataset=.../type=...` while the Gold layout is `dataset/project/version/dt`. Several schemas do not match template outputs (e.g., `microstructure.py` view expects `bid_ask_spread_pct` and `quoted_depth_*` while templates output `spread_bps` and `bid_depth`/`ask_depth`; `flow.py` view expects `net_call_premium` but templates output `call_premium_24h`; `volatility.py` view expects `bollinger_upper/lower` but templates output `bb_width_20`). Feast registry paths are also hardcoded to `/data/feast/...`.
Recommendation: Make paths configurable via settings/env, and align feature view schemas to the actual Gold outputs (or update templates/pipelines to produce the expected columns). Add a schema/contract test that validates view fields exist in a sample Gold file.

**TD-034: Feature templates set `ts_available` to the current time.**
Evidence: `compute_momentum_features`, `compute_volatility_features`, `compute_microstructure_features`, `compute_flow_features`, and `compute_relative_features` set `ts_available = pd.Timestamp.now(tz="UTC")`, which is not tied to source data availability.
Recommendation: Derive `ts_available` from input data (e.g., max of input `ts_available` or `ts_event` + processing lag) to preserve point-in-time correctness.

**TD-035: Alert labels pipeline queries bars using raw symbols, not canonical instrument keys.**
Evidence: `AlertLabelsPipeline._load_bars()` passes `instrument_keys=symbols` where symbols are `["AAPL", "SPY", ...]`, but Silver bars use canonical keys like `equity:AAPL`.
Recommendation: Map symbols to canonical instrument keys (prefix with `equity:` or use `HeberClient.resolve_instrument`) before querying Silver.

**TD-036: Alert labels pipeline references a non-existent dataset.**
Evidence: `_load_intraday_bars()` queries dataset `bars_5min`, which is not present in Silver schemas or writer outputs.
Recommendation: Either implement `bars_5min` ingestion or use existing bars with a timeframe column/filter.

**TD-037: Intraday label windows are computed in days, not minutes.**
Evidence: In `alert_labels.py`, `ts_available` uses `timedelta(days=max_window_bars // 24)` and SPY-relative returns use `timedelta(days=max_window_bars)`. For intraday horizons (24 five-minute bars), this becomes 1–24 days instead of ~2 hours.
Recommendation: Track bar duration explicitly and compute windows in minutes/hours for intraday labels.

**TD-038: Flow feature rolling windows may be incorrect.**
Evidence: `compute_flow_features()` uses `df["premium"].rolling(..., on="ts_event")` on a Series (the `on` parameter is ignored or invalid for Series) and does not normalize `ts_event` to datetime.
Recommendation: Convert `ts_event` to datetime and use DataFrame-level rolling with `on=ts_event`, or set a DatetimeIndex for correct time-window rolling.

**TD-039: Tracing decorator crashes when OpenTelemetry is not installed.**
Evidence: In `heber/ops/tracing.py`, the `traced()` decorator sets `span_kind = SpanKind.INTERNAL` before checking `OTEL_AVAILABLE`. When OpenTelemetry is missing, `SpanKind` is undefined and any call to a `@traced` function raises `NameError`, despite the `_NoopTracer` fallback.
Recommendation: Guard `SpanKind` usage behind `OTEL_AVAILABLE` and default to `None` for noop tracing, or define a safe fallback enum when OpenTelemetry is not installed.

**TD-040: Async shutdown wait can hang if shutdown is signaled early.**
Evidence: `LifecycleManager.initiate_shutdown()` sets `_async_shutdown_event` only if it already exists. If shutdown happens before `async_wait_for_shutdown()` is called, a new event is created and awaited forever even though shutdown already occurred.
Recommendation: In `async_wait_for_shutdown()`, return immediately if `self._shutdown_event.is_set()` or create `_async_shutdown_event` and set it when shutdown is already in progress.

**TD-041: Shutdown timeouts are logged but still reported as success.**
Evidence: `execute_shutdown()` logs `drain_timeout` when the deadline passes but still increments `shutdown_completed` with status `success` and returns True.
Recommendation: Increment `shutdown_completed` with `status="timeout"` and return False (or a distinct status) when draining exceeds the configured deadline.

**TD-042: `configure_logging()` accepts a log level but does not apply it.**
Evidence: `configure_logging()` has a `log_level` argument but does not set stdlib logging levels or apply it to structlog. This results in no effective filtering.
Recommendation: Wire log level into Python `logging` configuration (or structlog filtering) and document expected values.

**TD-043: Bloom filter deduplication has no TTL/rotation.**
Evidence: `EventDeduplicator` uses a Bloom filter that grows in false-positive rate over time. When no backing store is configured, Bloom matches are treated as hard duplicates, which will drop valid events increasingly as the filter saturates.
Recommendation: Add time-based rotation (rolling Bloom filters), a TTL backing store, or a periodic reset strategy. If no backing store is configured, consider treating Bloom matches as “suspect” instead of hard duplicates.

**TD-044: In-memory DLQ is non-persistent.**
Evidence: `DeadLetterQueue` stores failed events in a process-local list. On restart, all queued failures are lost, and there is no disk or stream persistence.
Recommendation: Back the DLQ with Redis/stream storage or write to disk and implement a replay path.

**TD-045: SCD join expects suffixed validity columns that may not exist.**
Evidence: `join_with_reference_asof()` always filters on `valid_from{suffix}`/`valid_to{suffix}`. Polars only applies suffixes on name collisions; if the left table does not have `valid_from` or `valid_to`, the reference columns are unsuffixed and the filter will fail with missing columns.
Recommendation: Normalize reference validity columns before the join (e.g., rename to fixed names) or detect whether suffixing occurred and use the correct column names.

**TD-046: Gold build validation treats warning-level violations as hard failures.**
Evidence: `validate_gold_build()` labels the `max_ts_event_used > max_ts_available_used` check as a warning, but appends it to `violations` and raises when `strict=True`.
Recommendation: Separate warnings from hard violations or only raise for the strict gates.

**TD-047: `lineage` type mismatch between models and Arrow schema.**
Evidence: `SilverBase.lineage` is `dict[str, Any] | None` but `SILVER_BASE_SCHEMA` defines `lineage` as `pa.string()` with “JSON serialized” comment. This mismatch creates inconsistent serialization expectations across ingestion and writing.
Recommendation: Standardize lineage as a structured type (e.g., JSON/struct) and enforce serialization in one place, or update the Pydantic model to store serialized JSON consistently.

**TD-048: `schema_version` defaults to `v1` across all models.**
Evidence: `SilverBase.schema_version` defaults to `v1` even for v2/v3/v4 datasets (news, filings, alternative data). Unless overridden at write time, stored rows will be mislabeled.
Recommendation: Set per-dataset defaults in each model or enforce schema_version injection in the writer based on dataset.

**TD-049: Date fields are inconsistently typed across Silver schemas.**
Evidence: Some models use `date` (e.g., `expiry` in options), while others use `str` for dates (e.g., `expiry` in `MaxPainRecord`, `HottestChainRecord`, `IVTermStructureRecord`). This leads to inconsistent parsing and schema drift across datasets.
Recommendation: Standardize date representation (prefer `date`/`datetime`) and enforce normalization in ingestion.

**TD-050: `read_label()` picks latest version by lexicographic sort.**
Evidence: `read_label()` sorts `version=` directories by name and picks the highest string. Versions like `v1.10.0` will sort before `v1.2.0`, yielding an older dataset as “latest.”
Recommendation: Parse semantic versions or use metadata (created_at) to choose the newest version.

**TD-051: Retention scanning does not match Gold layout.**
Evidence: `ReaperWorker.scan_partitions()` expects partitions under `<storage_root>/<layer>/<dataset>/dt=*` and never populates `PartitionInfo.version`. Gold datasets are written as `dataset=.../type=.../version=...` (and labels have no `dt=`), so Gold partitions are never discovered and version pruning cannot work.
Recommendation: Implement Gold-specific scanning that walks `dataset=.../type=.../version=...` and records version identifiers and optional `dt` partitions.

**TD-052: Hot Store and DLQ retention policies are never applied.**
Evidence: `ReaperScheduler._process_dataset()` only evaluates Bronze, Silver, and Gold layers; `HOT_STORE` and `DLQ` are defined but ignored.
Recommendation: Include Hot Store and DLQ layers in the reaper or remove the unused policies to avoid false safety assumptions.

**TD-053: Retention uses hardcoded storage root defaults.**
Evidence: `DEFAULT_STORAGE_ROOT = "/data/heber"` and `create_reaper()` default paths do not reference `Settings` or `HEBER_DATA_ROOT`, while other parts of the system use `/Volumes/heber/data`.
Recommendation: Wire retention defaults to the shared configuration and document expected paths.

**TD-054: `read_label()` bypasses ts_available guard if column is missing.**
Evidence: `read_label()` only filters by `ts_available` when the column exists. A malformed or externally-written label dataset without `ts_available` will return all rows, including future data.
Recommendation: Require `ts_available` for label datasets (raise or warn) and fail closed in training contexts.

**TD-055: Retention version pruning uses lexicographic ordering.**
Evidence: `find_expired_versions()` sorts version keys as strings. This can delete or keep the wrong versions for semver patterns.
Recommendation: Parse semantic versions or use explicit creation timestamps to decide which versions to retain.

**TD-056: Default Feast repo path is hardcoded.**
Evidence: `DEFAULT_REPO_PATH = "features/"` and the helpers default to that location, ignoring any configured environment or settings for the repo path.
Recommendation: Allow repo path to be set via config/env (e.g., `HEBER_FEAST_REPO_PATH`) and use that as the default.

**TD-057: Materialization does not report row counts.**
Evidence: `materialize_features()` returns `-1` for each view and does not surface actual row counts, making monitoring or alerting on materialization health impossible.
Recommendation: Capture row counts from Feast logs/metrics or implement a lightweight count query after materialization where feasible.

**TD-058: `search_features()` matches tags by key only.**
Evidence: `search_features()` checks `t in view_tags` where `view_tags` is a dict, so it only matches tag keys, not values. This can miss intended matches or produce false positives.
Recommendation: Support key:value tag filters or compare against values explicitly.

**TD-059: ClickHouse backup script logs S3 bucket/prefix but doesn’t enforce them.**
Evidence: `scripts/backup/clickhouse-backup.sh` defines `S3_BUCKET` and `S3_PREFIX` but never passes them to `clickhouse-backup`. The printed S3 path may not match the actual upload destination, which is controlled by clickhouse-backup’s own config.
Recommendation: Pass bucket/prefix via the clickhouse-backup config/env or remove the misleading output.

**TD-060: Catalog backup validation can leak the test DB instance on failure.**
Evidence: `validate-catalog-backup.sh` uses `set -euo pipefail`, so if restore or validation queries fail, the cleanup section that deletes the test instance is skipped. This can leave `heber-catalog-backup-test` running indefinitely.
Recommendation: Add a `trap` to ensure cleanup on exit and capture/handle validation failures before teardown.

**TD-061: Volume init script assumes macOS tooling.**
Evidence: `scripts/init_volume.sh` calls `dot_clean` unconditionally, which is macOS-only. On Linux, the script fails even if directory creation succeeded.
Recommendation: Guard `dot_clean` behind an OS/tool check or provide a no-op fallback for non-macOS hosts.

**TD-062: Labeling docs reference outdated API location/signature.**
Evidence: `docs/labeling_strategy.md` points to `heber/firewall/splits.py` and shows a `validate_train_test_split` signature that does not exist; the current function lives in `heber/firewall/validation.py` with different parameters.
Recommendation: Update the docs to match the current module path and function signature.

**TD-063: Data contract Gold layout doesn’t match label writer layout.**
Evidence: `docs/data_contract.md` states Gold is partitioned as `dataset/project/version/dt`, but label writes use `dataset={name}/type=label/version={version}` without `project` or `dt`.
Recommendation: Align docs with actual Gold write layout, or update the writer to match the documented layout.

**TD-064: UW endpoint coverage summary conflicts with its own tables.**
Evidence: `docs/UW_endpoints.md` summary says “Complete (11)” while the tables above list many more endpoints as ✅. This makes the summary unreliable.
Recommendation: Recompute totals automatically or remove summary counts to avoid drift.

**TD-065: Security scan doesn’t fail on filesystem findings.**
Evidence: `scripts/security-scan.sh` runs `trivy fs` without `--exit-code`, so secrets/misconfig findings do not fail the script.
Recommendation: Add `--exit-code 1` and optionally `--severity` to make failures actionable in CI.

**TD-066: lakeFS repo creation hardcodes the storage namespace.**
Evidence: `LakeFSVersionManager._get_repo()` always creates repositories with `storage_namespace="s3://heber-lakehouse/{repo}"`, ignoring environment or configuration (e.g., MinIO, different bucket, or lakeFS defaults).
Recommendation: Add a configurable storage namespace (e.g., `LAKEFS_STORAGE_NAMESPACE`) and use it when creating repositories.

**TD-067: lakeFS metrics coverage is incomplete.**
Evidence: Metrics are emitted for `create_branch` and `commit`, but not for `create_tag`, `list_tags`, `diff`, or `merge` error paths. This makes operational monitoring partial and inconsistent.
Recommendation: Instrument all lakeFS operations (success/failure/duration) consistently.

**TD-068: Market calendar crashes on naive datetimes.**
Evidence: `MarketCalendar` calls `pd.Timestamp(dt).tz_convert(ET)` in multiple methods. If `dt` is naive (no timezone), pandas raises. Callers may pass naive datetimes (common in this repo).
Recommendation: Normalize inputs by assuming UTC when tzinfo is missing (or require tz-aware inputs and validate early with a clear error).

**TD-069: `include_extended` flag is unused.**
Evidence: `MarketCalendar.include_extended` is stored but never used to expand the trading session to include pre/post-market. Methods always rely on the default exchange calendar schedule.
Recommendation: Either wire in extended hours support or remove the flag to avoid misleading behavior.

**TD-070: Hot Store DDL omits some base columns.**
Evidence: `heber/hotstore/tables.py` defines Hot Store tables without `quality_flags` or `lineage` columns that exist in Silver base schema. This prevents storing provenance/quality flags in Hot Store and creates schema drift.
Recommendation: Decide which base columns must be preserved in Hot Store and add them (or document the intentional omission).

**TD-071: Hot Store DDL creation assumes async client.**
Evidence: `create_all_tables()` is `async` and calls `await client.execute(stmt)`, but the repo’s primary ClickHouse client (`clickhouse_connect`) is synchronous. This mismatch can lead to runtime errors depending on which client is passed.
Recommendation: Provide separate sync/async helpers or normalize on a single client and call pattern.

**TD-072: Additional schema tests hardcode the schema count.**
Evidence: `tests_additional.py` asserts `len(schemas) == 16`. As new schemas are added, the test will fail even if behavior is correct.
Recommendation: Assert on minimum required schemas or specific known names rather than total count.

**TD-073: Terraform references modules that are missing from the repo.**
Evidence: `infrastructure/terraform/main.tf` references `./modules/vpc`, `./modules/s3`, `./modules/rds`, etc., but there is no `modules/` directory under `infrastructure/terraform/`. Terraform will fail at init/plan.
Recommendation: Add the modules or replace with a remote module source. If infrastructure is managed elsewhere, remove or clearly mark these files as placeholders.

**TD-074: Kubernetes deployments reference non-existent module entrypoints.**
Evidence: `k8s/base/deployments/consumer.yaml` runs `python -m heber.bus.consumer`, and writer/compactor run `heber.writer.service` and `heber.writer.compaction`. These module paths do not exist in the repo (writer is `consumer.py`/`compactor.py`).
Recommendation: Update commands to valid module paths (e.g., `heber.writer.consumer`, `heber.writer.compactor`) and verify entrypoints.

**TD-075: HPA targets custom metrics that are not exported.**
Evidence: HPAs reference `heber_consumer_lag_seconds`, `heber_writer_pending_batch_rows`, and `heber_catalog_request_latency_p99_seconds`. Only `heber_consumer_lag_seconds` exists in `ops/metrics.py`, and the other two metrics are not defined.
Recommendation: Export the needed metrics or change the HPA configuration to CPU/memory scaling or existing metrics.

**TD-076: Probes target endpoints that are not implemented.**
Evidence: Deployments probe `/health` and `/ready` on the metrics port for consumer/writer/compactor/hotloader. Those services do not expose HTTP health endpoints in the codebase.
Recommendation: Add health endpoints or update probes to use a TCP or exec check, or to an actual HTTP server if one exists.

**TD-077: Image references do not align with kustomize image rewrite.**
Evidence: Deployments use images like `heber:writer-latest` and `heber:consumer-latest`. Kustomize rewrites only `name: heber` to `ghcr.io/jacobmcmillan/heber`, which will not match those images.
Recommendation: Use a consistent image name (e.g., `ghcr.io/jacobmcmillan/heber:<tag>`) with distinct tags per component, or update kustomize image rules to match the actual names.

**TD-078: Namespace/secret references may drift across overlays.**
Evidence: Base namespace is `heber` and secrets are referenced by name `heber-secrets`. Overlays change namespace but do not include secrets or ServiceAccount manifests, so deployments may reference missing secrets unless applied separately.
Recommendation: Add namespace-scoped secrets/serviceaccounts per overlay or document required prerequisites in deployment steps.

**TD-079: Terraform environment settings are hardcoded.**
Evidence: Each env `main.tf` pins `region = "us-east-1"` and backend config is fixed. This makes multi-region deployment or account reuse harder.
Recommendation: Parameterize region and backend settings via variables or separate workspace configs.

**TD-080: Backfill does not update Bronze or Catalog metadata.**
Evidence: `BackfillWriter.write_batch()` writes only to Silver temp partitions and logs that compactor will merge. It does not write Bronze, nor does it update catalog coverage or schema metadata.
Recommendation: Add an explicit Bronze write path (or document why it’s skipped), and update catalog coverage once backfill completes.

**TD-081: Backfill jobs are in-memory only.**
Evidence: `BackfillCoordinator` stores jobs in a process-local dict. On restart, in-flight jobs and progress are lost; the API is described as in-memory only in docs.
Recommendation: Persist backfill state in the catalog DB or Redis and add resume/retry support.

**TD-082: Missing `pyarrow` silently drops backfill writes.**
Evidence: `_write_parquet()` catches `ImportError` and logs `pyarrow_not_available` but does not raise, so the backfill job continues and reports progress even though nothing was written.
Recommendation: Fail fast when `pyarrow` is missing, or track a failed write and mark the job as failed.

**TD-083: Gap detection assumes a storage layout that may not exist.**
Evidence: `GapDetector.detect_gaps()` reads `silver/{provider}_{feed}/dt=*`, while other components use feed/instrument_type/dt or dataset-based layouts. This can incorrectly report full gaps.
Recommendation: Align gap detection with actual Silver partition layout and/or use the Catalog to discover coverage.

**TD-084: Backtest labels use `read_gold()` without a version.**
Evidence: `BacktestDataLoader` passes `label_dataset` into `read_gold()` without specifying `version`. If the label dataset is versioned, this may read an unintended or incompatible version.
Recommendation: Add a label version parameter (or reuse `label_version`) and pass it to `read_gold()`.

**TD-085: Backtest reproducibility omits data as-of cutoffs.**
Evidence: `ExperimentConfig` and results capture dataset names and versions but do not persist the as-of timestamp used for feature/label reads, which is critical for reproducibility.
Recommendation: Record `asof_time` per split or overall experiment in the config/results metadata.

## Suggested Remediation Plan

Phase 1 (Stabilize correctness, 1-2 days):
- Fix TD-001, TD-002, TD-003, TD-005, TD-015, TD-016, TD-033, TD-034, TD-039, TD-045, TD-054, TD-074.
- Add minimal regression tests for Silver flush and SDK default URL.

Phase 2 (Operational reliability, 2-4 days):
- Fix TD-006, TD-008, TD-010, TD-012, TD-017, TD-018, TD-030, TD-035..TD-038, TD-040..TD-043, TD-046..TD-049, TD-051, TD-056..TD-058, TD-060, TD-066, TD-068, TD-071, TD-075, TD-076, TD-081, TD-082.
- Add a DLQ stream and pending-entries recovery policy.

Phase 3 (Performance and maintainability, 3-7 days):
- Address TD-004, TD-007, TD-009, TD-011, TD-014, TD-019..TD-029, TD-031..TD-032, TD-044, TD-050, TD-052..TD-053, TD-055, TD-059, TD-061..TD-065, TD-067, TD-069, TD-070, TD-072, TD-073, TD-077..TD-079, TD-080, TD-083..TD-085.
- Unify Hot Store implementation and schema definitions.

## Open Questions for Future Audits

- How is schema evolution governed and enforced in production?
- What are the SLAs and current performance baselines for ingestion and Hot Store?
- Are there existing CI checks on GitHub Actions beyond linting and tests?
