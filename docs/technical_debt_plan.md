# Technical Debt Remediation Plan (High Severity)

Date: 2026-02-05

This plan converts high-severity audit items into ticket-ready tasks with clear scope and acceptance criteria. It references the audit IDs in `docs/technical_debt_audit.md`.

## Implementation Status

Updated: 2026-02-05

- `T-01` complete (`TD-015`): event-bus claimed pending messages are now yielded to consumers.
- `T-02` complete (`TD-016`): watch outcome writer and dataset builder now use aligned canonical outcome columns.
- `T-03` complete (`TD-034`): feature templates derive `ts_available` from source availability.
- `T-04` complete (`TD-033`): Feast feature views now align with template outputs and Gold `dataset/project/version/dt` layout, with schema/path regression tests.
- `T-05` complete (`TD-001`): pytest now discovers tests under both `tests/` and `heber/`, including files named `tests.py` and `tests_*.py`.
- `T-06` complete (`TD-003`, `TD-074`): Docker and Kubernetes entrypoints now reference existing runtime modules (`heber.writer.consumer`, `heber.writer.compactor`), with regression checks.
- `T-07` complete (`TD-073`): added local Terraform module scaffolds (`vpc`, `s3`, `rds`, `elasticache`, `ecr`, `eks`) so root references resolve; added regression checks for module sources.
- `T-08` complete (`TD-002`): SDK now defaults to `HEBER_CATALOG_URL` (`http://localhost:8085/api/v1`) to match docker-compose host exposure; docs and regression tests updated.
- `T-09` complete (`TD-004`, `TD-071`): unified Hot Store sync/write logic under `heber.hotstore.sync` with `clickhouse-connect`; legacy `heber.writer.hotstore` now re-exports the unified path.
- `T-10` complete (`TD-008`): writer consumer now retries failures, claims idle pending messages on startup, and dead-letters unrecoverable messages to a Redis DLQ stream.
- `T-11` complete (`TD-005`): Silver writer flush cadence now correctly uses `silver_max_flush_time_seconds` instead of Bronze flush settings, with regression tests.
- `T-12` complete (`TD-006`): replaced naive `datetime.utcnow()` usage across `heber/` runtime modules with timezone-aware `datetime.now(UTC)`, with regression coverage to prevent reintroduction.
- `T-13` complete (`TD-007`): compactor now performs streamed merge writes into temp files, promotes output atomically, and only removes source files after successful promotion; regression tests added for success/failure paths.
- `T-14` complete (`TD-009`): Silver Arrow schema definitions moved from `heber.writer.silver` into shared `heber.schemas.silver`, with writer/transformer wired to the shared module and regression coverage preventing inline schema duplication.
- `T-15` complete (`TD-011`): Hot Store event sync (`sync_quote`/`sync_trade`/`sync_bar`) now buffers records and writes batched inserts based on row/time thresholds, with flush-on-stop and regression coverage.
- `T-16` complete (`TD-010`): host runtime defaults now align with docker-compose exposed ports (`5433` Postgres, `6380` Redis) across settings/docs/env templates, with regression coverage.
- `T-17` complete (`TD-012`): Catalog startup now limits SQLAlchemy `create_all` bootstrapping to `dev` only; Alembic migration scaffolding and baseline revision were added with regression tests.
- `T-18` complete (`TD-017`): watch service async loops now offload Redis-bound sync calls via async wrappers / `asyncio.to_thread`, reducing event-loop blocking risk with regression tests.
- `T-19` complete (`TD-018`): watch consumer now retries flow-alert processing and routes terminal failures to a Redis DLQ, acknowledging only after success or successful dead-lettering.
- `T-20` complete (`TD-030`): stream naming now uses a unified `heber:events` namespace across bus stream constants, stream registry keys, watch-consumer defaults, and SRE troubleshooting/runbook commands.
- `T-21` complete (`TD-035`, `TD-036`): alert labels pipeline now canonicalizes underlying instrument keys for bar joins and loads intraday data from `bars` with `5Min` timeframe filtering (with daily fallback), replacing the stale `bars_5min` read path.
- `T-22` complete (`TD-037`): alert-label intraday windows now use minute-based 5-minute bar durations for `ts_available` and SPY-relative windows instead of day-based offsets.

## Prioritization Approach

P0 = data loss or leakage risk, pipeline correctness, or critical deploy blockers.
P1 = developer productivity and reliability improvements that unlock safe iteration.
P2 = structural cleanups that require larger refactors.

## P0 Tickets (Immediate)

### T-01: Fix Event Bus Pending Claim Handling (TD-015)

Priority: P0

Description: `RedisEventBus.consume()` claims idle messages but never yields them, risking silent drops. Ensure claimed messages are processed before new reads.

Scope:
- `heber/bus/__init__.py`
- Any tests for consume/claim behavior

Acceptance Criteria:
- Claimed messages are yielded to consumer processing.
- Integration test simulates XPENDING/XCLAIM and confirms delivery.
- No regression in normal consume flow.

Risk Notes:
- Requires careful handling to avoid double-processing.

Estimate: 1-2 days

### T-02: Align Meta-Label Writer and Dataset Builder (TD-016)

Priority: P0

Description: Label writer outputs columns (`outcome_reason`, `contract_hit_tp_first`) that the dataset builder does not consume. Align names and semantics to produce non-empty training sets.

Scope:
- `heber/watch/checker.py`
- `heber/watch/writer.py`
- `heber/ml/datasets.py`

Acceptance Criteria:
- End-to-end test: watch outcome -> label write -> dataset build returns expected labels.
- Column names and meanings are consistent and documented.

Estimate: 1-2 days

### T-03: Fix `ts_available` in Feature Templates (TD-034)

Priority: P0

Description: Feature templates use `pd.Timestamp.now()` for `ts_available`, causing leakage. Derive `ts_available` from input data availability.

Scope:
- `heber/features/templates/*.py`

Acceptance Criteria:
- `ts_available` is derived from input data (max `ts_available` or computed lag).
- Unit test validates `ts_available` is not later than processing time and respects source availability.

Estimate: 1-2 days

### T-04: Align Feast Feature Views With Gold Outputs (TD-033)

Priority: P0

Description: Feature view schemas and paths do not match Gold outputs, breaking materialization and training.

Scope:
- `features/feature_views/*.py`
- `features/feature_store.yaml`
- `heber/features/templates/*.py`

Acceptance Criteria:
- Feature view fields match Gold output columns.
- Paths configurable and aligned with Gold layout.
- Schema validation test ensures all feature view fields exist in sample Gold files.

Estimate: 2-4 days

## P1 Tickets (Next)

### T-05: Expand Test Discovery to Include In-Package Tests (TD-001)

Priority: P1

Description: Pytest only discovers tests under `tests/`, leaving many tests under `heber/` unexecuted.

Scope:
- `pyproject.toml`
- Move or include in-package tests

Acceptance Criteria:
- Running `pytest` collects tests under `tests/` and `heber/`.
- CI reports total collected tests and fails on test collection errors.

Estimate: 0.5-1 day

### T-06: Fix Runtime Entry Points (Docker + K8s) (TD-003, TD-074)

Priority: P1

Description: Dockerfile stages and k8s deployments reference module paths that do not exist.

Scope:
- `Dockerfile`
- `k8s/base/deployments/*.yaml`

Acceptance Criteria:
- Docker build targets use valid module paths.
- K8s deployments run valid module entrypoints.
- Smoke test: containers start and run for consumer/writer/compactor.

Estimate: 1-2 days

### T-07: Terraform Modules Availability (TD-073)

Priority: P1

Description: Terraform root module references local modules that are not present in the repo.

Scope:
- `infrastructure/terraform/main.tf`
- `infrastructure/terraform/modules/*` (add or replace)

Acceptance Criteria:
- `terraform init` and `terraform plan` run without missing module errors.
- Modules are either added or referenced from a remote source with pinned versions.

Estimate: 1-3 days

### T-08: SDK Default Port Alignment (TD-002)

Priority: P1

Description: SDK defaults to port 8080, while docker-compose exposes 8085 on host.

Scope:
- `heber/config.py`
- `heber/sdk/client.py`
- `README.md` or `docs/sdk.md`

Acceptance Criteria:
- Default SDK URL works in local docker-compose without overrides.
- Documentation matches default behavior.

Estimate: 0.5-1 day

### T-16: Align Local Service Port Defaults (TD-010)

Priority: P1

Description: Local host defaults for Postgres and Redis diverged from docker-compose host-exposed ports. Align defaults and docs/templates so host-run SDK/services work without manual overrides.

Scope:
- `heber/config.py`
- `tests/test_sdk_catalog_defaults.py`
- `docs/configuration.md`
- `README.md`
- `.env.example`

Acceptance Criteria:
- `Settings` defaults use `localhost:5433` for Postgres and `localhost:6380` for Redis.
- Regression tests validate the new defaults.
- Configuration docs and environment template reflect the same host defaults.

Estimate: 0.5 day

### T-17: Add Catalog Migration Baseline and Non-Dev Guard (TD-012)

Priority: P1

Description: Catalog API previously called `Base.metadata.create_all` at startup in all environments with no migration path. Add Alembic baseline and restrict startup auto-create behavior to local dev only.

Scope:
- `heber/catalog/api.py`
- `alembic.ini`
- `alembic/env.py`
- `alembic/versions/*`
- `tests/test_catalog_migrations.py`
- `docs/operations/deployment.md`

Acceptance Criteria:
- Catalog startup applies `create_all` only in `HEBER_ENVIRONMENT=dev`.
- Non-dev environments skip runtime table creation and rely on migrations.
- Alembic configuration and an initial Catalog revision exist in-repo.
- Regression tests verify dev/non-dev startup behavior and migration asset presence.

Estimate: 1 day

### T-18: Remove Blocking Redis Calls from Watch Async Loops (TD-017)

Priority: P1

Description: Watch consumer and poller async loops previously called sync Redis / manager methods directly, risking event-loop stalls. Move these calls behind async wrappers that offload blocking work.

Scope:
- `heber/watch/manager.py`
- `heber/watch/consumer.py`
- `heber/watch/poller.py`
- `heber/watch/writer.py`
- `tests/test_watch_async_redis.py`

Acceptance Criteria:
- Consumer stream read/ack and watch creation in async paths no longer perform direct blocking sync calls.
- Poller quote update loop uses async manager wrappers for watch/snapshot updates.
- Check/write loop offloads synchronous barrier checks from the event loop.
- Regression tests verify async paths are used and event loop remains responsive while reading stream messages.

Estimate: 1 day

### T-19: Add Watch Consumer Retry + DLQ Policy (TD-018)

Priority: P1

Description: Watch consumer previously acknowledged stream messages even when processing failed, causing silent drops with no DLQ trace. Add retry/backoff and dead-letter handling with ACK policy tied to processing outcome.

Scope:
- `heber/watch/consumer.py`
- `tests/test_watch_consumer_reliability.py`

Acceptance Criteria:
- Flow-alert messages are retried with bounded attempts and backoff.
- Terminal failures are written to Redis DLQ stream with context metadata.
- Messages are ACKed only after successful processing or successful DLQ write.
- If DLQ write fails, message remains pending (not ACKed) for later recovery.
- Regression tests verify retry count, DLQ routing, and ACK decision behavior.

Estimate: 1 day

### T-20: Unify Stream Naming Convention (TD-030)

Priority: P1

Description: Stream names diverged between `stream:*`, `heber:stream:*`, and `heber:events`, creating wiring and ops confusion. Standardize stream naming under a single `heber:events` namespace.

Scope:
- `heber/bus/__init__.py`
- `heber/bus/streams.py`
- `heber/watch/consumer.py`
- `heber/sre/runbooks.py`
- `docs/operations/troubleshooting.md`
- `heber/ops/tests_remaining.py`
- `tests/test_stream_naming_conventions.py`

Acceptance Criteria:
- Event bus stream enum values use `heber:events:*` names.
- Stream registry helper keys use `heber:events:{name}`.
- Watch consumer defaults to `settings.redis_stream_name` instead of hardcoded stream literals.
- Ops runbook/troubleshooting commands reference the same namespace for backlog and DLQ checks.
- Regression tests guard stream naming conventions against drift.

Estimate: 0.5 day

### T-21: Fix Alert Label Bar Key + Intraday Dataset Wiring (TD-035, TD-036)

Priority: P1

Description: Alert labeling used raw symbols for bar reads and queried a non-existent `bars_5min` dataset. This caused empty joins and missing intraday labels. Normalize to canonical instrument keys and query `bars` with timeframe filtering.

Scope:
- `heber/features/pipelines/alert_labels.py`
- `heber/features/templates/alert_labels.py`
- `tests/test_alert_labels_pipeline_keys.py`

Acceptance Criteria:
- Pipeline normalizes alert underlyings to canonical `equity:*` keys for bar joins.
- Silver bar reads include both canonical and legacy raw symbol filters for backward compatibility.
- Intraday path reads `dataset=\"bars\"` and filters to 5-minute timeframe values, with a daily fallback when intraday bars are unavailable.
- Regression tests verify key normalization, intraday dataset selection, and fallback behavior.

Estimate: 1 day

### T-22: Correct Intraday Label Window Units (TD-037)

Priority: P1

Description: Intraday label calculations previously converted bar counts into day offsets, causing `ts_available` and SPY-relative windows to drift by up to days. Use bar-duration-aware intraday windows.

Scope:
- `heber/features/templates/alert_labels.py`
- `tests/test_alert_label_intraday_windows.py`

Acceptance Criteria:
- Intraday horizon window length is computed in minutes from the configured 5-minute bar count.
- `ts_available` for intraday labels advances by the correct intraday duration (e.g., 24 bars -> 2 hours).
- SPY-relative return end-time uses the same intraday duration for intraday labels.
- Regression tests verify intraday and daily horizon window behavior.

Estimate: 0.5 day

## P2 Tickets (Structural)

### T-09: Unify Hot Store Implementation (TD-004)

Priority: P2

Description: Hot Store has divergent implementations and clients (sync vs async). Consolidate on one client and data path.

Scope:
- `heber/hotstore/client.py`
- `heber/writer/hotstore.py`
- `heber/hotstore/sync.py`
- `heber/hotstore/tables.py`

Acceptance Criteria:
- Single ClickHouse client/library across code paths.
- Consistent async/sync model with documented usage.
- Load test shows stable ingestion without per-row inserts.

Estimate: 3-5 days

### T-13: Harden Compactor Atomic Merge Flow (TD-007)

Priority: P2

Description: Compaction previously loaded all small files into memory and deleted source files directly after writing output. Harden compaction with streamed writes, atomic promotion, and safer cleanup semantics.

Scope:
- `heber/writer/compactor.py`
- `tests/test_compactor_safety.py`

Acceptance Criteria:
- Compaction writes merged output to a temp file first and atomically promotes to final `.parquet`.
- Source small files are deleted only after merged output promotion succeeds.
- Failure during merge/write keeps source files intact and does not leave lock/temp artifacts.
- Regression tests cover successful compaction and failure recovery behavior.

Estimate: 1-2 days

### T-14: Centralize Silver Schema Definitions (TD-009)

Priority: P2

Description: Silver schema constants were defined inline in `heber.writer.silver`, causing drift risk and duplicated ownership. Centralize schema definitions into a shared schema module and keep runtime writers/transforms consuming that source.

Scope:
- `heber/schemas/silver.py`
- `heber/writer/silver.py`
- `heber/writer/transformer.py`
- `tests/test_silver_schema_source.py`

Acceptance Criteria:
- Canonical Silver Arrow schemas live in one shared module.
- Writer and transformer import schemas from shared module, not from duplicated inline constants.
- Regression tests verify writer behavior still resolves known/default schemas and guard against reintroducing inline schema constants.

Estimate: 1 day

### T-15: Batch Hot Store Event Inserts (TD-011)

Priority: P2

Description: Event sync paths in `HotStoreSync` previously called `write_batch(..., [event])`, causing one ClickHouse insert per event. Add buffering and threshold-based flushing to reduce insert overhead.

Scope:
- `heber/hotstore/sync.py`
- `tests/test_hotstore_unification.py`

Acceptance Criteria:
- `sync_quote`, `sync_trade`, and `sync_bar` buffer events and insert in batches.
- Buffer flushes when row threshold is hit or max wait time elapses.
- Pending buffered rows are flushed during shutdown/stop.
- Regression tests verify threshold batching and stop-time flush behavior.

Estimate: 1 day

## Suggested Execution Order

1. T-01 (Event bus claim handling)
2. T-02 (Meta-label alignment)
3. T-03 (ts_available fix)
4. T-04 (Feast feature view alignment)
5. T-05 (Test discovery)
6. T-06 (Docker/K8s entrypoints)
7. T-07 (Terraform modules)
8. T-08 (SDK port alignment)
9. T-09 (Hot Store unification)
10. T-10 (Consumer DLQ + pending recovery)
11. T-11 (Silver flush config alignment)
12. T-12 (Timezone-aware UTC normalization)
13. T-13 (Compactor atomic merge hardening)
14. T-14 (Silver schema centralization)
15. T-15 (Hot Store event batching)
16. T-16 (Local service port alignment)
17. T-17 (Catalog migration baseline + non-dev startup guard)
18. T-18 (Watch async Redis non-blocking refactor)
19. T-19 (Watch consumer retry + DLQ policy)
20. T-20 (Stream naming convention unification)
21. T-21 (Alert label bar key + intraday dataset wiring)
22. T-22 (Intraday label window unit correction)
