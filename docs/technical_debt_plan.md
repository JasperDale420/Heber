# Technical Debt Remediation Plan (High Severity)

Date: 2026-02-05

This plan converts high-severity audit items into ticket-ready tasks with clear scope and acceptance criteria. It references the audit IDs in `docs/technical_debt_audit.md`.

## Implementation Status

Updated: 2026-02-07

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
- `T-23` complete (`TD-038`): flow-feature computation now normalizes `ts_event` to UTC before indexing, drops invalid timestamps, and enforces rolling 24-hour time-window behavior with regression tests.
- `T-24` complete (`TD-040`): lifecycle async shutdown wait now returns immediately when shutdown is already signaled and handles async event creation races to prevent hung waits.
- `T-25` complete (`TD-041`): lifecycle shutdown timeout paths now report `timeout` status in metrics/logs and return `False` instead of reporting successful shutdown.
- `T-26` complete (`TD-042`): logging setup now validates and applies `log_level` to both stdlib and structlog filtering, with regression tests for INFO/DEBUG behavior across JSON and console renderers.
- `T-27` complete (`TD-043`): dedupe now uses rolling Bloom-filter rotation with bounded windows and emits rotation stats, with regression tests that cover duplicate behavior before and after rotation boundaries.
- `T-28` complete (`TD-066`): lakeFS repository creation now resolves storage namespace from configurable base/template settings (`LAKEFS_STORAGE_NAMESPACE_BASE` / `LAKEFS_STORAGE_NAMESPACE_TEMPLATE`) with regression tests.
- `T-29` complete (`TD-075`, `TD-076`): HPA manifests now use CPU/memory resource metrics and worker probes now use exec checks matching real process entrypoints, with k8s manifest regression tests.
- `T-30` complete (`TD-059`): clickhouse backup script output now aligns with effective destination behavior by reporting config-managed remote destination/entries instead of an unenforced hardcoded S3 path.
- `T-31` complete (`TD-060`): catalog backup validation script now guarantees test-instance cleanup via `EXIT` trap and preserves failure status on restore/query errors.
- `T-32` complete (`TD-065`): `scripts/security-scan.sh` now enforces filesystem `trivy fs` failure gating with `--exit-code 1` and explicit failure handling for HIGH/CRITICAL findings.
- `T-33` complete (`TD-039`): tracing decorators now avoid unconditional `SpanKind` access when OpenTelemetry is unavailable, with regression coverage for `@traced` no-OTEL execution.
- `T-34` complete (`TD-061`): volume init now performs explicit host/tool checks before running `dot_clean`, with explicit skip behavior on non-macOS hosts and regression checks guarding against implicit fallback.
- `T-35` complete (`TD-062`): labeling strategy docs now reference `heber/firewall/validation.py` and current split-validation parameter names.
- `T-36` complete (`TD-063`): data contract docs now reference `heber/schemas/silver.py` and concrete Gold key-value path conventions used by SDK and label writers.
- `T-37` complete (`TD-086`): backfill package now provides an executable `python -m heber.backfill` service entrypoint with API/probe routes.
- `T-38` complete (`TD-087`): hotloader facade now provides a real service CLI (`python -m heber.writer.hotstore`) with continuous sync-loop and one-shot mode.
- `T-39` complete (`TD-064`): UW endpoint tracker summary now reports table-derived status counts and no longer contains stale manual buckets.
- `T-40` complete (`TD-088`): scrape-annotated services now start metrics exporters via `start_metrics_server_from_env`, and regression tests validate deployment-to-entrypoint metrics alignment.
- `T-41` complete (`TD-068`): `MarketCalendar` now normalizes all supported datetime inputs to UTC before exchange conversion, assumes naive inputs are UTC, and includes regression tests for naive/aware/pandas timestamp handling.
- `T-42` complete (`TD-069`): `MarketCalendar` now fails fast for `include_extended=True` with explicit unsupported-mode messaging instead of silently ignoring the flag.
- `T-43` complete (`TD-070`): Hot Store DDL now includes `quality_flags` and `lineage` base columns, and sync insert mappings/tests were updated to preserve those fields.
- `T-44` complete (`TD-072`): additional schema registry tests now validate required schema contracts and unknown-schema handling instead of asserting a fixed global schema count.
- `T-45` complete (`TD-067`): lakeFS versioning now emits consistent success/error/duration metrics for `create_tag`, `list_tags`, `merge`, and `diff`, including repository-resolution failure paths covered by regression tests.
- `T-46` complete (`TD-079`): Terraform environment modules now accept `aws_region` variables, backend blocks are partial, and per-env backend configuration moved to `backend.hcl` without hardcoded region keys.
- `T-47` complete (`TD-080`, `TD-082`): backfill chunk writes now produce Bronze raw output, update catalog dataset/coverage metadata, and fail fast when `pyarrow` is unavailable.
- `T-48` complete (`TD-081`): backfill jobs now persist to disk with progress checkpoints and are reloaded on coordinator startup for restart-safe resume behavior.
- `T-49` complete (`TD-083`): gap detection now scans both legacy and canonical Silver partition layouts for `dt=*` coverage, preventing false full-gap reports when storage layout differs.
- `T-50` complete (`TD-084`): backtest data-loader label reads now pass explicit `label_version` pinning to `read_gold()`, with regression tests for explicit and default (`latest`) version behavior.
- `T-51` complete (`TD-085`): backtest experiment config/results now record feature/label as-of timestamps plus per-fold as-of metadata in tracker logs for reproducible reruns.
- `T-52` complete (`TD-051`, `TD-055`): retention now scans canonical Gold key-value layouts for dataset/version partitions and prunes versions using semantic-version-aware ordering.
- `T-53` complete (`TD-052`, `TD-053`): reaper now enforces retention for `HOT_STORE` and `DLQ` layers, and default retention paths now resolve from configured data-root settings/env.
- `T-54` complete (`TD-050`, `TD-054`): label reads now use semantic-version-aware latest resolution and fail closed when `ts_available` is missing.
- `T-55` complete (`TD-044`): dead-letter queue now persists queued failure events and reloads them on startup.
- `T-56` complete (`TD-045`, `TD-046`): firewall SCD joins now resolve validity columns without suffix assumptions, and strict Gold validation now raises only on hard leakage violations.
- `T-57` complete (`TD-047`, `TD-048`, `TD-049`): Silver models now normalize lineage JSON serialization, apply release-aware schema-version defaults, and align expiry/date typing with canonical Arrow schemas.
- `T-58` complete (`TD-056`, `TD-057`, `TD-058`): Feast helpers now resolve repo path from config/env, return non-placeholder materialization counts, and support tag value / key:value search filters.
- `T-59` complete (`TD-014`): core consumer/silver/compactor runtime paths now emit concrete metrics via shared Prometheus helpers, with instrumentation regression tests.
- `T-60` complete (`TD-031`, `TD-032`): watch model timestamps now default to aware UTC values and watch poller quote fetches now gate by per-horizon due intervals.
- `T-61` complete (`TD-013`): consumer processing now enforces canonical instrument-key validation before Bronze/Silver writes and rejects invalid keys with regression coverage.
- `T-62` complete (`TD-019`): watch feature extraction now normalizes alert timestamps to market timezone before computing time-of-day/session features, with naive timestamps treated as UTC.
- `T-63` complete (`TD-020`): watch Data Gateway HTTP callers now share API-prefix-first endpoint construction with legacy fallback, removing path drift between poller/consumer/features.
- `T-64` complete (`TD-021`, `TD-022`): meta-label dataset defaults now use configured Gold-root paths with legacy fallback, and watch feature extraction now persists feature rows to Gold partitions during ingestion.
- `T-65` complete (`TD-023`): training feature order is now persisted in model metadata and inference uses that saved ordering when constructing feature vectors.
- `T-66` complete (`TD-024`, `TD-025`): Soda scanner defaults now resolve from shared Silver-root settings and contract non-null reporting now uses each contract threshold.
- `T-67` complete (`TD-026`, `TD-027`): framework schedule API presence is now explicitly regression-tested and testing environment local-service defaults now align with docker-compose host port mappings.
- `T-68` complete (`TD-028`): Iceberg Silver table creation now builds a concrete PyIceberg `PartitionSpec` for day-partitioning on `ts_event`, with regression coverage preventing list-based partition drift.
- `T-69` complete (`TD-029`): quarantine partition routing now reads canonical envelope top-level `provider`/`feed` first, with legacy `meta` fallback retained for compatibility.
- `T-70` complete (`TD-073`): Terraform root-module output references are now regression-tested against local module output declarations to prevent wiring drift.
- `T-71` complete (`TD-077`): overlay image-transformer rules now target the base-rewritten image name so env tags override correctly, with regression checks for both kustomization config and rendered manifests.
- `T-72` complete (`TD-078`): base kustomize resources now include service-account and external-secret prerequisites, with rendered-overlay conformance tests guarding deployment `serviceAccountName` and `secretRef` contracts.
- `T-73` complete (`TD-074`): runtime-entrypoint conformance checks now validate command-module wiring across all base deployments (`catalog`, `consumer`, `writer`, `compactor`, `hotloader`, `backfill`).
- `T-74` complete (`TD-071`): Hot Store table-creation helpers now harden sync/async contract boundaries by rejecting awaitable results on the sync path (with coroutine cleanup), plus regression tests for sync misuse and async-helper execution.
- `T-75` complete (`TD-089`): PostgreSQL readiness checks now execute SQLAlchemy 2.x-compatible statements (`text(\"SELECT 1\")`) with regression coverage for healthy and failing engine paths.
- `T-76` complete (`TD-090`): watch manager now normalizes Redis byte IDs before key lookups, preserving active/symbol watch retrieval under default `redis.from_url` byte-response behavior with regression coverage.
- `T-77` complete (`TD-091`): watch poller/checker now treat `0.0` quote values as valid inputs (`None` checks instead of truthiness), preserving zero-price return paths and SL detection with regression coverage.
- `T-78` complete (`TD-092`): label writer parquet part filenames are now collision-safe (unique suffix), preventing same-second flush overwrites with regression coverage.
- `T-79` complete (`TD-093`): watch entrypoint now invokes `service.stop()` on unexpected runtime exceptions (not only keyboard interrupts), preserving shutdown cleanup/flush behavior with regression coverage.
- `T-80` complete (`TD-094`): watch feature Greeks enrichment now preserves valid `0.0` values (explicit `None` checks), with regression coverage for zero-valued delta/gamma/theta/vega/IV payloads.
- `T-81` complete (`TD-095`): gateway URL candidate construction now normalizes custom `api_prefix` values without a leading slash, preventing malformed prefix-first URLs and preserving fallback behavior.
- Audit Pass 14 revalidated `TD-066`, `TD-075`, and `TD-076` as still open (versioning + k8s runtime conformance).
- Audit Pass 15 revalidated `TD-059`, `TD-060`, and `TD-065` as still open (backup/security script hardening).
- Audit Pass 16 revalidated `TD-039`, `TD-061`, `TD-062`, and `TD-063` as still open (tracing optional-dependency safety + script/docs drift).
- Audit Pass 17 revalidated `TD-066`, `TD-067`, `TD-075`, and `TD-076` as still open, and added `TD-086`/`TD-087` for non-running k8s worker entrypoints.
- Audit Pass 18 revalidated `TD-042`, `TD-043`, and `TD-064` as still open (logging filter wiring, dedupe rotation policy, UW endpoint summary drift).
- Audit Pass 19 revalidated `TD-059`, `TD-060`, `TD-062`, `TD-063`, and `TD-065` as still open (backup/security script hardening + docs alignment drift).
- Audit Pass 20 revalidated `TD-086` and `TD-087` as still open (backfill/hotloader deployment entrypoints remain non-runnable).
- Audit Pass 21 revalidated `TD-075` and `TD-076` as still open, and added `TD-088` for Prometheus scrape/metrics-exporter wiring drift.
- Audit Pass 22 revalidated `TD-068`, `TD-069`, `TD-070`, and `TD-072` as still open; `TD-071` was confirmed resolved by the `T-09` Hot Store table-helper refactor.
- Audit Pass 23 revalidated `TD-068` as resolved via `T-41`; `TD-069`, `TD-070`, and `TD-072` remain open.
- Audit Pass 24 revalidated `TD-072` as resolved via `T-44`; `TD-069` and `TD-070` remain open.
- Audit Pass 25 revalidated `TD-069` as resolved via `T-42`; `TD-070` remains open.
- Audit Pass 26 revalidated `TD-070` as resolved via `T-43`.
- Audit Pass 27 revalidated `TD-065` as resolved via `T-32`.
- Audit Pass 28 revalidated `TD-060` as resolved via `T-31`.
- Audit Pass 29 revalidated `TD-059` as resolved via `T-30`.
- Audit Pass 30 revalidated `TD-062` and `TD-063` as resolved via `T-35` and `T-36`.
- Audit Pass 31 revalidated `TD-064` as resolved via `T-39`.
- Audit Pass 32 revalidated `TD-042` as resolved via `T-26`.
- Audit Pass 33 revalidated `TD-043` as resolved via `T-27`.
- Audit Pass 34 revalidated `TD-066` as resolved via `T-28`.
- Audit Pass 35 revalidated `TD-075` and `TD-076` as resolved via `T-29`.
- Audit Pass 36 revalidated `TD-039` as resolved via `T-33`.
- Audit Pass 37 revalidated `TD-061` as resolved via `T-34`.
- Audit Pass 38 revalidated `TD-086` and `TD-087` as resolved via `T-37` and `T-38`.
- Audit Pass 39 revalidated `TD-088` as resolved via `T-40`.
- Audit Pass 40 revalidated `TD-067` as resolved via `T-45`.
- Audit Pass 41 revalidated `TD-079` as resolved via `T-46`.
- Audit Pass 42 revalidated `TD-080` and `TD-082` as resolved via `T-47`.
- Audit Pass 43 revalidated `TD-081` as resolved via `T-48`.
- Audit Pass 44 revalidated `TD-083` as resolved via `T-49`.
- Audit Pass 45 revalidated `TD-084` as resolved via `T-50`.
- Audit Pass 46 revalidated `TD-085` as resolved via `T-51`.
- Audit Pass 47 revalidated `TD-051` and `TD-055` as resolved via `T-52`.
- Audit Pass 48 revalidated `TD-052` and `TD-053` as resolved via `T-53`.
- Audit Pass 49 revalidated `TD-050` and `TD-054` as resolved via `T-54`.
- Audit Pass 50 revalidated `TD-044` as resolved via `T-55`.
- Audit Pass 51 revalidated `TD-045` and `TD-046` as resolved via `T-56`.
- Audit Pass 52 revalidated `TD-047`, `TD-048`, and `TD-049` as resolved via `T-57`.
- Audit Pass 53 revalidated `TD-056`, `TD-057`, and `TD-058` as resolved via `T-58`.
- Audit Pass 54 revalidated `TD-014` as resolved via `T-59`.
- Audit Pass 55 revalidated `TD-031` and `TD-032` as resolved via `T-60`.
- Audit Pass 56 revalidated `TD-013` as resolved via `T-61`.
- Audit Pass 57 revalidated `TD-019` as resolved via `T-62`.
- Audit Pass 58 revalidated `TD-020` as resolved via `T-63`.
- Audit Pass 59 revalidated `TD-021` and `TD-022` as resolved via `T-64`.
- Audit Pass 60 revalidated `TD-023` as resolved via `T-65`.
- Audit Pass 61 revalidated `TD-024` and `TD-025` as resolved via `T-66`.
- Audit Pass 62 revalidated `TD-026` and `TD-027` as resolved via `T-67`.
- Audit Pass 63 revalidated `TD-028` as resolved via `T-68`.
- Audit Pass 64 revalidated `TD-029` as resolved via `T-69`.
- Audit Pass 65 revalidated `TD-004` as resolved via `T-09`.
- Audit Pass 66 revalidated `TD-073` as resolved via `T-07` and `T-70`.
- Audit Pass 67 revalidated `TD-077` as resolved via `T-71`.
- Audit Pass 68 revalidated `TD-078` as resolved via `T-72`.
- Audit Pass 69 revalidated `TD-074` as resolved via `T-06` and `T-73`.
- Audit Pass 70 revalidated `TD-071` as resolved via `T-09` and `T-74`.
- Audit Pass 71 revalidated `TD-089` as resolved via `T-75`.
- Audit Pass 72 revalidated `TD-090` as resolved via `T-76`.
- Audit Pass 73 revalidated `TD-091` as resolved via `T-77`.
- Audit Pass 74 revalidated `TD-092` as resolved via `T-78`.
- Audit Pass 75 revalidated `TD-093` as resolved via `T-79`.
- Audit Pass 76 revalidated `TD-094` as resolved via `T-80`.
- Audit Pass 77 revalidated `TD-095` as resolved via `T-81`.

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

### T-23: Harden Flow Feature Time-Window Rolling (TD-038)

Priority: P1

Description: Flow-feature rolling windows needed stronger guarantees around timestamp normalization and time-window correctness to avoid subtle drift with string/invalid timestamps.

Scope:
- `heber/features/templates/flow.py`
- `heber/features/templates/tests.py`

Acceptance Criteria:
- `compute_flow_features` normalizes `ts_event` to UTC datetimes before sorting/indexing.
- Invalid `ts_event` rows are dropped before rolling-window calculations.
- Rolling premium/sweep aggregates are verified to be true time-windowed (not row-count based).
- Regression tests verify UTC timestamp normalization and 24-hour boundary behavior.

Estimate: 0.5 day

### T-24: Fix Async Shutdown Wait Hang Path (TD-040)

Priority: P1

Description: `async_wait_for_shutdown()` could hang forever when shutdown was initiated before the async event existed. Add pre-signaled checks and race-safe async event initialization.

Scope:
- `heber/ops/lifecycle.py`
- `tests/test_lifecycle_shutdown_wait.py`

Acceptance Criteria:
- `async_wait_for_shutdown()` returns immediately if shutdown is already signaled.
- If shutdown is signaled during async event creation, waiters are still released.
- Regression tests cover both pre-signaled and late-signaled async wait scenarios.

Estimate: 0.5 day

### T-25: Report Shutdown Timeouts Correctly (TD-041)

Priority: P1

Description: Shutdown drains that exceed timeout were logged as timeouts but still emitted success metrics and success return codes. Align return status + metrics with actual timeout behavior.

Scope:
- `heber/ops/lifecycle.py`
- `tests/test_lifecycle_shutdown_timeout.py`

Acceptance Criteria:
- `execute_shutdown()` increments `shutdown_completed{status=\"timeout\"}` and returns `False` when drain timeout occurs.
- `async_execute_shutdown()` matches the same timeout behavior.
- Success path still emits `status=\"success\"` and returns `True`.
- Regression tests cover sync timeout, async timeout, and non-timeout success behavior.

Estimate: 0.5 day

### T-26: Apply Effective Log-Level Filtering (TD-042)

Priority: P1

Description: `configure_logging()` accepts `log_level` but does not apply it to either stdlib logging or structlog filtering. Implement consistent level parsing/application for production and dev outputs.

Scope:
- `heber/ops/logging.py`
- New regression tests for level filtering behavior
- `docs/configuration.md` (if env var/value expectations are documented)

Acceptance Criteria:
- `configure_logging(log_level=...)` enforces filtering for emitted logs.
- Invalid log level values fail fast with a clear error (or are normalized deterministically).
- Behavior is consistent between JSON and console renderers.
- Regression tests validate that DEBUG messages are suppressed at INFO and emitted at DEBUG.

Estimate: 0.5 day

### T-27: Add Dedupe Rotation / Backing-Store Policy (TD-043)

Priority: P1

Description: In-memory Bloom dedupe currently accumulates false positives without rotation and treats Bloom matches as hard duplicates when no backing store exists. Add bounded-state dedupe behavior with safer fallback semantics.

Scope:
- `heber/ops/reliability.py`
- New regression tests for dedupe rollover/reset behavior
- Runtime config/docs for dedupe strategy selection

Acceptance Criteria:
- Deduper uses a bounded-time strategy (rotation/reset/TTL) to limit Bloom saturation effects.
- No-backing-store mode does not permanently increase hard-drop risk over long runtimes.
- Dedupe stats expose rollover/reset events for observability.
- Regression tests cover duplicate detection before and after rotation boundaries.

Estimate: 1-2 days

### T-28: Make lakeFS Storage Namespace Configurable (TD-066)

Priority: P1

Description: lakeFS repository creation currently hardcodes `s3://heber-lakehouse/{repo}`, which prevents environment-specific storage namespace configuration (e.g., MinIO/staging buckets).

Scope:
- `heber/versioning/__init__.py`
- Versioning docs/config references for new namespace setting
- Regression tests for config/env resolution

Acceptance Criteria:
- `LakeFSConfig` supports configurable storage namespace template or base namespace.
- Repository creation uses the configured namespace value instead of hardcoded literals.
- Default behavior remains backward-compatible when new env vars are unset.
- Regression tests validate repository namespace selection from env/config.

Estimate: 0.5-1 day

### T-29: Align K8s HPA Metrics and Probes With Runtime Reality (TD-075, TD-076)

Priority: P1

Description: HPA specs reference metrics that are not emitted, and several deployments probe HTTP endpoints that worker processes do not expose. Align manifests with exported metrics and actual health-check surfaces.

Scope:
- `k8s/base/hpa/*.yaml`
- `k8s/base/deployments/*.yaml` (consumer, writer, compactor, hotloader, and related workers)
- Optional runtime metrics/health wiring if preferred over manifest-only changes

Acceptance Criteria:
- HPA rules use existing/exported metrics or fallback to CPU/memory autoscaling.
- Liveness/readiness probes target real endpoints/check mechanisms for each worker type.
- Updated manifests pass a dry-run schema validation (`kubectl kustomize`/`kubectl apply --dry-run=client`).
- Added regression/static checks to prevent future drift between manifests and runtime endpoints/metrics.

Estimate: 1-2 days

### T-30: Align ClickHouse Backup Script With Effective S3 Destination (TD-059)

Priority: P1

Description: Backup script currently prints `S3_BUCKET/S3_PREFIX` values but does not apply them to `clickhouse-backup` commands, creating misleading operational output.

Scope:
- `scripts/backup/clickhouse-backup.sh`
- Backup/runbook docs referencing S3 backup path behavior

Acceptance Criteria:
- Script either applies bucket/prefix via clickhouse-backup config/env integration or removes misleading destination output.
- Verification output reflects the actual remote destination used by `clickhouse-backup`.
- Regression/smoke checks verify backup list/verification path consistency.

Estimate: 0.5 day

### T-31: Guarantee Catalog Backup Cleanup on Failure (TD-060)

Priority: P1

Description: Validation script may leak the temporary restored RDS instance when restore/query steps fail due to `set -e` exit before cleanup.

Scope:
- `scripts/backup/validate-catalog-backup.sh`

Acceptance Criteria:
- Script uses a `trap` (or equivalent) to always delete the test instance on exit/failure.
- Failure paths still preserve enough logs/output for diagnosis.
- Success/failure runs both exercise cleanup path deterministically.

Estimate: 0.5 day

### T-32: Enforce Failure on Filesystem Secret/Misconfig Findings (TD-065)

Priority: P1

Description: `trivy fs` scan in `security-scan.sh` currently does not set a non-zero exit code on findings, reducing CI gate effectiveness.

Scope:
- `scripts/security-scan.sh`
- Any CI docs that describe security scan failure behavior

Acceptance Criteria:
- Filesystem scan uses explicit non-zero exit behavior (`--exit-code 1`) for configured severities.
- Script exits non-zero when critical/high filesystem findings are present.
- Documentation reflects expected blocking behavior.

Estimate: 0.5 day

### T-33: Harden Optional OpenTelemetry Tracing Path (TD-039)

Priority: P1

Description: `traced()` still dereferences `SpanKind` before checking OpenTelemetry availability. In environments without OTel installed, decorated functions raise `NameError` instead of safely falling back to noop tracing.

Scope:
- `heber/ops/tracing.py`
- Regression tests covering OTel-missing fallback behavior

Acceptance Criteria:
- `@traced` functions execute without error when OpenTelemetry packages are absent.
- Span kind resolution is guarded behind availability checks and defaults safely in noop mode.
- Regression tests simulate OTel-unavailable import/runtime path and verify no `NameError` is raised.

Estimate: 0.5 day

### T-34: Make Volume Init Script Explicitly Cross-Platform (TD-061)

Priority: P1

Description: `init_volume.sh` always invokes macOS `dot_clean` and relies on `|| true` fallback. Replace implicit shell fallback with explicit platform/tool detection and clear logging.

Scope:
- `scripts/init_volume.sh`
- `docs/operations/deployment.md` (or relevant setup docs if script behavior is documented)

Acceptance Criteria:
- Script checks OS/tool availability before invoking `dot_clean`.
- Non-macOS runs emit explicit skip messaging rather than implicit command fallback behavior.
- macOS behavior remains unchanged for AppleDouble cleanup.

Estimate: 0.5 day

### T-35: Refresh Labeling Strategy API References (TD-062)

Priority: P1

Description: Labeling documentation still references `heber/firewall/splits.py` and stale split-validation parameters that no longer match runtime code.

Scope:
- `docs/labeling_strategy.md`
- `heber/firewall/validation.py` (for signature/source-of-truth cross-check only)

Acceptance Criteria:
- Docs reference current split-validation module path.
- Function snippet and parameter names match the current implementation.
- Docs include at least one concrete call example that stays in sync with current API.

Estimate: 0.5 day

### T-36: Align Data Contract Doc With Canonical Schema Source + Gold Paths (TD-063)

Priority: P1

Description: Data contract documentation still points Silver schema ownership at `heber/writer/silver.py` and uses abstract Gold path notation that drifts from concrete key-value partition paths.

Scope:
- `docs/data_contract.md`
- `heber/schemas/silver.py`
- `heber/sdk/client.py`
- `heber/watch/writer.py`

Acceptance Criteria:
- Doc references `heber/schemas/silver.py` as canonical Silver Arrow schema source.
- Gold layout section includes concrete path examples in `dataset=.../project=.../version=.../dt=...` form.
- Contract examples align with SDK/label writer output conventions.

Estimate: 0.5 day

### T-37: Make Backfill Deployment Entrypoint Executable (TD-086)

Priority: P1

Description: Backfill deployment currently runs `python -m heber.backfill`, but the package has no `__main__` module, so pods fail at startup.

Scope:
- `k8s/base/deployments/backfill.yaml`
- Backfill runtime module(s) under `heber/backfill/` (add executable entrypoint)
- Optional docs for backfill runtime mode

Acceptance Criteria:
- `python -m ...` command used by deployment resolves to an executable module with a running process model.
- Backfill pod no longer exits immediately due to module execution error.
- Probes/ports in deployment match the actual backfill runtime mode.

Estimate: 0.5-1 day

### T-38: Add Real Hotloader Service Entrypoint (TD-087)

Priority: P1

Description: Hotloader deployment runs a compatibility facade module that exits immediately. Add a long-running hotloader entrypoint and align deployment command/probes.

Scope:
- `heber/hotstore/sync.py` (or new dedicated service module)
- `heber/writer/hotstore.py` (if keeping compatibility facade separate)
- `k8s/base/deployments/hotloader.yaml`

Acceptance Criteria:
- Deployment command targets a long-running hotloader process (not a facade import module).
- Hotloader process performs continuous sync/event handling as intended.
- Probe strategy reflects real runtime behavior (HTTP if exposed, otherwise tcp/exec).

Estimate: 1 day

### T-39: Reconcile UW Endpoint Tracker Summary With Table Statuses (TD-064)

Priority: P1

Description: `docs/UW_endpoints.md` summary totals and status buckets are manually maintained and now diverge from endpoint table rows, reducing trust in integration coverage tracking.

Scope:
- `docs/UW_endpoints.md`
- Optional helper script/check that derives summary counts from table status cells

Acceptance Criteria:
- Summary counts match table row statuses exactly.
- “In Progress” and “Not Started” sections only include endpoints actually marked that way in tables.
- Add a lightweight repeatable check or generation note to prevent future drift.

Estimate: 0.5 day

### T-40: Align Prometheus Scrape Targets With Running Metrics Exporters (TD-088)

Priority: P1

Description: Deployments advertise Prometheus scrape on port `9090`, but runtime entrypoints do not start a metrics HTTP server, so scrape targets and related dashboards/alerts are partially non-functional.

Scope:
- Service entrypoints for catalog/consumer/writer/compactor/hotloader/backfill
- `heber/ops/metrics.py` usage (`start_metrics_server`)
- `k8s/base/deployments/*.yaml` scrape annotations and metrics container ports

Acceptance Criteria:
- Each deployment with `prometheus.io/scrape: "true"` exposes a reachable `/metrics` endpoint on the declared port.
- Service entrypoints either start metrics exporters or deployment annotations/ports are corrected to match actual runtime behavior.
- Add a lightweight validation check (script/test) that confirms metrics endpoint reachability assumptions stay in sync with manifests.

Estimate: 1 day

### T-41: Harden MarketCalendar Timezone Input Handling (TD-068)

Priority: P1

Description: `MarketCalendar` converts input timestamps with `tz_convert()` without handling naive datetimes, which raises at runtime and can break watch/calendar flows.

Scope:
- `heber/calendar/market.py`
- Calendar-related unit tests in `heber/watch/` and/or new focused calendar tests

Acceptance Criteria:
- All public `MarketCalendar` methods either accept naive timestamps by localizing with documented semantics (for example assume UTC) or reject them with deterministic, explicit validation errors.
- Regression tests cover naive `datetime`, timezone-aware `datetime`, and `pd.Timestamp` inputs.
- Behavior is documented in class/module docstrings.

Estimate: 0.5 day

### T-42: Implement or Remove Extended-Hours Calendar Flag (TD-069)

Priority: P1

Description: `include_extended` is currently configuration-only and does not alter scheduling/trading-window logic, which is misleading for callers.

Scope:
- `heber/calendar/market.py`
- Any watch-service callsites that construct `MarketCalendar`
- Documentation for calendar behavior

Acceptance Criteria:
- `include_extended=True` produces a materially different, tested session window behavior (pre/post-market) OR the flag is removed and callers/docs are updated.
- Constructor/docs accurately reflect supported behavior.
- Regression tests assert the chosen behavior.

Estimate: 0.5 day

### T-43: Resolve Hot Store Base-Column Drift in DDL (TD-070)

Priority: P1

Description: Hot Store tables omit base provenance/quality columns (`quality_flags`, `lineage`) that exist in Silver schemas, causing schema drift and reduced traceability.

Scope:
- `heber/hotstore/tables.py`
- `heber/schemas/silver.py` (contract reference)
- Any writers/sync code that inserts into Hot Store

Acceptance Criteria:
- Hot Store DDL either includes required base columns with compatible types or explicitly documents a deliberate omission in code/docs.
- Insert paths are updated (or validated) so writes remain successful with the selected schema.
- A schema-conformance regression test checks expected base columns for each hot table definition.

Estimate: 0.5-1 day

### T-44: Make Additional Schema Tests Growth-Tolerant (TD-072)

Priority: P1

Description: `tests_additional.py` asserts a fixed total schema count (`16`), creating brittle failures when valid schemas are added.

Scope:
- `heber/schemas/tests_additional.py`
- Optional schema-registry docs if test expectations are codified

Acceptance Criteria:
- Tests assert required schema names/contracts rather than an exact global count.
- Adding a valid new schema does not fail unrelated assertions.
- Test output remains clear about which required schema contract failed when a regression occurs.

Estimate: 0.5 day

### T-45: Complete lakeFS Operation Metrics Coverage (TD-067)

Priority: P1

Description: lakeFS metrics existed for `create_branch`/`commit`, but `create_tag`, `list_tags`, `merge`, and `diff` lacked complete operation instrumentation (especially error paths), leaving observability partial.

Scope:
- `heber/versioning/__init__.py`
- `tests/test_lakefs_operation_metrics.py`

Acceptance Criteria:
- `create_tag`, `list_tags`, `merge`, and `diff` all emit `lakefs_operations` success/error counters and `lakefs_operation_duration` histogram observations.
- Error metrics are emitted for operation failures including repository-resolution failures.
- Regression tests assert success and error metric behavior for all four operations.

Estimate: 0.5 day

### T-46: Parameterize Terraform Environment Region/Backend Wiring (TD-079)

Priority: P1

Description: Environment Terraform configs hardcoded `us-east-1` in module inputs and S3 backend blocks, reducing portability across regions/accounts.

Scope:
- `infrastructure/terraform/environments/dev/main.tf`
- `infrastructure/terraform/environments/staging/main.tf`
- `infrastructure/terraform/environments/prod/main.tf`
- `infrastructure/terraform/environments/*/backend.hcl`
- `tests/test_terraform_environment_config.py`

Acceptance Criteria:
- Environment module region inputs use `var.aws_region` instead of hardcoded region literals.
- Environment `main.tf` files use partial backend blocks (`backend "s3" {}`) and backend settings are provided via per-env config files.
- Backend config files keep bucket/key/lock defaults but do not hardcode `region`.
- Regression tests verify env Terraform wiring remains overrideable.

Estimate: 0.5 day

### T-47: Harden Backfill Bronze/Catalog Writes and PyArrow Failure Path (TD-080, TD-082)

Priority: P1

Description: Backfill writes previously only produced Silver temp files and silently continued when `pyarrow` was missing. Add Bronze persistence + catalog coverage updates and fail-fast semantics for missing parquet dependencies.

Scope:
- `heber/backfill/__init__.py`
- `tests/test_backfill_writer_reliability.py`

Acceptance Criteria:
- `BackfillWriter.write_batch()` writes raw records to Bronze partitions in addition to Silver temp outputs.
- Backfill coordinator updates catalog dataset entries and coverage metadata for completed chunks (best effort when catalog DB is unavailable).
- Missing `pyarrow` now raises a runtime error so backfill jobs fail instead of silently reporting progress.
- Regression tests cover Bronze+Silver write behavior, missing-`pyarrow` failure behavior, and catalog metadata updater invocation.

Estimate: 1 day

### T-48: Persist Backfill Jobs and Resume Progress After Restart (TD-081)

Priority: P1

Description: Backfill coordinator state was process-local only. Persist job state and per-date progress so restarts do not drop active/incomplete backfills.

Scope:
- `heber/backfill/__init__.py`
- `tests/test_backfill_job_persistence.py`

Acceptance Criteria:
- Backfill jobs are persisted on create/start/progress/complete/failure/cancel transitions.
- Coordinator startup reloads persisted jobs from disk.
- Persisted progress drives `_generate_chunks()` so resumed runs skip completed dates.
- Stale persisted `running` jobs recover to a resume-safe status on startup.
- Regression tests verify reload, failure + restart resume, and stale-running recovery semantics.

Estimate: 1 day

### T-49: Align Backfill Gap Detection With Silver Layout Variants (TD-083)

Priority: P1

Description: `GapDetector` only scanned legacy `silver/{provider}_{feed}/dt=*` paths, while canonical Silver writes use `silver/feed={feed}/instrument_type=.../dt=*`. This caused false positives for missing coverage.

Scope:
- `heber/backfill/__init__.py`
- `tests/test_backfill_gap_detector_layout.py`

Acceptance Criteria:
- Gap detection discovers `dt=*` partitions under both legacy provider-feed and canonical feed/instrument-type Silver trees.
- Date coverage is unioned across discovered layouts for the same provider/feed query.
- Regression tests cover legacy-only, canonical-only, and mixed-layout scenarios.

Estimate: 0.5 day

### T-50: Pin Backtest Label Reads to Explicit Version (TD-084)

Priority: P1

Description: Backtest label loads previously called `read_gold()` without a `version`, allowing unintended label version drift across experiments.

Scope:
- `heber/backtest/integration.py`
- `heber/backtest/tests.py`

Acceptance Criteria:
- `BacktestDataLoader` accepts `label_version` and passes it to label `read_gold()` calls in train/test loaders.
- Default label version remains deterministic (`"latest"` unless explicitly set).
- Regression tests verify explicit label-version pass-through and default behavior.

Estimate: 0.5 day

### T-51: Persist Backtest As-Of Metadata for Reproducibility (TD-085)

Priority: P1

Description: Backtest artifacts captured dataset/version but not the as-of cutoffs used to read historical features/labels, weakening experiment reproducibility.

Scope:
- `heber/backtest/integration.py`
- `heber/backtest/tests.py`

Acceptance Criteria:
- `ExperimentConfig` supports feature/label as-of timestamps and includes them in serialized config + reproducibility checklist output.
- `BacktestResult` stores dataset as-of metadata in persisted results.
- `ExperimentTracker.log_fold()` supports per-fold as-of timestamps in fold metadata.
- Regression tests verify round-trip persistence and checklist/result inclusion of as-of fields.

Estimate: 0.5 day

### T-52: Align Gold Retention Scanning and Version Ordering (TD-051, TD-055)

Priority: P1

Description: Retention scanning only looked for `<layer>/<dataset>/dt=*` and missed canonical Gold key-value layouts (`dataset=.../(project|type)=.../version=...[/dt=...]`). Version pruning also sorted versions lexicographically, which can retain older semver versions.

Scope:
- `heber/retention/__init__.py`
- `tests/test_retention_gold_layout.py`

Acceptance Criteria:
- Gold partition scanning discovers canonical `dataset=.../project=.../version=.../dt=*` paths and `dataset=.../type=.../version=...` label paths without `dt=*`.
- `PartitionInfo.version` is populated for scanned Gold partitions so version pruning has real version keys.
- Gold version retention keeps newest semantic versions (for example, keeps `v1.10.0` over `v1.2.0`) with deterministic fallback ordering for non-semver strings.
- Regression tests cover project-layout scan, label-layout scan without `dt=*`, and semantic-version pruning behavior.

Estimate: 0.5 day

### T-53: Enforce Hot Store/DLQ Retention and Config-Aligned Defaults (TD-052, TD-053)

Priority: P1

Description: Retention configs defined `HOT_STORE`/`DLQ` policies but scheduler execution only processed Bronze/Silver/Gold. Defaults also used hardcoded `/data/heber` roots, diverging from configured data-root behavior.

Scope:
- `heber/retention/__init__.py`
- `tests/test_retention_gold_layout.py`

Acceptance Criteria:
- `ReaperScheduler._process_dataset()` evaluates `HOT_STORE` and `DLQ` layers with their configured retention policies.
- `DatasetRetentionConfig` includes explicit `hot_store` and `dlq` policy fields in serialized config output.
- `create_reaper()` and `ReaperWorker` resolve default storage roots from explicit args, `HEBER_DATA_ROOT`, then shared settings (with a legacy fallback).
- Archive-root defaults are derived from the resolved storage root when not explicitly provided.
- Regression tests verify scheduler coverage for all layers and configuration-aligned default root resolution.

Estimate: 0.5 day

### T-54: Harden Label Read Version Selection and PIT Guarding (TD-050, TD-054)

Priority: P1

Description: Label reads previously selected latest `version=*` folders lexicographically and returned unfiltered rows when `ts_available` was missing, creating correctness and leakage risks.

Scope:
- `heber/gold/labels.py`
- `heber/gold/label_tests.py`

Acceptance Criteria:
- `read_label()` resolves latest versions using semantic-version-aware ordering with stable fallback behavior for non-semver version names.
- `read_label()` fails closed when `ts_available` is missing (raise by default), with an explicit compatibility override path.
- Regression tests verify latest-version semantics (`v1.10.0` over `v1.2.0`) and fail-closed behavior for malformed label datasets.

Estimate: 0.5 day

### T-55: Persist Dead-Letter Queue State Across Restarts (TD-044)

Priority: P1

Description: `DeadLetterQueue` existed only in-process, so restarts dropped failed events and removed replay visibility.

Scope:
- `heber/ops/reliability.py`
- `tests/test_dead_letter_queue_persistence.py`

Acceptance Criteria:
- `DeadLetterQueue` supports optional persisted storage path with atomic writes.
- Startup reload restores prior DLQ events and counters when a persisted file is present.
- Queue add/retry/pop mutations persist state consistently.
- Regression tests verify restart recovery, retry-attempt persistence, and persisted pop/reprocess semantics.

Estimate: 0.5 day

### T-56: Harden Firewall SCD Validity Join + Strict Validation Gates (TD-045, TD-046)

Priority: P1

Description: Firewall SCD joins assumed suffixed validity column names that are not always present after Polars joins, and strict Gold validation treated warning-level checks as hard failures.

Scope:
- `heber/firewall/scd.py`
- `heber/firewall/validation.py`
- `tests/test_firewall_scd_and_validation.py`

Acceptance Criteria:
- `join_with_reference_asof()` correctly resolves reference validity columns when they are suffixed and when they remain unsuffixed after join.
- Join paths fail explicitly with actionable errors when validity columns are missing entirely.
- `validate_gold_build(strict=True)` raises only for hard leakage gates and does not raise for warning-only metadata inconsistencies.
- Regression tests cover both SCD column-resolution modes and strict warning-only validation behavior.

Estimate: 0.5 day

### T-57: Align Silver Model Defaults and Schema Types (TD-047, TD-048, TD-049)

Priority: P1

Description: Silver model contracts had drift between Pydantic and Arrow representations (`lineage` dict vs string, global `v1` schema-version defaults, and mixed string/date expiry typing).

Scope:
- `heber/models/silver.py`
- `heber/schemas/silver.py`
- `tests/test_silver_model_schema_alignment.py`

Acceptance Criteria:
- `SilverBase` normalizes dict lineage input to deterministic JSON strings so model values match string-backed schema expectations.
- `SilverBase` applies release-aware default `schema_version` values for v2-v6 dataset families while preserving explicit overrides.
- `MaxPainRecord`, `HottestChainRecord`, and `IVTermStructureRecord` use `date`-typed expiry fields, and canonical Arrow schemas use `pa.date32()` for those feeds.
- Regression tests cover lineage normalization, schema-version defaults, and model/schema date-type alignment.

Estimate: 0.5 day

### T-58: Feast Materialization/Search Behavior Alignment (TD-056, TD-057, TD-058)

Priority: P1

Description: Feast helper defaults and outputs drifted from operational expectations (hardcoded repo path, placeholder `-1` row counts, and key-only tag filtering).

Scope:
- `heber/config.py`
- `heber/feast/materialization.py`
- `.env.example`
- `docs/configuration.md`
- `tests/test_feast_materialization_behavior.py`
- `tests/test_sdk_catalog_defaults.py`

Acceptance Criteria:
- Feast helper default repo path resolves from `settings.feast_repo_path`, supporting `HEBER_FEAST_REPO_PATH` (and legacy `FEAST_REPO_PATH`) configuration.
- `materialize_features()` no longer emits hardcoded `-1` row counts; it uses Feast-provided counts when available and falls back to offline-source row estimation.
- `search_features()` supports case-insensitive key, value, and `key:value` tag filtering.
- Regression tests cover default path wiring, count extraction/estimation behavior, and value-aware tag filtering.

Estimate: 0.5 day

### T-59: Wire Runtime Metrics Helpers Into Core Services (TD-014)

Priority: P1

Description: Shared Prometheus metrics helpers were defined but only weakly connected to live code paths, leaving core ingestion/storage telemetry sparse.

Scope:
- `heber/writer/consumer.py`
- `heber/writer/silver.py`
- `heber/writer/compactor.py`
- `tests/test_metrics_runtime_wiring.py`

Acceptance Criteria:
- Consumer event processing records received/processed status metrics and anti-leakage ingest/availability lag metrics.
- Consumer loop records batch-size metrics for processed stream batches.
- Silver flush writes record rows/bytes/duration metrics and emit write-error metrics for failed flush paths.
- Compactor records success/error run metrics with merged-file and reclaimed-byte values for actual compaction attempts.
- Regression tests validate instrumentation calls for consumer, Silver writer, and compactor runtime paths.

Estimate: 0.5 day

### T-60: Watch Timestamp + Polling Cadence Hardening (TD-031, TD-032)

Priority: P1

Description: Watch model timestamps were still defaulting to naive UTC and poller quote fetches were not cadence-aware per horizon, causing avoidable over-polling for long-horizon watches.

Scope:
- `heber/watch/models.py`
- `heber/watch/poller.py`
- `tests/test_watch_async_redis.py`

Acceptance Criteria:
- `AlertWatch.created_at` and `AlertWatch.updated_at` default to timezone-aware UTC timestamps.
- Poller evaluates per-watch due status from horizon interval configuration and skips quote fetches for not-yet-due watches.
- Polling stats expose due-watch counts so behavior is observable in tests/runtime logs.
- Regression tests cover aware timestamp defaults and cadence gating between intraday and long-horizon watches.

Estimate: 0.5 day

### T-61: Enforce Instrument-Key Validation In Consumer (TD-013)

Priority: P1

Description: Instrument-key validation existed in envelope helpers but was not enforced in ingestion, allowing malformed keys to flow into Bronze/Silver storage.

Scope:
- `heber/models/envelope.py`
- `heber/writer/consumer.py`
- `tests/test_writer_consumer_reliability.py`

Acceptance Criteria:
- Consumer processing validates `instrument_key` format against `instrument_type` before persistence.
- Invalid keys fail processing before Bronze/Silver writes.
- Existing retry/DLQ behavior handles invalid-key failures consistently as processing errors.
- Regression tests verify invalid-key rejection and confirm no writes occur for rejected records.

Estimate: 0.5 day

### T-62: Normalize Watch Timing Features To Market Timezone (TD-019)

Priority: P1

Description: Watch feature extraction computed hour/day/session timing directly from raw alert timestamps without ET normalization, producing incorrect market-time features when source timestamps are UTC.

Scope:
- `heber/watch/features.py`
- `tests/test_watch_feature_timezones.py`

Acceptance Criteria:
- Alert timestamp normalization converts aware timestamps to `America/New_York` before timing-feature extraction.
- Naive alert timestamps are treated as UTC before market-time conversion.
- `hour_of_day`, `minute_of_hour`, `day_of_week`, `minutes_since_open`, and `minutes_to_close` derive from normalized market time.
- Regression tests validate aware UTC conversion and naive-as-UTC equivalence.

Estimate: 0.5 day

### T-63: Unify Watch Data Gateway Endpoint Paths (TD-020)

Priority: P1

Description: Watch-service gateway routes drifted between callers (`/api/v1/...` in poller/consumer vs unprefixed `/alpaca` and `/uw` paths in feature enrichment), creating brittle runtime behavior across deployments.

Scope:
- `heber/watch/gateway.py`
- `heber/watch/poller.py`
- `heber/watch/consumer.py`
- `heber/watch/features.py`
- `tests/test_watch_gateway_paths.py`

Acceptance Criteria:
- Watch modules use a shared Data Gateway endpoint-construction helper.
- Gateway calls attempt `/api/v1`-prefixed routes first and can fall back to legacy unprefixed routes.
- Poller quote fetch, consumer entry-price fetch, and feature-enrichment HTTP calls all use the shared strategy.
- Regression tests validate candidate ordering and fallback behavior for poller and consumer paths.

Estimate: 0.5 day

### T-64: Align Meta-Label Paths + Persist Features To Gold (TD-021, TD-022)

Priority: P1

Description: Meta-label dataset defaults were hardcoded to `/tmp/heber/gold` and feature extraction wrote only to Redis, leaving builder parquet reads disconnected from live watch ingestion outputs.

Scope:
- `heber/ml/datasets.py`
- `heber/watch/features.py`
- `heber/watch/consumer.py`
- `tests/test_meta_label_dataset_paths.py`
- `tests/test_watch_feature_persistence.py`

Acceptance Criteria:
- `DatasetConfig` defaults resolve outcomes/features roots from configured `settings.gold_path` canonical dataset layout.
- Dataset loaders support legacy-path fallback for historical watch-output layouts.
- Watch feature extraction persists rows to Gold partitions during alert processing (while preserving Redis cache behavior when configured).
- Feature partition writes are append-safe and avoid clobbering existing partition data.
- Regression tests cover default path wiring, fallback loading, append-safe persistence, and consumer persistence invocation.

Estimate: 0.5 day

### T-65: Persist Training Feature Order For Inference (TD-023)

Priority: P1

Description: Meta-model inference previously relied on `AlertFeatures.numeric_feature_names()` while training used dataset-derived feature ordering, creating a mismatch risk when dataset feature columns changed order/content.

Scope:
- `heber/ml/trainer.py`
- `heber/ml/inference.py`
- `tests/test_meta_feature_order_contract.py`

Acceptance Criteria:
- Trainer stores training feature names in model config artifacts during train/save.
- Loaded models retain stored feature-name ordering.
- Inference scorer uses stored training feature names for feature-vector construction when available.
- Regression tests validate save/load feature-name persistence and inference feature-order usage.

Estimate: 0.5 day

### T-66: Align Soda Silver Path + Non-Null Threshold Reporting (TD-024, TD-025)

Priority: P1

Description: Data quality defaults diverged from shared storage settings (`/Volumes/heber/silver` vs `/Volumes/heber/data/silver`), and non-null column reporting used a hard-coded 0.99 threshold instead of each contract threshold.

Scope:
- `heber/quality/soda_scanner.py`
- `heber/quality/contracts.py`
- `heber/quality/tests.py`
- `tests/test_quality_soda_contracts.py`

Acceptance Criteria:
- `SodaConfig` defaults and `from_env()` fallback resolve `silver_path` from `settings.silver_path`.
- `HEBER_SILVER_PATH` override remains supported.
- Non-null column threshold classification uses the active contract threshold, not a hard-coded cutoff.
- Regression tests cover both Soda default-path wiring and threshold-specific column reporting.

Estimate: 0.5 day

### T-67: Revalidate Framework Schedule API + Align Test Environment Ports (TD-026, TD-027)

Priority: P1

Description: Debt audit flagged missing `E2ETestSuite.get_schedule()` and local test-environment port drift. Current framework API exists, but explicit regression coverage and default port alignment were still needed for sustained conformance.

Scope:
- `heber/testing/environments.py`
- `heber/testing/framework.py`
- `heber/testing/tests_framework.py`
- `tests/test_testing_environment_defaults.py`

Acceptance Criteria:
- `E2ETestSuite.get_schedule()` remains present and regression-tested.
- Local environment defaults match docker-compose host mappings for Postgres/Redis (`5433:5432`, `6380:6379`).
- Regression tests validate both schedule API availability and local service port alignment.

Estimate: 0.5 day

### T-68: Use PyIceberg PartitionSpec For Silver Table Creation (TD-028)

Priority: P1

Description: Iceberg Silver table creation previously passed a list-style partition definition to `partition_spec`, which risks incompatibility with PyIceberg APIs that expect `PartitionSpec` objects.

Scope:
- `heber/storage/iceberg_catalog.py`
- `tests/test_iceberg_partition_spec_contract.py`

Acceptance Criteria:
- `create_silver_table()` passes a concrete `PartitionSpec` object to `catalog.create_table()`.
- Partition transform remains day-based on `ts_event`.
- Regression test prevents reintroduction of list-based `partition_spec` wiring.

Estimate: 0.5 day

### T-69: Align Quarantine Partition Keys With Canonical Envelope Fields (TD-029)

Priority: P1

Description: Quarantine writes previously partitioned using only `envelope.meta.provider/feed`, but canonical `EventEnvelope` stores these fields at the top level. This caused partition path drift when `meta` was absent.

Scope:
- `heber/bus/backpressure.py`
- `tests/test_backpressure_quarantine_paths.py`

Acceptance Criteria:
- Quarantine partition extraction prefers top-level `provider`/`feed` fields.
- Legacy `meta.provider/feed` fallback remains supported for older payload shapes.
- Regression tests cover canonical and legacy envelope partition-path behavior.

Estimate: 0.5 day

### T-70: Guard Terraform Root-Module Output Contracts (TD-073)

Priority: P1

Description: Root Terraform configuration can reference module outputs that are renamed/removed in local modules. Add static regression coverage so root output references stay aligned with module output declarations.

Scope:
- `tests/test_terraform_module_sources.py`
- `tests/test_terraform_root_module_contract.py`
- `infrastructure/terraform/main.tf`
- `infrastructure/terraform/modules/*/main.tf`

Acceptance Criteria:
- Tests fail when `infrastructure/terraform/main.tf` references a local module output not declared by that module.
- Existing module source-path checks remain in place.
- Audit docs/changelog capture pass-level revalidation of `TD-073`.

Estimate: 0.5 day

### T-71: Align Overlay Image Transformers With Base-Rewritten Name (TD-077)

Priority: P1

Description: Overlay image-tag rules targeted `name: heber` while base kustomization rewrote images to `ghcr.io/jacobmcmillan/heber`. This caused overlay tags to be ignored during render for some environments.

Scope:
- `k8s/base/kustomization.yaml`
- `k8s/overlays/dev/kustomization.yaml`
- `k8s/overlays/staging/kustomization.yaml`
- `k8s/overlays/prod/kustomization.yaml`
- `tests/test_k8s_kustomize_image_tags.py`

Acceptance Criteria:
- Overlay image rules target the base-rewritten image name and apply expected tags per env.
- Regression tests validate both YAML-level image-rule contracts and rendered `kubectl kustomize` image tags.
- Audit docs/changelog record `TD-077` closure and pass-level revalidation.

Estimate: 0.5 day

### T-72: Add Namespace-Scoped Runtime Prerequisites To Base Kustomize (TD-078)

Priority: P1

Description: Deployments require `serviceAccountName: heber` and `secretRef: heber-secrets`, but these prerequisite resources were not consistently present in rendered overlays because base kustomization did not include service-account and external-secret resource manifests.

Scope:
- `k8s/base/kustomization.yaml`
- `k8s/base/serviceaccount.yaml`
- `k8s/base/secrets/cluster-secret-store.yaml`
- `k8s/base/secrets/external-secret.yaml`
- `tests/test_k8s_namespace_prerequisites.py`

Acceptance Criteria:
- Base kustomization resources include service-account and external-secret manifests required by deployments.
- Rendered overlays include `ServiceAccount heber`, `ExternalSecret heber-secrets`, and `ClusterSecretStore aws-secrets-manager`.
- Regression tests validate rendered overlay prerequisites and deployment env/serviceaccount references.
- Audit docs/changelog capture `TD-078` closure and pass-level revalidation.

Estimate: 0.5 day

### T-73: Expand Deployment Entrypoint Conformance Coverage (TD-074)

Priority: P1

Description: Initial runtime-entrypoint checks guarded only a subset of deployment manifests. Expand conformance coverage so all base deployment command modules remain importable and legacy missing paths cannot reappear unnoticed.

Scope:
- `tests/test_runtime_entrypoints.py`
- `k8s/base/deployments/catalog.yaml`
- `k8s/base/deployments/consumer.yaml`
- `k8s/base/deployments/writer.yaml`
- `k8s/base/deployments/compactor.yaml`
- `k8s/base/deployments/hotloader.yaml`
- `k8s/base/deployments/backfill.yaml`

Acceptance Criteria:
- Regression tests validate Python command-module wiring for every base deployment.
- Legacy missing module paths remain explicitly blocked.
- Audit docs/changelog record pass-level revalidation for `TD-074`.

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
23. T-23 (Flow feature time-window hardening)
24. T-24 (Lifecycle async shutdown wait hang fix)
25. T-25 (Lifecycle shutdown timeout status fix)
26. T-26 (Logging level filtering)
27. T-27 (Dedupe rotation/backing-store policy)
28. T-28 (lakeFS storage namespace configurability)
29. T-29 (K8s HPA/probe conformance)
30. T-30 (ClickHouse backup S3 destination alignment)
31. T-31 (Catalog backup cleanup trap)
32. T-32 (Security scan filesystem exit enforcement)
33. T-33 (Tracing optional-dependency hardening)
34. T-34 (Cross-platform volume init script)
35. T-35 (Labeling strategy doc refresh)
36. T-36 (Data contract doc alignment)
37. T-37 (Backfill deployment entrypoint fix)
38. T-38 (Hotloader runtime entrypoint)
39. T-39 (UW endpoint tracker summary reconciliation)
40. T-40 (Prometheus scrape/exporter alignment)
41. T-41 (MarketCalendar timezone input hardening)
42. T-42 (Extended-hours calendar flag behavior)
43. T-43 (Hot Store DDL base-column conformance)
44. T-44 (Additional schema test stability)
45. T-45 (lakeFS operation metrics coverage)
46. T-46 (Terraform environment region/backend parameterization)
47. T-47 (Backfill Bronze/catalog write reliability)
48. T-48 (Backfill job persistence and resume)
49. T-49 (Backfill gap-detection layout conformance)
50. T-50 (Backtest label-version pinning)
51. T-51 (Backtest as-of reproducibility metadata)
52. T-52 (Gold retention layout + semver pruning)
53. T-53 (Hot Store/DLQ retention + config-root defaults)
54. T-54 (Label semver + PIT guard hardening)
55. T-55 (Persistent DLQ queue state)
56. T-56 (Firewall join + strict gate hardening)
57. T-57 (Silver model defaults + schema type alignment)
58. T-58 (Feast materialization/search behavior alignment)
59. T-59 (Runtime metrics helper wiring)
60. T-60 (Watch timestamp + polling cadence hardening)
61. T-61 (Consumer instrument-key validation enforcement)
62. T-62 (Watch timing market-timezone normalization)
63. T-63 (Watch Data Gateway path unification)
64. T-64 (Meta-label path + feature persistence alignment)
65. T-65 (Training feature-order persistence for inference)
66. T-66 (Soda path + contract-threshold quality alignment)
67. T-67 (Framework schedule API + test-environment port alignment)
68. T-68 (Iceberg partition-spec object alignment)
69. T-69 (Quarantine partition-key envelope alignment)
70. T-70 (Terraform root-module output contract guard)
71. T-71 (Overlay image-transformer name alignment)
72. T-72 (Namespace-scoped runtime prerequisite conformance)
73. T-73 (Deployment entrypoint conformance expansion)
