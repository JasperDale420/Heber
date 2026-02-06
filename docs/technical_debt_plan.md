# Technical Debt Remediation Plan (High Severity)

Date: 2026-02-05

This plan converts high-severity audit items into ticket-ready tasks with clear scope and acceptance criteria. It references the audit IDs in `docs/technical_debt_audit.md`.

## Implementation Status

Updated: 2026-02-06

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
- `T-41` complete (`TD-068`): `MarketCalendar` now normalizes all supported datetime inputs to UTC before exchange conversion, assumes naive inputs are UTC, and includes regression tests for naive/aware/pandas timestamp handling.
- `T-42` complete (`TD-069`): `MarketCalendar` now fails fast for `include_extended=True` with explicit unsupported-mode messaging instead of silently ignoring the flag.
- `T-44` complete (`TD-072`): additional schema registry tests now validate required schema contracts and unknown-schema handling instead of asserting a fixed global schema count.
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
