# Heber Technical Debt Audit

Date: 2026-02-05

This audit is based on static code inspection only. No services were run and no tests or linters were executed.

## Scope

Audited in this run (files reviewed directly):
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

Not yet audited in this run (recommend a future pass):
- docs/architecture.md and docs/operations/*
- infrastructure/ and k8s/
- scripts/ (including init and data tooling)
- features/ and heber/features/
- heber/watch/
- heber/ml/
- heber/quality/
- heber/sre/
- heber/testing/
- heber/storage/
- heber/universe/
- heber/backfill/ and heber/backtest/
- heber/catalog/service.py and heber/catalog/datasources.py
- heber/bus/
- heber/schemas/ and docs/schema_registry.md

## Executive Summary

The core architecture is clear, but several operational hazards and correctness gaps remain. The most urgent issues are test discovery (most in-package tests are not being executed), mismatched service ports (SDK defaults do not match docker-compose), invalid Dockerfile targets, and inconsistent Hot Store implementations. There are also multiple time-handling risks and data pipeline resiliency gaps that could lead to leakage or data loss.

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

## Suggested Remediation Plan

Phase 1 (Stabilize correctness, 1-2 days):
- Fix TD-001, TD-002, TD-003, TD-005.
- Add minimal regression tests for Silver flush and SDK default URL.

Phase 2 (Operational reliability, 2-4 days):
- Fix TD-006, TD-008, TD-010, TD-012.
- Add a DLQ stream and pending-entries recovery policy.

Phase 3 (Performance and maintainability, 3-7 days):
- Address TD-004, TD-007, TD-009, TD-011, TD-014.
- Unify Hot Store implementation and schema definitions.

## Open Questions for Future Audits

- How is schema evolution governed and enforced in production?
- What are the SLAs and current performance baselines for ingestion and Hot Store?
- Are there existing CI checks on GitHub Actions beyond linting and tests?
