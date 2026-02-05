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
