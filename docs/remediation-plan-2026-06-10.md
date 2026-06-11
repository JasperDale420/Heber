# Remediation Implementation Plan — 2026-06-10

Execution plan for the remaining findings in [audit-2026-06-10.md](audit-2026-06-10.md). Milestone 0/1 items and the aspirational-layer prune already landed on `chore/audit-remediation` (commits 152cda3..0ebc3b9). This plan covers everything still open, orchestrated as parallel Opus subagent workstreams followed by an adversarial Codex review.

## Ground rules (every workstream)

- **Branch**: all work lands on `chore/audit-remediation` in the main working tree. Workstreams are file-disjoint and phased so no two agents touch the same file.
- **Do not edit `CHANGELOG.md`** — entries are consolidated at integration to avoid merge races. Each agent instead returns a draft changelog entry.
- **Do not commit** — the orchestrator commits per-workstream after verifying.
- **Known-failure baseline** (pre-existing, quarantined for a separate session — do NOT fix, do NOT break further):
  `test_consumer_concurrency.py::test_dlq_failure_under_concurrency_leaves_message_pending`, `test_data_gateway_rest_feed_contract.py::test_non_contracted_route_derived_feeds_are_blocked_from_silver`, `test_exception_hardening.py::TestConsumerSendToDlq::test_redis_error_returns_false`, `test_gold_labels.py::TestWriteAndReadLabel` (4), `test_gold_labels_integration.py::TestGoldLabelsRefactor` (2), `test_watch_gateway_key_contract.py::test_resolve_gateway_api_key_raises_when_missing`, `test_writer_coverage.py::TestSendToDlq::test_send_to_dlq_failure`.
  Acceptance for every workstream: `uv run pytest tests/ -q` shows **exactly these 11 failures and no others**.
- New tests use **fixed timestamps**, never bare `datetime.now()` assertions.
- Style: match surrounding code; ruff must pass (`ruff check .`).

## Phase 1 workstreams (parallel, file-disjoint)

### W1 — Config hardening: no default credentials outside dev (audit F-3, task 1.4)
**Files**: `heber/config.py`, new `tests/test_config_credential_guard.py`.
**Work**: when `environment != "dev"`, fail fast at settings load if `postgres_url` still carries `heber_dev_password` (or password came from the fallback default). Keep dev behavior unchanged. Use a Pydantic model validator; clear error message naming the env var to set.
**Accept**: prod/staging env without explicit credentials raises at startup with actionable message; dev unchanged; tests prove both.

### W2 — Deep behavioral tests: HeberReader (audit F-16, task 2.2a)
**Files**: `tests/test_heber_reader_edge_cases.py` (new only — do not modify `heber/reader/core.py` or existing tests).
**Work**: asof_join — gaps, out-of-order rows, exact-boundary `ts_available == left_time`, tolerance boundary, `ts_available > ts_event` (late availability respected), suffix collisions, empty right table. read_asof/read_silver — boundary equality on `ts_event` range ends, `prune_by_dt` partition-boundary correctness, `batch_size` path equivalence vs unbatched, column projection keeps essentials, mixed string/large_string/dictionary fragments unify. Use `tmp_path` Silver layouts; fixed timestamps.
**Accept**: ≥25 new behavioral assertions; all pass; reader line coverage ≥80% (`uv run pytest tests/test_heber_reader*.py --cov=heber.reader --cov-report=term`). If a test exposes a REAL reader bug, document it in the final report and mark the test `xfail(strict=True)` with the bug description — do not change reader code.

### W3 — Deep behavioral tests: normalizer coercion + label availability (audit F-16, task 2.2b)
**Files**: `tests/test_normalizer_coercion.py`, `tests/test_label_availability_math.py` (new only).
**Work**: normalizer — every coercion branch in `heber/writer/normalizer.py` (numeric strings, out-of-range, None-in-required, type mismatches, timezone handling), `enforce_required_non_null_fields` error details. Labels — `compute_availability_time` math across forward_window/availability_lag combinations incl. DST-crossing windows, `LabelMetadata` round-trip, `parse_duration` edge cases. Same xfail(strict) protocol for real bugs.
**Accept**: every public function in both modules exercised; all green.

### W4 — Feature-pipeline base extraction + single result shape (audit F-9/F-13, task 2.3)
**Files**: `heber/features/pipelines/*` , new `heber/features/pipelines/base.py`, `heber/gold_poller/service.py`, existing pipeline tests under `tests/` as needed, new `tests/test_pipeline_result_contract.py`.
**Work**: extract shared helpers (`_ensure_ts_available`, `_ensure_market_instrument_key`, lookback-window calc, empty-frame-with-OUTPUT_COLUMNS) into a base module/class; migrate all pipelines. Enforce ONE result shape — nested `{gold_dataset_name: {"status", "rows", "path"}}` — from every pipeline `run()`; delete the flat→nested adapter in `gold_poller/service.py` (lines ~429-443) and replace with a loud contract violation error. Contract test imports every registered pipeline and asserts the shape statically where feasible.
**Accept**: zero byte-identical duplicated helpers across pipelines (`grep -c "_ensure_ts_available" heber/features/pipelines/*.py` shows definitions only in base); adapter gone; all existing pipeline tests green.

### W5 — Watch enrichment observability + hotspot decomposition (audit F-14/F-15, tasks 3.3+3.4)
**Files**: `heber/watch/features.py`, existing watch tests as needed, new `tests/test_watch_enrichment_failures.py`.
**Work**: (a) add `enrichment_failures: list[str]` (or per-field flags) to `AlertFeatures` so callers distinguish complete from partial features; populate it in each `except` block that currently logs-and-continues; narrow `except Exception` to specific exception types where the call site allows (httpx errors, KeyError/ValueError on payload shape). (b) Split `backfill_uw_fields()` (~lines 1171-1342) into `_backfill_gex_vex()` + `_backfill_market_tide()`; decompose `extract()`'s 8-step chain into named steps with a data-driven enrichment table. Pure refactor — behavior identical, existing tests must stay green unmodified except where they assert internals.
**Accept**: `backfill_uw_fields` ≤40 lines orchestrating two strategy functions; `AlertFeatures` exposes failure visibility with tests; all watch tests green.

### W6 — Docs consolidation + repo-root scripts (audit F-19/F-20, tasks 3.1+3.2)
**Files**: `docs/` (UPPERCASE set), `README.md`, move `test_read_trades.py`, `find_bad_trades.py`, `run_compact_trades.py` → `scripts/debug/`.
**Work**: move legacy UPPERCASE docs to `docs/legacy/` with a banner line pointing to the lowercase successor; fix any README claims that contradict the post-prune tree (verify each command/path actually works — note Redis 6380 is CORRECT for native, compose uses host Redis 6379 internally; clarify rather than "fix"). `git mv` the three root scripts into `scripts/debug/` and update any references.
**Accept**: repo root has no stray scripts; every doc link in README resolves; no doc contradicts the code.

### W7 — Dedupe store benchmark (audit F-6 follow-up; decision: enable if <5% overhead)
**Files**: new `scripts/debug/bench_dedupe_store.py` only (read-only otherwise — config flip happens in Phase 2).
**Work**: microbenchmark `RedisDedupeStore.add`/`contains` and `EventDeduplicator.check+register` against a real Redis. If `redis://localhost:6380` is unreachable, start an ephemeral container (`docker run --rm -d -p 16399:6379 redis:7-alpine`) and clean it up. Measure: ops/sec and per-event overhead of register (Bloom+Redis vs Bloom-only) at 10k events; report projected overhead at 1,215 events/sec average and 5k/sec peak. Also measure a pipelined-batch register variant (redis-py `pipeline()`, batch 100) for comparison.
**Accept**: benchmark script committed; final message reports numbers and a recommendation (enable / enable-with-pipelining / keep off) with arithmetic shown.

## Phase 2 workstreams (after Phase 1 integrates)

### W8 — Quality gates: bandit + mypy-strict in CI and pre-commit (audit F-4, task 2.4)
**Files**: `.pre-commit-config.yaml`, `.github/workflows/ci.yaml`, `pyproject.toml`, and type fixes confined to the strict module list: `heber/reader/*`, `heber/config.py`, `heber/core/*`, `heber/universe/*`, `heber/utils/*`.
**Work**: re-enable bandit (severity high, confidence high) in pre-commit and as a blocking CI step; add a blocking CI step `mypy heber/reader heber/config.py heber/core heber/universe heber/utils` and fix all errors in those modules (annotations only — no behavior changes).
**Accept**: both gates run clean locally; CI yaml valid; full suite still at baseline-only failures.

### W9 — Time-dependent test sweep (audit F-17, tasks 0.4+3.6)
**Files**: existing test files using wall-clock time; `pyproject.toml` dev extras if `freezegun` is added.
**Work**: audit all `datetime.now(`/`date.today(` in `tests/` (~101 sites). Classify: (a) assertion-fragile (test can fail near midnight/DST/weekend) → freeze or inject fixed times; (b) benign payload timestamps → convert to module-level fixed constants where trivial, else leave. Fix ALL of class (a) — start with `test_watch_performance_verification.py:73`'s hour-arithmetic.
**Accept**: zero class-(a) sites remain (report the classification table); suite green at baseline.

### W10 — Dedupe enablement (conditional on W7 result)
**Files**: `heber/config.py`, `heber/ops/reliability.py`/`heber/writer/consumer.py` only if pipelining is needed, `.env.example`.
**Work**: if W7 measured <5% overhead → flip `dedupe_redis_enabled` default to `True` and document the measured cost in the field description. If 5%+ → implement pipelined batch registration (buffer event_ids, flush via Redis pipeline in `_flush_layers`, registering only after successful flush keeps semantics), re-measure projection, then flip if it now clears the gate. If still failing the gate, leave off and say so.
**Accept**: decision implemented with the benchmark numbers cited; dedupe tests green.

## Phase 3 — Integration & verification (orchestrator)

1. Per-workstream commits with consolidated CHANGELOG entries.
2. Full suite: exactly the 11 baseline failures. `ruff check .` clean. `docker compose config -q` valid.
3. `uv run pytest tests/test_heber_reader*.py --cov=heber.reader` ≥80%.

## Phase 4 — Adversarial Codex review

Run the Codex rescue skill (`gpt-5.5-codex`, reasoning effort xhigh) over `git diff <pre-workflow-sha>..HEAD` with a brief: hunt for correctness bugs, behavior changes disguised as refactors (especially W4/W5), leakage-contract violations, and test weakening; Codex fixes what it finds, orchestrator re-verifies the baseline and commits fixes.

## Explicitly out of scope

The 11 baseline test failures (separate spawned session); catalog token-auth wiring (Mac-only deployment, loopback binds cover it); Discord webhook rotation (user action); EmpireUI/monorepo-side changes.
