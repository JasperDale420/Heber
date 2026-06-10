# Heber Repository Hygiene Audit

> ⚠️ **STALE — superseded by [audit-2026-06-10.md](audit-2026-06-10.md).** Several findings below are obsolete as of 2026-06-10: the Prometheus metric-collision that blocked pytest is fixed (full suite collects and passes), and the `heber/hotstore/` ClickHouse findings reference code that no longer exists. Verify against the current tree before acting on anything here.

> **Date**: 2026-02-07
> **Scope**: Full codebase — 85 source files, 63 test files, 24,495 LOC (Python)
> **Tools used**: ruff, bandit, detect-secrets, pytest, docker compose, git

---

## Summary

| Category | Status | Issues Found |
|---|---|---|
| Lint (ruff) | ✅ Clean | 0 violations |
| Unused imports (F401) | ✅ Clean | 0 |
| Security (bandit) | ⚠️ 5 medium | 2× bind `0.0.0.0`, 3× SQL injection |
| Secret scanning | ✅ Baseline current | 8 files tracked |
| Test suite | ❌ Blocked | 10 collection errors → 0/170 tests run |
| Dependencies | ❌ Conflict | `openmetadata-ingestion` vs SQLAlchemy ≥2.0 |
| .gitignore | ❌ Incomplete | 81 `.pyc` files tracked; missing common patterns |
| Pre-commit | ⚠️ Partial | mypy + bandit disabled (495+ type errors) |
| Docker/Compose | ⚠️ Stale ref | `heber-redis` container defined but unused |
| K8s manifests | ⚠️ Duplicate | `writer` deployment mirrors `consumer` |
| Documentation | ✅ Current | Gaps closed in audit pass 1 + 2 |

---

## 1. Linting & Code Quality

### ✅ Ruff — Clean

```
ruff check heber/ --statistics → 0 violations
ruff check heber/ --select F401 → All checks passed!
```

Config: line-length=120, target=py311, rules: E, F, I, UP, B.

### ⚠️ Mypy — Disabled (495+ errors)

Mypy is commented out in `.pre-commit-config.yaml` due to 495+ type errors in the existing codebase. Type coverage should be addressed incrementally.

**Fix needed**: Triage the 495+ errors, enable mypy on a per-module basis starting with `heber/config.py`, `heber/models/`, and `heber/reader/`.

---

## 2. Security

### Bandit — 5 Medium Severity Issues

| # | Rule | File | Line | Description |
|---|---|---|---|---|
| 1 | B104 | `backfill/__main__.py` | 39 | Hardcoded bind to `0.0.0.0` |
| 2 | B104 | `config.py` | 79 | Hardcoded bind to `0.0.0.0` |
| 3 | B608 | `hotstore/client.py` | 164 | SQL injection via f-string (ClickHouse query) |
| 4 | B608 | `hotstore/client.py` | 186 | SQL injection via f-string (ClickHouse query) |
| 5 | B608 | `hotstore/sync.py` | 442 | SQL injection via f-string (ClickHouse query) |

> [!NOTE]
> B104 (`0.0.0.0`) is acceptable for containerized services. B608 (SQL injection) in ClickHouse queries uses internal-only table names derived from config, not user input — low actual risk, but should be parameterized.

**Fix needed**: Parameterize ClickHouse table names or add `# nosec B608` with justification comments.

### detect-secrets — Baseline Current

8 files tracked with known secrets (dev passwords in `.env.example`, `config.py` defaults). Baseline version 1.5.0 is current.

---

## 3. Test Suite

### ❌ Critical: All Tests Blocked by Prometheus Metric Collision

```
collected 170 items / 10 errors
ValueError: Duplicated timeseries in CollectorRegistry: {'heber', 'heber_info'}
```

**Root cause**: `heber/ops/metrics.py` registers Prometheus metrics at module import time (line 22: `heber_info = Info(...)`). When multiple test files import modules that transitively import `ops/metrics.py`, the global `CollectorRegistry` raises duplicate registration errors.

**Affected tests** (10 collection errors):

- `test_catalog_migrations.py`
- `test_compactor_safety.py`
- `test_dead_letter_queue_persistence.py`
- `test_event_deduplicator_rotation.py`
- `test_metrics_runtime_wiring.py`
- `test_silver_flush_config.py`
- `test_silver_schema_source.py`
- `test_tracing_no_otel.py`
- `test_worker_entrypoint_services.py`
- `test_writer_consumer_reliability.py`

**Fix needed**: Wrap metric definitions in a lazy-init pattern or use a custom `CollectorRegistry` per test. Example:

```python
# Option A: Use REGISTRY environment guard
from prometheus_client import CollectorRegistry, REGISTRY

_registry = CollectorRegistry() if os.getenv("TESTING") else REGISTRY
heber_info = Info("heber", "...", registry=_registry)

# Option B: conftest.py fixture
@pytest.fixture(autouse=True, scope="session")
def reset_prometheus():
    from prometheus_client import REGISTRY
    collectors = list(REGISTRY._names_to_collectors.values())
    for c in collectors:
        try:
            REGISTRY.unregister(c)
        except Exception:
            pass
```

**Impact**: ❌ Zero test coverage until this is fixed.

---

## 4. Dependencies

### ❌ Unsatisfiable Dependency Graph

```
heber[catalog] depends on openmetadata-ingestion>=1.4
openmetadata-ingestion requires sqlalchemy>=1.4.0,<2
heber core depends on sqlalchemy>=2.0
→ Unsatisfiable
```

The `[catalog]` optional dep group cannot be installed alongside core deps. This was previously acknowledged with a comment in `pyproject.toml` but `uv` fails resolution when both are present.

**Fix needed**: Either pin `openmetadata-ingestion` to a version that supports SQLAlchemy 2.x (if one exists), or remove the `[catalog]` optional group entirely and document OpenMetadata as a standalone service.

### Dependency Inventory

| Category | Key Packages |
|---|---|
| Core | pydantic ≥2.0, fastapi ≥0.109, sqlalchemy ≥2.0 |
| Data | pandas ≥2.0, pyarrow ≥15.0, polars ≥0.20 |
| Storage | pyiceberg ≥0.7, lakefs ≥0.7, clickhouse-connect ≥0.7 |
| Observability | structlog ≥24.0, prometheus-client ≥0.19 |
| Quality | soda-core-duckdb ≥3.0 |

---

## 5. Git Hygiene

### ❌ .gitignore — Severely Incomplete

Current `.gitignore` (2 lines):

```
__pycache__/
*.pyc
```

Despite the `*.pyc` rule, **81 `.pyc` files are tracked by git** (they were committed before the rule was added).

**Missing patterns** (standard Python project):

```gitignore
# Environments
.env
.venv/
venv/

# Caches
.mypy_cache/
.pytest_cache/
.ruff_cache/

# Build artifacts
*.egg-info/
dist/
build/

# IDE
.idea/
.vscode/
*.swp

# macOS
.DS_Store

# Coverage
htmlcov/
.coverage
```

**Fixes needed**:

1. Expand `.gitignore` with standard Python patterns
2. Remove tracked `.pyc` files: `git rm -r --cached '*.pyc'`

---

## 6. Docker & Deployment

### ⚠️ docker-compose.yml — Stale Redis Container

Line 27 defines `container_name: heber-redis`, but Heber services now connect to Data Gateway's Redis instance (`data-gateway-redis`). The `heber-redis` container is no longer needed for data flow.

**Fix needed**: Determine if `heber-redis` should be removed or repurposed. If removed, update `docker-compose.yml` and delete the service block.

### ⚠️ Dockerfile — Writer Stage Duplicates Consumer

```dockerfile
# Stage 4: Writer service
FROM runtime AS writer
CMD ["python", "-m", "heber.writer.consumer"]  # ← Same as consumer stage
```

The `writer` and `consumer` stages have identical CMD. This is either intentional (both run the same consumer code) or a copy-paste error.

**Fix needed**: Clarify intent. If they're the same service, remove the `writer` stage.

### Docker Compose Services

12 services defined: `clickhouse`, `elasticsearch`, `postgres`, `redis`, `heber-catalog`, `heber-consumer`, `heber-watch`, `minio`, `lakefs`, `apicurio-registry`, `heber-compactor`, `openmetadata`.

4 Heber services use `build:` (catalog, consumer, watch, compactor). External services use pinned images:

- `clickhouse/clickhouse-server:24.1`
- `postgres:16-alpine`
- `redis:7-alpine`
- `elasticsearch:8.11.3`
- `treeverse/lakefs:latest` ← ⚠️ not pinned
- `minio/minio:latest` ← ⚠️ not pinned
- `openmetadata/server:1.4.0`

**Fix needed**: Pin `lakefs` and `minio` images to specific versions for reproducibility.

---

## 7. K8s Manifests

### ⚠️ Writer Deployment Duplicates Consumer

K8s has 26 YAML files across `k8s/base/` and `k8s/overlays/`. The `deployments/writer.yaml` mirrors `deployments/consumer.yaml` — same CMD (`python -m heber.writer.consumer`), same structure.

Services with dedicated K8s manifests: `catalog`, `consumer`, `writer`, `compactor`, `backfill`, `hotloader`.

Overlay environments: `dev`, `staging`, `prod`.

**Fix needed**: If `writer` = `consumer`, remove the duplicate deployment + associated HPA/PDB/service manifests.

---

## 8. Pre-Commit Pipeline

### Active Hooks

- ✅ ruff (lint + format, v0.9.4)
- ✅ detect-secrets (v1.5.0, with baseline)
- ✅ Standard hooks (trailing-whitespace, end-of-file, check-yaml, check-added-large-files, check-merge-conflict, debug-statements)

### Disabled Hooks

- ❌ mypy — 495+ type errors
- ❌ bandit — "known security warnings"

**Fix needed**: Create a plan to incrementally enable mypy (per-module) and bandit (suppressions for known issues).

---

## 9. Documentation

### ✅ Completed in Audit Pass 1 + 2

- Fixed stale container refs (`heber-redis` → `data-gateway-redis`)
- Fixed DLQ commands (`LLEN` → `XLEN`)
- Added operational runbook
- Added missing README doc links
- Added missing env vars to `.env.example`
- Added watch service to architecture docs
- Added watch service settings to configuration docs

No further documentation gaps identified.

---

## 10. Code Patterns

### Bare `pass` Statements (25 instances)

Most are legitimate:

- Protocol/ABC stubs in `bus/__init__.py` (11 instances) — expected for abstract interface
- Exception class bodies in `firewall/validation.py`, `ops/circuit_breaker.py` — expected
- Try/except swallowing in `watch/consumer.py`, `feast/materialization.py` — **review needed** for silent error swallowing

**Fix needed**: Review the 4-5 try/except `pass` instances for potential silent error swallowing.

### Custom Exceptions

Only 2 custom exception types:

- `LeakageError` (`firewall/validation.py`)
- `CircuitOpenError` (`ops/circuit_breaker.py`)

**Consideration**: As the codebase grows, more domain-specific exceptions should be introduced.

---

## Priority Fix List

### 🔴 Critical (Blocks Development)

| # | Issue | Effort |
|---|---|---|
| 1 | Prometheus metric collision blocks all tests | ~1 hour |
| 2 | Expand `.gitignore` + remove tracked `.pyc` | ~15 min |

### 🟡 High (Code Quality)

| # | Issue | Effort |
|---|---|---|
| 3 | Resolve `openmetadata-ingestion` dependency conflict | ~30 min |
| 4 | Pin `lakefs` and `minio` Docker images | ~5 min |
| 5 | Remove or differentiate k8s `writer` vs `consumer` | ~30 min |
| 6 | Remove stale `heber-redis` from docker-compose | ~15 min |
| 7 | Remove duplicate Dockerfile `writer` stage | ~5 min |

### 🟢 Low (Incremental)

| # | Issue | Effort |
|---|---|---|
| 8 | Parameterize ClickHouse queries (bandit B608) | ~30 min |
| 9 | Enable mypy per-module incrementally | ~4 hours |
| 10 | Enable bandit in pre-commit with suppressions | ~1 hour |
| 11 | Review silent `except: pass` patterns | ~30 min |

---

## Still Needs Auditing

The following areas were **not covered** in this audit and should be reviewed in a future session:

- [ ] **Full mypy run** — run `mypy heber/ --ignore-missing-imports` and categorize the 495+ errors by module/severity
- [ ] **Test coverage analysis** — once Prometheus collision is fixed, run `pytest --cov=heber` to identify untested code paths
- [ ] **Unused function detection** — 549 public functions/methods exist; a dead-code analysis tool (e.g., `vulture`) would identify unused ones
- [ ] **Alembic migrations** — verify migrations are consistent with current SQLAlchemy models
- [ ] **K8s manifest validation** — run `kubectl --dry-run=client` or `kubeval` against manifests
- [ ] **Docker image build** — verify the Dockerfile builds successfully end-to-end
- [ ] **Feast feature store** — verify feature views in `features/` are consistent with current schemas
- [ ] **SonarQube scan** — run the local SonarQube instance for a comprehensive code quality report
- [ ] **License compliance** — verify all dependencies are license-compatible
- [ ] **API endpoint testing** — smoke test the Catalog API endpoints
