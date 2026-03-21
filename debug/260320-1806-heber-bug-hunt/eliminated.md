## Disproven Hypotheses

### Timezone-naive datetime usage corrupts data
- **Result:** Disproven. All datetime usage correctly imports and uses `UTC` from `datetime`. No calls to `datetime.now()`, `datetime.utcnow()`, or `datetime.today()` found in source.

### Unclosed file handles / resource leaks
- **Result:** Disproven. All `open()` calls use context managers (`with` statement). No bare file handle assignments found.

### Mutable default arguments
- **Result:** Disproven. No `def f(x=[])` or `def f(x={})` patterns found in source (false positives from docstring path templates excluded).

### Bare except clauses
- **Result:** Disproven. No `except:` (bare) found. All broad catches use `except Exception` with `noqa: BLE001` annotations explaining why (top-level loops that must not crash).

### Field mapping / normalization bugs in ingest
- **Result:** Disproven. Normalizer is comprehensive with type coercion for float, int, date, timestamp, bool, list, string. Division-by-zero guards in place.

### Watch consumer error handling issues
- **Result:** Disproven. Retry logic, DLQ, batch processing, auth failure escalation all well-implemented.

### Compactor race conditions / data loss
- **Result:** Disproven. Uses `O_CREAT | O_EXCL` file locking, atomic temp-then-rename writes, and event_id deduplication on merge.

### Hardcoded secrets / credential leaks
- **Result:** Disproven. No `.env` files committed. No hardcoded secrets in source. Docker-compose uses `pragma: allowlist secret` on dev-only defaults.

### SQL injection in catalog API
- **Result:** Disproven. All database access via SQLAlchemy ORM, no raw SQL string concatenation.

### Path traversal vulnerabilities
- **Result:** Disproven. Reader/writer paths are constructed from config + enum values, not user input.

### DLQ reprocessor logic errors
- **Result:** Disproven. Cursor advancement, batch deletion, feed filtering, and Redis stream operations are all correct. `_advance_cursor` properly increments sequence numbers.

### SLO framework calculation errors
- **Result:** Disproven. Budget, burn rate, and health calculations are correct. PromQL rule generation produces valid threshold expressions.

### Daily health report timezone issues
- **Result:** Disproven. Uses `date.today()` as a CLI default (reasonable for local operator), and MarketCalendar for trading day detection with proper UTC handling.

### Retention policy unsafe deletion
- **Result:** Disproven. `DeletionSafetyChecker` validates pinned status and Gold lineage before deletion. Dry-run mode prevents accidental data loss. Atomic archive-then-delete for ARCHIVE action.

### Silver writer concurrent buffer mutation
- **Result:** Disproven. `flush_if_needed` iterates `self.buffers.items()` and sets values for existing keys — this doesn't change dict size and is safe in Python 3. No concurrent access (single-threaded event loop).

### Watch poller stale quote handling
- **Result:** Disproven. Multi-route fallback with stale tracking, best-partial selection, and proper staleness thresholds. Route failures are logged with full context.

### Backfill writer non-atomic Bronze writes
- **Result:** Noted but acceptable. Backfill Bronze writes go directly to final path (not temp-then-rename like the main BronzeWriter). This is intentional for backfill use case — data is idempotent and re-runnable.
