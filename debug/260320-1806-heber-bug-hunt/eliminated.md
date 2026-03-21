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

### Feature pipeline symbol column bugs in non-equity pipelines
- **Result:** Disproven. Grep for `["symbol"] =` across all feature pipelines confirmed only `equity_features.py` had the `instrument_key` copy-paste bug (already fixed). `darkpool_features.py` correctly uses `result["underlying"]`. Other pipelines (`flow_context`, `flow_toxicity`, `flow_normalization`, `oi_momentum`, `sector_flow`, etc.) use `underlying` directly or have no symbol column.

### Division-by-zero in market sentiment computation
- **Result:** Disproven. `market_tide_context_features.py` handles zero total premium with explicit guard: `daily.loc[total == 0, "market_sentiment_score"] = 0.0`.

### OLS regression degenerate case in trend_scan_features
- **Result:** Disproven. `compute_ols_t_stat` has thorough guards: checks for `n < 2`, all-NaN, zero std, `ss_tt == 0`, `mse <= 0`, and `se_beta == 0`. All edge cases return `(NaN, NaN)`.

### Market intel pipeline missing symbol column
- **Result:** Disproven (not a bug). `compute_darkpool_features`, `compute_greek_exposure_features`, `compute_options_sentiment_features` keep `instrument_key` as their primary key. They don't need a separate `symbol` column since they operate at the instrument_key level.

### Watch consumer alert processing concurrency bugs
- **Result:** Disproven. `_dispatch_messages` processes messages sequentially within a batch. Retry logic correctly uses `_normalize_process_result` to handle both bool and tuple returns. DLQ write failures are caught and logged without crashing the consumer.

### Runtime retry exponential backoff overflow
- **Result:** Disproven. `calculate_retry_delay` has `min()` cap at `max_seconds` (default 30s), and `normalized_attempt = max(1, attempt)` prevents zero/negative exponents. Jitter is additive-only (never negative).

### Normalizer type coercion edge cases
- **Result:** Disproven. `_coerce_value` wraps all coercion in try/except returning None on any error. Each type handler (`_coerce_float`, `_coerce_int`, `_coerce_date`, etc.) handles empty strings and invalid formats. No unguarded conversions.

### Feature extractor response cache unbounded growth
- **Result:** Disproven (bounded in practice). `_response_cache` in `AlertFeatureExtractor` is a dict with TTL-based expiration on read. Since alerts are processed one at a time and cache TTL is 30 seconds, the cache size is bounded by the number of unique symbols with active alerts within a 30-second window.

### Barrier labeling logic errors
- **Result:** Disproven. `_compute_barrier_outcome` correctly handles call/put direction flipping, empty price paths (returns NaN), slippage adjustment, edge_ratio computation, and time_efficiency. ATR computation delegates to canonical volatility module with per-instrument groupby. All edge cases guarded.

### Write audit null detection false positives
- **Result:** Disproven. `audit_null_fields` only checks columns in `EXPECTED_NON_NULL` for known datasets; unknown datasets check all columns. The audit is observe-and-continue (doesn't block writes). Pandas and PyArrow both handled correctly.
