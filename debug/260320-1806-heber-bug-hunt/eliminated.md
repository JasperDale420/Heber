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

### HeberReader schema conflict resolution bugs
- **Result:** Disproven. `_open_dataset_safe` has correct fallback chain: unified schema → coerce dict→string → per-file unification. All edge cases (empty dirs, single file) handled.

### Catalog service upsert race conditions
- **Result:** Disproven. Uses SQLAlchemy async sessions with proper commit/rollback patterns. Upsert operations are atomic within a session.

### Gold poller pipeline registry stale entries
- **Result:** Disproven. All 15 registered pipelines correspond to existing module files with correct class names.

### Straddle momentum ATM selection logic errors
- **Result:** Disproven. `_select_atm_by_strike` uses `argsort().iloc[0]` correctly — returns the index of the strike closest to spot. Handles empty chains with early return.

### Bronze writer atomic write issues
- **Result:** Disproven. Uses proper `tmp_path.rename(file_path)` pattern consistently. Buffer flush is synchronized.

### Compactor event_id deduplication correctness
- **Result:** Disproven. Uses `to_pylist()` on event_id column → set for O(1) lookup → filter mask. Memory concern noted but acceptable for compaction batch sizes.

### EventEnvelope validator timezone edge cases
- **Result:** Disproven. Pydantic validators correctly coerce naive datetimes to UTC and handle all timezone-aware inputs.

### Watch poller multi-route fallback logic
- **Result:** Disproven. Correctly tries routes in order, tracks stale/partial quotes, selects best available. Timeout and error handling are comprehensive.

### Watch gateway URL candidate generation bugs
- **Result:** Disproven. `gateway_url_candidates` correctly orders API prefix variants with legacy fallback. `coerce_optional_float` properly rejects None, booleans, NaN, Inf.

### Retention/reaper unsafe deletion of Gold dependencies
- **Result:** Disproven. `DeletionSafetyChecker` validates Gold lineage before any partition deletion. Archive path uses atomic compress-then-delete.

### Market calendar early close handling
- **Result:** Disproven. `add_trading_hours` and `trading_minutes_until` correctly use exchange_calendars' session schedules including early closes.

### Backtest integration config hash collisions
- **Result:** Disproven. `ExperimentConfig` hashes all relevant fields including fold parameters. SHA256 collision probability is negligible.

### Version sort key edge cases
- **Result:** Disproven. Handles both semver (v1.2.3) and non-semver strings with proper fallback sorting.

### Survivor bias filter incorrect default
- **Result:** Disproven. `exclude_future_delistings=True` is an intentional design choice for point-in-time correctness, not a bug.

### Walk-forward splits embargo period logic
- **Result:** Disproven. `purge_window` correctly excludes overlapping labels between train and test splits. Embargo periods handle both fixed-offset and label-duration modes.

### Feature template rolling calculations off-by-one
- **Result:** Disproven. All rolling window calculations in momentum, volatility, microstructure, and cross-asset templates use standard pandas rolling with correct min_periods settings.

### Key normalization OCC symbol extraction errors
- **Result:** Disproven. Comprehensive regex patterns for all instrument types. Feed-specific normalizers handle all known payload formats with proper fallback to None on failure.

### _coalesce_split_or_fallback None vs 0.0 handling
- **Result:** Disproven. The function correctly distinguishes None (missing) from 0.0 (valid zero) using `is not None` checks.

### embargo_days sub-day misreporting
- **Result:** Noted but acceptable. `embargo_days = embargo_delta.days` truncates sub-day embargo to 0 for metadata, but the actual `embargo_delta` timedelta is used correctly for split computation. The field is informational only.

### expanding_window_splits zero test period
- **Result:** Disproven. Zero test period would produce `test_start == test_end`, and the DateRange validation catches this.

### Bloom filter hash randomization across restarts
- **Result:** Disproven (by design). The bloom filter is in-process and ephemeral — it's reconstructed on restart. Hash randomization doesn't matter since the filter lifetime matches the process lifetime.

### Backpressure lag estimation accuracy
- **Result:** Noted but acceptable. The lag estimation in `bus/backpressure.py` uses a documented heuristic. It's a best-effort metric for monitoring, not a correctness-critical calculation.

### backfill_scanner date.today() timezone
- **Result:** Noted but acceptable. `date.today()` used for partition directory scanning — the partition date granularity (YYYY-MM-DD) makes timezone off-by-one irrelevant for the scanning use case.

### SRE capacity planner division-by-zero
- **Result:** Disproven. No division operations without zero guards. `base_cost` defaults to 1575.0. All scaling uses multiplication with positive constants.

### SRE chaos experiment unsafe state mutation
- **Result:** Disproven. `ChaosExperimentRegistry` is a passive data structure for tracking experiments. `start_run`/`complete_run` is a standard lifecycle pattern — caller responsibility to use try/finally. The `tc qdisc` references are runbook documentation strings, not executable code.

### Test generator producing invalid test data
- **Result:** Disproven. All generators (`generate_bar`, `generate_trade`, `generate_quote`, `generate_golden_dataset`) produce valid, properly-typed records with all required fields populated.

### CI gate flaky threshold off-by-one
- **Result:** Disproven. `0 < failure_rate <= threshold` correctly identifies flaky tests: sometimes failing but not too often. Tests at exactly the threshold ARE flaky. Tests above the threshold are broken, not flaky. The `<=` is semantically correct.

### Leakage detector false negatives
- **Result:** Disproven. Implemented detectors (LK-001, LK-003, LK-004) are sound with direct row-by-row validation. 4/7 test cases lack implementation but that's a coverage gap, not a false negative.

### Test environment isolation failures
- **Result:** Disproven. Each environment has distinct port mappings (Postgres 5433, Redis 6380, MinIO 9000). Independent data structures, no shared mutable state.

### Catalog API missing auth/validation on write operations
- **Result:** Disproven. POST endpoints use Pydantic request validation. Database unique constraints on `dataset_name`. Duplicate creation raises IntegrityError (poor UX but not a validation gap).

### LabelWriter partial flush inconsistent Gold state
- **Result:** Disproven. Exception handler at lines 166-168 calls `_cleanup_staged_files()` which explicitly deletes already-promoted files in reverse order. Rollback is correctly implemented.

### Gold pipeline version resolution race condition
- **Result:** Disproven. Version registration is a manual seeding operation. `HeberReader` discovers versions via glob + sort key. No TOCTOU race detected.

### Feature pipeline silently returning empty DataFrames
- **Result:** Disproven as a bug. Returning empty DataFrames for empty inputs is standard defensive behavior for pure transformation functions. Caller (Gold poller) is responsible for logging pipeline output sizes.

### Catalog dataset registration duplicate handling
- **Result:** Disproven. `Dataset` table has unique constraint on `dataset_name`. Seed functions use idempotent patterns.

### Config validation gaps — invalid combinations accepted
- **Result:** Disproven. No cross-field validators needed — ranges are intentionally permissive for flexibility. Field-level constraints (ge, le, Literal) are sufficient.

### Schema registry client caching stale schemas
- **Result:** Disproven. Heber wrapper delegates directly to the underlying Karapace-compatible library for all operations. No explicit caching in the Heber layer.

### Compactor partition selection off-by-one on date boundaries
- **Result:** Disproven. Uses `rglob("*")` to recursively walk all partitions. Partition path structure `dt=YYYY-MM-DD` handled correctly by glob pattern.

### ClickHouse hot store writer type mapping mismatches
- **Result:** Disproven. No ClickHouse writer exists in the current codebase. Storage uses Parquet files, Iceberg, and Postgres.

### Event bus backpressure dropping events without DLQ
- **Result:** Disproven. Non-retryable errors go to DLQ immediately. Retryable errors use exponential backoff then DLQ after max retries. BackpressureMonitor only monitors lag, never drops events.

### Enrichment pipeline stale cache serving outdated data
- **Result:** Disproven. `AlertFeatureExtractor` has proper TTL-based cache with monotonic clock. Stale entries actively popped. Default 30s TTL is reasonable.

### SRE runbook parameter injection
- **Result:** Disproven. Runbooks are purely data structures (dataclasses with text fields). No parameter substitution, command execution, or dynamic evaluation anywhere.

### Bus stream consumer cursor management bug
- **Result:** Disproven. XREADGROUP uses `">"` cursor correctly. XACK only after successful flush. Pending message recovery via xclaim for idle messages. At-least-once delivery guaranteed.

### Bus deduplication bloom filter false positive on hash collision
- **Result:** Disproven. Uses double hashing with proper rotation. False positive rate is statistical (configured at 1% per PRD §12.11.2) and documented. Not a design flaw.

### Catalog URN parsing edge cases
- **Result:** Disproven. `DatasetURN` is an internal module — URNs are constructed programmatically, not from user input. Invalid URNs raise ValueError. The regex anchors correctly.

### Quality contracts fill_rate division-by-zero with expected_days=0
- **Result:** Disproven. Passing `expected_days=0` is a caller precondition violation. The function computes observed/expected days — zero expected is nonsensical input.

### Feature template _utils.py numeric edge cases
- **Result:** Disproven. Both `rolling_max_timestamp` functions use `pd.to_datetime(errors="coerce")` for NaT handling, `min_periods=1` for short series. No division or NaN propagation issues.

### Watch manager state machine invalid transitions
- **Result:** Disproven. `heber-watch` is a single-process service with single async event loop. No horizontal scaling of the watch manager, so multi-process race conditions don't apply.

### Feature template momentum.py rolling window with short series
- **Result:** Disproven. pandas `rolling()` and `shift()` return NaN for insufficient data. `min_periods=1` in utilities ensures no crashes on small series.

### Feature template volatility.py Parkinson estimator bugs
- **Result:** Disproven. Formula `√[mean(log(H/L)²) / (4·ln(2))] × √252` is mathematically correct. Constant 4·ln(2) ≈ 2.7726 is right. Log transform applied correctly.

### Feature template cross_asset.py correlation matrix singularity
- **Result:** Disproven. Zero variance replaced with NaN before division (`replace(0, np.nan)`). pandas `corr()` returns NaN for constant series. All edge cases handled.

### Catalog service async session leak
- **Result:** Disproven. Standard async SQLAlchemy with FastAPI dependency injection. Session lifecycle managed by `get_session()` context manager — handles rollback and cleanup on exception.

### Iceberg catalog namespace/table naming collision
- **Result:** Disproven. Namespace and table joined with dot (Iceberg-standard delimiter). Feed-to-table mapping uses explicit keys. No collision risk.

### HTTP client retry not respecting 429 Retry-After
- **Result:** Disproven (intentional design). Comments explicitly state "Only retries on transport errors and timeouts." HTTP calls go to Data-Gateway (internal service), not directly to rate-limited external APIs.
