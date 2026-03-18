# Heber Debug Findings

## Confirmed Bugs

### [MEDIUM] Bug 1: GoldFeaturePoller hardcodes EDT offset, incorrect during EST
- **Location:** `heber/gold_poller/service.py:171` and `heber/gold_poller/service.py:258`
- **Hypothesis:** Hardcoded `timedelta(hours=4)` is wrong during EST (UTC-5)
- **Evidence:** Both `_last_trading_day()` and `_should_run()` use `datetime.now(UTC) - timedelta(hours=4)` for ET conversion. During EST (Nov-Mar), real ET is UTC-5, so computed time is 1h ahead of actual ET.
- **Reproduction:** During any EST month, `_should_run()` fires the EOD trigger 1 hour early (e.g., at 3:35 PM EST instead of 4:35 PM EST). `_last_trading_day()` may return today when market hasn't closed yet.
- **Impact:** Feature pipelines run before market data is fully settled, producing incomplete Gold features.
- **Root cause:** No DST-aware timezone conversion. Uses arithmetic offset instead of `zoneinfo.ZoneInfo("America/New_York")`.
- **Suggested fix:** Replace `datetime.now(UTC) - timedelta(hours=4)` with `datetime.now(ZoneInfo("America/New_York"))`.

### [MEDIUM] Bug 2: Naive datetimes passed to pipeline.run()
- **Location:** `heber/gold_poller/service.py:425-426`
- **Hypothesis:** `datetime(year, month, day)` creates tz-naive datetimes
- **Evidence:** `kwargs = {"start_date": datetime(start_date.year, start_date.month, start_date.day), ...}` — no `tzinfo` parameter.
- **Reproduction:** Pipeline code reads Silver data using `ts_event >= start_date` where Silver `ts_event` is UTC-aware. Comparing aware and naive datetimes raises TypeError in strict mode or silently produces wrong results.
- **Impact:** Potential filtering errors or silent data exclusion in Gold feature generation.
- **Root cause:** Missing UTC tzinfo when constructing pipeline date bounds.
- **Suggested fix:** Use `datetime(y, m, d, tzinfo=UTC)` or `datetime.combine(start_date, datetime.min.time(), tzinfo=UTC)`.

### [LOW] Bug 3: Misleading EST/EDT comment
- **Location:** `heber/gold_poller/service.py:257`
- **Hypothesis:** Comment direction is wrong
- **Evidence:** Comment: "Using -4 is safe — if we're in EST the trigger fires 1h late". Reality: UTC-4 when offset should be UTC-5 means computed ET time is 1h AHEAD, so trigger fires 1h EARLY, not late.
- **Impact:** Misleads future developers into believing the behavior is conservative (late) when it's actually aggressive (early).

### [MEDIUM] Bug 4: Unbounded memory growth in `_silver_validation_warning_counts`
- **Location:** `heber/writer/consumer.py:76,585-592`
- **Evidence:** Dict keyed by `(provider, feed, error_type, str(error))`. Error messages include unique event IDs and instrument keys.
- **Impact:** Slow memory leak in long-running consumer. ~2.5 MB/day at 1% error rate.
- **Suggested fix:** Use `(provider, feed, error_class)` as key (exclude variable error text), or periodically clear.

### [LOW] Bug 5: BronzeWriter.buffer_counts is dead code that leaks memory
- **Location:** `heber/writer/bronze.py:30,53`
- **Evidence:** Incremented but never read or cleared. One entry per unique partition key.
- **Suggested fix:** Remove `buffer_counts` entirely.

### [MEDIUM] Bug 6: Float-epoch timestamp strings silently become None
- **Location:** `heber/writer/normalizer.py:180-182` and `heber/writer/normalizer.py:250`
- **Evidence:** `str.isdigit()` returns `False` for `"1710000000.5"` → falls to `fromisoformat()` → `ValueError` → `None`.
- **Impact:** Providers sending timestamps as float strings have timestamps silently nullified in Silver.
- **Suggested fix:** Try `float()` conversion before ISO parsing.

### [MEDIUM] Bug 7: Missing traceback in WatchService error handler
- **Location:** `heber/watch/writer.py:282-283`
- **Evidence:** `logger.error("Check/write error", error=str(e))` — no `exc_info=True`.
- **Suggested fix:** Add `exc_info=True`.

### [LOW] Bug 8: LabelWriter buffer grows on persistent write failures
- **Location:** `heber/watch/writer.py:87-102`
- **Evidence:** `self._buffer = []` only on success. Persistent disk failures cause unbounded growth.
- **Suggested fix:** Cap buffer or clear on persistent failure.

### [MEDIUM] Bug 9: Silent exception swallowing in `_open_dataset_safe`
- **Location:** `heber/reader/core.py:106-107,116`
- **Evidence:** `except Exception: pass` and `except Exception: continue` with no logging.
- **Suggested fix:** Add `logger.debug(..., exc_info=True)`.

### [MEDIUM] Bug 10: Bronze writes non-atomic — duplicates on crash recovery
- **Location:** `heber/writer/bronze.py:82-90`
- **Evidence:** `gzip.open()` writes directly to final path. Partial file persists on failure, retry creates duplicate.
- **Suggested fix:** Write to `.tmp` file, then `os.rename()`.

### [MEDIUM] Bug 11: Silver Parquet writes also non-atomic
- **Location:** `heber/writer/utils.py:119-125`
- **Evidence:** Same pattern as Bronze — `pq.write_table` to final path. Crash leaves corrupt partial file.
- **Suggested fix:** Write to `.tmp` file, then `os.rename()`.

### [HIGH] Bug 12: `_normalize_symbol` rejects valid dot-class equity tickers
- **Location:** `heber/writer/key_normalization.py:41,501-505`
- **Evidence:** `SYMBOL_PATTERN = r"^[A-Z]{1,5}$"` rejects `BRK.B`, `BF.A`, etc. Used by flow_alerts, congress_trades, insider_trades, news normalization.
- **Impact:** Alternative data for ~20 dot-class tickers silently loses symbol information.
- **Suggested fix:** Replace `_normalize_symbol` with `_normalize_equity_symbol` where equity symbols are expected.

### [MEDIUM] Bug 13: WatchService.run() doesn't cancel siblings on component failure
- **Location:** `heber/watch/writer.py:260-268`
- **Evidence:** `asyncio.gather(*coroutines)` — first exception propagates but other tasks continue.
- **Suggested fix:** Use `asyncio.TaskGroup` or custom gather with cancellation.

### [LOW] Bug 14: Silver salvage path bypasses null field audit
- **Location:** `heber/writer/utils.py:158-163`
- **Evidence:** Salvaged writes skip `audit_null_fields()` call present in normal path.
- **Suggested fix:** Add audit call before salvage write.

### [LOW] Bug 15: Widespread `logger.error()` missing `exc_info=True`
- **Location:** 12+ files: schema_registry, bus/backpressure, watch/consumer, watch/writer, quality/soda_scanner, ops/tracing, ops/metrics, catalog/openmetadata_client, gold/labels
- **Evidence:** Pattern: `logger.error("...", error=str(e))` without `exc_info=True`
- **Impact:** Tracebacks lost in production error logs
- **Suggested fix:** Batch-add `exc_info=True` to all `logger.error()` calls in exception handlers.

### [MEDIUM] Bug 16: Division by zero in `SliceTracker.generate_report()`
- **Location:** `heber/ops/slices.py:180`
- **Evidence:** `completed / total * 100` where `total = len(self.slices)`. Crashes when no slices exist.
- **Suggested fix:** Guard with `(completed / total * 100) if total > 0 else 0`

### [LOW] Bug 17: Silent `except Exception: pass` in Feast feature view resolution
- **Location:** `heber/feast/materialization.py:47-48,56-57`
- **Evidence:** Two `except Exception: pass` blocks with no logging. Hides Feast connection errors.
- **Suggested fix:** Add `logger.debug("feast_resolve_fallback", exc_info=True)`

### [LOW] Bug 18: Naive datetime in ml/datasets.py
- **Location:** `heber/ml/datasets.py:46`
- **Evidence:** `datetime.now()` without UTC — creates naive datetime for quarantine filename.
- **Impact:** Minor — filename only, but violates codebase timezone-aware contract.
- **Suggested fix:** `datetime.now(UTC)`

### [MEDIUM] Bug 19: WatchService.stop() doesn't close Redis connection
- **Location:** `heber/watch/writer.py:287-294`
- **Evidence:** `stop()` calls `consumer.stop()`, `poller.stop()`, `writer.flush()` but never `self.redis.close()`.
- **Impact:** Redis connection leak on service restart. Connection pool entries never returned.
- **Suggested fix:** Add `self.redis.close()` at end of `stop()`.

### [MEDIUM] Bug 20: Compactor blocks event loop during entire compaction cycle
- **Location:** `heber/writer/compactor.py:471`
- **Evidence:** `scan_and_compact("silver")` is synchronous (rglob + Parquet I/O), called directly from async `run()` without `asyncio.to_thread()`.
- **Impact:** Event loop blocked during compaction — SIGTERM/SIGINT signal handling delayed until cycle completes. On large partitions, could be minutes.
- **Suggested fix:** Wrap in `await asyncio.to_thread(self.scan_and_compact, "silver")`

### [MEDIUM] Bug 21: `upsert_instrument` ignores `instrument_type` and `canonical_symbol` on update
- **Location:** `heber/catalog/service.py:120-136`
- **Evidence:** Update path (line 122-124) only iterates `**kwargs`. The `instrument_type` and `canonical_symbol` positional args are not in kwargs, so they're silently dropped on existing instruments.
- **Impact:** Cannot change an instrument's type or canonical symbol through the upsert endpoint. PUT `/api/v1/instruments/{key}` silently ignores these fields.
- **Suggested fix:** Add `existing.instrument_type = instrument_type` and `existing.canonical_symbol = canonical_symbol` in the update branch.

### [LOW] Bug 22: `update_coverage` uses falsy check for `approx_row_count`
- **Location:** `heber/catalog/service.py:180`
- **Evidence:** `if approx_row_count:` is falsy for `0`. A zero row count is valid but gets skipped.
- **Impact:** Minor edge case — empty partitions don't update row counts.
- **Suggested fix:** `if approx_row_count is not None:`

### [LOW] Bug 23: Dead expression in Archiver
- **Location:** `heber/retention/__init__.py:256`
- **Evidence:** `archive_path / f"{archive_name}.tar.gz"` creates a Path object that is never assigned or used. Dead code.
- **Suggested fix:** Remove the line.

### [LOW] Bug 24: LakeFS `_get_repo` auto-create fallback never fires
- **Location:** `heber/versioning/__init__.py:165-173`
- **Evidence:** `lakefs.Repository(repo_name, client=client)` is a lazy reference that doesn't validate repo existence. The `except Exception` block (auto-create) never triggers because the constructor doesn't raise.
- **Impact:** If repo doesn't exist, error surfaces at call site (branch/commit) instead of being handled here.
- **Suggested fix:** Call `repo.metadata()` to verify existence, or remove the auto-create fallback.

### [HIGH] Bug 25: `create_dataset` API passes wrong keyword argument
- **Location:** `heber/catalog/api.py:429-438`
- **Evidence:** `service.create_dataset(dataset_name=request.dataset_name, ...)` but service method signature is `async def create_dataset(self, name: str, ...)`. The keyword `dataset_name` doesn't match parameter `name`.
- **Impact:** **Runtime TypeError** on every `POST /api/v1/datasets` call. Endpoint is completely broken.
- **Suggested fix:** Change `dataset_name=request.dataset_name` to `name=request.dataset_name`.

### [MEDIUM] Bug 26: CancelledError re-raised during lifespan shutdown
- **Location:** `heber/catalog/api.py:121-123`
- **Evidence:** After `discovery_task.cancel()` + `await discovery_task`, the `except asyncio.CancelledError` block re-raises. This propagates to FastAPI's lifespan handler, causing Uvicorn to log an unclean shutdown error.
- **Suggested fix:** Remove `raise` — the cancellation was deliberate.

### [LOW] Bug 27: `_normalize_put_call` silently defaults "unknown" values to Call
- **Location:** `heber/watch/consumer.py:766-774`
- **Evidence:** Non-C/P strings (e.g., "buy", "sell", `None`, empty string, numeric) all default to `"C"`. Put options with unexpected format are corrupted to Call.
- **Impact:** Data corruption for options with non-standard put_call labels.
- **Suggested fix:** Return `None` for unrecognized values, or log a warning.

### [LOW] Bug 28: Misleading docstring in zero-leakage health check
- **Location:** `heber/ops/daily_health.py:212`
- **Evidence:** Docstring says "verify ts_available <= ts_event" but the contract is ts_available >= ts_event. Code correctly flags `ts_available < ts_event` as violations.
- **Impact:** Misleads developers about the zero-leakage contract direction.
- **Suggested fix:** Change to "verify ts_available >= ts_event".

### [LOW] Bug 29: Redundant `if quotes:` check in SnapshotPoller.poll_once()
- **Location:** `heber/watch/poller.py:148`
- **Evidence:** `if quotes:` is always true at this point — line 132 already exits early with `if not quotes: return`.
- **Impact:** Dead conditional clutters logic.
- **Suggested fix:** Remove the `if quotes:` guard.
