# Heber Debug — Eliminated Hypotheses

These hypotheses were tested and disproven. Recording them prevents re-investigation.

| # | Hypothesis | Why Disproven |
|---|-----------|---------------|
| 6 | Race condition on writer buffers from async tasks | asyncio.gather with sync code — no true parallelism in single-threaded event loop |
| 8 | flush_if_needed modifies dict during iteration | Value replacement during dict iteration is safe in Python 3; no concurrent modification possible |
| 11 | Forward return labels use future data (ts_available wrong) | Design decision — ts_available=now() is conservative per zero-leakage contract |
| 13 | `_coerce_int` loses precision for large ints via float | Theoretical — Silver integer fields don't exceed 2^53 |
| 14 | Watch consumer sync Redis in async context is unsafe | Correctly uses asyncio.to_thread() wrapper for all sync calls |
| 18 | Bloom filter false positives drop legitimate events | Documented design tradeoff — conservative approach with ~0.03% FPR |
| 19 | `_coerce_strike` wrong for fractional strikes | Decimal math is correct and avoids float precision issues |
| 21 | `DLQEvent.from_dict` crashes on missing fields | `_load()` exception handler catches all parse errors, resets cleanly |
| 22 | Catalog auto-discovery TOCTOU race | Irrelevant in mounted volume deployment context |
| 23 | Feature pipelines have division by zero | All divisions consistently guarded with `np.where(... > 0, ...)` |
| 24 | Market calendar has stale holiday data | Uses `exchange_calendars` (dynamically maintained), correctly uses `ZoneInfo` |
| 25 | `_last_trading_day` date mismatch with `_should_run` | Trigger hour check masks the UTC/ET date discrepancy |
| 27 | EventBus streams.py has connection leak | Config dataclasses only — no connections or resources |
| 29 | Pipeline thread safety with `asyncio.to_thread` | Pipelines run sequentially, not concurrently |
| 30 | Circuit breaker state machine race condition | Race between lock acquisitions is benign (counter increment, harmless) |
