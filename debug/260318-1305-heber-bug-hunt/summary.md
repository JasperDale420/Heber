# Heber Debug Summary — 2026-03-18

## Overview
- **Iterations:** 55 (40 manual + 6 from recon agents, 9 verified from recon)
- **Hypotheses tested:** 55 (27 confirmed, 28 disproven)
- **Bugs found:** 27 (2 High, 14 Medium, 10 Low, 1 discovery)
- **Files investigated:** ~100 / ~120 in scope
- **Test suite status:** 1667 pass, 0 fail, 0 lint errors

## All Bugs (Priority Order)

| # | Sev | Location | Description |
|---|-----|----------|-------------|
| 12 | **HIGH** | `key_normalization.py:41` | `_normalize_symbol` rejects dot-class tickers (BRK.B, BF.A) |
| 25 | **HIGH** | `catalog/api.py:429` | `create_dataset` passes `dataset_name=` but param is `name=` — **runtime TypeError** |
| 1 | MED | `gold_poller/service.py:171,258` | Hardcoded EDT offset, wrong during EST |
| 2 | MED | `gold_poller/service.py:425` | Naive datetimes passed to pipelines |
| 4 | MED | `consumer.py:76,585` | `_silver_validation_warning_counts` unbounded growth |
| 6 | MED | `normalizer.py:180` | Float-epoch strings silently become None |
| 9 | MED | `reader/core.py:106` | `_open_dataset_safe` swallows exceptions silently |
| 10 | MED | `bronze.py:82` | Non-atomic gzip writes → duplicates on crash |
| 11 | MED | `utils.py:119` | Non-atomic Parquet writes → duplicates on crash |
| 13 | MED | `watch/writer.py:260` | `asyncio.gather` no sibling cancellation |
| 16 | MED | `ops/slices.py:180` | Division by zero when no slices |
| 19 | MED | `watch/writer.py:287` | WatchService.stop() doesn't close Redis |
| 20 | MED | `compactor.py:471` | Compactor blocks event loop during compaction |
| 21 | MED | `catalog/service.py:120` | `upsert_instrument` ignores type/symbol on update |
| 26 | MED | `catalog/api.py:121` | CancelledError re-raised during lifespan shutdown |
| 3 | LOW | `gold_poller/service.py:257` | Misleading EST direction comment |
| 5 | LOW | `bronze.py:30` | Dead `buffer_counts` field |
| 7 | LOW | `watch/writer.py:282` | Missing `exc_info=True` in error handler |
| 8 | LOW | `watch/writer.py:87` | LabelWriter buffer grows on persistent failures |
| 14 | LOW | `utils.py:158` | Silver salvage path skips null audit |
| 15 | LOW | 12+ files | `logger.error()` missing `exc_info=True` widespread |
| 17 | LOW | `feast/materialization.py:47` | Silent `except: pass` in Feast resolution |
| 18 | LOW | `ml/datasets.py:46` | Naive datetime violates tz-aware contract |
| 22 | LOW | `catalog/service.py:180` | `if approx_row_count:` falsy for 0 |
| 23 | LOW | `retention/__init__.py:256` | Dead expression (orphaned Path object) |
| 24 | LOW | `versioning/__init__.py:165` | LakeFS auto-create fallback never fires |
| 27 | LOW | `watch/consumer.py:766` | `_normalize_put_call` defaults unknowns to "C" |

## Recommendations (Priority Order)

1. **Fix Bug 25** (HIGH) — Change `dataset_name=` to `name=` in API endpoint
2. **Fix Bug 12** (HIGH) — Update `SYMBOL_PATTERN` or use `_normalize_equity_symbol` for non-market feeds
3. **Fix timezone bugs** (Bugs 1-3) — Use `ZoneInfo("America/New_York")` from existing `calendar/market.py`
4. **Fix float-epoch parsing** (Bug 6) — Try `float()` before ISO parsing in `_coerce_timestamp`
5. **Fix catalog service** (Bugs 21, 22, 26) — upsert_instrument update, falsy check, CancelledError
6. **Add atomic writes** (Bugs 10-11) — Follow compactor's temp-file-then-rename pattern
7. **Fix memory leaks** (Bugs 4-5) — Bound warning counts dict, remove dead `buffer_counts`
8. **Fix async patterns** (Bugs 13, 20) — TaskGroup for WatchService, to_thread for compactor
9. **Close Redis** (Bug 19) — Add `self.redis.close()` in WatchService.stop()
10. **Division by zero** (Bug 16) — Guard `completed / total` in SliceTracker
11. **Add `exc_info=True`** (Bugs 7, 15) — Batch fix across 12+ error handlers
12. **Silent exception swallowing** (Bugs 9, 17) — Add logging to fallback paths
13. **Minor cleanup** (Bugs 18, 23, 24, 27) — Naive datetime, dead code, put_call default
</content>
</invoke>
