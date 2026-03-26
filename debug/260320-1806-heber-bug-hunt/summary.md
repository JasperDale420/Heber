## Debug Session Summary

**Scope:** Entire Heber codebase (138 Python source files + infrastructure)
**Duration:** 32+ iterations (14 initial + 6 deep pass + 8 deeper pass + 4+ deepest pass)
**Mode:** Unlimited autonomous hunt with auto-fix

### Results

| Metric | Value |
|--------|-------|
| Bugs found | 11 (1 HIGH, 5 MEDIUM, 5 LOW) |
| Bugs fixed | 11 |
| Hypotheses tested | 86 (11 confirmed, 75 disproven/noted) |
| Files investigated | ~138 source files + docker-compose.yml |
| Techniques used | Direct inspection, git bisect, pattern search, differential debugging, cross-file consistency analysis, label leakage analysis |
| Tests before | 1665 passed, 2 failed |
| Tests after | 1669 passed, 0 failed |

### Fixes Applied

1. **docker-compose.yml** — Restored all 5 healthchecked services to production-grade settings (interval=10s, timeout=5s, retries=5, start_period=120s)
2. **heber/gold_poller/service.py** — Changed `_last_run_date` and `_last_trading_day()` to use Eastern Time consistently (was mixing UTC and ET)
3. **heber/features/pipelines/equity_features.py** — Fixed `symbol` column in 4 compute functions to strip `equity:` prefix, consistent with flow_features and daily bar resampling
4. **heber/backfill/__init__.py** — Added cancellation check in the chunk processing loop so `cancel_job` actually stops running jobs
5. **heber/features/pipelines/ticker_base_rates.py** — Fixed label leakage: rolling window now excludes current alert's own outcome (`<= current_ts` → `< current_ts`). First alerts emit NaN features instead of being silently dropped.
6. **heber/ml/datasets.py** — Fixed misleading success log after lock timeout in `persist_features_to_gold`: added `continue` so "Persisted features partition" log only appears when write actually succeeds.
7. **heber/writer/utils.py** — Fixed Silver writer salvage path writing directly to final path instead of atomic temp-then-rename. Crash during salvage would leave corrupt Parquet.
8. **heber/gold/duration.py** — Anchored `parse_duration` regex with `^...$` so strings like "5dGARBAGE" raise ValueError instead of silently parsing as 5 days.
9. **heber/watch/checker.py** — Fixed `outcome_to_label_row` setting `instrument_key` to `alert_id` (UUID) instead of actual instrument key. Poisoned the `labels_alert_barriers` Gold dataset and broke `ticker_base_rates` groupby.
10. **heber/gold/labels.py** — Fixed `compute_availability_time` applying `availability_lag` before market close snap for daily labels, which erased the lag entirely.
11. **heber/gold/splits.py** — Added zero-step guard to `walk_forward_splits` to prevent infinite loop when `step="0d"` or `step="0s"`.

### Codebase Health Assessment

The Heber codebase is in excellent shape:
- Ruff: clean (zero lint issues)
- All file I/O uses context managers
- No mutable default arguments
- No bare except clauses
- No hardcoded secrets
- No SQL injection vectors
- Atomic writes throughout (Bronze, Silver, compactor)
- Comprehensive error handling with structured logging
- Zero-leakage contract enforced at read AND write paths
- Bloom filter deduplication with rotation for bounded false-positive risk
- Multi-route gateway fallback with stale/partial quote handling
- Robust DLQ reprocessor with analyze/reprocess/purge modes
- SLO framework with proper burn rate calculations

### Deep Pass Areas Investigated (iterations 15-20)

- DLQ reprocessor (`heber/writer/dlq_reprocessor.py`)
- Daily health report (`heber/ops/daily_health.py`)
- SLO framework (`heber/sre/slo.py`)
- Backfill coordinator (`heber/backfill/__init__.py`)
- Watch snapshot poller (`heber/watch/poller.py`)
- Feature pipelines (`heber/features/pipelines/*.py`)
- Silver writer and utilities (`heber/writer/silver.py`, `utils.py`)
- Event consumer with retry/DLQ (`heber/writer/consumer.py`)
- Retention/reaper system (`heber/retention/__init__.py`)
- Event deduplication (`heber/ops/reliability.py`)
- Market calendar (`heber/calendar/market.py`)
- Schema registry client (`heber/schema_registry/registry_client.py`)
- Enrichment backfill scanner (`heber/watch/backfill_scanner.py`)
- Configuration (`heber/config.py`)

### Deeper Pass Areas Investigated (iterations 21-28)

- ML dataset builder (`heber/ml/datasets.py`)
- Alert feature extractor (`heber/watch/features.py`)
- Alert watch consumer (`heber/watch/consumer.py`)
- Runtime retry helpers (`heber/ops/runtime_retry.py`)
- Silver normalizer (`heber/writer/normalizer.py`)
- All 16 feature pipelines in `heber/features/pipelines/`
  - market_intel_features (darkpool, greek_exposure, options_sentiment, ftd)
  - flow_context_features, flow_normalization_features, flow_toxicity_features
  - oi_momentum_features, sector_flow_features, market_tide_context_features
  - market_regime_features, iv_surface_features, trend_scan_features
  - straddle_momentum_features, ticker_base_rates, alert_labels
  - darkpool_features, gex_regime_features, equity_features

### Duration Update
**Duration:** 36+ iterations (14 initial + 6 deep + 8 deeper + 4 deepest + 4+ exhaustive)

### Deepest Pass Areas Investigated (iterations 29-36)

- ML inference (`heber/ml/inference.py`, `trainer.py`)
- Ops tracing, circuit breaker, alerting, dataflow health, lifecycle (`heber/ops/`)
- Writer transformer (`heber/writer/transformer.py`)
- Watch gateway helpers (`heber/watch/gateway.py`)
- Backtest integration (`heber/backtest/integration.py`)
- SRE error budget (`heber/sre/error_budget.py`)
- Iceberg writer (`heber/storage/iceberg_writer.py`)
- Catalog access control, db, datasources (`heber/catalog/`)
- Feature templates alert_labels (`heber/features/templates/alert_labels.py`)
- HeberReader core (`heber/reader/core.py`)
- SRE capacity, chaos experiments (`heber/sre/capacity.py`, `chaos.py`)
- Testing generators, CI gates, leakage detector, environments (`heber/testing/`)
- Catalog API (`heber/catalog/api.py`)
- Watch label writer (`heber/watch/writer.py`)
- Schema registry client (`heber/schema_registry/registry_client.py`)
- Compactor (`heber/writer/compactor.py`)
- Bus backpressure (`heber/bus/backpressure.py`)
- Watch feature enrichment (`heber/watch/features.py`)
- SRE runbooks (`heber/sre/runbooks.py`)

### Exhaustive Pass Areas Investigated (iterations 37-48)

- Bus streams consumer cursor (`heber/bus/streams.py` via `writer/consumer.py`)
- Bus deduplication bloom filter (`heber/bus/dedupe.py`)
- Catalog URN parsing (`heber/catalog/urn.py`)
- Quality contracts validation (`heber/quality/contracts.py`)
- Feature template utilities (`heber/features/templates/_utils.py`)
- Watch manager state machine (`heber/watch/manager.py`)
- Feature template momentum (`heber/features/templates/momentum.py`)
- Feature template volatility (`heber/features/templates/volatility.py`)
- Feature template cross-asset (`heber/features/templates/cross_asset.py`)
- Catalog service async sessions (`heber/catalog/service.py`)
- Iceberg catalog naming (`heber/storage/iceberg_catalog.py`)
- HTTP client retry logic (`heber/core/http_client.py`)

### Also Cleaned Up (pre-debug)

- Removed orphaned `heber/sdk/` directory (stale `__pycache__` from deleted module)
