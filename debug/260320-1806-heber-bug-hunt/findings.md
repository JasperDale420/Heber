## Confirmed Bugs

### [HIGH] Docker-compose healthcheck regression
- **Location:** `docker-compose.yml:19-24` (postgres) + lines 49-54, 82-87, 112-116, 145-149 (all healthchecked services)
- **Hypothesis:** Postgres healthcheck was regressed from production-grade values
- **Evidence:** Commit `5e14f24` ("repo hygiene remediation") changed postgres from `interval=10s, timeout=5s, retries=5, start_period=120s` to `interval=5s, timeout=3s, retries=10, start_period=10s`. All 5 healthchecked services had `start_period` regressed below the 120s minimum required by contract tests.
- **Reproduction:** `uv run pytest tests/test_compose_postgres_health_contract.py tests/test_compose_restart_contract.py` (2 failures)
- **Impact:** Services marked unhealthy before Postgres finishes recovery from WAL replay or external volume mount delays. Cascading restarts.
- **Root cause:** Automated remediation commit overwrote production healthcheck settings with aggressive dev values
- **Fix:** Restored all 5 services to `interval=10s, timeout=5s, retries=5, start_period=120s` matching the contract test expectations from commit `21579e7`.

### [MEDIUM] Gold poller UTC/ET timezone mismatch — potential double-run
- **Location:** `heber/gold_poller/service.py:307` (`_last_run_date`) and `service.py:169` (`_last_trading_day`)
- **Hypothesis:** `_last_run_date` set with UTC date but `_should_run()` compares against ET date
- **Evidence:** `_run_all_pipelines()` sets `self._last_run_date = datetime.now(UTC).date()` but `_should_run()` uses `datetime.now(ZoneInfo("America/New_York")).date()`. After midnight UTC (7-8pm ET), these dates diverge.
- **Reproduction:** If pipeline execution takes >3.5 hours (finishing after midnight UTC), UTC date advances while ET date stays the same. The guard `self._last_run_date == today` fails and pipelines re-run.
- **Impact:** Double execution of all Gold feature pipelines (wasteful compute, but no data corruption due to idempotent writes)
- **Root cause:** Mixed timezone usage between `_should_run()` (ET) and `_run_all_pipelines()` (UTC)
- **Fix:** Changed both `_last_run_date` assignment and `_last_trading_day()` to use ET consistently.

### [MEDIUM] Symbol column inconsistency in equity feature pipelines
- **Location:** `heber/features/pipelines/equity_features.py` lines 325, 426, 504, 555
- **Hypothesis:** `symbol` column in Gold datasets contains instrument_key prefix (`equity:AAPL`) instead of plain ticker (`AAPL`)
- **Evidence:** `compute_momentum_features`, `compute_volatility_features`, `compute_microstructure_features`, and `compute_return_labels` all set `out["symbol"] = out["instrument_key"]` — copying the full `equity:AAPL` format. Meanwhile `compute_flow_features` correctly uses just the ticker via `rename(columns={"underlying": "symbol"})`, and `_resample_bars_to_daily` correctly strips with `.str.replace(r"^equity:", "", regex=True)`.
- **Reproduction:** Run any equity feature pipeline → inspect `symbol` column in output DataFrame → shows `equity:AAPL` instead of `AAPL` for momentum/volatility/microstructure/returns.
- **Impact:** Cross-dataset joins between flow_features (`symbol=AAPL`) and momentum_features (`symbol=equity:AAPL`) would silently produce zero matches. Display/reporting shows prefixed keys.
- **Root cause:** Copy-paste from `instrument_key` without stripping the `equity:` prefix
- **Fix:** Changed all 4 locations to use `.str.replace(r"^equity:", "", regex=True)` consistent with `_resample_bars_to_daily`.

### [MEDIUM] Label leakage in ticker_base_rates — current alert's outcome in own feature
- **Location:** `heber/features/pipelines/ticker_base_rates.py:138`
- **Hypothesis:** `ticker_win_rate_90d` includes the current alert's own `hit_tp_first` outcome in the rolling window
- **Evidence:** Line 138 used `<= current_ts` (inclusive), which includes the current alert row in its own feature computation. The comment even said "inclusive of current." For a batch pipeline computing historical features, this is textbook label leakage — the feature partially encodes the answer it's meant to predict.
- **Reproduction:** Compute base rates for a ticker with a single alert → win_rate was 1.0 (should be NaN, no priors). For a ticker with N alerts, the last row's frequency was N (should be N-1).
- **Impact:** Overfit meta-models that don't generalize to production. Most visible on low-sample tickers where including/excluding 1 alert shifts win_rate significantly (e.g., 5 alerts: 80% vs 100%).
- **Root cause:** `<=` should be `<` to exclude the current row from its own feature computation
- **Fix:** Changed to `< current_ts`. When count==0 (first alert, no priors), emit NaN features instead of skipping the row. Updated 29 unit tests to match non-leaky behavior (all pass).

### [LOW] Misleading success log after lock timeout in persist_features_to_gold
- **Location:** `heber/ml/datasets.py:511`
- **Hypothesis:** `logger.info("Persisted features partition")` runs even when write was skipped due to lock timeout
- **Evidence:** The `logger.info` at line 511 is outside the `try/except Timeout` block (lines 488-509). When `FileLock` times out, the except handler logs "Could not acquire partition lock, skipping write" but execution falls through to the success log.
- **Reproduction:** Two concurrent writers contending for the same partition lock → one gets Timeout → logs both "skipping write" AND "Persisted features partition" with row count.
- **Impact:** Misleading operator logs — appears that data was written when it wasn't. Could mask data loss during concurrent enrichment backfill runs.
- **Root cause:** Success log placed outside try block instead of inside or after a continue
- **Fix:** Added `continue` after the `except Timeout` handler to skip the success log and move to the next partition.

### [LOW] Silver writer salvage path non-atomic write
- **Location:** `heber/writer/utils.py:166` (salvage code path in `write_silver_parquet`)
- **Hypothesis:** The salvage path (row-by-row fallback after ArrowTypeError) writes directly to the final file path without atomic temp-then-rename
- **Evidence:** Normal write path uses `tmp_path = file_path.with_suffix(".parquet.tmp")` → `pq.write_table(table, tmp_path, ...)` → `tmp_path.rename(file_path)`. Salvage path wrote `pq.write_table(table, file_path, ...)` directly — no temp file, no rename.
- **Reproduction:** Trigger a Silver batch with mixed-type rows → ArrowTypeError → salvage path fires → crash or power loss during write leaves partial/corrupt Parquet at the final path.
- **Impact:** Partial/corrupt Parquet files on crash during salvage writes. Downstream readers would get ArrowInvalid errors. The normal path is immune (atomic rename is all-or-nothing).
- **Root cause:** Salvage path was added later and didn't follow the same atomic write pattern as the normal path
- **Fix:** Added `salvage_tmp = file_path.with_suffix(".parquet.tmp")` with temp-then-rename, matching the normal path pattern.

### [LOW] Duration parser regex not anchored — accepts trailing garbage
- **Location:** `heber/gold/duration.py:35`
- **Hypothesis:** `parse_duration` regex `r"(\d+)([Mwdhms])"` is not anchored — strings like "5dGARBAGE" silently parse as 5 days
- **Evidence:** `re.match(r"(\d+)([Mwdhms])", "5dGARBAGE")` matches at position 0 and returns `("5", "d")`. The parser never validates that the entire input is consumed.
- **Reproduction:** `parse_duration("5dGARBAGE")` → `timedelta(days=5)` (should raise ValueError). Also `parse_duration(" 5d ")` → ValueError (should parse after stripping).
- **Impact:** Invalid duration strings from config or user input silently accepted with potentially wrong values. The DLQ reprocessor's duration parser already had correct anchoring — this was inconsistent.
- **Root cause:** Regex used `re.match` without `$` anchor, which only checks the start of the string
- **Fix:** Changed to `r"^(\d+)([Mwdhms])$"` with `.strip()` on input.

### [LOW] Backfill cancel_job doesn't stop running job
- **Location:** `heber/backfill/__init__.py` lines 721, 759, 809-818
- **Hypothesis:** `cancel_job` sets status=CANCELLED but running job ignores it and overwrites to COMPLETED
- **Evidence:** `cancel_job` sets `job.status = BackfillStatus.CANCELLED`, but the `run_job` chunk loop at line 721 never checks `job.status`. After the loop finishes, line 759 unconditionally sets `job.status = BackfillStatus.COMPLETED`, overwriting the cancellation.
- **Reproduction:** Start a backfill job → call `cancel_job` while running → job completes normally, status=COMPLETED (not CANCELLED).
- **Impact:** Users cannot cancel running backfill jobs; the DELETE /backfill/{id} API endpoint appears to work but has no effect.
- **Root cause:** Missing cancellation check in the chunk processing loop
- **Fix:** Added `if job.status == BackfillStatus.CANCELLED: return job` check at the top of the chunk loop, with proper metric recording and logging.
