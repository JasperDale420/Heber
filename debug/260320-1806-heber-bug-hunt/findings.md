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

### [LOW] Backfill cancel_job doesn't stop running job
- **Location:** `heber/backfill/__init__.py` lines 721, 759, 809-818
- **Hypothesis:** `cancel_job` sets status=CANCELLED but running job ignores it and overwrites to COMPLETED
- **Evidence:** `cancel_job` sets `job.status = BackfillStatus.CANCELLED`, but the `run_job` chunk loop at line 721 never checks `job.status`. After the loop finishes, line 759 unconditionally sets `job.status = BackfillStatus.COMPLETED`, overwriting the cancellation.
- **Reproduction:** Start a backfill job → call `cancel_job` while running → job completes normally, status=COMPLETED (not CANCELLED).
- **Impact:** Users cannot cancel running backfill jobs; the DELETE /backfill/{id} API endpoint appears to work but has no effect.
- **Root cause:** Missing cancellation check in the chunk processing loop
- **Fix:** Added `if job.status == BackfillStatus.CANCELLED: return job` check at the top of the chunk loop, with proper metric recording and logging.
