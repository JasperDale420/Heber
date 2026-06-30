# Changelog

All notable changes to the Heber Data Lakehouse project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Watch `meta_label_features` backfill now writes `expiry` as `date32`, matching the live writer**: `EnrichmentBackfillScanner` coerced `expiry` to a YYYYMMDD **integer** (on a stale "the Gold schema stores expiry as an integer" assumption), while the live watch writer persists it as a `date` (`date32`). A gold dataset spanning both writers' `dt=` partitions then failed to read with `ArrowNotImplementedError: Unsupported cast from int64 to date32` — observed 2026-06-30, recurring on backfill days (06-23/24/30), which blinded Orion's ML feature reads for the whole session. The backfill now coerces `expiry` to a `datetime.date` (date32) like the live path, so partitions are type-consistent. Existing int64 partitions still need a one-off re-backfill or removal.
- **`heber backfill --since/--until` no longer crashes on naive datetimes** (`heber/writer/transformer.py`): the CLI parses `--since`/`--until` as naive datetimes, but `_transform_feed` compared them against UTC-aware partition dates, raising `TypeError: can't compare offset-naive and offset-aware datetimes` on every date-filtered backfill. `_transform_feed` now normalizes naive bounds to UTC before comparison. Regression test in `tests/test_transformer_dedup.py::test_transform_since_accepts_naive_datetime`.

### Added

- **Massive daily raw archive sync** (`heber massive-daily`): added an unattended, idempotent daily job that catches up missing publishable trading days and writes Massive stock flat files directly into `/Volumes/heber/data/_vendor_raw/massive` (`day_aggs_v1` + `minute_aggs_v1`). The same run captures daily raw REST pages for splits, dividends, and the active U.S. stock ticker snapshot under `massive_corp_actions/daily/YYYY-MM-DD/`. The job is wired as `launchd/com.empire.heber.massive-daily.plist` with `RunAtLoad=true`, failure retry throttling, and a 9:15 AM Pacific schedule so it runs after Massive's next-day publish window and recovers after a restart/login. Regression coverage in `tests/backfill/massive/test_daily_sync.py`, `tests/backfill/massive/test_corporate_actions.py`, `tests/backfill/massive/test_downloader.py`, and `tests/test_massive_daily_launchd.py`.
- **Massive REST raw backlog sweep** (`scripts/massive_rest_backlog_sweep.py`): added a resumable raw downloader for Massive REST datasets that are small enough to keep locally without the historical trades/quotes firehose. The sweep writes gzip-compressed raw pages plus manifests under `/Volumes/heber/data/_vendor_raw/massive/massive_rest_backlog/`, covering news, short interest, short volume, float, financial ratios, EDGAR index, 8-K text, 13F, risk factors, risk taxonomies, and Form 3/Form 4 insider filings. Added `massive-rest-backlog` to the native service runner and installer with a `launchd/com.empire.heber.massive-rest-backlog.plist` LaunchAgent using `RunAtLoad=true`, failure retry throttling, and `KeepAlive` on failed exits so the sweep resumes after reboots or transient API failures. Regression coverage in `tests/backfill/massive/test_rest_backlog.py` and `tests/test_massive_daily_launchd.py`.
- **Massive per-ticker raw metadata sweeps**: wired the existing ticker details/events and delisted-details sweep scripts into restart-safe native LaunchAgents (`massive-ticker-meta`, `massive-delisted-details`). These jobs fill the raw per-equity metadata gaps from the captured active/inactive ticker universe, writing details/events under `/Volumes/heber/data/_vendor_raw/massive/massive_ticker_meta/` and delisted as-of details under `massive_corp_actions/delisted_details.jsonl.gz`. LaunchAgent tests now assert the installed Heber root for the runner, working directory, and service environment so the jobs cannot point at a temporary checkout.
- **Dataflow-health alerts on consumer-lag-vs-stream-cap and DLQ floods** (`heber/ops/dataflow_health.py`): a new `consumer_lag` check fails (critical) when the consumer group's lag reaches a high fraction of the stream length — ≥80% → fail, ≥50% → warn, with a 5,000 absolute floor to avoid noise on small/overnight streams. This is the silent cross-feed data-loss condition behind the 2026-06-09 option-quote flood (lag pinned at the ~300K MAXLEN cap → un-consumed events of other feeds evicted before they were written), which the existing reachability / consumer-group / feed-freshness / DLQ-size checks all failed to surface. The DLQ-size check now also escalates from warning to **critical** when the DLQ exceeds 100,000 entries (a rejection flood vs a steady-state trickle). `_collect_redis_signals` now reports `stream_len` via `XLEN`. Tests in `tests/test_dataflow_health.py`.

### Fixed

- **CI gate hygiene restored** (`uv.lock`, `.github/scripts/create-ci-stubs.sh`, `heber/reader/core.py`, `tests/test_dead_code_cleanup.py`): refreshed vulnerable lockfile packages (`mako`, `pyarrow`, `urllib3`, `pyjwt`, `starlette`) to fixed releases, committed the ruff autofix that CI was applying during `pre-commit run --all-files`, aligned the CI `empire-core` stub with Heber's logger helper imports, fixed single-file parquet reads used by label tests, and brought stale DLQ tests in line with the durable file fallback contract.
- **Silver `short_data` production restored** (`heber/writer/ingest_contracts.py`): the 2026-05-20 "67 reference/metadata feeds → Bronze-only" reconciliation wrongly swept `short_data` into `BRONZE_ONLY_SILVER_DATASETS`, even though it has a typed Silver Arrow schema (`heber/schemas/silver.py`) and is the normalized dataset for the actively-polled `short_interest` / `short_volume` feeds. From 2026-05-20 the Silver writer skipped it — Silver `short_data` stopped at `dt=2026-05-20` while Bronze `short_interest` kept arriving through 06-09 (~3 weeks of missing Silver). Removed `short_data` from the Bronze-only set; it stays in `CONTRACTED_RAW_FEEDS`, so no `silver_feed_uncontracted` DLQ noise returns and well-formed payloads now type cleanly into Silver. Restores forward Silver production; the 2026-05-21→06-09 historical gap can be rebuilt with `heber backfill --feed short_interest --since 2026-05-21 --until 2026-06-09` (and `--feed short_volume`; raw feed names, dedup-safe). Regression test `tests/test_ingest_feed_contract_matrix.py::test_short_data_feeds_route_to_silver_not_bronze_only`. NOTE: 7 other feeds (`etf_metadata`, `forex`, `option_contract`, `politician_trades`, `screener_result`, `seasonality`, `stock_fundamentals`) sit in the Bronze-only set despite having typed Silver schemas, but have no active producer today — flagged for a separate audit.

### Changed

- **`WatchManager.get_watches_for_symbol` now batches Redis reads** (`heber/watch/manager.py`): symbol lookups issue one `MGET` instead of one `GET` per watch, preserving malformed-payload skip-and-log behavior while reducing hot-path Redis round trips for contracts with many active watches. Regression coverage in `tests/test_watch_manager_redis_bytes.py`.
- **Dataflow health reports no longer waste time on after-hours freshness probes** — `heber.ops.dataflow_health` now skips metrics polling and filesystem freshness fallback when the market is closed because those checks are reported as skipped anyway. When market-hours metrics are missing, the filesystem fallback now inspects recent Silver date partitions by directory activity instead of recursively walking entire feed trees. This keeps the native `dataflow-health` LaunchD pilot writing reports promptly on startup while preserving market-hours fallback coverage. Regression coverage in `tests/test_dataflow_health.py`.
- **`heber datasets --layer silver` no longer lists Atlas hypothesis materializations** — Atlas writes hypothesis outputs directly to `silver/feed=atlas_features_*/` with an `_atlas_materialization_meta.json` marker. They share storage but are NOT Bronze→Silver pipeline outputs. Today there are 57 of them polluting the inventory listing. The CLI now hides any `feed=*` directory that either contains the marker file or matches the `atlas_features_` name prefix. Listing now shows only the 17 contracted Heber-managed feeds. Regression test in `tests/test_cli_datasets_atlas_filter.py`.
- **Silenced `silver_feed_uncontracted` DLQ noise from reference / metadata feeds** — 13 days of logs contained 3,350 `silver_feed_uncontracted` warnings across 67 unique gateway feed names (`account`, `calendar`, `congress`, `congress-trading`, `corporate-actions`, `crypto`, `etf_metadata`, `filings`, `forex`, `13f`, `insider`, `insider-sentiment`, `insider-transactions`, `institution_holdings`, `option-contract`, `option_contract`, `politician_trades`, `screener_result`, `seasonality`, `sentiment`, `short_data`, `stock_fundamentals`, and 45 others) that the writer rejected to DLQ because they weren't in `CONTRACTED_RAW_FEEDS`. These are UnusualWhales REST endpoint names for reference / metadata / lookup data — they have no time-series tabular shape and don't warrant Silver typing. `heber/writer/ingest_contracts.py` now lists each one in `CONTRACTED_RAW_FEEDS` (so the writer doesn't DLQ them) and in `BRONZE_ONLY_SILVER_DATASETS` (so the Silver writer skips them). Bronze JSONL.gz capture is unchanged. Promote to Silver later by adding a typed Arrow schema in `heber/schemas/silver.py` and removing the canonical name from the Bronze-only set. Regression coverage in `tests/test_ingest_feed_contract_matrix.py::test_previously_uncontracted_feeds_now_route_to_bronze_only`.
- **heber-watch 401 logs now include credential-safe diagnostics** — When the gateway returns `401 unauthorized` from a feature-enrichment call, the `Feature enrichment request failed` warning now carries `auth_header_sent` (bool), `auth_header_names_sent` (the actual header name list, e.g. `["X-Gateway-Key"]`), and `auth_env` (which env vars were checked, which one supplied the key, and the key length — no values). Previously a 401 log only said `error=unauthorized`, leaving operators unable to tell "no key was sent" from "key was sent but rejected" without re-running the request. Implemented in `heber/watch/features.py::_handle_auth_failure`; helper `describe_gateway_auth_env` and constants `GATEWAY_API_KEY_ENV_VARS` / `GATEWAY_AUTH_HEADER_NAME` added to `heber/watch/gateway.py`. Regression tests in `tests/test_watch_gateway_auth_diagnostics.py` verify the log payload AND that credential values never leak into diagnostics.
- **`MetaLabelScorer` no longer spams "Model not loaded" on every score call** (`heber/ml/inference.py`). Previously the scorer logged a WARNING per `score()` invocation when the model was unavailable — 192 records/day in production at perfectly even intervals (one per request). Behaviour now: a single `meta_model_unavailable` WARNING on startup (or on the first score call if the model went missing later), with `reason` field distinguishing `model_load_failed`, `model_not_configured`, and `model_not_loaded`. Subsequent score calls still return `None` (graceful degradation preserved) but emit nothing, and `MetaLabelScorer._missing_model_skips` counts the silent skips for observability. `initialize()` now also catches `MetaModelTrainer.load` failures so a missing/corrupt model file no longer crashes the service at boot. Regression tests in `tests/test_meta_label_scorer_missing_model_one_shot.py`.
- **`lag_check_failed` log emit downgraded from ERROR to WARNING** (`heber/bus/backpressure.py`). The `BackpressureMonitor.monitor_loop` is a defensive periodic check that self-recovers on the next tick, so a transient failure (e.g. Redis blip) was logging 237 ERROR records over 13 days and drowning out real production alerts. The `consumer_lag_seconds` Prometheus gauge is the authoritative signal for actual lag problems. Regression test in `tests/test_backpressure_monitor_log_level.py`.

### Removed

- **Quarantined three uncontracted Silver/Bronze paths created before the contract gate landed** — moved to `silver/_quarantine/2026-04-29/` and `bronze/_quarantine/2026-04-29/`:
  - `silver/feed=data/` (3 files, 32 rows, last write 2026-02-11; one partition was `dt=2023-01-01` from a payload-internal date) — uncontracted feed name `data` from a misconfigured publisher.
  - `silver/feed=stocks/` (1 file, 2 rows, 2026-02-12) — `stocks` is not in `CONTRACTED_RAW_FEEDS` and resolves to `None`.
  - `bronze/provider=unknown/feed=data/` (~30 MB across 2026-02-10/11 + the spurious `dt=2023-01-01`) — `provider=unknown, feed=data` indicates a publisher that didn't set the envelope's provider/feed correctly.
  Files were moved (not deleted) following the existing `gold/_quarantine/` convention so they remain recoverable; they can be permanently removed after a soak period if nothing surfaces. Silver inventory is now clean — only the 17 contracted feeds plus the 57 `atlas_features_hyp_*` materializations remain.

### Added

- Critical-feed data-quality alarm: a scheduled `heber alert-check` (run every
  5 min via its own launchd job) posts a Discord alert the moment a must-flow UW
  feed (flow_alerts, darkpool, oi_change, greek_exposure) goes dark or drops to a
  trickle during the hours it should be flowing — catching the kind of silent
  outage that previously went unnoticed for weeks. Absolute per-feed floors avoid
  the "boiling-frog" blind spot of rolling baselines; a missing partition counts
  as zero (not silently skipped); per-feed hourly cooldown plus one-line recovery
  notes prevent spam. Configured via `HEBER_ALERT_*` (incl.
  `HEBER_ALERT_FLOOR_OVERRIDES` to focus feeds and set trickle floors). New CLI:
  `heber alert-test` (verify the webhook), `heber alert-check` (run one cycle),
  and `heber alert-calibrate` (suggest trickle floors from healthy history). The
  alarm runs as a fast, isolated scheduled process so the multi-tier health
  monitor's heavy Tier-2/Tier-3 sweeps can't starve it, and its reads are
  dt-partition-pruned so they stay fast even on large feeds.
- **Native LaunchD pilot scaffolding for low-risk Heber workers** — added `scripts/run_native_heber_service.sh`, `scripts/install_native_launchd.sh`, LaunchAgent plists under `launchd/`, and `docs/operations/native-launchd.md` so `dataflow-health`, `health-monitor`, `gold-poller`, and `compactor` can run as local macOS services without moving the ingestion consumer yet. The runner uses host paths (`/Volumes/heber/data`), host service URLs (`localhost`), per-service metrics ports, and explicitly excludes `heber-consumer`, `heber-watch`, and `heber-catalog` from the first migration wave. Regression coverage in `tests/test_native_launchd_pilot.py`.
- **Durable, eviction-proof DLQ** — every dead-lettered event (writer consumer `heber/writer/consumer.py` and watch consumer `heber/watch/consumer.py`) is now persisted to a JSON file under `${HEBER_DATA_ROOT}/dlq_fallback/dt=YYYY-MM-DD/` **first**, then best-effort enqueued to the Redis DLQ stream for reprocessing. Previously the file was written only when the Redis `XADD` *failed* — but the DLQ stream lives on a cache-mode Redis (`allkeys-lru` + TTLs), so successfully-enqueued dead-letters could be silently evicted and lost (which is how a ~1300-event backlog of rejected option-flow records disappeared). The on-disk file is now the durable audit record: written atomically via tempfile + `os.replace`, named `{stream_basename}_{event_id_hash}.json`, configurable via `HEBER_DLQ_FALLBACK_DIR`. The dead-letter helpers return success when the event is captured by the file *or* Redis (caller may ACK) and only fail if both do. Both consumers log the current day's backlog file count at startup.
- **heber-watch gateway auth preflight at boot** — `WatchService.run()` now performs a single auth-required probe against the gateway (`/api/v1/uw/SPY/iv-rank`) before starting the consumer/poller/backfill loops. On `401`, the service emits a `CRITICAL` log explicitly naming the cascade (`Feature enrichment WILL fail and Gold meta_label_features WILL be written with null Greeks unless this is fixed`) and stores `service._auth_preflight_ok = False`. Transport errors log a warning and leave the flag `None` (unknown). The service still starts in all cases — preflight is observational, not gating — so a transient probe failure cannot turn a credential outage into a service outage. Implemented in `heber/watch/writer.py::WatchService._gateway_auth_preflight`. Tests in `tests/test_watch_service_auth_preflight.py`.

### Fixed

- **Gold feature poller no longer silently stops producing 6 datasets when one pipeline OOMs** — the `heber-gold-poller` runs all 16 feature pipelines in a single sequential in-process loop. The `straddle_momentum` pipeline (#9 of 16) loaded the entire ~71-day lookback window of the 12 GB `option_chain_snapshot` Silver feed into one DataFrame and exploded every `chain_json` blob into per-contract rows, exhausting memory. The resulting OOM SIGKILL could not be caught by the per-pipeline `try/except` (it kills the whole process), so with `restart: always` the poller crash-looped through each EOD window and **never reached the pipelines scheduled after straddle** — `trend_scan_features`, `flow_context_features`, `market_regime_features`, `iv_surface_features`, `flow_normalization_features`, `ticker_base_rates` (frozen ~2026-04-23 through 04-29) plus `straddle_momentum_features` and `excursion_analytics` (never written). No alert fired; `heber-sync` and Orion's feature store faithfully mirrored/read the last-written values, so Orion silently scored on 5–7-week-old features. Three fixes:
  1. **Poller crash isolation** (`heber/gold_poller/service.py`) — each pipeline now runs in an isolated `spawn` subprocess with a wall-clock timeout (`HEBER_GOLD_POLLER_PIPELINE_TIMEOUT_SECONDS`, default 1800). A hard crash, OOM, or hang in one pipeline is contained to its child and surfaces to the parent as a normal exception that the existing retry/continue logic handles, so a single failing pipeline can never again starve the rest. Pipeline-failure logs now name the affected `gold_datasets`.
  2. **Straddle memory** (`heber/features/pipelines/straddle_momentum_features.py`) — `option_chain_snapshot` holds a handful of underlyings but hundreds of intraday snapshots/day, each a ~4 MB `chain_json` blob. The daily ATM straddle value only needs each underlying's **closing** chain, so the pipeline now reads **one day at a time with a two-pass read**: pass 1 reads just `(underlying, ts_event)` (no blobs) to find each underlying's last snapshot; pass 2 reads only the closing tail and explodes those. Peak RSS dropped from ~8 GB (OOM) to ~1 GB and runtime from ~7 min to ~2.5 min, with identical output. (Note: the daily value is now taken from the end-of-day chain rather than pooled across the day's snapshots.)
  3. **`dt`-partition pruning** (`heber/reader/core.py`) — `read_silver(prune_by_dt=True)` adds a predicate on the `dt` hive partition derived from `time_range`, so a per-day read opens only that day's partition instead of all of a feed's files (4650 for `option_chain_snapshot`). Safe because Silver writes `dt = ts_event.strftime("%Y-%m-%d")`; off by default, opt-in per caller.

  Regression coverage in `tests/unit/test_straddle_momentum_features.py` (two-pass per-day load + end-of-day snapshot reduction) and `tests/unit/test_gold_poller_isolation.py` (subprocess crash/hang containment).

- **Metrics-server bind failures no longer kill the consumer (or any other Heber service)** — when the Prometheus metrics HTTP server cannot bind its port (most commonly `OSError: [Errno 48] Address already in use` when another local Heber service already holds 9090), `heber.ops.metrics.start_metrics_server` now logs a WARNING (`metrics_server_bind_failed`) and continues instead of re-raising. Service entry points (`heber.writer.consumer`, `heber.watch`, `heber.gold_poller`, `heber.health_monitor`, `heber.writer.compactor`, `heber.catalog.api`, `heber.backfill`) additionally wrap the call site with a defense-in-depth try/except so any other unexpected metrics-startup error logs `metrics_server_startup_skipped` and the service still comes up. Net effect: the consumer (and friends) start without metrics rather than crash-looping. The lakehouse simply runs without a Prometheus scrape endpoint until the port conflict is resolved.

- **Silent data loss when Redis DLQ writes failed** — when message processing failed AND the subsequent `XADD` to `heber:events:dlq` also failed, the writer consumer left the message pending and the watch consumer logged a single error line; the event was effectively lost with no audit trail (98 occurrences in catalog/writer logs and 88 in watch logs over 13 days). `_send_to_dlq` (writer) and `_dead_letter_message` (watch) now wrap the `XADD` in a bounded exponential retry (3 attempts at 0.5s/1s/2s) and on exhaustion write the event to the local `dlq_fallback/` directory before returning success so the source Redis message can be ACKed. Neither path raises from the DLQ helper anymore.

- **heber-watch alert parser hardening** (P2 data-quality cleanup) — four classes of warnings flooded the watch logs (34 `unrecognized_put_call`, 44 `Could not parse alert`, 264 `Alert missing required fields`, 44 `Could not get entry price` over 13 days) with too little context to diagnose. `heber/watch/consumer.py` now: (1) `_normalize_put_call` accepts case-insensitive variants, single-char aliases (`C`/`P`), compound strings (`call_sweep`, `PUT_BLOCK`), dict-wrapped values (`{"option_type": "put"}`), and falls back to deriving put/call from the OCC option-symbol letter when the explicit field is unrecognized; the warning emit now logs `raw_value=repr(value)`, `raw_type`, `event_id`, and `occ_symbol` instead of an opaque `value=value` that serialised non-string scalars as bare integers; (2) the "Alert missing required fields" log now includes `event_id`, `provider`, `feed`, the explicit `missing_fields` list, and `payload_keys` (key names only, no values, to avoid leaking PII or large blobs); (3) the "Could not parse alert" log gains the same `event_id` / `provider` / `feed` / `payload_keys` diagnostic context; (4) entry-price resolution is now a documented fallback chain — `_resolve_entry_price` tries `contract_px` on the alert → live gateway quote → bid/ask mid on the alert → last trade price on the alert → exhausted, and the final "Could not get entry price" warning records which leg fired via `fallback_used` plus all the price hints the alert carried. Coverage: `tests/watch/test_alert_parser_hardening.py` (19 unit tests).

- **Silent null-Greek corruption of Gold `meta_label_features`** — `heber/ml/datasets.py::persist_features_to_gold` now diverts feature rows where every Greek enrichment column (`delta`, `gamma`, `theta`, `vega`, `iv`) is null to a sibling `_quarantine/all_greeks_null/dt=<date>/` partition instead of writing them to the canonical `dt=<date>/` partition. The 2026-03-20 incident (132 gateway 401s) produced 880 null-Greek rows that were detected by `heber/quality/write_audit.py` but not blocked — the audit was observation-only. Now those rows are quarantined with an ERROR-level structured log pointing at the upstream cause (`check heber-watch logs for 401/5xx around this time`), so downstream ML training (`heber.ml.datasets.MetaLabelDataset`, Orion scorers) never sees the corruption. Partial-Greek rows (one or two values present) are kept — only the all-null fingerprint is treated as a gateway-failure casualty. Quarantine writes are best-effort: if the quarantine write itself fails the rows are dropped, never silently promoted to canonical. Regression tests in `tests/test_meta_label_features_greeks_guard.py`. Existing `tests/test_watch_feature_persistence.py` was updated to populate Greeks on its sample features so the happy-path persists.

- **`heber-catalog_errors_*.log` contained emits from `writer/consumer.py`, `bus/backpressure.py`, and `writer/compactor.py`** — once any code path in a Python process called `configure_logging(service_name="heber-catalog")`, the underlying `empire_core.logger.setup_logging` short-circuited on its module-global `_configured` flag, so a *second* `configure_logging("heber-consumer")` (or any other service) silently kept the catalog's service name and log filename. Every subsequent structlog emit anywhere in the process was stamped `service="heber-catalog"` and rotated into the catalog's daily log files. The historical leakage into production `logs/` came specifically from pytest runs that booted the catalog API's lifespan before running writer/consumer tests, with the pre-March-26 conftest writing to the production log dir. Two-part fix: (1) `heber/ops/logging.configure_logging` now passes `force=True` to `setup_logging` by default, so the requested service name always wins even when re-entered in the same process (the `force` kwarg is opt-out for callers who need the legacy no-op behaviour); (2) root `conftest.py` autouse fixture resets `empire_core.logger._service_name` between tests so a single test that calls `configure_logging` can't pollute downstream tests' service binding. Regression tests in `tests/test_configure_logging_service_binding.py`.

- **Watch service produced 176 "Watch service stop failed" errors per shutdown** (`heber/watch/__main__.py`, `heber/watch/writer.py`). Two compounding causes: (1) `WatchService.stop()` was not idempotent — when SIGTERM-driven shutdown called it, the CLI's `finally` block called it again and hit an already-closed redis client / already-flushed writer, raising; (2) the entrypoint used `signal.signal()` directly, which produced `ValueError: signal only works in main thread` warnings whenever the service was launched from a non-main-thread context (test harness, ASGI worker, embedded usage). Fixes: `WatchService.stop()` now guards with `self._stopped` and runs every component's cleanup best-effort (one failure no longer aborts the rest); `_install_signal_handlers()` in `heber/watch/__main__.py` switches to `loop.add_signal_handler()` (the asyncio-native path that runs the callback inside the event loop rather than in an OS-signal-interrupt context), and refuses to install with a single WARNING when called off the main thread or on a platform without unix signals — the service still starts and exits cleanly via the `finally` block. Regression tests in `tests/test_watch_service_stop_idempotent.py`.

- **`treasury_yields` 100 % DLQ'd — `instrument_type=macro` rejected by envelope key validator** — Data-Gateway emits AlphaVantage treasury yields as `instrument_type=macro, instrument_key=macro:treasury_yield:{maturity}`, but `INSTRUMENT_KEY_PATTERNS` only knew `equity|crypto|forex|option`, so every event was rejected at `normalize_envelope_for_silver` with `Invalid instrument_key format for instrument_type macro`. Bronze accumulated 22+ partitions (last 2026-04-27); Silver had zero rows for the dataset. Fix: register a `macro` pattern (`^macro:[a-z][a-z0-9_]*(?::[a-z0-9_]+)*$`) that accepts `macro:treasury_yield:2year`, `macro:cpi`, `macro:fed_funds_rate`, etc. Existing DLQ entries can be drained via `python -m heber.writer.dlq_reprocessor --reprocess --feed treasury_yields`. Regression tests in `tests/test_equity_instrument_key_format.py::test_macro_*` and `tests/test_ingest_feed_contract_matrix.py` (treasury_yields case added to the feed matrix).

- **Zero-leakage contract violated by future-dated provider `ts_event` (flow_alerts)** — Unusual Whales emits `ts_event` 5–11 s in the future relative to `ts_ingest` (window-end batch timestamps). `envelope_to_silver_row` was setting `ts_available = envelope.ts_available or ts_ingest`, which landed BEFORE `ts_event`, breaking the `ts_available >= ts_event` invariant on ~0.17% of flow_alerts rows daily (155 / 88,956 over the last 9 trading days). Any Gold pipeline asserting the invariant would reject these rows. Fix: clamp `ts_available` up to `ts_event` when the provider sends a future-dated event time. Regression tests in `tests/test_writer_coverage.py::TestEnvelopeToSilverRow::test_ts_available_clamped_*`.
- **`trend_scan_features` (and any reader of mixed-encoding Silver feeds) crashed with `ArrowTypeError: Unable to merge: Field feed has incompatible types: large_string vs string`** — Silver `bars` partitions are a mix of files where the `feed`/`symbol`/`instrument_key`/etc. string columns are encoded as `string` (older real-time writer output, ~436 files) and `large_string` (newer compactor output, ~2073 files), plus `trade_count` is `int64` in some files and `double` in others. pyarrow's dataset auto-merge picks one encoding as the unified schema and then refuses to read fragments with the other, raising `ArrowTypeError` from `ds.dataset(...)` itself when the file list (rglob-unsorted) starts with a `large_string` fragment. `heber/reader/core.py:_open_dataset_safe` now (1) catches `ArrowTypeError` in addition to `ArrowInvalid`/`ArrowNotImplementedError` and falls into manual schema unification, (2) folds all string-family encodings (`string`, `large_string`, `dictionary<string, *>`, `dictionary<large_string, *>`) down to plain `pa.string()` via a new `_normalize_string_type` helper, (3) widens `int64`-vs-`float64` numeric disagreements to `float64` (lossless for values under 2^53) so int-typed schemas don't reject float fragments, and (4) re-introduces hive partition columns (e.g. `dt`) into the unified schema — without them pyarrow raises `ArrowInvalid: No match for FieldRef.Name(dt)` when re-opening the dataset with an explicit `schema=`. `_coerce_dict_columns_to_string` was extended symmetrically so any post-read `large_string` columns are also coerced to `string` for downstream consumers.
- **Write-audit false-positive warnings on canonical-row-per-snapshot datasets** — `option_chain_snapshot`, `oi_change`, and `darkpool` were producing 5,179+ "Null values detected at write time" WARNING events over 48 h. Investigation: this is NOT an upstream Data-Gateway data-quality issue. `option_chain_snapshot` is canonically a row-per-underlying-snapshot with the full chain serialized in `chain_json`, and the per-contract Arrow columns (`occ_symbol`, `strike`, `bid`, `ask`, `last`, `volume`, `put_call`, `underlying_price`) are LEGITIMATELY null because they exist in the schema only for legacy `ChainSnapshotRecord` compatibility. Same shape for `oi_change` aggregate rows (`last_ask`/`last_bid`/`avg_price` are optional) and `darkpool` (NBBO context absent for off-hours prints). Fix: add the three datasets to `EXPECTED_NON_NULL` in `heber/quality/write_audit.py` with their truly-required columns (matching `REQUIRED_FIELDS_BY_FEED` in `ingest_contracts.py`). Audit still warns when a canonical required field (e.g. `chain_json`) is actually null. Regression tests in `tests/test_write_audit.py::TestCanonicalRowPerSnapshotDatasets`.
- **heber-compactor stuck silently on macOS `._` resource-fork files** — `Compactor.scan_and_compact` walked the silver tree with `rglob("*")` and called `is_dir()` on every entry. AppleDouble files (`._foo`) on the exFAT/NTFS-mounted external volume raised `PermissionError [Errno 1] Operation not permitted` from the underlying `stat()`, the exception propagated to the top-level handler, and the cycle slept 60s before crashing on the same file again — explaining 20+ days of zero compaction output. Fix mirrors the catalog fix in 8af04ef: skip names starting with `.` before the `is_dir()` check. Regression test in `tests/test_compactor_safety.py::test_scan_and_compact_skips_macos_resource_fork_paths`.
- **`darkpool_features` Gold partitions polluted by colliding pipeline writes** — both the dedicated `darkpool` pipeline (`darkpool_features.py`) and `MarketIntelPipeline` were writing to `gold/dataset=darkpool_features` with disjoint schemas. The dedicated pipeline produces the per-ticker schema (`darkpool_notional_1d`, `darkpool_premium_ratio`, `darkpool_activity_zscore`) that downstream consumers (Orion ML scorer's `EQUITY_DARKPOOL_FEATURES`) configure against; `MarketIntelPipeline.run()` defaults to `ALL_DATASETS`, which includes a separate `compute_darkpool_features` writing the legacy NBBO-classification schema (`total_volume`, `total_notional`, `pct_above_nbbo`, `pct_below_nbbo`). PyArrow's multi-file dataset read merged both into a frame missing the columns consumers actually requested — Orion's per-ticker darkpool features were returning empty for every recent partition. The gold-poller registry already declared `market_intel`'s output as just `["greek_exposure_features", "options_sentiment_features", "ftd_features"]`, so the dedicated `darkpool_features` pipeline was the intended owner. Fix: the registry now passes `datasets=("greek_exposure", "options_sentiment", "ftd")` to `MarketIntelPipeline.run()` so the legacy darkpool branch never executes during scheduled poller runs. CLI users can still pass `--datasets darkpool` explicitly for the legacy schema if needed.
- **`ticker_base_rates` pipeline reads alert labels from non-existent `project=quant`** — `TickerBaseRatesPipeline.__init__` defaulted `labels_project="quant"`, but `heber/watch/writer.py` writes `labels_alert_barriers` under `project="watch"` (matching `heber/ml/datasets.py`'s `_PROJECT_WATCH` constant). Every scheduled run logged `heber_reader_gold_not_found` for `/data/gold/dataset=labels_alert_barriers/project=quant` and finished with `total_rows=0`, so the `ticker_base_rates` Gold dataset has been empty since the pipeline shipped. Default changed to `labels_project="watch"`.

- **equity_features OOM guard** — when more than 1 million bar rows are loaded and daily (`1Day`) bars cover all dates, intraday rows are dropped before resampling. This prevents an out-of-memory crash in the gold-poller that occurred when the Silver bars table grew to cover long date ranges with mixed timeframes.
- **heber-watch gateway key not required in dev** — `_resolve_gateway_api_key` no longer raises a hard error when `HEBER_WATCH_GATEWAY_API_KEY` is unset in non-production environments. It now emits a structured warning and returns an empty key, allowing local development and testing without credentials. Production (`HEBER_ENVIRONMENT=prod`) still enforces the key.



- **heber-watch NOGROUP self-healing** — if the `watch-consumer` Redis consumer group is ever dropped (e.g. after a Redis flush/restart without persistence), `WatchConsumer._read_messages` now catches the `NOGROUP` `ResponseError`, re-runs `setup_consumer_group`, and retries the `XREADGROUP` call exactly once. Previously the service got stuck in an indefinite retry loop (observed today with 450+ consecutive errors over ~4 hours) because the one-shot group-create at startup couldn't recover from mid-lifecycle group loss. The group was manually recreated (`XGROUP CREATE heber:events watch-consumer $`) to unblock today's session.

### Known issues (not yet fixed)

- **Duplicate rows in `labels_alert_barriers`** — steady-state dup ratio is ~1.05–1.32× (one alert on 2026-03-11 appears 918×, likely from a one-off replay). Root cause is a concurrency race: `WatchManager.update_watch_price` reads the watch with `get_watch`, mutates it in memory, and calls `_save_watch` — while the checker may concurrently call `complete_watch` to set status to terminal and srem from `ACTIVE_WATCHES`. If the checker wins the race on the save but the poller's stale in-memory watch is saved afterward, the watch is resurrected with status=WATCHING and re-added to the active set, so the checker re-evaluates it on the next cycle and writes a second outcome row. Proper fix needs a Redis WATCH/MULTI transaction or status re-check in `update_watch_price`. Downstream readers should `drop_duplicates(subset=['alert_id'], keep='first')` for now.
- **All-zero 0DTE label rows** — every `labels_alert_barriers` row for 0DTE alerts (210 unique alert_ids over 90 days) has `bars_to_hit=0, mfe=0, mae=0, outcome="expired"` via the `_handle_no_snapshots` path. **Root cause is a data-pipeline characteristic, not a heber-watch code bug.** Three compounding factors: (1) ~35% of 0DTE alerts (74/210) are SPX/SPXW index options, which Alpaca doesn't support, so gateway returns nothing; (2) 14 of 15 historical trading days with 0DTE flow have ALL alerts firing between 16:00–16:10 ET — settlement prints from UW's flow feed for trades at the closing bell, by which time the contract has expired and the 4-hour trading-hour window rolls to the next day when the contract is dead; (3) gateway's Alpaca options subscription universe covers only the most liquid 0DTE tickers (SPY works; QQQ/AAPL/most equity 0DTE return `GW-E4004 No quotes found`). Decision: accept this and filter downstream (Orion's pattern miner already skips training when target has no variance). Not worth a heber-side filter unless the upstream data flow gets richer — real fix would be getting Data-Gateway to subscribe to broader intraday 0DTE quotes and/or persuading UW to publish 0DTE alerts earlier in the session.

- **Gold poller OOM crash on equity_features pipeline** — The `equity_features` pipeline loaded ~100 days of Silver bars for all equities into memory at once via `to_table()`, exceeding available memory and causing the heber-gold-poller container to crash-loop (1400+ restarts). Two fixes applied: (1) `HeberReader.read_silver()` now accepts a `batch_size` parameter that uses PyArrow's `Scanner` to stream data in fixed-size record batches instead of materializing everything at once; (2) the equity_features pipeline now uses column projection (12 of 20+ columns) and batched reading (`batch_size=500_000`), reducing peak memory by ~50-60%.

### Fixed

- **Feature enrichment timeouts for large option chains (QQQ, SPY)** — Option chain enrichment requests used a hardcoded 10-second HTTP timeout, but large chains (QQQ ~9,800 contracts, SPY ~3,200 contracts) take 6-7 seconds to respond from the Data-Gateway. Combined with semaphore queueing and retries, this caused 551 "Feature enrichment request failed" errors and 250 backfill re-enrichment failures per day. The option chain enrichment endpoint now uses a dedicated 30-second timeout (`HEBER_WATCH_ENRICHMENT_OPTION_CHAIN_TIMEOUT_SECONDS`), while other enrichment endpoints keep the 10-second default (`HEBER_WATCH_ENRICHMENT_TIMEOUT_SECONDS`). Both are configurable.
- **Reduced false-positive null warnings for `historic_option_volume`** (`heber/quality/write_audit.py`): Added explicit `EXPECTED_NON_NULL` entry for the `historic_option_volume` Silver dataset, limiting null auditing to `hov_date`, `volume`, and `ts_event`. Previously, the audit checked ALL columns (including legitimately nullable fields like `call_volume`, `put_volume`, `premium`, `expiry`), generating thousands of spurious "Null values detected at write time" warnings. Root cause of the volume nulls was upstream in Data-Gateway (volume=0 treated as None); see Data-Gateway changelog.

### Changed

- **Silver writer min-rows gate for backfill throughput** — Added `HEBER_SILVER_MIN_ROWS_PER_FLUSH` setting (default 50) that prevents flushing partitions with fewer rows than the threshold. During backfill, records scatter across hundreds of date partitions with only 2-4 rows each, creating tiny parquet files and tanking throughput to 2-7 msg/s. The min-rows gate lets rows accumulate across XREADGROUP batches before flushing, reducing file count ~15x. The time-based safety valve (`silver_max_flush_time_seconds`) still ensures data is eventually persisted.
- **Increased default Redis read batch size** — `HEBER_REDIS_READ_BATCH_SIZE` set to 2000 in docker-compose (was 500 default) for better backfill throughput.

### Removed

- `DataQualityValidator` contracts module (`heber/quality/contracts.py`) — superseded by the Data Health Monitor's comprehensive check system
- Daily health checks for partition freshness, fill rate, zero-leakage, and DLQ status — superseded by health_monitor (checks/partition.py, checks/volume.py, checks/ml_readiness.py, checks/stream_health.py). The daily report now runs 3 unique checks: cross-feed completeness, Soda quality, and Gold freshness.

### Fixed

- Replaced `(str, Enum)` multiple-inheritance on `Severity` and `Status` in `heber/health_monitor/models.py` with `StrEnum` (ruff UP042)
- Fixed `AttributeError` in `heber/bus/backpressure.py` where `_get_or_create` accessed the removed `REGISTRY._collectors` attribute; updated to use the current `REGISTRY._names_to_collectors` dict instead, restoring test collection for `test_backpressure_quarantine_paths.py`
- Test runs no longer write to production `logs/` directory — `EMPIRE_LOG_DIR` is redirected to `/tmp/heber-test-logs` via root `conftest.py`
- Debug investigation artifacts (`debug/`) removed from git tracking and added to `.gitignore`
- Removed unused `UTC` import from `heber/features/pipelines/trend_scan_features.py` (lint F401)
- Broke two over-length lines in `heber/ml/datasets.py` into named boundary variables (lint E501)
- Fixed `test_baseline_written_after_check` in `tests/health_monitor/test_volume.py` — test was incorrectly asserting that `run_volume_checks()` writes the volume baseline; baseline writing is a separate step (`write_volume_baseline()`) called from the Tier 3 service loop, so the test now calls `write_volume_baseline` directly

### Added

- **Typed config section accessors** — `Settings` now exposes `@property` methods (`settings.redis`, `settings.storage`, `settings.gold_poller`, `settings.watch`, `settings.health_monitor`, etc.) that return typed NamedTuples for grouped dot-access (e.g. `settings.redis.url`). All existing flat field access (`settings.redis_url`) is unchanged.

- **Data Health Monitor** — New tiered monitoring service (`python -m heber.health_monitor`) that detects data gaps, volume anomalies, schema drift, and ML quality issues across all Silver feeds. Three check tiers: stream health (30s), partition completeness (15min), statistical profiling (EOD). Market-calendar-aware to suppress false positives. Results stored in `gold/dataset=data_health/` and exposed via Prometheus metrics and `/api/v1/health/summary` catalog endpoint.
- **Health Monitor service orchestrator**: New `HealthMonitorService` that runs three tiered check loops in parallel — Tier 1 (stream health, every 30s), Tier 2 (partition + volume, every 15min during market hours), Tier 3 (schema + statistical + ML readiness, once daily after EOD). Includes `__main__.py` entry point for `python -m heber.health_monitor`.
- **Health Monitor Docker Compose service**: New `heber-health-monitor` container in `docker-compose.yml` exposing Prometheus metrics on port 9093.
- **Health summary API endpoint**: New `GET /api/v1/health/summary?days=N` endpoint on the Catalog API that returns the latest health check results and pass/warn/fail/error counts.
- **Tier 1 stream health checks**: New health monitor check that verifies Redis stream reachability, consumer group status, consumer lag (pending messages), and DLQ depth every 30 seconds. Severity is suppressed to P2_INFO outside market hours.
- **Tier 2 partition completeness checks**: New health monitor check that verifies expected Silver partitions exist and contain data every 15 minutes during market hours. Checks dt= partitions for all feeds and hour= sub-partitions for bars, trades, and quotes. Skips non-trading days.
- **Tier 2 volume trending checks**: New health monitor check that compares today's Silver partition row counts against a trailing N-day baseline median. Alerts at configurable warn (50%) and critical (20%) thresholds. Reads Parquet metadata only (no data loading). Skips feeds with no baseline on first run.
- **Tier 3 schema drift detection**: New health monitor check that fingerprints Arrow schemas per Silver feed and detects column additions (WARN), column removals (FAIL/P0), and type changes (FAIL/P0). Stores schema baselines via HealthStore and increments `health_schema_changes_total` Prometheus counter on drift.
- **Cross-system excursion analytics Gold pipeline**: New pipeline that reads LedgerTrade records from all trading system ledgers (3Roses, Kairos, Cerberus, Orion, whalehunter, trading-bot, options-bot) and writes unified excursion profiles (MFE/MAE, capture efficiency, excursion velocity, holding time) to the Gold layer. Includes Feast feature view and gold poller registration.
- **Temporal excursion fields in alert label feature views**: Added `time_to_mfe_seconds`, `time_to_mae_seconds`, `mfe_mae_ratio`, `excursion_velocity`, and `capture_efficiency` to all three alert barrier label Feast feature views (all-horizons, intraday, swing).

### Fixed

- **`DataLayer` implicit re-export causing mypy `attr-defined` error**: `heber.catalog.access_control` now explicitly re-exports `DataLayer` using the `import X as X` form so that mypy recognises it as a public symbol when `openmetadata_client` imports it from that module.
- **Feature view alignment test out of sync with alert_barrier_labels schema**: Test expected fields list for `alert_barrier_labels` did not include the five temporal excursion fields (`time_to_mfe_seconds`, `time_to_mae_seconds`, `mfe_mae_ratio`, `excursion_velocity`, `capture_efficiency`) added in the prior commit. Updated `test_feature_view_alignment.py` to match the live schema.
- **Compactor OOM crash-loop on option_chain_snapshot**: Added a 50 MB compressed-bytes-per-batch budget cap in addition to the existing file-count cap. `option_chain_snapshot` files contain a `chain_json` column that expands ~6× in memory (210 MB compressed → 1.2 GB uncompressed); the previous 50-file batch was attempting to load ~60 GB into RAM. The new budget limit skips batches that would exceed memory, stopping the 1932-restart crash loop.
- **Compactor OOM on large partitions**: Capped compaction batch size to 50 files per pass to prevent OOM kills on partitions with many small files (e.g., `option_chain_snapshot` with 189 files / 565MB).
- **Gold poller write_gold tz comparison crash**: Fixed `write_gold()` look-ahead check comparing tz-naive `ts_available` against tz-aware `ts_event` — now normalizes both to UTC before comparison.
- **Sector tide read failure from macOS resource fork file**: Deleted stale `._compacted-*` resource fork file in `sector_tide` partition that blocked PyArrow dataset reads.
- **Gold poller crash loop — tz-aware ValueError in 15 feature pipelines**: All Gold feature pipelines used `pd.Timestamp(start_date, tz="UTC")` which raises `ValueError` when `start_date` is already timezone-aware (as passed by the gold poller). Replaced with `pd.to_datetime(start_date, utc=True)` which handles both naive and aware inputs. Affected: darkpool, flow_toxicity, market_tide_context, sector_flow, flow_context, and 10 other pipelines.
- **OI momentum pipeline KeyError on 'underlying' column**: `oi_momentum_features.py` referenced a non-existent `underlying` column — the Silver oi_change schema uses `symbol`. Fixed all groupby/sort references and removed the now-unnecessary rename.
- **Compactor crash on large string partitions**: Cast string columns to `large_string` before sort/dedup to prevent PyArrow 32-bit offset overflow on partitions with large cumulative string data (e.g., `option_chain_snapshot`).
- **Compactor crash on zero-byte parquet files**: Skip 0-byte corrupt files during compaction instead of crashing when `ParquetFile()` tries to read them.
- **Consumer NOGROUP infinite retry loop**: When `data-gateway-redis` restarts without persistence, the `heber:events` stream and consumer group disappear. The consumer now auto-recreates the stream and consumer group on NOGROUP errors instead of entering an indefinite error retry loop.
- **Docker healthcheck regression**: Restored all 5 healthchecked services in `docker-compose.yml` to production-grade settings (`interval=10s, timeout=5s, retries=5, start_period=120s`) — regressed by repo hygiene commit to aggressive dev values, causing premature unhealthy marks during slow recovery.
- **Gold Poller timezone mismatch**: Fixed `_last_run_date` using UTC date while `_should_run()` uses ET date — could cause double pipeline execution if runs finished after midnight UTC. Both now use Eastern Time consistently.
- **Equity feature symbol column inconsistency**: Fixed 4 compute functions (`compute_momentum_features`, `compute_volatility_features`, `compute_microstructure_features`, `compute_return_labels`) setting `symbol` to full `instrument_key` value (e.g., `equity:AAPL`) instead of plain ticker (`AAPL`). Cross-dataset joins with `flow_features` would silently produce zero matches.
- **Backfill cancel ignored**: Fixed `cancel_job` setting status to CANCELLED but running job continuing to completion and overwriting status to COMPLETED. Added cancellation check in the chunk processing loop.
- **Ticker base rates label leakage**: Fixed `compute_ticker_base_rates` including the current alert's own outcome in its `ticker_win_rate_90d` feature — textbook label leakage that causes meta-model overfit. Rolling window now uses strictly-before comparison (`< current_ts`). First alerts with no prior history emit NaN features instead of being silently dropped.
- **Misleading Gold write success log**: Fixed `persist_features_to_gold` logging "Persisted features partition" even when the write was skipped due to file lock timeout. Now skips the success log on lock contention.

- **Label instrument_key was alert UUID**: Fixed `outcome_to_label_row` setting `instrument_key` to the alert UUID instead of the actual instrument key (e.g., `option:OCC:AAPL260116C00200000`). Downstream `ticker_base_rates` grouped by UUID instead of ticker, producing garbage base rate features (`watch/checker.py`).
- **Daily label availability_lag silently erased**: Fixed `compute_availability_time` applying `availability_lag` before snapping to market close time for daily labels — the snap overwrote the lag, making labels available earlier than intended (`gold/labels.py`).
- **Walk-forward splits infinite loop on zero step**: Fixed `walk_forward_splits` hanging forever when `step="0d"` or `step="0s"` — now raises `ValueError` for zero-step inputs (`gold/splits.py`).
- **Silver writer salvage non-atomic write**: Fixed `write_silver_parquet` salvage code path (row-by-row fallback after type errors) writing directly to the final file path instead of using atomic temp-then-rename. Crash during salvage write could leave corrupt Parquet files (`writer/utils.py`).
- **Duration parser accepts trailing garbage**: Fixed `parse_duration` regex not being anchored — strings like "5dGARBAGE" silently parsed as 5 days. Now rejects malformed input with `ValueError` (`gold/duration.py`).

- **Maintenance 2026-03-20**: Fixed `exc_info=True` in `EventConsumer._process_stream_messages` producing `"exception": "MISSING"` in logs — exceptions caught via `asyncio.gather(return_exceptions=True)` are not in the active exception context, so structlog captured nothing. Now passes `exc_info=result` to include the actual exception traceback (`writer/consumer.py`).

- **27-bug sweep across Heber codebase** (2026-03-18):
  - **[HIGH]** Fixed `POST /api/v1/datasets` endpoint crash — `dataset_name=` keyword didn't match service's `name=` parameter (`catalog/api.py`)
  - **[HIGH]** Fixed `_normalize_symbol` rejecting dot-class equity tickers like BRK.B, BF.A (`writer/key_normalization.py`)
  - Fixed Gold Poller hardcoded EDT offset (wrong during EST Nov-Mar) — now uses `ZoneInfo("America/New_York")` (`gold_poller/service.py`)
  - Fixed naive datetimes passed to Gold pipeline.run() — now UTC-aware (`gold_poller/service.py`)
  - Fixed `upsert_instrument` silently ignoring `instrument_type`/`canonical_symbol` on updates (`catalog/service.py`)
  - Fixed `CancelledError` re-raised during catalog API lifespan shutdown (`catalog/api.py`)
  - Fixed unbounded `_silver_validation_warning_counts` dict growth in consumer (`writer/consumer.py`)
  - Fixed float-epoch timestamp strings (e.g. "1710000000.5") silently becoming None (`writer/normalizer.py`)
  - Fixed non-atomic Bronze gzip and Silver Parquet writes — now use temp-file-then-rename (`writer/bronze.py`, `writer/utils.py`)
  - Fixed `asyncio.gather` not cancelling siblings on failure in WatchService — now uses `TaskGroup` (`watch/writer.py`)
  - Fixed WatchService.stop() not closing Redis connection (`watch/writer.py`)
  - Fixed compactor blocking event loop during entire compaction cycle — now uses `asyncio.to_thread` (`writer/compactor.py`)
  - Fixed division by zero in `SliceTracker.generate_report()` when no slices exist (`ops/slices.py`)
  - Fixed `_normalize_put_call` silently defaulting unknown values to "C" (Call) — now returns None (`watch/consumer.py`)
  - Fixed `_open_dataset_safe` silently swallowing exceptions — now logs on fallback paths (`reader/core.py`)
  - Fixed LakeFS `_get_repo` auto-create fallback that could never fire (`versioning/__init__.py`)
  - Fixed `update_coverage` falsy check treating `approx_row_count=0` as missing (`catalog/service.py`)
  - Removed dead `buffer_counts` field from BronzeWriter (`writer/bronze.py`)
  - Removed dead expression in Archiver (`retention/__init__.py`)
  - Fixed naive `datetime.now()` in ml/datasets.py — now uses UTC (`ml/datasets.py`)
  - Added `exc_info=True` to 12+ `logger.error()` calls missing tracebacks across ops, bus, quality, catalog, gold modules
  - Added null field audit to Silver salvage write path (`writer/utils.py`)
  - Added LabelWriter buffer overflow cap at 10,000 entries (`watch/writer.py`)
  - Added debug logging to silent Feast resolution fallbacks (`feast/materialization.py`)
  - Fixed misleading docstring in zero-leakage health check (said `<=` but contract is `>=`) (`ops/daily_health.py`)
  - Removed dead `if quotes:` conditional in SnapshotPoller (always true after early return) (`watch/poller.py`)

- **Health-check: dev deps installed and test suite fully passing** (2026-03-13):
  - Installed `dev` optional-dependencies (`pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `bandit`, etc.) into the Heber venv via `uv sync --all-extras`. Previously `uv run pytest` resolved to the system conda pytest, which ran outside the venv and could not find `empire_core`, causing all 52 test modules that import any `heber.ops.*` symbol to fail at collection with `ModuleNotFoundError: No module named 'empire_core'`.
  - Added input validation to `configure_logging()` in `heber/ops/logging.py`: passing an unrecognised log level (e.g. `"TRACE"`) now raises `ValueError("Invalid log level: …")` instead of silently falling back to INFO. Fixes `test_invalid_level_raises_value_error`.
  - Added `bind_context`, `clear_context`, and `unbind_context` to `heber/ops/__init__.__all__` — they were imported for re-export but omitted from `__all__`, causing three `F401` ruff violations.
  - Removed unused `typing.Any` import and fixed import block ordering in `heber/ops/logging.py` (ruff `F401`, `I001`).

- **Bronze-to-Silver Transformer Deduplication** (2026-03-12):
  - Added `_collect_existing_event_ids()` to `heber/writer/transformer.py` that reads existing `event_id`s from Silver partition files before writing, preventing duplicates during backfill reruns.
  - Also deduplicates within the same batch run via an in-memory set cache.
  - Root cause: March 11 flow_alerts had 229,600 duplicate rows (200 events × 341 copies) from a backfill that re-wrote events already in the compacted partition. Compactor cleaned the existing duplicates; transformer now prevents recurrence.
  - Added 7 regression tests in `tests/test_transformer_dedup.py` covering existing-event skipping, within-batch dedup, empty Silver writes, and idempotent rerun behavior.

- **Writer Consumer Early Dedupe + Fallback Drain Guard** (2026-03-11):
  - Added consumer-side `event_id` dedupe in `heber/writer/consumer.py` so duplicate events are dropped before Bronze/Silver writes, while still being acknowledged as handled.
  - Recorded duplicate drops through `heber_consumer_dedupe_drops_total` and structured `consumer_dedupe_dropped` logs for easier RCA.
  - Added regression coverage in `tests/test_writer_consumer_dedupe.py` to lock the duplicate-drop behavior in place.

- **Option chain snapshot spot persistence** (2026-03-10):
  - Added optional `underlying_price` to the canonical `option_chain_snapshot` silver schema and `OptionChainSnapshotRecord`.
  - Extended ingest-contract coverage so Gateway snapshots carrying a top-level spot price survive normalization into Silver rows unchanged.

- **HeberReader Code Quality Review** (2026-03-10):
  - Fixed `read_gold` version resolution bug: when `version=None`, the reader now correctly filters to only the latest version instead of scanning all versions.
  - Promoted `_read_parquet_dataset` to public `read_parquet_dataset` API and updated all callers (`ml/datasets.py`, `gold/labels.py`).
  - Changed `write_gold` to return `None` for empty DataFrames instead of `Path()` (meaningless sentinel).
  - Extracted `_build_scan_filter`, `_detect_time_col`, `_tuple_filters_to_exprs`, `_resolve_gold_scan_path` helpers to reduce cognitive complexity and eliminate duplicated filter-building logic.
  - Fixed unparameterized `tuple` type annotation and removed orphaned section comment.
  - Added 40 new tests in `tests/test_heber_reader.py` covering Silver/Gold reads, writes, asof_join, read_parquet_dataset, list_gold_versions, and all module-level helpers.

- feat: accept raw `alpaca/option_chain_snapshot` feed contracts and route them into the existing `option_chain_snapshot` silver dataset
- docs: clarify that the canonical option chain snapshot dataset stores one row per underlying snapshot with `chain_json`
- test: extend cross-repo parity and ingest contract coverage for the new Alpaca chain snapshot feed

- chore: workspace sync checkpoint and gitignore audit (2026-02-21)

### Fixed

- **Pre-commit** (`detect-secrets`): ignore `logs/` directories in secret scans to avoid false-positive detections from generated log files.

- **Consumer RCA: Malformed `insider_trades` Identifiers** (2026-03-09):
  - Added explicit `MissingInstrumentIdentifierError` and `InvalidInstrumentKeyError` classification in Silver normalization so blank `symbol` / `ticker` values from upstream insider-trade events fail with concrete data-quality context instead of a generic invalid-key error.
  - Added bounded `silver_validation_failed` warning logging in the consumer so repeated malformed events still write Bronze and route to the DLQ without flooding the error logs with duplicate tracebacks.
  - Added regression coverage in `tests/test_instrument_key_synthesis.py` and `tests/test_writer_consumer_reliability.py`.

- **Docker Build: Workspace Path Dependencies** (2026-03-09):
  - Switched Heber service image builds to the monorepo root context and updated `Dockerfile` copy paths so local `uv` dependencies on `../empire-core` and `../empire-schemas` resolve during container builds.
  - Added a static contract test in `tests/test_compose_restart_contract.py` to keep future Docker changes from reintroducing the same build failure.

- **Watch Enrichment: Index Symbol and Concurrent Write Fixes** (2026-03-09):
  - Added `INDEX_SYMBOLS` frozenset (`SPX`, `SPXW`, `NDX`, `VIX`, `RUT`, `DJX`, `XSP`, `IXIC`) to skip Alpaca stock bars calls for non-stock underlyings that return 400 errors (4,994 daily occurrences eliminated).
  - Added `/uw/max-pain/{symbol}` fallback route in `_enrich_max_pain()` to address 3,042 daily 404 errors from route pattern mismatch.
  - Added `filelock.FileLock` around the read-merge-write cycle in `persist_features_to_gold()` to prevent concurrent writers from corrupting Gold parquet partitions (6 daily quarantined files eliminated).
  - Added `filelock>=3.13` dependency to `pyproject.toml`.
  - Added regression tests in `tests/test_watch_index_symbol_skip.py` and `tests/test_features_gold_file_lock.py`.

### Fixed

- **Test Warning Cleanup: Catalog Discovery + As-Of Join** (2026-03-04):
  - Updated catalog auto-discovery tests to use synchronous `session.add()` mocks so async test sessions no longer emit un-awaited coroutine warnings.
  - Set `check_sortedness=False` in grouped `join_asof` calls to remove known Polars sortedness warnings when `by` keys are provided.

- **Watch Quote Polling Recovery + RCA Hardening** (2026-03-04):
  - Restored async `httpx.AsyncClient` event hooks in `heber/core/http_client.py` so async HTTP requests no longer fail with `NoneType can't be awaited`.
  - Hardened response logging hook elapsed-time handling to avoid `RuntimeError` when elapsed timing is unavailable.
  - Updated `heber/watch/poller.py` to mark poll cycles as `error` when due watches return zero quotes, returning `errors=1` in poll stats for clearer outage visibility.
  - Added stack traces (`exc_info=True`) to poller error logs for quote-fetch and poll-cycle exceptions.
  - Added regression coverage in `tests/test_http_client_async_hooks.py` and `tests/test_watch_poller_metrics.py`.

- **Watch Backfill Crash on Corrupt Feature Partitions** (2026-03-03):
  - `heber/watch/backfill_scanner.py` now catches Polars/PyO3 panic exceptions while reading partition parquet files, logs full context, and skips only the bad partition instead of crashing the whole watch service.
  - `heber/ml/datasets.py` now quarantines unreadable existing `data.parquet` files as `data.parquet.corrupt-<timestamp>` before writing fresh data.
  - Feature partition writes now use an atomic temp-file rename to prevent partial-write reads that can create unreadable parquet artifacts.
  - Added regression tests in `tests/test_enrichment_backfill_scanner.py` and `tests/test_meta_label_dataset_paths.py`.

- **Noisy `greek_exposure` Null Warnings Reduced** (2026-03-03):
  - Added `("silver", "greek_exposure")` to write-audit expected non-null contracts so audit checks only truly required fields.
  - `strike`, `expiry`, and `dte` are now treated as optional for aggregation rows, removing high-volume warning noise from normal ingestion.
  - Added regression coverage in `tests/test_write_audit.py`.

- **Heber Watch Gateway Connection**:
  - Reverted the `DATA_GATEWAY_URL` port in `docker-compose.yml` back to `8080`. A recent change incorrectly set it to `8081`, causing `All connection attempts failed` when the watch service tried to fetch quotes.

- **Data Flow Integrity: Flush failure no longer silently drops data** (2026-02-22):
  - `_flush_layers()` now returns a boolean indicating success. If Bronze or Silver flush raises an exception, `_consume_iteration()` and `_recover_pending_messages()` skip the Redis ACK, leaving messages pending for redelivery on the next iteration instead of silently losing data.

### Changed

- **Cross-Repo Audit: Schema Version Constant** (2026-02-21):
  - Replaced hardcoded `"v1"` with `ENVELOPE_SCHEMA_VERSION` constant in `heber/models/envelope.py` and `heber/catalog/openmetadata_client.py`.
- Centralized Redis payload field extraction in the writer consumer to avoid drift.
- **Cross-Repo Audit: HTTP Client Wrapper** (2026-02-21):
  - Replaced raw `httpx.Client` with `create_http_client` in `heber/sdk/client.py`.
- **Alert Labels: Contract Label Caching** (2026-02-24):
  - Cache OCC symbol lookups and option bar groups before per-alert processing to avoid repeated DataFrame filters.

### Fixed

#### SonarQube Code Quality Remediation

- **cli.py** — `_cmd_health_dataflow` now returns 1 on exception (was always returning 0, BLOCKER S3516)
- **catalog/api.py** — Re-raise `asyncio.CancelledError` after cleanup in lifespan shutdown (MAJOR S7497)
- **writer/compactor.py** — Replaced dict comprehension with `dict()` constructor (MINOR S7500); extracted `_resolve_numeric_type`, `_resolve_string_or_temporal_type` helpers from `_resolve_column_type` (CC 25→~8); extracted `_collect_small_files`, `_merge_tables_to_parquet` helpers from `compact_partition` (CC 18→~12)
- **watch/poller.py** — Resolved TODO comment (INFO S1135); extracted `_build_updates_from_quotes` from `poll_once` (CC 17→~11)
- **writer/consumer.py** — Extracted `_parse_and_validate_envelope`, `_write_silver_candidates` from `_process_event_once` (CC 23→~13); replaced redundant `ValueError` in except tuples with explicit `MissingRequiredFieldsError` (MINOR S5713)
- **watch/consumer.py** — Extracted `_classify_alert_results`, `_process_one_alert_safe` from `_process_alert` (CC 25→~11); extracted `_coerce_float_or_default`, `_resolve_put_call`, `_resolve_alert_type` from `_map_alert_fields` (CC 22→~8)
- **ops/dataflow_health.py** — `main()` now returns 1 on error (BLOCKER S3516); extracted `_safe_stat_mtime` from `_latest_file_mtime` (CC 18→~8); extracted `_build_gateway_check` from `generate_dataflow_report` (CC 18→~12)
- **watch/backfill_scanner.py** — Let `CancelledError` propagate naturally instead of flag pattern (MAJOR S7497)
- **writer/transformer.py** — Removed redundant `ValueError` from except tuple (MINOR S5713)
- **catalog/seeds.py** — Removed redundant `list()` call wrapping `sorted()` (MINOR S7508)

### Changed

#### Docker Restart Resilience

- Changed all 9 services in `docker-compose.yml` from `restart: unless-stopped` to `restart: always` so containers restart after Docker daemon restarts (previously stayed `Exited(255)`)
- Added `start_period: 120s` to lakeFS, Apicurio, and heber-catalog healthchecks to prevent false-unhealthy verdicts while Postgres initializes
- Raised MinIO `start_period` from 60s to 120s for consistency with Postgres warm-up window
- Root cause: external volume `/Volumes/heber` not mounted on daemon restart caused Postgres bind-mount failure, cascading to all dependent services
- Added contract test `tests/test_compose_restart_contract.py` enforcing restart policy and start_period requirements

#### Consumer Concurrent Message Processing

- Refactored `_process_stream_messages()` in `heber/writer/consumer.py` to use `asyncio.gather()` with a configurable semaphore (`redis_process_concurrency`, default 10) instead of serial iteration, enabling up to 10× throughput improvement during backfill bursts.
- Optimized `_consume_iteration()` to only flush Bronze/Silver on idle iterations (no messages), eliminating a redundant `_flush_layers()` call under load.
- Added batch throughput logging (`batch_processed` event) with messages/second, elapsed time, and concurrency level for backfill observability.
- Added `redis_process_concurrency` setting to `heber/config.py` (configurable 1–50, env: `HEBER_REDIS_PROCESS_CONCURRENCY`).
- Added 5 regression tests in `tests/test_consumer_concurrency.py` covering concurrent execution, error isolation, DLQ routing, semaphore limits, and exception handling.

#### Silver Normalization Throughput

- Eliminated double `normalize_envelope_for_silver()` call per event — consumer now passes pre-normalized rows to `SilverWriter.write_row()`, cutting Silver CPU per event by ~50%.
- Cached `_target_to_source_map()` with `@lru_cache` in `heber/writer/normalizer.py` — field mapping dicts are now built once per feed instead of per event.
- Moved `_KNOWN_QUOTES` frozenset to module level in `heber/writer/key_normalization.py` to avoid per-call reconstruction.
- Downgraded per-event `bronze_write_success` log from INFO to DEBUG to reduce I/O under backfill load.

### Fixed

#### SonarQube NOSONAR Suppression Syntax

- Fixed `# NOSONAR` comment format in `heber/ml/trainer.py` (lines 313, 316) — removed `# noqa` prefix and rule ID suffix so SonarQube correctly suppresses the false-positive S930 warnings on `Path.with_suffix()` calls

#### Flow Alerts Payload Schema: Allow `sentiment` Key

- Added `sentiment` to `PAYLOAD_ALLOWED_FIELDS["flow_alerts"]` in `heber/writer/ingest_contracts.py` — the Data Gateway now includes a `sentiment` field in flow alert payloads, which was producing ~9,900 `payload_unexpected_keys` warnings per day.

#### Bronze-Only Policy: `institution_holdings` Feed

- Added `institution_holdings` to `BRONZE_ONLY_SILVER_DATASETS` — this feed has no Silver schema and was incorrectly routing to Silver writes.

#### Silver Writer Metrics Instrumentation

- Wired `record_write` / `record_write_error` from `heber.ops.metrics` into `SilverWriter._flush_partition` for flush duration, rows written, and bytes written tracking.

#### K8s Kustomization: Remove Deleted Hotloader References

- Removed `deployments/hotloader.yaml`, `services/hotloader.yaml`, and `pdb/hotloader.yaml` from `k8s/base/kustomization.yaml` — these manifests were deleted when Hot Store was removed but the references remained, breaking `kubectl kustomize`.

#### Test Suite: Fix 16 Failing Tests (725/725 passing)

- Added `update_watch_prices_bulk_async` to `_AsyncOnlyManager` and `_Manager` test mocks to match the poller's bulk-update API.
- Added `mget` to `_BytesRedis` test mock for `WatchManager.get_active_watches()` bulk-fetch.
- Fixed `test_process_event_rejects_invalid_instrument_key` to use a truly non-normalizable key (key normalization was auto-fixing the previous test input).
- Fixed catalog migration tests to mock `async_session` and seed functions, avoiding `greenlet` dependency.
- Fixed feast materialization test `MagicMock(name=...)` serialization issue by using `SimpleNamespace`.

#### Heber Watch Port Configuration

- Fixed `DATA_GATEWAY_URL` in `docker-compose.yml` to point to port `8081` for the `heber-watch` service, matching the external port exposed by the Data Gateway container.
- Resolves `ConnectError: All connection attempts failed` and restores quote fetching for active watches.

#### Backfill Scanner Polars DateTime Parsing

- Fixed `str.to_datetime()` call in `heber/watch/backfill_scanner.py` to pass `time_zone="UTC"`, allowing Polars to parse timezone-aware ISO 8601 `alert_time` strings.
- Resolves `polars.exceptions.ComputeError` that caused 100% of enrichment backfill attempts to fail.

#### Catalog Event Loop Blocking During Startup

- Refactored `heber/catalog/seeds.py` to offload blocking file I/O (`iterdir`, `rglob`) to threads via `asyncio.to_thread`, preventing the event loop from freezing during data discovery and coverage seeding
- Extracted `_scan_silver_feeds_blocking()` helper for thread-safe feed directory scanning
- Fixes `heber-catalog` healthcheck timeouts that prevented dependent services (`consumer`, `watch`, `dataflow-health`) from starting

#### Compactor Schema Conflict on Temporal ↔ String Columns

- Added `_is_temporal()` helper and widening rules to `_resolve_column_type()` in `heber/writer/compactor.py`
- Temporal types (`date32`, `timestamp`, etc.) now safely widen to `string` instead of raising `SchemaConflictError`
- Added `large_string` ↔ `string` unification (resolves to `large_string`)
- Resolves 500+ compaction failures on `greek_exposure` feed where `expiry` column changed from `date32[day]` to `string`

### Changed

#### Structured JSON Logging Across All Services

- Explicitly set `json_output=True` in `configure_logging()` for `catalog`, `consumer`, `watch`, `compactor`, and `dataflow_health`
- Added `log_level` field to `Settings` in `heber/config.py` (default: `"INFO"`)

### Removed

#### Unused Hot Store (ClickHouse) Functionality

- Deleted `heber/hotstore/` directory (client, sync, tables, init) and `heber/writer/hotstore.py`
- Removed 7 ClickHouse/hotloader config fields from `heber/config.py`
- Removed ClickHouse service from `docker-compose.yml` and related env vars from consumer/catalog services
- Removed `clickhouse-connect` dependency from `pyproject.toml`
- Removed Hot Store metrics, alerts, dashboard panels from `heber/ops/`
- Removed Hot Store runbook, SLI, capacity entries, and chaos experiment from `heber/sre/`
- Removed Hot Store performance SLOs, environment configs, mock strategies, and test specs from `heber/testing/`
- Deleted `tests/test_hotstore_unification.py`, `tests/test_hotstore_facade_alignment.py`, and hotloader test cases
- Deleted all `k8s/**/hotloader.yaml` manifests (deployments, PDBs, services)
- Deleted `docs/hot_store.md` and removed all Hot Store/ClickHouse references from `README.md`
- Updated 8 test files to adjust assertion counts after removal

### Changed

#### Consumer Bandwidth Tuning

- Increased default consumer batch size from 100 to 500 messages per `XREADGROUP` call via new `redis_read_batch_size` setting (configurable 10–5,000), reducing Redis round trips ~5× during backfill bursts
- Increased default block timeout from 1,000ms to 2,000ms via new `redis_read_block_ms` setting (configurable 100–10,000), allowing larger batches to fill before returning

### Added

#### Catalog Coverage Seeding from Disk

- Added `seed_coverage_from_disk()` to `heber/catalog/seeds.py` — scans Silver `feed=` directories for `dt=YYYY-MM-DD` partitions and upserts aggregate `DataCoverage` records with min/max date ranges.
- Extracted `_scan_partition_dates()` helper for partition directory scanning (no Parquet I/O involved).
- Coverage seeding runs during catalog startup alongside existing dataset/feed seeds in `heber/catalog/api.py`.
- Added unit tests in `tests/test_seed_coverage.py` covering partition scanning, empty directories, and file-vs-directory edge cases.
- Enables Atlas manifest generation to render Heber coverage windows for agent hypothesis generation.

### Added

#### Catalog Feed Mapping Auto-Seed

- Extracted seed data and functions from `scripts/seed_catalog.py` into `heber/catalog/seeds.py` for importability.
- Auto-seed `feed_mappings`, `datasets`, and `schema_versions` tables on catalog startup via the `lifespan` handler in `heber/catalog/api.py`.
- Ensures all 55 provider→feed→Silver dataset mappings are present without manual script execution.
- `scripts/seed_catalog.py` now imports from `heber/catalog/seeds.py`, retaining only coverage scanning and CLI logic.

#### Catalog Auto-Discovery

- Added `discover_datasets_from_disk()` to `heber/catalog/seeds.py` — scans Silver directory for `feed=X` partitions and auto-registers unknown datasets with default identity feed mappings (`provider="discovered"`).
- New `catalog_auto_discover` config flag (default `True`) gates automatic discovery on catalog startup.
- Auto-discovery now runs in **all environments**, not just dev (idempotent).
- Periodic background re-scan every `catalog_discover_interval_seconds` (default 300s, set to 0 to disable).
- Added `--discover` CLI flag to `scripts/seed_catalog.py` for manual trigger.
- Skips macOS `._` resource forks and non-`feed=` directories.

#### Enrichment Backfill Scanner

- New `EnrichmentBackfillScanner` in `heber/watch/backfill_scanner.py` — periodic background task that scans recent Gold feature partitions for rows with null enrichment fields (Greeks, GEX, IV rank, max pain, market tide), re-enriches via the Data Gateway, and patches parquet using dedup-on-alert_id
- Integrated as optional fourth coroutine in `WatchService.run()`, gated by `HEBER_ENRICHMENT_BACKFILL_ENABLED`
- Config settings: `enrichment_backfill_enabled`, `enrichment_backfill_interval`, `enrichment_backfill_lookback_days`, `enrichment_backfill_batch_size`
- Prometheus metrics: `heber_enrichment_backfill_{scanned,patched,failed}_total`, `heber_enrichment_backfill_duration_seconds`
- 12 unit tests covering row detection, partition reading, re-enrichment flow, batch limiting, and market hours gating

#### Write-Time Null Field Audit Logging

- New `audit_null_fields()` in `heber/quality/write_audit.py` — inspects DataFrames at write time for unexpected null values, emitting structured warning logs with per-column null counts and Prometheus counters (`heber_write_null_fields_total`)
- Hooked into 4 critical write paths: Silver writer (`write_silver_parquet`), Gold feature persistence (`persist_features_to_gold`), Gold label writer (`LabelWriter._write_to_parquet`), and SDK Gold writer (`HeberClient.write_gold`)
- `EXPECTED_NON_NULL` registry maps `(layer, dataset)` → required non-null columns; unknown datasets get all columns audited
- Supports pandas, Polars, and PyArrow data types
- 13 unit tests covering all three data types, Prometheus metrics, and config validation

### Fixed

#### SonarQube Code Quality Remediation

- **`features.py`** — Reduced cognitive complexity of 5 functions by extracting 7 shared helpers:
  - `_request_json_with_retry` (CC 37→~8): decomposed into `_try_route`, `_send_request`, `_process_response`, `_parse_success`, `_handle_auth_failure`, `_handle_retryable_failure`
  - `_enrich_gex` (CC 41→~10): extracted `_coalesce_split_or_fallback`, `_unwrap_data_payload`
  - `_enrich_max_pain` (CC 23→~10): used `_coalesce_first`, `_unwrap_data_payload`
  - `_enrich_market_tide` (CC 16→~8): used `_classify_direction`, `_coalesce_first`
  - `_enrich_iv_rank`: used `_build_enrichment_routes` to eliminate duplicated route-building loops
  - Added `_LOG_ENRICHMENT_FAILED` constant to deduplicate string literal
- **`ingest_contracts.py`** — Simplified `_AMOUNT_RANGE_RE` regex via `_NUM_PATTERN` constant; extracted `_build_insider_relationships()` from `_normalize_insider_payload`
- **`trainer.py`** — Added `# NOSONAR python:S930` to suppress false positive on `Path.with_suffix()` calls

#### Data Health Remediation

- Removed dead `sentiment` field from `flow_alerts` Silver schema, Pydantic model, and ingest allowed fields — UW never populates this for flow alerts (it exists only in `market_tide`/`sector_tide`).
- Aligned `greek_exposure` Silver schema, Pydantic model, ingest contracts, and GEX enrichment with UW API's split call/put fields (`call_gamma`, `put_gamma`, `call_delta`, `put_delta`, `call_vanna`, `put_vanna`, `call_charm`, `put_charm`) plus grouping fields (`strike`, `expiry`, `dte`).

#### News Feed Bronze-Only Policy

- Added `"news"` to `BRONZE_ONLY_SILVER_DATASETS` in `heber/writer/ingest_contracts.py` so news events are persisted in Bronze but skipped for Silver writes.
- News is free-text content without a structured Silver use-case; this prevents null-heavy Silver rows.

#### Core Parquet Read Helper

- Added `heber/core/parquet.py` with `read_parquet_dataset()` for standardized Parquet reading with time-range and as-of-time filtering.
- Added `tests/test_core_parquet.py` with 5 unit tests covering basic read, time-range filtering, as-of filtering, column pruning, and filter pushdown.

#### Writer / SDK / Backtest / ML DRY Refactoring

- Extracted `get_partition_key`, `write_silver_parquet`, and `build_silver_candidates` into `heber/writer/utils.py`.
- Refactored `SilverWriter` and `BronzeToSilverTransformer` to use shared writer utilities.
- Added `HeberClient._read_parquet_dataset` wrapper delegating to `heber.core.parquet.read_parquet_dataset`.
- Consolidated `BacktestDataLoader.load_train_data`/`load_test_data` into shared `_load_data_split` helper.
- Refactored `MetaLabelDatasetBuilder` to use `HeberClient._read_parquet_dataset`, removing legacy path fallbacks.

#### Trades Silver Field Mapping: Dual-Key Support

- Added explicit full-name key aliases (`price`, `size`, `exchange`, `trade_id`, `tape`) to `FIELD_MAPPINGS["trades"]` in `heber/writer/ingest_contracts.py` alongside existing short Alpaca WebSocket keys (`p`, `s`, `x`, `i`, `z`).
- Removed invalid `taker_side` mapping that doesn't exist in the Silver trades schema.
- Added 4 regression tests in `tests/test_trades_dual_key_normalization.py` covering short-key mapping, full-name-key mapping, short-key precedence, and empty-payload handling.

#### Docker Startup: Multi-Database Init and Healthcheck Timing

- Created `scripts/init-databases.sh` to provision auxiliary Postgres databases (`heber_lakefs`, `heber_apicurio`, `heber_openmetadata`) at container first-init time via `/docker-entrypoint-initdb.d/`.
- Root cause: Postgres only creates the database specified by `POSTGRES_DB` (`heber_catalog`); lakeFS, Apicurio Registry, and OpenMetadata each need their own database and were crash-looping with misleading DNS/connection errors.
- Mounted the init script in `docker-compose.yml` Postgres volumes.
- Added `start_period` to healthchecks for ClickHouse (30s), MinIO (60s), and Elasticsearch (60s) to prevent false-unhealthy verdicts during slow startup on the external NTFS volume.
- Enhanced `scripts/init_volume.sh` with explicit `find -delete` cleanup of macOS `._*` resource fork files as a fallback to `dot_clean`.
- Added contract test in `tests/test_compose_postgres_health_contract.py` to enforce Postgres healthcheck parameters.

### Removed

#### Unused Docker Services: OpenMetadata and Elasticsearch

- Removed `openmetadata` and `elasticsearch` services from `docker-compose.yml` — neither is used by the core Heber pipeline.
- OpenMetadata client (`heber/catalog/openmetadata_client.py`) already gracefully falls back to `MockOpenMetadataClient` when the server is unavailable.
- Elasticsearch had zero code references outside the compose file.
- Removed `heber_openmetadata` database from `scripts/init-databases.sh`.
- Deleted `tests/test_compose_metadata_contract.py` (tested removed services).
- Saves ~1GB RAM and significantly speeds up Docker startup.

### Added

#### Alert Feature Enrichment Expansion

- Added `gex`, `vex`, `max_pain_strike`, `max_pain_distance_pct`, `market_tide_net_premium`, `market_tide_direction` fields to `AlertFeatures` dataclass
- Implemented `_enrich_gex()`, `_enrich_max_pain()`, `_enrich_market_tide()` methods in `AlertFeatureExtractor` using Data Gateway endpoints
- Fixed `0.0` falsy evaluation bug in all enrichment methods — replaced `or` chains with explicit `None` checks
- Added 16 unit tests covering new enrichment paths in `test_watch_feature_enrichment_expansion.py`

### Changed

#### Market Data Normalization for Backtesting Accuracy

- Added instrument-type-aware normalization for core market data feeds (bars, quotes, trades) in `heber/writer/key_normalization.py`
- Added `_normalize_crypto_symbol()` handling `BTC/USD`, `BTC-USD`, `BTCUSD`, `BTC_USD`, `ETHUSDT` crypto formats with known-quote-currency disambiguation
- Added `_normalize_equity_symbol()` supporting extended tickers like `BRK.B`, `JRI.RT`
- Added `_normalize_market_data_envelope()` routing normalization by instrument type (equity, crypto, option)
- Added `validate_market_data_payload()` with quality flags (`high_below_low`, `negative_price`, `negative_volume`, `inverted_spread`, `non_positive_price`, `negative_size`) for suspect market data — flags data rather than rejecting to preserve backtesting completeness
- Added option trades instrument key preservation through `option:OCC:` prefix detection
- Previously, bars/quotes/trades feeds fell through to a no-op generic branch with no validation
- Added 12 new tests to `tests/test_instrument_key_synthesis.py` covering all instrument types and quality validation

#### Catalog Module DRY Refactor

- Deleted ~45 LOC of dead code from `heber/catalog/api.py` (`ErrorResponse`, `ErrorEnvelope`, `check_rate_limit`, `verify_api_key`, and associated imports)
- Consolidated duplicate `DataLayer` enum from `openmetadata_client.py` to import from `access_control.py`
- Extracted `_instrument_response()` helper in `api.py`, replacing 3 identical 8-field constructions

#### Ops Module DRY Refactor

- Deleted ~90 LOC of dead `retry_with_backoff` / `retry_with_backoff_async` from `heber/ops/reliability.py` (superseded by `runtime_retry.py`)
- Removed unused `logger` / `import structlog` from `slices.py` and `gap_resolutions.py`
- Removed unused `import random` from `reliability.py`
- Cleaned up stale exports in `heber/ops/__init__.py`

#### Features Module DRY Refactor

- Extracted shared `rolling_max_timestamp` / `rolling_max_timestamp_time` into new `features/templates/_utils.py`
- Consolidated 4 duplicate `_derive_ts_available` / `_rolling_max_datetime` implementations from `momentum.py`, `volatility.py`, `flow.py`, and `cross_asset.py`

#### Writer Module DRY Refactor

- Deleted 92 LOC of dead coercion code from `heber/writer/silver.py` (`_coerce_value`, `_coerce_to_*`) and `heber/writer/transformer.py` (`_coerce_value`, `_coerce_to_date`, `_coerce_to_timestamp`)
- Extracted shared `extract_item_event_timestamp()` and `explode_aggregate_payload()` into `heber/writer/normalizer.py`, consolidating ~190 LOC of duplicated logic from `consumer.py` and `transformer.py`
- Moved payload validation schemas (`PAYLOAD_REQUIRED_FIELDS`, `PAYLOAD_ALLOWED_FIELDS`) from `heber/writer/consumer.py` inline dicts to `heber/writer/ingest_contracts.py` module-level constants

#### Watch Module DRY Refactor

- Extracted `coerce_optional_float` from `consumer.py`, `poller.py`, and `features.py` (3 duplicates) into `heber/watch/gateway.py`
- Extracted `should_continue_route_fallback` from `consumer.py` and `poller.py` (2 duplicates) into `heber/watch/gateway.py`
- Rewrote `consumer.py` timestamp helpers (`_normalize_alert_time`, `_parse_timestamp`, `_timestamp_from_numeric`) to delegate to `gateway.coerce_utc_timestamp`
- Added boolean guard to `coerce_utc_timestamp` (Python `bool` is subclass of `int`)
- Net reduction of ~80 LOC of duplicated logic

#### Watch Module DRY Refactor (Phase 2)

- Added `is_retryable_http_status` and `parse_retry_after` to `heber/watch/gateway.py`.
- **Refactoring**:
  - `watch` module: Consolidated retry logic and data coercion helper functions into `gateway.py` to eliminate DRY violations.
  - `backtest` module: Extracted shared `_load_split_data` logic in `BacktestDataLoader` to reduce duplication.
  - `sdk` module: Extracted shared `_read_parquet_dataset` helper in `HeberClient` to consolidate parquet reading and filtering logic.
  - `ml` module: Refactored `MetaLabelDatasetBuilder` to use `HeberClient` for data loading, removing duplicate parquet reading logic and legacy path support.
- Consolidated duplicate `_coerce_finite_outcome_return` in `heber/watch/manager.py` to use `gateway.coerce_optional_float`.

### Fixed

#### Docker Startup RCA: Postgres Healthcheck vs Slow Recovery

- Updated `/Users/jacobmcmillan/Empire/Heber/docker-compose.yml` Postgres healthcheck:
  - now uses container env vars for DB/user (`POSTGRES_DB`, `POSTGRES_USER`) instead of hard-coded values,
  - added `start_period: 120s` so slow recovery windows on external volumes do not mark Postgres unhealthy too early.
- Added regression coverage in `/Users/jacobmcmillan/Empire/Heber/tests/test_compose_postgres_health_contract.py` to enforce this startup contract.
- Operational remediation applied to local volume:
  - removed `._*` macOS resource-fork sidecar files from `/Volumes/heber/postgres/data` that were inflating Postgres recovery/fsync time and causing dependency startup failures.

#### Watch Consumer Feature-Payload Mapping for Meta-Label Rows

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/consumer.py` so parsed flow alerts now retain feature-critical fields when creating watch feature records:
  - `premium`, `volume`, `open_interest`
  - `alert_type`, `side`, `aggressor`, `tags`
  - `is_sweep`, `is_unusual`, `sentiment`, `trade_count`, `volume_oi_ratio`
- Added alias support for common payload variants (`total_premium`, `size`, `oi`, etc.) and derived `volume_oi_ratio` when missing.
- This fixes sparse meta-label feature rows caused by dropping flow payload fields before extraction.
- Added regression coverage in `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_consumer_reliability.py` to enforce payload preservation and extractor input fidelity.

#### Watch Consumer Stale-Window Guard for Backfilled Alerts

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/consumer.py` to skip watch creation when an alert's computed watch window has already ended at processing time.
- This prevents stale/backfilled flow alerts from becoming active watches that produce no-snapshot expirations and contaminate training labels.
- Added normalized alert timestamp handling for robust stale-window evaluation.
- Added regression coverage in `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_consumer_reliability.py` for stale-window skip behavior.

#### Watch Outcomes for Expired No-Snapshot Windows

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/checker.py` so watches with no snapshots are completed as `EXPIRED` once `window_end` has passed, and now emit `WatchOutcome` records for writer persistence.
- Preserved existing behavior for active windows with no snapshots (`None` until expiry), avoiding premature outcomes.
- Added regression coverage in `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_zero_price_handling.py` for expired no-snapshot watches to prevent stalled unlabeled records.

#### Dataflow Health Market-Closed Passive Check Noise

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/ops/dataflow_health.py` so `gateway_passive_activity` is marked `skipped` (instead of `warn`) when the market is closed and no passive gateway success metric is expected.
- Kept existing behavior during market-open windows: missing passive gateway success evidence still reports `warn`.
- Added regression tests in `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health.py` for both market-closed skip behavior and market-open warning behavior.

#### Log RCA Hardening: Watch Enrichment + Catalog Paths + Compactor Noise

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/features.py` so IV-rank enrichment tries both canonical and options-prefixed route shapes, and no longer fails valid mixed-route environments.
- Updated `/Users/jacobmcmillan/Empire/Heber/heber/sdk/client.py` to use relative request paths with `httpx` `base_url`, preventing accidental path resets that produced catalog `404` noise.
- Added unprefixed route aliases in `/Users/jacobmcmillan/Empire/Heber/heber/catalog/api.py` for `/datasets` and `/feeds` families so existing callers stop generating avoidable 404s.
- Updated `/Users/jacobmcmillan/Empire/Heber/heber/writer/compactor.py` to log hidden macOS parquet sidecars at `debug` level instead of `warning`.
- Added regression tests:
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_feature_enrichment_resilience.py`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_dataflow_health.py`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_sdk_catalog_defaults.py`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_catalog_route_aliases.py`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_compactor_safety.py`

#### Watch Expiration Log Flood Reduction

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/manager.py` so `EXPIRED` watch completions no longer emit per-watch `Completed alert watch` info logs.
- Retained completion info logs for terminal outcomes that matter for trading analysis (`HIT_TP`/`HIT_SL`).
- Added regression coverage in `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_manager_redis_bytes.py` to enforce:
  - no per-watch completion info log for `EXPIRED`,
  - completion info log remains for `HIT_TP`.

#### Watch Gateway Auth Fail-Fast Contract

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/watch/writer.py` to enforce a required gateway API key contract during watch-service initialization.
- Added `_resolve_gateway_api_key()` so watch startup uses explicit CLI key first, then settings key, and raises a clear `ValueError` when neither is configured.
- This prevents runtime unauthenticated gateway calls that surface as noisy `401` log entries.
- Added regression coverage:
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_gateway_key_contract.py`
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_watch_writer_entrypoint_shutdown.py`

#### Alert Labels Gateway Auth Contract

- Updated `/Users/jacobmcmillan/Empire/Heber/heber/features/pipelines/alert_labels.py` so contract-label option-bars fetches now require a configured gateway API key and send authenticated `X-Gateway-Key` headers.
- Added fail-fast key validation for `use_contract_labels=True` to prevent unauthenticated background calls and noisy `401` failures when this pipeline runs.
- Added regression coverage in:
  - `/Users/jacobmcmillan/Empire/Heber/tests/test_alert_labels_gateway_auth.py`

### Fixed

#### Watch + Consumer Runtime Log RCA Cleanup

- Updated `heber/watch/consumer.py` to prefer alert-carried `contract_px` for watch entry price and only call Data Gateway quote routes when alert price is missing/invalid, removing stale quote fallback noise for normal flow-alert processing.
- Added sync Redis feature-storage fallback in `heber/watch/consumer.py` (`storage_mode=sync_fallback`) when async Redis client is not configured, replacing repeated `Skipping feature storage (no async redis)` behavior.
- Updated payload schema allowlists in `heber/writer/consumer.py`:
  - `market_tide` now allows `call_put_ratio`.
  - `flow_alerts` now allows `id`, `alert_id`, and `event_id`.
- Added regression coverage in `tests/test_watch_consumer_reliability.py` and `tests/test_writer_consumer_reliability.py` for:
  - contract price preference without gateway lookup,
  - sync Redis feature-storage fallback behavior,
  - no warning for valid `market_tide.call_put_ratio`,
  - no warning for valid `flow_alerts.id`.

#### Watch Gateway Fallback Hardening

- Stopped legacy unprefixed route fallback for non-route HTTP statuses in watch polling paths (notably `429`), preventing duplicate failing calls to Data-Gateway.
- Updated watch feature enrichment retry logic to honor `Retry-After` headers as the minimum backoff when upstream throttles.
- Added shared fallback policy helper so only true route-miss statuses (`404`) trigger legacy path retries.

### Changed

#### Watch Runtime Defaults

- Added `watch_gateway_legacy_fallback_enabled` setting and wired it through watch poller/consumer/feature enrichment URL candidate generation.
- Set `HEBER_WATCH_GATEWAY_LEGACY_FALLBACK_ENABLED=false` in compose for environments where Data-Gateway serves only `/api/v1/...` routes.

### Added

#### Documentation Standardization Baseline

- Added `AGENTS.md` as the canonical AI-agent instruction file and removed `CLAUDE.md`.
- Added missing documentation baseline files:
  - `CONTRIBUTING.md`
  - `TESTING.md`
  - `SECURITY.md`
  - `DEVELOPER_NOTES.md`
  - `docs/RUNBOOK.md`
  - `docs/API_REFERENCE.md`
  - `docs/DATA_CONTRACTS.md`
  - `docs/DEPLOYMENT.md`
  - `docs/MIGRATION_GUIDE.md`
- Normalized canonical naming for required docs:
  - `prd.md` -> `PRD.md`
  - `docs/architecture.md` -> `docs/ARCHITECTURE.md`
- Updated `README.md` documentation index to point to standardized canonical docs and required environment/testing references.

#### Gateway Feed Alias Compatibility

- Fixed `flow_alerts` ingestion producing invalid `equity:` instrument keys by adding defensive fallback logic in `heber/writer/key_normalization.py`.
- Added defensive ingest aliases in `heber/writer/ingest_contracts.py` so legacy/rest feed names still resolve to canonical Silver datasets:
  - `flow` -> `flow_alerts`
  - `greeks` -> `greek_exposure`
  - `gex` -> `greek_exposure`
- Expanded Data-Gateway feed coverage list with legacy alias names so contract checks route them through canonical schemas.
- Updated catalog seed mappings in `scripts/seed_catalog.py` for the same alias set, ensuring catalog metadata includes legacy-to-canonical mappings.
- Expanded alias routing tests in `tests/test_feed_alias_routing.py` to enforce alias + catalog-seed parity.

#### Comprehensive Feed Coverage Audit

- Added 7 new `FEED_ALIASES` in `heber/writer/ingest_contracts.py`: `ticker_flow`→`flow_alerts`, `darkpool_ticker`→`darkpool`, `option_trades`→`trades`, `crypto_bars`→`bars`, `crypto_trades`→`trades`, `institutions`→`institution_holdings`, `filings`→`news`
- Expanded `DATA_GATEWAY_FEEDS` from 14 to 46 entries covering all Silver schemas
- Added `forex` Silver schema in `heber/schemas/silver.py` with bid/ask/mid/OHLC columns
- Added `forex` field mapping in `heber/writer/ingest_contracts.py`

#### Bronze→Silver Contract Hardening

- Added contract-first ingestion tests for Data Gateway feed coverage, alias routing, instrument-key synthesis, and Bronze-first behavior:
  - `tests/test_ingest_feed_contract_matrix.py`
  - `tests/test_bronze_first_ingestion.py`
  - `tests/test_feed_alias_routing.py`
  - `tests/test_instrument_key_synthesis.py`
  - `tests/test_data_gateway_feed_parity.py` (extracts emitted feeds from Data Gateway source and enforces Heber mapping coverage)
- Added shared ingest contract and normalization modules used by both live consumer and backfill transformer:
  - `heber/writer/ingest_contracts.py`
  - `heber/writer/key_normalization.py`

### Performance

- Switched universe DataFrame ingestion to `itertuples()` to avoid per-row Series allocation, reducing CPU and memory overhead when building large universe snapshots.
  - `heber/writer/normalizer.py`
- Added dedicated Silver dataset schema for `historic_option_volume` in `heber/schemas/silver.py`.

#### Bronze→Silver Training-Feed Scope

- Added explicit raw feed allowlist for Silver routing in `heber/writer/ingest_contracts.py`:
  - `CONTRACTED_RAW_FEEDS`
  - `is_contracted_feed()`
  - `DLQ_REASON_UNCONTRACTED`
- Extended Data-Gateway parity test in `tests/test_data_gateway_feed_parity.py` to include:
  - stream feeds (`stream.py`)
  - UW poller feeds (`uw_poller.py`)
  - backfill dispatch feeds (`backfill.py`)
- Added REST overflow guard test `tests/test_data_gateway_rest_feed_contract.py` that parses Data-Gateway route segments and enforces Bronze+DLQ for non-contracted derived feeds.
- Expanded feed contract matrix and alias seed coverage for training feeds:
  - `option_trades`, `crypto_bars`, `crypto_trades`, `ticker_flow`, `darkpool_ticker`, `institutions`, `earnings`

#### Canonical Contracts

- Standardized canonical darkpool naming to `darkpool` across provider/stream/slice metadata:
  - `heber/catalog/datasources.py` provider capability updated from `darkpool_trades` to `darkpool`
  - `heber/bus/streams.py` stream and consumer-group dataset name updated to `darkpool`
  - `heber/bus/__init__.py` canonical stream enum key updated to `intel.darkpool`
  - `heber/ops/slices.py` slice 3 dataset list updated to include `darkpool`
- Added darkpool contract tests:
  - `heber/catalog/tests_datasources.py::TestProviderRegistry.test_get_by_capability_uses_canonical_darkpool_name`
  - `heber/ops/tests_remaining.py::TestStreamRegistry.test_darkpool_stream_uses_canonical_name`
  - `heber/ops/tests_remaining.py::TestSliceManager.test_slice_3_uses_canonical_darkpool_dataset_name`
- Updated flow template dependency docs and model schema naming text to `darkpool` while retaining `get_darkpool_trades_schema()` compatibility alias in `heber/models/silver.py`.

#### Documentation

- Added `docs/silver_gold_scope.md` with a concrete Silver keep/drop matrix and Gold input plan, including `KEEP_CORE`, `KEEP_CONTEXT`, and `BRONZE_ONLY` feed policy decisions tied to current pipelines.
- Updated `README.md` documentation index to include `docs/silver_gold_scope.md`.
- Updated `docs/data_contract.md` with:
  - Bronze-first / Silver-strict ingest policy
  - Data Gateway feed coverage matrix and alias routing
  - key synthesis rules and unknown-feed handling semantics
- Updated `docs/architecture.md` with Bronze→Silver normalization architecture and shared normalizer module references.
- Updated `docs/data_contract.md` and `docs/architecture.md` for training-feed scope:
  - added plain-English contract terminology
  - added contracted raw feed matrix for stream/UW poller/backfill
  - documented Bronze+DLQ behavior for `uncontracted_feed` and `unmapped_feed`

- Added operational runbook (`docs/operations/runbook.md`) covering system overview, startup/shutdown, daily operations, common ops, incident response, data recovery, and configuration reference
- Added missing documentation links to `README.md`: `labeling_strategy.md`, `schemaaudit.md`
- Added watch service to `docs/architecture.md` Core Services section
- Added Watch Service Settings section to `docs/configuration.md` (`DATA_GATEWAY_URL`, `HEBER_GOLD_PATH`)
- Added missing env vars to `.env.example`: `HEBER_CATALOG_URL`, `HEBER_CLICKHOUSE_DATABASE`, `DATA_GATEWAY_URL`, `HEBER_GOLD_PATH`

### Fixed

#### Compactor Sidecar and Lock Resilience

- Updated `heber/writer/compactor.py` to ignore hidden sidecar parquet files (for example `._*.parquet`) during partition scans and compaction so non-data filesystem artifacts do not break compaction cycles.
- Added safe file-size probing in compactor to skip unreadable parquet paths with structured warning logs instead of raising `PermissionError` and aborting the whole cycle.
- Updated compaction lock acquisition to reclaim stale self-owned `.compaction.lock` files (same PID) before retrying so prior crash paths do not permanently block a partition.
- Ensured partition locks are always released via top-level `finally`, including early-return paths before merge execution.
- Added regression coverage in `tests/test_compactor_safety.py` for:
  - hidden sidecar parquet files,
  - stale self-lock recovery,
  - unreadable parquet file stat handling without crashes.

#### Dataflow Health Filesystem Scan Race

- Updated `_latest_file_mtime()` in `heber/ops/dataflow_health.py` to:
  - skip hidden files like `.compaction.lock`,
  - handle files disappearing between discovery and `stat()` without crashing the health loop.
- Added warning-level structured logging for filesystem stat failures (`dataflow_health_filesystem_stat_failed`) so scan anomalies are visible but non-fatal.
- Added regression tests in `tests/test_dataflow_health.py` for hidden-file exclusion and disappearing-file race handling.

#### Silver Normalization For Aggregate REST Payloads

- Updated `heber/writer/consumer.py` to defensively expand aggregate REST payload envelopes for `bars` and `trades` into item-level Silver writes, while preserving Bronze-first durability.
- Updated `heber/writer/transformer.py` to apply the same aggregate expansion during Bronze→Silver backfill so `payload.bars[]` and `payload.trades[]` are written as typed item rows instead of null-heavy aggregate rows.
- Added skip behavior for empty aggregate lists so `trades=[]` / `bars=[]` no longer produce null-heavy Silver rows.
- Added required non-null field enforcement for Silver rows in `heber/writer/normalizer.py` and wired it into both live writes (`heber/writer/silver.py`) and backfill writes (`heber/writer/transformer.py`) so invalid rows are rejected before landing in Silver.
- Added Bronze-only Silver scope enforcement in `heber/writer/ingest_contracts.py`, `heber/writer/consumer.py`, and `heber/writer/transformer.py` so policy-designated feeds (for example `news`, `ftd`, `congress_trades`, `insider_trades`, `institution_holdings`) continue to persist in Bronze but are skipped for Silver writes by default.
- Added per-item aggregate failure logging and summary counts in consumer logs (`silver_aggregate_item_failed`, `silver_aggregate_write_summary`) for faster root-cause analysis.
- Added regression tests in `tests/test_bronze_first_ingestion.py` for:
  - multi-item `bars[]` fan-out to multiple Silver writes,
  - empty `trades[]` skip behavior,
  - mixed valid/invalid aggregate items and missing required-field rejection.
- Added transformer backfill regression coverage in `tests/test_transformer_aggregate_payloads.py` for bars/trades expansion and mixed valid/invalid aggregate items.
- Added Silver-scope policy regression coverage in:
  - `tests/test_bronze_first_ingestion.py` (Bronze-only feeds skip Silver in live consumer path),
  - `tests/test_transformer_feed_scope.py` (Bronze-only feeds skip Silver in backfill transformer path).
- Improved field alias handling in `heber/writer/normalizer.py` so one target column can resolve from multiple source keys (instead of first-match only), preventing silent drops when payload variants differ.
- Added `bars.timestamp -> bar_start_ts` mapping in `heber/writer/ingest_contracts.py` and regression coverage in `tests/test_bars_timestamp_mapping.py`.

#### Metadata Stack Shutdown + Health Contracts

- Added compose contract regression tests in `tests/test_compose_metadata_contract.py` to lock critical metadata infra guarantees:
  - OpenMetadata healthcheck command must target an unauthenticated endpoint.
  - OpenMetadata and Elasticsearch must define explicit `stop_grace_period`.
- Updated `docker-compose.yml` metadata services:
  - Added OpenMetadata healthcheck using `wget -q --spider http://localhost:8585/api/v1/system/version`.
  - Added `stop_grace_period: 90s` to both `openmetadata` and `elasticsearch`.
  - Added OpenMetadata `start_period: 90s` to avoid false-negative health during JVM bootstrap.
- Root cause addressed:
  - OpenMetadata/Elasticsearch were being force-killed with exit `137` during recreate/shutdown when default Docker stop timeout was too short.
  - Initial OpenMetadata healthcheck attempt used an authenticated endpoint and a missing binary (`curl`), which kept container health in `starting`.

#### Redis Runtime Stability (Consumer + Watch)

- Added shared runtime retry helpers in `heber/ops/runtime_retry.py`:
  - transient Redis/runtime error classification
  - bounded exponential backoff with jitter for long-running loops
- Updated `heber/writer/consumer.py` run loop to:
  - detect transient Redis transport/loading errors,
  - log them as structured warnings without traceback spam,
  - apply bounded exponential backoff instead of fixed 1-second retry loops.
- Updated `heber/watch/consumer.py` run loop with the same transient classification and backoff behavior.
- Added regression tests to lock behavior:
  - `tests/test_writer_consumer_reliability.py`
  - `tests/test_watch_consumer_reliability.py`
  - verifies transient Redis errors back off and avoid noisy error logging,
  - verifies unknown runtime errors still emit error logs with traceback context.

#### Dataflow Metrics Foundation (Writer + Watch Startup)

- Added `heber_writer_last_write_unixtime{layer,dataset}` in `heber/ops/metrics.py` and wired it to `record_write()` so every successful write updates a freshness timestamp.
- Instrumented Bronze flush writes in `heber/writer/bronze.py` with:
  - `record_write(layer="bronze", ...)` on success
  - `record_write_error(layer="bronze", ...)` on failure
- Started metrics server in watch entrypoint `heber/watch/__main__.py` using `start_metrics_server_from_env(default_port=9090)` so watch-service metrics are available at runtime.
- Added regression tests:
  - `tests/test_metrics_runtime_wiring.py` (Bronze write metrics + last-write gauge)
  - `tests/test_watch_metrics_startup.py` (watch metrics server startup)

#### Watch Passive Gateway Evidence Metrics

- Added watch observability metrics in `heber/ops/metrics.py`:
  - `heber_watch_gateway_requests_total{component,endpoint,outcome,status_code}`
  - `heber_watch_gateway_request_duration_seconds{component,endpoint,outcome}`
  - `heber_watch_gateway_last_success_unixtime{component,endpoint}`
  - `heber_watch_watches_created_total`
  - `heber_watch_last_watch_created_unixtime`
  - `heber_watch_poll_cycles_total{status}`
  - `heber_watch_last_poll_unixtime`
  - `heber_watch_alert_parse_total{status}`
- Instrumented watch enrichment/quote paths for passive Data Gateway proof:
  - `heber/watch/features.py` now records success/failure outcomes, status codes, and latency for enrichment calls.
  - `heber/watch/poller.py` now records poll cycle status and gateway request outcomes including `partial_coverage` and `stale_fallback`.
  - `heber/watch/consumer.py` now records alert parse outcomes, watch creation activity, and entry-price gateway request outcomes.
- Added/extended regression tests:
  - `tests/test_watch_feature_enrichment_resilience.py`
  - `tests/test_consumer_parsing.py`
  - `tests/test_watch_poller_metrics.py`

#### Dataflow Health JSON Command

- Added end-to-end dataflow verification engine in `heber/ops/dataflow_health.py` with JSON report output and status levels (`ok`, `warn`, `fail`).
- Added new CLI command `heber health-dataflow` in `heber/cli.py` with support for:
  - `--window-seconds`
  - `--consumer-metrics-url`
  - `--watch-metrics-url`
  - `--report-dir`
  - `--loop`
  - `--interval-seconds`
  - `--mode manual|scheduled`
- Added health settings to `heber/config.py`:
  - `HEBER_HEALTH_CONSUMER_METRICS_URL`
  - `HEBER_HEALTH_WATCH_METRICS_URL`
  - `HEBER_HEALTH_FRESHNESS_SECONDS`
  - `HEBER_HEALTH_REPORT_DIR`
  - `HEBER_HEALTH_INTERVAL_SECONDS`
- Added regression tests:
  - `tests/test_dataflow_health.py`
  - `tests/test_cli_health_dataflow.py`
- Updated `run_dataflow_health_once()` in `heber/ops/dataflow_health.py` to treat report file write errors as warn-only (no command crash), preserving exit-0 behavior.
- Added regression guard in `tests/test_dataflow_health.py` for report-write failure handling.
- Updated `tests/test_watch_entrypoint_shutdown.py` to stub metrics startup during shutdown-path tests, preventing port-bind flakiness in full-suite runs.
- Reduced noisy import-time tracing logs by downgrading missing OpenTelemetry startup message to debug in `heber/ops/tracing.py`.

#### Scheduled Dataflow Health Runner and Docs

- Added Docker service `heber-dataflow-health` in `docker-compose.yml` to run scheduled checks every interval using `python -m heber.ops.dataflow_health --loop --mode scheduled`.
- Exposed metrics endpoints on host for manual verification parity:
  - `heber-consumer`: `localhost:9090`
  - `heber-watch`: `localhost:9091`
- Added health config examples in `.env.example` for metrics URLs, freshness window, report path, and interval.
- Added static coverage tests:
  - `tests/test_config_health_settings.py`
  - `tests/test_dataflow_health_compose_contract.py`
- Updated operator docs:
  - `README.md` (new service + metrics ports + CLI usage)
  - `docs/configuration.md` (dataflow health settings table)
  - `docs/operations/runbook.md` (manual/scheduled JSON proof commands)

#### Dataflow Verification Validation Notes

- Validation completed for this slice:
  - `pytest -q` passes (571 tests).
  - `ruff check .` passes.
  - `mypy .` currently reports existing repo-wide typing/import-stub issues outside this feature slice.

#### Watch Flow Alert Batch Parsing

- Updated `heber/watch/consumer.py` to parse both single alert payloads and batched payloads (`payload.items[]`) from the flow stream.
- Added per-item batch handling so malformed items are skipped while valid items still create watches.
- Added structured parse summary logging fields on each message parse:
  - `alert_parse_success`
  - `alert_parse_failed`
  - `batch_items_total`
  - `batch_items_failed`
- Added regression tests in `tests/test_consumer_parsing.py` for single payload parsing, batched payload parsing, malformed batch handling, and mixed valid/invalid batch processing.

#### Watch Gold Path Resolution and Startup Preflight

- Updated `heber/config.py` so Gold root path resolution accepts `HEBER_GOLD_PATH` aliases and consistently feeds `settings.gold_path`.
- Added startup output path preflight in `heber/watch/__main__.py` to create missing directories and verify write access before service startup.
- Added regression tests:
  - `tests/test_config_paths.py`
  - `tests/test_watch_startup_preflight.py`

#### Watch Feature Enrichment Resilience

- Added shared retry/throttle/cache request handling in `heber/watch/features.py` for upstream enrichment calls.
- Added bounded retries with jitter for retryable statuses (`429`, `5xx`) and structured failure logging fields:
  - `endpoint`
  - `status_code`
  - `symbol`
  - `alert_id`
  - `attempt`
  - `retryable`
  - `duration_ms`
- Added rolling-window auth failure tracking with `EnrichmentAuthFailure` fail-fast escalation after repeated `401` responses.
- Updated watcher flow to propagate fatal enrichment auth failures for restart behavior while still handling non-fatal enrichment errors.
- Added regression tests in `tests/test_watch_feature_enrichment_resilience.py` for retry success, retry exhaustion logging, auth fail-fast thresholding, and request throttling.

#### Silver Compactor Schema Evolution

- Updated `heber/writer/compactor.py` to build a unified schema per partition before writing merged output.
- Added table-to-schema alignment during compaction so missing columns in older files are filled with nulls.
- Added explicit schema conflict detection for incompatible column types; conflicting partitions are skipped with detailed error logs and without deleting source files.
- Added regression tests in `tests/test_compactor_schema_union.py` for:
  - mixed old/new schema compaction with null fill,
  - optional column value preservation across merged files,
  - incompatible type conflict skip behavior.

#### Stabilization Verification

- Rebuilt `heber-watch` and `heber-compactor` images and recreated both services after the watcher/compactor fixes.
- Verified startup logs show watcher output path resolved to container storage (`/data/gold/...`) instead of host-only `/Volumes/...`.
- Verified recent watcher and compactor logs show no recurring permission or schema mismatch failures after rollout.

#### DLQ Timestamp Normalization

- Normalized `EventEnvelope` timestamps (`ts_event`, `ts_ingest`, `ts_available`) to timezone-aware UTC values at validation time in `heber/models/envelope.py`.
- Added regression tests in `tests/test_event_envelope_timezones.py` to prevent mixed naive/aware timestamp errors during consumer lag calculations.

#### Equity Instrument Key Compatibility

- Expanded equity instrument-key validation in `heber/models/envelope.py` to accept dotted/suffixed symbols emitted by gateway data (for example `BRK.B`, `JRI.RT`, `VAL.WS`).
- Added regression tests in `tests/test_equity_instrument_key_format.py` covering valid extended tickers and malformed-key rejections.

#### Docker Python 3.14 Builder Compatibility

- Added `cmake` and `ninja-build` to the Docker builder stage so dependency installation succeeds when Python 3.14 requires source builds for some packages (for example `pyarrow`).
- Switched Docker dependency install to lockfile-driven resolution (`uv export --frozen` + `uv pip install -r requirements.txt`) so container builds use the same pinned package set as CI test runs.

#### CI Security Scan Stabilization

- Sanitized tracked lakeFS credentials in `.env` to placeholder values so Trivy secret scanning no longer flags a committed AWS-style key.
- Added explicit `duckdb>=1.1.0` dependency and regenerated `uv.lock` to remove vulnerable `duckdb==1.0.0` (CVE-2024-41672) from CI scan results.
- Added `.trivyignore` entry for `CVE-2026-0994` (transitive `protobuf` via `soda-core-duckdb`) to keep CI scanning deterministic until upstream dependencies support a non-vulnerable graph with `duckdb>=1.1.0`.

#### CI Ruff Config Portability

- Added repo-local Ruff base config at `ruff-base.toml` and updated `pyproject.toml` to extend the local file.
- This fixes PR CI pre-commit failures where GitHub Actions cannot resolve `../ruff-base.toml`.
- Added explicit `joblib` dependency in `pyproject.toml` for `heber/ml/trainer.py` save/load paths.
- This fixes CI unit test failure `ModuleNotFoundError: No module named 'joblib'` in `test_meta_feature_order_contract.py`.

#### Bronze→Silver Runtime Hardening

- Refactored `heber/writer/consumer.py` to Bronze-first processing order:
  - parse envelope
  - assign `ts_available` when missing
  - write Bronze immediately
  - normalize feed/key/payload for Silver
  - write Silver only for mapped feeds, otherwise DLQ with `unmapped_feed`
- Added explicit observability events for ingestion outcomes:
  - `bronze_write_success`
  - `silver_normalization_failed`
  - `silver_schema_unmapped`
- Added shared Silver row normalization engine (`heber/writer/normalizer.py`) and wired both live writer and backfill transformer to it.
- Updated backfill transformer to route feed aliases consistently and skip unmapped feeds explicitly.
- Updated consumer reliability/metrics tests for the new Bronze-first contract:
  - `tests/test_bronze_first_ingestion.py`
  - `tests/test_writer_consumer_reliability.py`
  - `tests/test_metrics_runtime_wiring.py`
- Fixed mypy plugin config path in `pyproject.toml` from `numpy.typing.mypy` to `numpy.typing.mypy_plugin` so type-checking can run in local/dev environments.
- Added `features/__init__.py` so mypy resolves `features.feature_views` consistently without duplicate module-path errors.

#### Repo Hygiene Remediation

- Fixed Prometheus metric registry collision: wrapped all 26 metrics in `_get_or_create()` helper to prevent `ValueError: Duplicated timeseries` during test collection (201 tests now pass, was 0)
- Expanded `.gitignore` from 2 entries to comprehensive Python project patterns; removed 81 tracked `.pyc` files
- Removed `openmetadata-ingestion` from `[catalog]` optional deps (unsatisfiable SQLAlchemy <2.0 conflict)
- Pinned Docker images: `minio:RELEASE.2025-01-20T14-49-07Z`, `lakefs:1.48.0` (was `:latest`)
- Removed duplicate k8s writer deployment (4 manifests: deployment, service, PDB, HPA) — identical to consumer
- Removed stale `heber-redis` container from `docker-compose.yml`; catalog now uses Data Gateway Redis via `host.docker.internal`
- Removed duplicate Dockerfile `writer` stage (same CMD as `consumer`)
- Suppressed Bandit B608 false positives on ClickHouse queries (table names from internal enums)
- Updated 3 test files to remove references to deleted writer k8s manifests

#### SonarQube Code Quality — Writer Module Remediation

- Removed unnecessary `async` from `BronzeWriter.write`, `flush_if_needed`, `flush`, `_flush_partition` (S7503, S7493)
- Removed unnecessary `async` from `SilverWriter.write`, `flush_if_needed`, `flush`, `_flush_partition` (S7503)
- Removed unnecessary `async` from `Compactor.compact_partition`, `scan_and_compact` (S7503)
- Re-raised `asyncio.CancelledError` in `Compactor.run()` after cleanup (S7497)
- Cascaded async removal to `EventConsumer`: `_process_event_once`, `process_event`, `_flush_layers`, `_final_flush`
- Removed unnecessary `list()` wrappers on `dict.items()` in Bronze and Silver writers (S7504)
- Updated 4 test files to use sync calls and `MagicMock` instead of `AsyncMock` for writer methods
- Reduced SonarQube issues to 2 known false positives (S930 in `ml/trainer.py`)

#### Module Audit — Tier 3/4 Fixes

- Fixed `PytestCollectionWarning`s: added `__test__ = False` to `TestDataConfig` and `TestFixture` in `testing/generators.py`, and `TestCategory` and `TestRun` in `testing/ci_gates.py`
- Fixed pandas `FutureWarning` in `test_edge_cases.py`: replaced `.fillna()` with `.where()` for pandas 2.x compatibility
- Test suite improvement: **514 passed, 0 failed, 1 warning** (from 433/18/13)

#### Codebase Audit Fixes

- Fixed `cli.py` backfill: `--since`/`--until` args are now passed to `transform()` when `--feed` is specified (previously silently ignored)
- Fixed `catalog/urn.py`: `resolve_path()` referenced non-existent `settings.storage_base_path`, now uses `settings.data_root`
- Fixed `monitoring.md`: markdown code block fence misplacement trapped "Logging Signals" section
- Fixed `troubleshooting.md`: replaced stale `heber-redis` container references with `data-gateway-redis`
- Fixed `troubleshooting.md` and `monitoring.md`: corrected DLQ commands from `LRANGE`/`LLEN` (list ops) to `XLEN` (stream op)
- Added context note to `backup-dr-runbook.md` clarifying AWS procedures are aspirational for future production
- Fixed `hotstore/tables.py`: sync table bootstrap now closes unexpected awaitable execute results before raising `TypeError`, preventing un-awaited coroutine warnings on sync misuse
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-071` revalidation in audit pass 70 and `T-74`
- Fixed `ops/health.py`: PostgreSQL readiness check now executes SQLAlchemy 2.x-compatible SQL (`text(\"SELECT 1\")`) instead of raw string execution that triggered false dependency failures
- Added regression tests for PostgreSQL health checks (`tests/test_ops_health_checks.py`) covering healthy and failing connection paths
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-089` remediation in audit pass 71 and `T-75`
- Fixed `watch/manager.py`: active/symbol watch queries now normalize Redis byte IDs before key lookup, preventing silent misses when using default `redis.from_url` byte responses
- Added watch-manager Redis byte-response regression tests (`tests/test_watch_manager_redis_bytes.py`) for active and symbol-index retrieval
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-090` remediation in audit pass 72 and `T-76`
- Fixed `watch/poller.py` and `watch/checker.py`: zero-valued option prices (`0.0`) are now treated as valid quote data (explicit `None` checks), so return paths and SL outcomes are not dropped by truthiness checks
- Added zero-price watch regression tests (`tests/test_watch_zero_price_handling.py`) for snapshot return computation and barrier SL classification
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-091` remediation in audit pass 73 and `T-77`
- Fixed `watch/writer.py`: parquet part filenames now include a collision-safe unique suffix so multiple flushes in the same second do not overwrite prior label files
- Added same-second writer collision regression test (`tests/test_watch_writer_file_collisions.py`) following TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-092` remediation in audit pass 74 and `T-78`
- Fixed `watch/__main__.py`: entrypoint now stops the watch service on non-interrupt runtime failures before re-raising, preserving cleanup/flush behavior
- Added watch entrypoint shutdown regression test (`tests/test_watch_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-093` remediation in audit pass 75 and `T-79`
- Fixed `watch/features.py`: Greeks enrichment now preserves valid `0.0` values (delta/gamma/theta/vega/IV) by using explicit `None` checks instead of truthiness
- Added zero-valued Greeks regression test (`tests/test_watch_feature_greeks_zero_values.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-094` remediation in audit pass 76 and `T-80`
- Fixed `watch/gateway.py`: gateway route candidate construction now normalizes custom `api_prefix` values without leading slash (e.g. `api/v1`) to avoid malformed prefixed URLs
- Added gateway prefix-normalization regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-095` remediation in audit pass 77 and `T-81`
- Fixed `watch/consumer.py`: entry-price quote midpoint logic now treats zero-valued bid/ask fields as valid values instead of dropping to fallback paths
- Added consumer zero-bid quote regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-096` remediation in audit pass 78 and `T-82`
- Fixed `watch/poller.py`: poll-cycle watch-price updates now preserve `mid_px=0.0` instead of incorrectly falling back to `last_price`, and snapshot bid/ask extraction now preserves zero-valued fields
- Added poller zero-midpoint update regression test (`tests/test_watch_async_redis.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-097` remediation in audit pass 79 and `T-83`
- Fixed `watch/manager.py`: watch price updates now guard return calculations when `entry_price <= 0` to prevent division-by-zero failures during poll/update flows
- Added zero-entry watch update regression test (`tests/test_watch_manager_redis_bytes.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-098` remediation in audit pass 80 and `T-84`
- Fixed `watch/models.py`: migrated `AlertWatch` config to Pydantic v2 `ConfigDict`, removing class-based `Config` deprecation warnings while preserving enum-value serialization behavior
- Added watch-model config warning regression test (`tests/test_watch_models_config.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-099` remediation in audit pass 81 and `T-85`
- Fixed `watch/checker.py`: watches with invalid/non-positive entry prices now still complete as `EXPIRED` when their watch window elapses, instead of remaining stuck due to missing return-path computation
- Added checker zero-entry expiry regression test (`tests/test_watch_zero_price_handling.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-100` remediation in audit pass 82 and `T-86`
- Fixed `watch/writer.py`: legacy `run_watch_service()` now stops the watch service on runtime exceptions before re-raising, preserving cleanup/flush behavior
- Added writer entrypoint shutdown regression test (`tests/test_watch_writer_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-101` remediation in audit pass 83 and `T-87`
- Fixed `watch/__main__.py`: entrypoint now always performs `service.stop()` in `finally`, ensuring cleanup on normal completion as well as error paths
- Added normal-completion shutdown regression test (`tests/test_watch_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-102` remediation in audit pass 84 and `T-88`
- Fixed `watch/consumer.py`: `_is_flow_alert()` now supports both byte-key and string-key stream payloads (`b\"data\"` / `\"data\"`) across bytes/str/dict envelope shapes
- Added consumer string-key envelope regression test (`tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-103` remediation in audit pass 85 and `T-89`
- Fixed `watch/poller.py`: due-check scheduling now normalizes naive and aware timestamps to UTC before subtraction, preventing mixed-datetime `TypeError` crashes during polling
- Added poller naive-timestamp due-check regression test (`tests/test_watch_async_redis.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-104` remediation in audit pass 86 and `T-90`
- Fixed `watch/manager.py`: expired-watch detection now normalizes naive `window_end` timestamps to UTC before comparison, preventing cleanup crashes on mixed datetime types
- Added manager naive-window expiry regression test (`tests/test_watch_manager_redis_bytes.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-105` remediation in audit pass 87 and `T-91`
- Fixed `watch/consumer.py`: alert-field mapping now preserves valid zero-valued `spot_px`/`contract_px` values by treating only `None` as missing before fallback
- Added consumer zero-price field-mapping regression test (`tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-106` remediation in audit pass 88 and `T-92`
- Fixed `watch/features.py`: market-context close-series handling now preserves day alignment for zero/invalid closes so return horizons do not silently skip prior sessions
- Added market-context zero-close alignment regression test (`tests/test_watch_feature_greeks_zero_values.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-107` remediation in audit pass 89 and `T-93`
- Fixed `watch/models.py`: `WatchOutcome.horizon` now enforces `WatchHorizon` enum values instead of accepting arbitrary strings
- Added watch-outcome invalid-horizon regression test (`tests/test_watch_models_config.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-108` remediation in audit pass 90 and `T-94`
- Fixed `watch/writer.py`: `run_watch_service()` now executes `service.stop()` in a `finally` block so normal completion still performs shutdown cleanup/flush
- Added writer normal-completion shutdown regression test (`tests/test_watch_writer_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-109` remediation in audit pass 91 and `T-95`
- Fixed `watch/checker.py`: watch outcome evaluation now normalizes naive `alert_time`/`window_end` timestamps to UTC-aware values before comparisons and window-duration calculations
- Added checker naive-timestamp regression test (`tests/test_watch_zero_price_handling.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-110` remediation in audit pass 92 and `T-96`
- Fixed `watch/gateway.py`: gateway URL candidate generation now avoids duplicate `/api/v1` prefixing when the configured base URL already includes API prefix segments
- Added gateway duplicate-prefix regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-111` remediation in audit pass 93 and `T-97`
- Fixed `watch/poller.py`: snapshot creation now normalizes quote payload numeric fields before midpoint math (handles numeric strings/non-numeric placeholders safely), and due-check logic now treats future-skewed `updated_at` values as immediately due
- Added poller payload-normalization and future-timestamp due-check regression tests (`tests/test_watch_zero_price_handling.py`, `tests/test_watch_async_redis.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-112`/`TD-113` remediation in audit pass 94 and `T-98`
- Fixed `watch/consumer.py`: entry-price quote parsing now tolerates malformed bid/ask numeric payloads and falls back to normalized `last_price` when midpoint inputs are invalid
- Fixed `watch/consumer.py`: timestamp parsing now normalizes ISO strings to UTC-aware datetimes and falls back to `datetime.now(UTC)` for invalid timestamp strings
- Added consumer entry-price fallback and timestamp-normalization regression tests (`tests/test_watch_gateway_paths.py`, `tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-114`/`TD-115` remediation in audit pass 95 and `T-99`
- Fixed `watch/features.py`: `AlertFeatures.from_dict()` now normalizes naive serialized `alert_time` values to UTC-aware datetimes during reconstruction
- Fixed `watch/features.py`: Greeks enrichment now skips malformed option-chain `strike_price` rows and continues extracting from valid contracts with tolerant numeric coercion
- Added feature deserialization timezone + malformed-strike Greeks regression tests (`tests/test_watch_feature_timezones.py`, `tests/test_watch_feature_greeks_zero_values.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-116`/`TD-117` remediation in audit pass 96 and `T-100`
- Fixed `watch/writer.py`: parquet flushes now stage partition files and promote only after all partition writes succeed, preventing partial-commit duplicates when a later partition write fails
- Fixed `watch/writer.py`: flush failure paths now clean staged temp files before re-raising write errors
- Added writer atomic-flush regression test (`tests/test_watch_writer_file_collisions.py`) covering partial-failure rollback and temp-file cleanup
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-118`/`TD-119` remediation in audit pass 97 and `T-101`
- Fixed `watch/__main__.py`: shutdown cleanup now isolates `service.stop()` failures so they do not mask original `service.run()` runtime errors
- Fixed `watch/__main__.py`: stop failures during normal completion are now logged and treated as non-fatal, preserving graceful CLI exit behavior
- Added watch entrypoint shutdown regression tests (`tests/test_watch_entrypoint_shutdown.py`) for stop-failure masking and normal-completion stop-failure handling
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-120`/`TD-121` remediation in audit pass 98 and `T-102`
- Fixed `watch/manager.py`: `delete_watch()` now normalizes byte-form watch IDs before key/index deletion so primary watch rows are removed correctly under byte-response Redis clients
- Fixed `watch/manager.py`: `_save_watch()` now removes non-watching statuses from `ACTIVE_WATCHES`, preventing stale active-index memberships after status transitions
- Added watch-manager byte-delete and active-index reconciliation regression tests (`tests/test_watch_manager_redis_bytes.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-122`/`TD-123` remediation in audit pass 99 and `T-103`
- Fixed `watch/poller.py`: numeric quote coercion now rejects non-finite values (`NaN`/`inf`) so invalid payload numerics are treated as missing rather than propagating into snapshots/watch updates
- Fixed `watch/poller.py`: poll cycle now skips `update_watch_price_async()` when neither midpoint nor last price is available, preventing `None` watch-price updates
- Added poller non-finite-quote and missing-price update regression tests (`tests/test_watch_zero_price_handling.py`, `tests/test_watch_async_redis.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-124`/`TD-125` remediation in audit pass 100 and `T-104`
- Fixed `watch/checker.py`: return-path extraction now filters non-finite snapshot returns so malformed `NaN` values do not propagate into MFE/MAE or expired outcome returns
- Added checker non-finite return regression test (`tests/test_watch_zero_price_handling.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-126` remediation in audit pass 101 and `T-105`
- Fixed `watch/writer.py`: `run_watch_service()` now preserves primary `service.run()` failures even when `service.stop()` cleanup fails
- Fixed `watch/writer.py`: stop failures during normal completion are now logged and treated as non-fatal
- Added writer entrypoint stop-failure regression tests (`tests/test_watch_writer_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-127`/`TD-128` remediation in audit pass 102 and `T-106`
- Fixed `watch/consumer.py`: quote numeric coercion now rejects non-finite values (`NaN`/`inf`) so entry-price midpoint logic falls back to finite `last_price` values
- Added consumer non-finite quote regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-129` remediation in audit pass 103 and `T-107`
- Fixed `watch/models.py`: watch datetime fields now normalize to UTC-aware values at validation time (naive inputs treated as UTC), removing mixed naive/aware timestamp drift
- Added watch-model naive-datetime normalization regression test (`tests/test_watch_models_config.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-130` remediation in audit pass 104 and `T-108`
- Fixed `watch/gateway.py`: base gateway URLs now strip query/fragment components before candidate route construction, preventing malformed request URLs
- Added gateway base-query sanitization regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-131` remediation in audit pass 105 and `T-109`
- Fixed `watch/features.py`: numeric feature coercion now rejects non-finite values (`NaN`/`inf`) to prevent invalid Greek/IV feature propagation
- Fixed `watch/features.py`: market-context close parsing now treats non-finite close values as missing, preventing `NaN` return features
- Added non-finite feature enrichment regression tests (`tests/test_watch_feature_greeks_zero_values.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-132`/`TD-133` remediation in audit pass 106 and `T-110`
- Fixed `watch/__main__.py`: signal-hook registration is now best-effort, so non-main-thread contexts no longer fail startup with `ValueError`
- Added watch entrypoint signal-registration failure regression test (`tests/test_watch_entrypoint_shutdown.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-134` remediation in audit pass 107 and `T-111`
- Fixed `watch/poller.py`: quote-fetch route fallback now treats malformed JSON on HTTP 200 responses as route-level failures and continues to legacy candidates
- Added malformed-prefixed-response fallback regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-135` remediation in audit pass 108 and `T-112`
- Fixed `watch/consumer.py`: entry-price quote fallback now treats malformed JSON on HTTP 200 responses as route-level failures and continues to legacy candidates
- Added consumer malformed-prefixed-response fallback regression test (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-136` remediation in audit pass 109 and `T-113`
- Fixed `watch/writer.py`: staged parquet flush now rolls back already-promoted partition files when promotion fails mid-batch, preserving all-or-nothing batch semantics
- Added writer promotion-failure rollback regression test (`tests/test_watch_writer_file_collisions.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-137` remediation in audit pass 110 and `T-114`
- Fixed `watch/manager.py`: watch-price updates now persist the provided snapshot timestamp (UTC-normalized) in `updated_at` instead of using processing-time wall clock values
- Added manager snapshot-timestamp persistence regression test (`tests/test_watch_manager_redis_bytes.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-138` remediation in audit pass 111 and `T-115`
- Fixed `watch/checker.py`: snapshots are now evaluated in chronological timestamp order so TP/SL-first outcomes remain correct under out-of-order ingestion
- Fixed `watch/checker.py`: outcome timing metadata now uses barrier-hit snapshot time or window-end time (not checker processing time) for `outcome_time` and `trading_minutes_to_hit`
- Fixed `watch/manager.py`: `complete_watch()` now accepts explicit `outcome_time` and persists it (UTC-normalized), enabling checker-derived timing semantics
- Added checker ordering/timing and manager completion-time regression tests (`tests/test_watch_zero_price_handling.py`, `tests/test_watch_manager_redis_bytes.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-139`/`TD-140`/`TD-141` remediation in audit pass 112 and `T-116`
- Fixed `watch/poller.py`: quote-fetch route fallback now treats malformed JSON payload shapes on HTTP 200 responses as route-level failures and continues to legacy candidates
- Fixed `watch/consumer.py`: entry-price route fallback now treats malformed JSON payload shapes on HTTP 200 responses as route-level failures and continues to legacy candidates
- Fixed `watch/poller.py`: snapshots now use quote-provided timestamps (`timestamp`/`ts_event`/`t`) with UTC normalization when available
- Added payload-shape fallback and quote-timestamp regression tests (`tests/test_watch_gateway_paths.py`, `tests/test_watch_zero_price_handling.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-142`/`TD-143`/`TD-144` remediation in audit pass 113 and `T-117`
- Fixed `watch/poller.py`: quote route fallback now treats request-layer route failures (timeouts/transport errors) as route-level failures and continues to legacy candidates
- Fixed `watch/consumer.py`: entry-price route fallback now treats request-layer route failures (timeouts/transport errors) as route-level failures and continues to legacy candidates
- Improved watch route-failure observability: poller/consumer now emit aggregated per-route failure summaries when all route candidates fail
- Added timeout-route fallback regression tests (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-145`/`TD-146`/`TD-147` remediation in audit pass 114 and `T-118`
- Fixed `watch/consumer.py`: explicit `retry_backoff_seconds=0.0` is now preserved instead of being overwritten by settings defaults
- Fixed `watch/consumer.py`: retry delays are now clamped to non-negative values to avoid invalid negative `asyncio.sleep()` delays under bad config
- Fixed `watch/consumer.py`: alert numeric/timestamp parsing now rejects malformed or non-finite values with fail-soft defaults instead of raising
- Added consumer backoff and parse-hardening regression tests (`tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-148`/`TD-149`/`TD-150` remediation in audit pass 115 and `T-119`
- Fixed `watch/consumer.py`: `max_process_retries` now uses explicit-`None` fallback semantics and enforces a minimum of one attempt
- Fixed `watch/consumer.py`: stream byte-key/value decoding now fails soft (`errors=\"replace\"`) to avoid parse crashes on malformed UTF-8 payloads
- Fixed `watch/consumer.py`: numeric timestamps now normalize millisecond epoch values before UTC conversion
- Added consumer retry-count/millisecond-timestamp/invalid-UTF8 regression tests (`tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-151`/`TD-152`/`TD-153` remediation in audit pass 116 and `T-120`
- Fixed `watch/consumer.py`: stream payload decoder now parses JSON envelopes with leading whitespace before flattening
- Fixed `watch/consumer.py`: put/call normalization now handles malformed non-string values with a safe default (`C`)
- Fixed `watch/consumer.py`: alert parsing now validates required identity fields (`id`, `underlying`) before watch creation
- Added consumer parse-normalization regression tests (`tests/test_watch_consumer_reliability.py`) for whitespace JSON, malformed `put_call`, and missing required fields
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-154`/`TD-155`/`TD-156` remediation in audit pass 117 and `T-121`
- Fixed `watch/consumer.py`: parse failures are now classified as non-retriable and ACKed without retry/DLQ churn
- Fixed `watch/consumer.py`: dead-lettered retry-exhaustion errors now include the terminal retry reason (`processing_failed_after_retries:<reason>`)
- Fixed `watch/consumer.py`: retry loop now normalizes bool/tuple process-result contracts to preserve backward-compatible retry semantics
- Added consumer retry-classification/telemetry regression tests (`tests/test_watch_consumer_reliability.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-157`/`TD-158`/`TD-159` remediation in audit pass 118 and `T-122`
- Fixed `watch/poller.py` and `watch/consumer.py`: gateway route-failure exception taxonomy now distinguishes `timeout` vs `transport_error` vs `request_error` using shared helpers
- Fixed `watch/poller.py` and `watch/consumer.py`: route-failure exception records now include `error_type` metadata for request/json decode failures
- Fixed `watch/poller.py` and `watch/consumer.py`: payload-shape route-failure records now include explicit `expected_type` metadata
- Added gateway failure-taxonomy regression tests (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-160`/`TD-161`/`TD-162` remediation in audit pass 119 and `T-123`
- Fixed `watch/poller.py`: route processing now treats partial/invalid per-symbol quote coverage as fallback-eligible degradation instead of terminal success
- Fixed `watch/poller.py`: when all routes are partial, poller now preserves best-effort partial quote coverage instead of dropping all quotes for the batch
- Fixed `watch/consumer.py`: entry-price lookup now continues fallback routing when requested symbol quotes are missing, malformed, or non-usable on a route
- Added partial-coverage and symbol-level fallback regression tests (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-163`/`TD-164`/`TD-165` remediation in audit pass 120 and `T-124`
- Fixed `watch/poller.py` and `watch/consumer.py`: shared quote timestamp-age helpers now classify stale route quotes and enable stale-aware fallback decisions
- Fixed `watch/poller.py`: complete but stale prefixed route batches now fall back to fresher legacy routes when available
- Fixed `watch/poller.py` and `watch/consumer.py`: when all routes are stale, freshest stale quote coverage is now preserved as controlled fallback
- Added stale-route fallback regression tests (`tests/test_watch_gateway_paths.py`) using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record `TD-166`/`TD-167`/`TD-168` remediation in audit pass 121 and `T-125`
- Fixed `watch/gateway.py`: timestamp coercion now normalizes epoch-millisecond quote timestamps (numeric and numeric-string forms) so stale-route fallback behavior stays consistent across `t` and ISO `timestamp` payloads
- Added epoch-millisecond timestamp parity regression tests (`tests/test_watch_gateway_paths.py`) for helper coercion and stale-route fallback selection
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 122 closure via `T-126`
- Fixed `watch/manager.py`: MFE/MAE update baselines now preserve prior `0.0` values instead of resetting through truthiness fallbacks during price updates
- Fixed `watch/poller.py`, `watch/consumer.py`, and `watch/features.py`: numeric coercion now rejects boolean payload values so quote prices, timestamps, Greeks, and market-context features are not polluted by `True`/`False` inputs
- Fixed `watch/features.py`: IV-rank enrichment now ignores non-finite payload values (`NaN`/`inf`) instead of persisting invalid metrics
- Added reliability regression tests (`tests/test_watch_manager_redis_bytes.py`, `tests/test_watch_zero_price_handling.py`, `tests/test_watch_gateway_paths.py`, `tests/test_watch_feature_greeks_zero_values.py`, `tests/test_watch_consumer_reliability.py`) covering zero-baseline MFE/MAE behavior, boolean quote handling, and non-finite IV-rank filtering
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 123 closure via `T-127`
- Fixed `watch/manager.py`: watch completion and expiry cleanup now sanitize non-finite outcome returns before persistence to prevent `NaN` outcomes in stored watch records
- Fixed `watch/consumer.py`: entry-price fallback now requires a positive finite contract fallback and defaults to `1.0` when alert `contract_px` is invalid or non-positive
- Added manager/consumer regression tests (`tests/test_watch_manager_redis_bytes.py`, `tests/test_watch_consumer_reliability.py`) covering non-finite outcome return sanitization and fallback entry-price defaults
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 124 closure via `T-128`
- Fixed `heber/features/templates/alert_labels.py`: entry extraction now treats non-finite/invalid `spot_px` values as fallback-eligible and enforces finite/positive ATR+spot guards before threshold computation
- Fixed `heber/features/templates/alert_labels.py`: SPY-relative return calculation now rejects non-finite raw/SPY values so infinite beta-neutral returns fail soft instead of propagating into labels
- Added alert-label regression tests (`tests/test_alert_label_intraday_windows.py`) covering `spot_px=NaN` fallback and non-finite SPY move handling using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 125 closure via `T-129`
- Fixed `heber/features/templates/alert_labels.py`: VIX enrichment/regime helpers now reject non-finite VIX inputs so invalid `vix_at_alert`/regime values fail soft instead of propagating
- Fixed `heber/features/templates/alert_labels.py`: beta-neutral helper now rejects non-finite underlying/SPY/beta inputs and returns `None` for invalid market context
- Added alert-label non-finite market-context regression tests (`tests/test_alert_label_intraday_windows.py`) for VIX and beta-neutral helper guardrails using TDD red/green flow
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 126 closure via `T-130`
- Fixed Feast test stubs (`tests/test_feature_view_alignment.py`, `tests/test_feast_materialization_behavior.py`) to emulate package semantics (`feast.__path__`, `feast.types`) so `from feast.types import ...` imports remain stable in full-suite runs
- Added Feast stub package-compatibility regression test (`tests/test_feature_view_alignment.py`) to guard against cross-test module-mocking import failures
- Updated technical debt docs (`docs/technical_debt_audit.md`, `docs/technical_debt_plan.md`) to record audit pass 127 closure via `T-131`
- Added `heber/config.py` LLM provider settings for OpenAI-compatible clients: `HEBER_LLM_PROVIDER`, `HEBER_LLM_MODEL`, `HEBER_LLM_BASE_URL`, `HEBER_LLM_API_KEY`, and `HEBER_LLM_QWEN_REGION`
- Added Qwen 2.5 endpoint resolution support via `settings.llm_effective_base_url` (intl/us/cn DashScope compatible endpoints)
- Added LLM provider/key alias regression tests (`tests/test_llm_provider_settings.py`) covering OpenAI and Qwen env-var wiring
- Updated API key setup docs in `README.md`, `docs/configuration.md`, and `.env.example` with explicit OpenAI/Qwen key locations
- Stabilized `heber/gold/tests.py` environment-based config test by clearing cached settings around env mutation
- Stabilized Feast feature-view alignment tests (`tests/test_feature_view_alignment.py`) by isolating per-test Feast stubs and evicting cached modules before imports
- Expanded `heber/models/__init__.py` exports to include phase- and version-scoped silver record models for a consistent import surface
- Fixed `bus/backpressure.py`: Prometheus counters/gauges/histograms now use shared get-or-create registration to avoid duplicate timeseries registration collisions
- Fixed `writer/transformer.py`: partition transform logging/return value now reports total records written across flushes instead of only final-batch count
- Updated `writer/transformer.py` earnings field mapping to avoid populating unsupported fiscal-period keys during Bronze-to-Silver conversion
- Updated firewall/catalog test expectations for current runtime semantics (`heber/firewall/tests.py`, `heber/catalog/tests_datasources.py`)

### Removed

- Removed backward-compatibility aliases `HotStoreWriter`/`HotStoreSyncer` from `writer/hotstore.py` (YAGNI, zero callers)
- Removed 4 stub functions from `catalog/urn.py`: `list_partitions()`, `discover_by_instrument()`, `discover_by_symbol()`, `trace_by_request()` (all returned empty, zero callers)

### Changed

- **Pydantic Settings Migration** — Migrated all `os.getenv`/`os.environ` calls to centralized `pydantic-settings.BaseSettings` class (`heber/config.py`)
  - Updated 14 files: `schema/registry_client.py`, `storage/iceberg_catalog.py`, `versioning/__init__.py`, `ops/logging.py`, `ops/health.py`, `ops/tracing.py`, `bus/__init__.py`, `watch/__main__.py`, `retention/__init__.py`, `catalog/api.py`, `backfill/__main__.py`, `writer/hotstore.py`, `ops/lifecycle.py`, `ops/metrics.py`
  - Removed all unused `import os` statements from migrated files
  - External service configs (Iceberg, LakeFS, Schema Registry) use `AliasChoices` for backward-compatible env var names
  - Redis event bus now parses connection details from `settings.redis_url` instead of individual `REDIS_HOST`/`REDIS_PORT`/`REDIS_DB` env vars
- Updated `writer/transformer.py`: `transform()` now accepts `since`/`until` parameters for date-range filtering
- Updated `test_hotstore_facade_alignment.py` to assert aliases stay removed

- Added Catalog API reference (`docs/catalog_api.md`)
- Added data contract (`docs/data_contract.md`)
- Added schema registry usage (`docs/schema_registry.md`)
- Added Iceberg migration status (`docs/iceberg_migration.md`)
- Added Hot Store guide (`docs/hot_store.md`)
- Added architecture overview (`docs/architecture.md`)
- Added configuration guide updates and host port mapping (`docs/configuration.md`)
- Added technical debt audit (`docs/technical_debt_audit.md`)
- Expanded technical debt audit (pass 2 findings and scope)
- Expanded technical debt audit (pass 3: features/Feast review)
- Expanded technical debt audit (pass 4: ops review)
- Expanded technical debt audit (pass 5: firewall/models review)
- Expanded technical debt audit (pass 6: gold/retention review)
- Expanded technical debt audit (pass 7: feast review)
- Expanded technical debt audit (pass 8: scripts/docs review)
- Expanded technical debt audit (pass 9: versioning/calendar review)
- Expanded technical debt audit (pass 10: hotstore/schemas review)
- Expanded technical debt audit (pass 11: infra/k8s review)
- Expanded technical debt audit (pass 12: backfill/backtest review)
- Expanded technical debt audit (pass 13: ops logging/reliability re-audit)
- Expanded technical debt audit (pass 14: versioning + k8s runtime conformance re-audit)
- Expanded technical debt audit (pass 15: backup/security scripts re-audit)
- Expanded technical debt audit (pass 16: tracing + init/docs drift re-audit)
- Expanded technical debt audit (pass 17: versioning/k8s runtime re-audit + worker entrypoint findings)
- Expanded technical debt audit (pass 18: ops logging/reliability + UW coverage-doc re-audit)
- Expanded technical debt audit (pass 19: backup/security scripts + labeling/data contract docs re-audit)
- Expanded technical debt audit (pass 20: backfill/hotloader runtime conformance re-audit)
- Expanded technical debt audit (pass 21: observability/runtime wiring + k8s metrics conformance re-audit)
- Expanded technical debt audit (pass 22: calendar/hotstore/schema conformance re-audit)
- Expanded technical debt audit (pass 23: MarketCalendar timezone hardening + regression-test re-audit)
- Expanded technical debt audit (pass 24: additional schema registry test hardening re-audit)
- Expanded technical debt audit (pass 25: include_extended behavior hardening re-audit)
- Expanded technical debt audit (pass 26: Hot Store base-column conformance re-audit)
- Expanded technical debt audit (pass 27: filesystem security scan gate hardening re-audit)
- Expanded technical debt audit (pass 28: catalog backup cleanup-trap hardening re-audit)
- Expanded technical debt audit (pass 29: clickhouse-backup destination-output alignment re-audit)
- Expanded technical debt audit (pass 30: labeling/data-contract docs alignment re-audit)
- Expanded technical debt audit (pass 31: UW endpoint summary reconciliation re-audit)
- Expanded technical debt audit (pass 32: log-level filtering remediation re-audit)
- Expanded technical debt audit (pass 33: dedupe Bloom-rotation remediation re-audit)
- Expanded technical debt audit (pass 34: lakeFS namespace configurability remediation re-audit)
- Expanded technical debt audit (pass 35: k8s HPA/probe conformance remediation re-audit)
- Expanded technical debt audit (pass 36: tracing optional-dependency safety remediation re-audit)
- Expanded technical debt audit (pass 37: cross-platform init-volume remediation re-audit)
- Expanded technical debt audit (pass 38: worker entrypoint runtime remediation re-audit)
- Expanded technical debt audit (pass 39: metrics-exporter wiring remediation re-audit)
- Expanded technical debt audit (pass 40: lakeFS operation-metrics coverage remediation re-audit)
- Expanded technical debt audit (pass 41: Terraform environment region/backend parameterization re-audit)
- Expanded technical debt audit (pass 42: backfill Bronze/catalog write reliability remediation re-audit)
- Expanded technical debt audit (pass 43: backfill job persistence and resume remediation re-audit)
- Expanded technical debt audit (pass 44: backfill gap-detection layout conformance remediation re-audit)
- Expanded technical debt audit (pass 45: backtest label-version pinning remediation re-audit)
- Expanded technical debt audit (pass 46: backtest as-of reproducibility metadata remediation re-audit)
- Expanded technical debt audit (pass 47: Gold retention layout + semver pruning remediation re-audit)
- Expanded technical debt audit (pass 48: retention layer coverage + config-root defaults remediation re-audit)
- Expanded technical debt audit (pass 49: label latest-version + PIT guard remediation re-audit)
- Expanded technical debt audit (pass 50: persistent DLQ queue remediation re-audit)
- Expanded technical debt audit (pass 51: firewall SCD + strict-gate remediation re-audit)
- Expanded technical debt audit (pass 52: Silver model/schema alignment remediation re-audit)
- Expanded technical debt audit (pass 53: Feast materialization/search behavior remediation re-audit)
- Expanded technical debt audit (pass 54: runtime metrics helper wiring remediation re-audit)
- Expanded technical debt audit (pass 55: watch timestamp/polling cadence remediation re-audit)
- Expanded technical debt audit (pass 56: consumer instrument-key validation remediation re-audit)
- Expanded technical debt audit (pass 57: watch feature market-timezone normalization remediation re-audit)
- Expanded technical debt audit (pass 58: watch gateway endpoint-path unification remediation re-audit)
- Expanded technical debt audit (pass 59: meta-label path/persistence alignment remediation re-audit)
- Expanded technical debt audit (pass 60: training/inference feature-order contract remediation re-audit)
- Expanded technical debt audit (pass 61: Soda path + contract-threshold quality remediation re-audit)
- Expanded technical debt audit (pass 62: framework schedule + test-environment port alignment re-audit)
- Expanded technical debt audit (pass 63: Iceberg partition-spec API alignment re-audit)
- Expanded technical debt audit (pass 64: quarantine partition-key envelope alignment re-audit)
- Expanded technical debt audit (pass 65: hotstore facade/client-stack revalidation)
- Expanded technical debt audit (pass 66: Terraform module wiring/output-contract revalidation)
- Expanded technical debt audit (pass 67: kustomize overlay image-tag alignment revalidation)
- Expanded technical debt audit (pass 68: k8s namespace/secret prerequisite conformance revalidation)
- Expanded technical debt audit (pass 69: deployment runtime-entrypoint conformance revalidation)
- Expanded technical debt audit (pass 70: Hot Store sync/async table-helper contract revalidation)
- Expanded technical debt audit (pass 71: ops health SQLAlchemy 2.x readiness conformance revalidation)
- Expanded technical debt audit (pass 72: watch manager Redis byte-ID retrieval conformance revalidation)
- Expanded technical debt audit (pass 73: watch zero-price return-path/barrier conformance revalidation)
- Expanded technical debt audit (pass 74: watch writer same-second file-collision conformance revalidation)
- Expanded technical debt audit (pass 75: watch entrypoint runtime-failure shutdown conformance revalidation)
- Expanded technical debt audit (pass 76: watch feature Greeks zero-value preservation conformance revalidation)
- Expanded technical debt audit (pass 77: watch gateway api-prefix normalization conformance revalidation)
- Expanded technical debt audit (pass 78: watch consumer zero-bid quote midpoint conformance revalidation)
- Expanded technical debt audit (pass 79: watch poller zero-midpoint update conformance revalidation)
- Expanded technical debt audit (pass 80: watch manager zero-entry update conformance revalidation)
- Expanded technical debt audit (pass 81: watch models pydantic-config warning conformance revalidation)
- Expanded technical debt audit (pass 82: watch checker zero-entry expiry conformance revalidation)
- Expanded technical debt audit (pass 83: watch writer legacy-entrypoint shutdown conformance revalidation)
- Expanded technical debt audit (pass 84: watch main-entrypoint normal-exit cleanup conformance revalidation)
- Expanded technical debt audit (pass 85: watch consumer decoded-stream payload conformance revalidation)
- Expanded technical debt audit (pass 86: watch poller naive-timestamp due-check conformance revalidation)
- Expanded technical debt audit (pass 87: watch manager naive-window expiry conformance revalidation)
- Expanded technical debt audit (pass 94: watch poller payload-normalization + future-timestamp cadence conformance revalidation)
- Expanded technical debt audit (pass 95: watch consumer timestamp/entry-price parsing conformance revalidation)
- Expanded technical debt audit (pass 96: watch feature deserialization + Greeks fallback conformance revalidation)
- Expanded technical debt audit (pass 97: watch writer atomic-flush durability conformance revalidation)
- Expanded technical debt audit (pass 98: watch entrypoint shutdown error-isolation conformance revalidation)
- Expanded technical debt audit (pass 99: watch manager byte-ID deletion + active-index consistency conformance revalidation)
- Expanded technical debt audit (pass 100: watch poller non-finite quote + missing-price update conformance revalidation)
- Expanded technical debt audit (pass 101: watch checker non-finite return-path conformance revalidation)
- Expanded technical debt audit (pass 102: watch writer entrypoint stop-failure isolation conformance revalidation)
- Expanded technical debt audit (pass 103: watch consumer non-finite quote coercion conformance revalidation)
- Expanded technical debt audit (pass 104: watch model UTC datetime-normalization conformance revalidation)
- Expanded technical debt audit (pass 105: watch gateway base-url sanitization conformance revalidation)
- Expanded technical debt audit (pass 106: watch feature non-finite numeric filtering conformance revalidation)
- Expanded technical debt audit (pass 107: watch entrypoint signal-registration resilience conformance revalidation)
- Added high-severity remediation plan (`docs/technical_debt_plan.md`)

#### Alert Watch Service (`heber/watch/`)

Real-time tracking of flow alert outcomes for ML labeling:

- **Watch Models** (`models.py`) - `AlertWatch`, `WatchSnapshot`, `WatchOutcome` Pydantic models with Redis key patterns
- **Watch Manager** (`manager.py`) - CRUD operations for active watches in Redis
- **Snapshot Poller** (`poller.py`) - Polls option quotes from Data Gateway every 5-15 min
- **Barrier Checker** (`checker.py`) - Detects TP/SL barrier hits and computes labels
- **Alert Consumer** (`consumer.py`) - Listens to `flow_alerts` Redis stream, auto-creates watches
- **Label Writer** (`writer.py`) - Writes completed outcomes to Gold layer
- **Service Orchestrator** (`WatchService`) - Runs all components concurrently

Polling strategy by horizon:

- Intraday (0-2 DTE): 5 min intervals, 4h max window
- Swing (3-21 DTE): 15 min intervals, 5 day max window
- LEAP (22+ DTE): 1 hour intervals, 30 day max window

#### Trading Calendar Integration (`heber/calendar/`)

Market-hours awareness for the watch service using `exchange-calendars`:

- **`MarketCalendar`** class wrapping NYSE calendar (XNYS)
- `is_market_open()` - Check if market is open for trading
- `add_trading_hours()` - Skip non-trading time in window calculations
- `trading_minutes_until()` - Count trading minutes between timestamps
- `seconds_until_open()` - Sleep until market opens

Integrated into watch service:

- **SnapshotPoller** - Skips polling when market closed, sleeps until open
- **WatchManager** - Window calculations use trading hours, not clock time
- **BarrierChecker** - Adds `trading_minutes_to_hit` metric to outcomes

#### Watch Service CLI & Docker

Full integration for standalone operation:

- **CLI entry point**: `python -m heber.watch [--redis URL] [--gateway URL] [--output PATH]`

#### Meta-Labeling Feature Capture (`heber/watch/features.py`)

Feature extraction for training meta-models that predict alert success:

- **`AlertFeatures` dataclass** - 30 features captured at alert time:
  - Contract info: strike, expiry, DTE, moneyness
  - Alert characteristics: premium, volume, OI ratio, alert type
  - Timing: hour, day of week, minutes since open/to close
  - Sentiment: bullish/bearish/sweep/block flags
- **`AlertFeatureExtractor`** - Extracts features from `FlowAlertRecord`
- **Market enrichment** - Fetches Alpaca bars via Data Gateway for returns/volatility
- **Greeks enrichment** - Fetches delta/gamma/theta/vega/IV from Alpaca option chain
- **IV rank enrichment** - Fetches IV rank from UW options endpoint
- **Redis storage** - Features stored with 7-day TTL for training
- Integrated into `AlertWatchConsumer` - auto-captures on alert arrival

#### ML Dataset Builder (`heber/ml/datasets.py`)

Training dataset construction for meta-models:

- **`MetaLabelDatasetBuilder`** - Joins features with outcomes
- **`DatasetConfig`** - Configurable paths, filters, split ratios
- **Temporal train/test split** - Purge/embargo to prevent leakage
- **`to_xy()` helper** - Converts to (X, y) for sklearn-compatible training
- Supports both Parquet files and Redis feature cache

#### ML Training Pipeline (`heber/ml/trainer.py`)

LightGBM-based meta-model training with MLflow integration:

- **`MetaModelTrainer`** - Trains binary classifier on meta-labels
- **`TrainingConfig`** - Hyperparameters and thresholds
- **MLflow logging** - Tracks experiments, params, metrics, models
- **`train_meta_model()`** - Convenience function for end-to-end training
- **Save/load** - Joblib serialization with config JSON

#### ML Inference Service (`heber/ml/inference.py`)

Real-time scoring of alerts with trained meta-model:

- **`MetaLabelScorer`** - Scores alerts with probability of TP hit
- **`AlertGate`** - Fail-open gate to filter low-probability alerts
- **`InferenceConfig`** - Thresholds and cache settings
- **Score caching** - Redis-backed for repeated lookups
- **Confidence classification** - "high" / "medium" / "low" buckets

- **Environment variables**: `HEBER_REDIS_URL`, `DATA_GATEWAY_URL`, `HEBER_GOLD_PATH`
- **Docker service**: `heber-watch` in `docker-compose.yml`

#### Contract-Based Barrier Labels

Enhanced `alert_labels.py` template with option contract labeling:

- **`ContractBarrierConfig`** - TP/SL thresholds for options (e.g., +25%/-15%)
- **`_compute_contract_barrier_outcome()`** - Barrier detection on option price path
- **Dual labeling** - Primary `contract_hit_tp_first` + secondary `hit_tp_first` (underlying)
- **Presets**: `aggressive()`, `moderate()`, `conservative()`

#### Alert Labels Pipeline Enhancement

Updated `heber/features/pipelines/alert_labels.py`:

- Fetches option bars from Data Gateway API
- Computes both underlying and contract barrier labels
- New CLI flags: `--no-contract`, `--gateway-url`

### Fixed

- **Data Quality Defaults + Threshold Contracts** (`heber/quality/soda_scanner.py`, `heber/quality/contracts.py`)
  - Soda scanner `silver_path` defaults now resolve from shared settings (`settings.silver_path`) and `from_env()` fallback matches that root
  - Non-null per-column threshold reporting now uses the active contract threshold instead of a hard-coded `0.99`
  - Added regression coverage for Soda defaults and threshold-aware column reporting (`tests/test_quality_soda_contracts.py`)
- **Testing Framework + Environment Defaults** (`heber/testing/environments.py`, `tests/test_testing_environment_defaults.py`)
  - Local testing environment defaults now align Postgres/Redis host mappings with docker-compose (`5433:5432`, `6380:6379`)
  - Added regression coverage for both `E2ETestSuite.get_schedule()` API availability and local service port alignment
- **Iceberg PartitionSpec Alignment** (`heber/storage/iceberg_catalog.py`, `tests/test_iceberg_partition_spec_contract.py`)
  - Silver Iceberg table creation now passes a concrete `PartitionSpec` object using `DayTransform()` on `ts_event`
  - Added regression coverage to prevent list-based `partition_spec` wiring from reappearing
- **Backpressure Quarantine Partition Alignment** (`heber/bus/backpressure.py`, `tests/test_backpressure_quarantine_paths.py`)
  - Quarantine path partitioning now prefers top-level envelope `provider`/`feed` fields
  - Legacy `meta.provider` / `meta.feed` fallback is preserved for compatibility with older payloads
  - Added regression coverage for both canonical and legacy envelope partition extraction
- **Hot Store Facade/Client Stack Regression Guard** (`tests/test_hotstore_facade_alignment.py`)
  - Added static checks that `heber.writer.hotstore` remains a compatibility facade over `heber.hotstore.sync`
  - Added guard to prevent reintroduction of `clickhouse_driver` references across Hot Store runtime modules
- **Silver Writer Type Coercion** (`heber/writer/silver.py`)
  - Added `_coerce_value()` method for automatic type conversion to Arrow types
  - Added field name mapping for UW flow_alerts: `price`→`contract_px`, `underlying_price`→`spot_px`, `option_chain`→`occ_symbol`, `alert_rule`→`alert_type`
  - Fixes `ArrowTypeError: object of type <class 'str'> cannot be converted to int` when processing UW flow alerts with string numeric values
- **Redis Event Bus Pending Claims** (`heber/bus/__init__.py`)
  - Claimed idle messages are now yielded to consumers instead of being dropped
  - Added regression test to ensure claimed messages are processed
- **Meta-Label Alignment** (`heber/watch/checker.py`, `heber/ml/datasets.py`)
  - Label rows now emit canonical outcome columns (`outcome`, `hit_tp_first`, `mfe`, `mae`, `bars_to_hit`)
  - Dataset builder normalizes legacy columns and uses correct outcome values
- **Feature Template Availability** (`heber/features/templates/*.py`)
  - `ts_available` is now derived from source data availability instead of wall-clock time
  - Flow rolling windows now use time-indexed aggregation for correctness
- **Feast Feature View Alignment** (`features/feature_views/*.py`, `features/feature_store.yaml`)
  - Feature view schemas now match produced template/pipeline columns for flow, microstructure, momentum, volatility, return labels, and alert labels
  - Gold offline source paths now follow `dataset/project/version/dt/*.parquet` layout with configurable roots/project/version globs
  - Feast local registry/online paths no longer hardcode `/data/feast`
  - Added regression coverage for schema/path alignment (`tests/test_feature_view_alignment.py`)
- **Pytest Discovery Expansion** (`pyproject.toml`)
  - Test discovery now includes both `tests/` and `heber/`
  - Added support for in-package test files named `tests.py` and `tests_*.py`
  - Default `pytest --collect-only` now sees in-package coverage that was previously skipped
- **Runtime Entrypoint Alignment** (`Dockerfile`, `k8s/base/deployments/*.yaml`)
  - Replaced stale module paths (`heber.bus.consumer`, `heber.writer.service`, `heber.writer.compaction`) with existing runtime modules
  - Docker consumer/writer now run `heber.writer.consumer`; compactor runs `heber.writer.compactor`
  - Kubernetes consumer/writer/compactor deployments now use matching module entrypoints
  - Added regression coverage for runtime module references (`tests/test_runtime_entrypoints.py`)
- **Terraform Module Availability** (`infrastructure/terraform/modules/*`)
  - Added local Terraform module scaffolds for `vpc`, `s3`, `rds`, `elasticache`, `ecr`, and `eks` so root module sources resolve
  - Preserved existing root module inputs/outputs wiring while unblocking initialization from missing-module failures
  - Added regression checks for module-source path resolution (`tests/test_terraform_module_sources.py`)
- **Terraform Root-Module Output Contract Guard** (`tests/test_terraform_root_module_contract.py`)
  - Added static regression checks that every `module.<name>.<output>` reference in `infrastructure/terraform/main.tf` is backed by a declared output in the target local module
  - Prevents root-to-module wiring drift when module outputs are renamed or removed
- **Kustomize Overlay Image-Tag Alignment** (`k8s/overlays/*/kustomization.yaml`, `tests/test_k8s_kustomize_image_tags.py`)
  - Updated `dev`/`staging`/`prod` overlays to target `name: ghcr.io/jacobmcmillan/heber` so overlay tags apply after base image-name rewrite
  - Added regression checks for both kustomization image-rule contracts and rendered `kubectl kustomize` image tags per environment
- **K8s Namespace/Secret Prerequisite Conformance** (`k8s/base/kustomization.yaml`, `k8s/base/serviceaccount.yaml`, `tests/test_k8s_namespace_prerequisites.py`)
  - Added `serviceaccount.yaml` plus external-secret resources to base kustomize resources so overlay renders include runtime prerequisites referenced by deployments
  - Added rendered-overlay regression checks for `ServiceAccount heber`, `ExternalSecret heber-secrets`, `ClusterSecretStore aws-secrets-manager`, and deployment `envFrom` secret/config references
- **Deployment Runtime Entrypoint Conformance Expansion** (`tests/test_runtime_entrypoints.py`)
  - Expanded entrypoint conformance checks to cover all base deployments (`catalog`, `consumer`, `writer`, `compactor`, `hotloader`, `backfill`) and validate importable command modules
  - Preserved explicit guards against legacy missing module paths (`heber.bus.consumer`, `heber.writer.service`, `heber.writer.compaction`)
- **SDK Catalog URL Alignment** (`heber/config.py`, `heber/sdk/client.py`)
  - Added `HEBER_CATALOG_URL` defaulting to `http://localhost:8085/api/v1` for SDK clients
  - `HeberClient` now defaults to `settings.catalog_url` instead of deriving URL from API service bind port
  - Updated SDK/config docs and added regression checks (`tests/test_sdk_catalog_defaults.py`)
- **Hot Store Unification** (`heber/hotstore/*`, `heber/writer/hotstore.py`)
  - Consolidated Hot Store sync/write logic into `heber.hotstore.sync` using the existing `clickhouse-connect` client path
  - Replaced legacy duplicate `heber.writer.hotstore` implementation with a compatibility re-export facade
  - Fixed async/sync mismatch points by using sync-safe table creation (`create_all_tables`) plus optional async helper (`create_all_tables_async`)
  - Added regression coverage for unified table creation, batch writes, and metrics (`tests/test_hotstore_unification.py`)
- **Consumer DLQ + Pending Recovery** (`heber/writer/consumer.py`)
  - Added startup recovery for idle pending Redis stream entries via `XPENDING`/`XCLAIM`
  - Added per-message retry with configurable backoff before dead-lettering
  - Added Redis DLQ routing for unrecoverable messages (`HEBER_REDIS_DLQ_STREAM_NAME`)
  - Added regression coverage for pending recovery and DLQ behavior (`tests/test_writer_consumer_reliability.py`)
- **Silver Flush Timing Fix** (`heber/writer/silver.py`)
  - Silver flush checks now use `silver_max_flush_time_seconds` instead of Bronze flush interval settings
  - Added regression tests to ensure Silver timing is independent from Bronze config (`tests/test_silver_flush_config.py`)
- **UTC Time Handling Standardization** (`heber/writer/*.py`, `heber/catalog/*.py`, `heber/sdk/client.py`)
  - Replaced remaining naive `datetime.utcnow()` calls with timezone-aware `datetime.now(UTC)` across runtime modules
  - Updated Silver flush timing tests for aware UTC datetimes
  - Added regression guard to block new `datetime.utcnow()` usage in `heber/` sources (`tests/test_utcnow_regression.py`)
- **Compactor Atomic Merge Hardening** (`heber/writer/compactor.py`)
  - Switched compaction from all-in-memory concatenate to streamed writes via `ParquetWriter`
  - Compaction now writes to temp files and promotes merged output atomically before removing source files
  - Added per-partition lock-file handling and failure cleanup so failed compactions keep source files intact
  - Added regression tests for successful merge cleanup and failure safety (`tests/test_compactor_safety.py`)
- **Silver Schema Source Consolidation** (`heber/schemas/silver.py`, `heber/writer/silver.py`, `heber/writer/transformer.py`)
  - Moved canonical Silver Arrow schema definitions out of `heber.writer.silver` into shared `heber.schemas.silver`
  - Updated writer and Bronze-to-Silver transformer to consume the shared schema module
  - Added regression tests to enforce single-source schema ownership and block inline schema constant reintroduction (`tests/test_silver_schema_source.py`)
- **Silver Model Contract Alignment** (`heber/models/silver.py`, `heber/schemas/silver.py`)
  - Normalized `SilverBase.lineage` dict inputs into deterministic JSON strings to match string-backed schema storage
  - Added release-aware default `schema_version` mapping for v2-v6 dataset families while preserving explicit overrides
  - Standardized `expiry` typing to `date` in `MaxPainRecord`, `HottestChainRecord`, and `IVTermStructureRecord`, and aligned canonical Arrow schemas to `pa.date32()`
  - Added regression coverage for lineage normalization, schema-version defaults, and date-type alignment (`tests/test_silver_model_schema_alignment.py`)
- **Feast Helper Behavior Alignment** (`heber/feast/materialization.py`, `heber/config.py`)
  - Feast helper defaults now resolve repo path from `HEBER_FEAST_REPO_PATH` (with legacy `FEAST_REPO_PATH` compatibility) instead of hardcoded literals
  - `materialize_features()` now extracts row counts from Feast materialization responses and falls back to offline-source row estimation instead of `-1` placeholders
  - `search_features()` now supports case-insensitive key, value, and `key:value` tag filters
  - Added regression coverage for repo-path defaults, materialization count behavior, and tag-filter semantics (`tests/test_feast_materialization_behavior.py`)
- **Runtime Metrics Wiring** (`heber/writer/consumer.py`, `heber/writer/silver.py`, `heber/writer/compactor.py`)
  - Consumer processing now emits received/processed/batch metrics and anti-leakage latency observations via shared metrics helpers
  - Silver flush paths now emit write throughput/duration metrics and explicit write-error metrics on failure
  - Compactor runs now emit success/error metrics with merged-file and reclaimed-byte values
  - Added regression coverage for runtime metrics instrumentation paths (`tests/test_metrics_runtime_wiring.py`)
- **Watch Timestamp + Polling Cadence Hardening** (`heber/watch/models.py`, `heber/watch/poller.py`)
  - `AlertWatch` timestamp defaults now use timezone-aware `datetime.now(UTC)` values instead of naive `datetime.utcnow()`
  - Snapshot poller now gates quote fetches by per-watch horizon cadence and skips not-yet-due long-horizon watches
  - Poller stats/logging now include `due_watches` counts for observability
  - Added regression coverage for UTC-aware defaults and per-horizon due gating (`tests/test_watch_async_redis.py`)
- **Consumer Instrument-Key Validation Enforcement** (`heber/writer/consumer.py`)
  - Consumer processing now enforces canonical `instrument_key` format checks against `instrument_type` before Bronze/Silver writes
  - Invalid keys now fail processing early and follow the existing retry/DLQ failure path instead of persisting malformed records
  - Added regression coverage for invalid-key rejection and no-write behavior (`tests/test_writer_consumer_reliability.py`)
- **Watch Feature Market-Timezone Normalization** (`heber/watch/features.py`)
  - Watch timing features now normalize alert timestamps to `America/New_York` before computing hour/day/session-derived fields
  - Naive alert timestamps are treated as UTC before market-time conversion to preserve consistent cross-service assumptions
  - Added regression coverage for UTC-aware conversion and naive-as-UTC equivalence in timing outputs (`tests/test_watch_feature_timezones.py`)
- **Watch Gateway Endpoint-Path Unification** (`heber/watch/gateway.py`, `heber/watch/*.py`)
  - Added shared watch-service Data Gateway URL candidate construction to standardize route handling
  - Poller, watch consumer, and feature-enrichment gateway calls now use `/api/v1`-prefixed routes first with legacy unprefixed fallback
  - Added regression tests for candidate ordering and fallback behavior in poller and consumer fetch paths (`tests/test_watch_gateway_paths.py`)
- **Meta-Label Path + Feature Persistence Alignment** (`heber/ml/datasets.py`, `heber/watch/features.py`, `heber/watch/consumer.py`)
  - Meta-label builder defaults now resolve outcomes/features roots from configured `settings.gold_path` canonical dataset paths
  - Dataset loaders now support legacy path fallback for historical watch-output layouts
  - Watch feature extraction now persists feature rows into Gold date partitions during ingestion, while keeping Redis cache writes
  - Feature partition persistence now appends safely to existing partition files instead of overwriting each call
  - Added regression coverage for path defaults/fallbacks, append-safe persistence, and consumer persistence invocation (`tests/test_meta_label_dataset_paths.py`, `tests/test_watch_feature_persistence.py`)
- **Training/Inference Feature-Order Contract** (`heber/ml/trainer.py`, `heber/ml/inference.py`)
  - Trainer now persists ordered training feature names into model config artifacts
  - Loaded models retain stored feature-name order for downstream scoring
  - Inference scorer now uses saved training feature order when constructing feature vectors
  - Added regression tests for save/load feature-name persistence and inference-order usage (`tests/test_meta_feature_order_contract.py`)
- **Hot Store Event Batching** (`heber/hotstore/sync.py`)
  - Added buffered quote/trade/bar event sync with configurable row and time flush thresholds
  - Replaced one-insert-per-event sync path with threshold-based batched inserts
  - Added best-effort buffer flush on sync loop exit and explicit stop shutdown
  - Added regression tests for threshold-triggered batch inserts and stop-time flush (`tests/test_hotstore_unification.py`)
- **Local Port Default Alignment** (`heber/config.py`, `README.md`, `docs/configuration.md`, `.env.example`)
  - Updated host runtime defaults to match docker-compose exposure (`Postgres: 5433`, `Redis: 6380`)
  - Synced configuration docs and environment template with the same host defaults
  - Extended settings regression coverage for Postgres/Redis defaults (`tests/test_sdk_catalog_defaults.py`)
- **Catalog Migration Baseline + Startup Guard** (`heber/catalog/api.py`, `alembic/*`)
  - Added Alembic migration scaffolding with an initial Catalog baseline revision
  - Catalog API lifespan now applies `Base.metadata.create_all` only in `dev` environment
  - Non-dev environments now skip runtime schema auto-create and are expected to run Alembic migrations
  - Added regression tests for dev/non-dev startup behavior and migration assets (`tests/test_catalog_migrations.py`)
- **Watch Async Redis Non-Blocking Refactor** (`heber/watch/*.py`)
  - Added async wrappers in `WatchManager` for Redis-backed CRUD/update operations used from async loops
  - Watch consumer stream read/ack and watch creation now offload sync Redis/manager calls via `asyncio.to_thread`
  - Snapshot poller now uses async manager wrappers for active-watch fetches, snapshot writes, and price updates
  - Check/write loop now offloads synchronous barrier checks from async context
  - Added regression tests to verify non-blocking async paths (`tests/test_watch_async_redis.py`)
- **Watch Consumer Retry + DLQ Reliability** (`heber/watch/consumer.py`)
  - Added bounded retry/backoff for flow-alert processing before terminal failure handling
  - Added Redis DLQ write path with message metadata for unrecoverable watch-consumer records
  - Updated ACK policy to acknowledge only on successful processing or successful DLQ write
  - Retains pending messages when DLQ write fails, avoiding silent drops
  - Added regression tests for retry count, DLQ routing, and ACK decision behavior (`tests/test_watch_consumer_reliability.py`)
- **Stream Naming Convention Unification** (`heber/bus/__init__.py`, `heber/bus/streams.py`, `heber/watch/consumer.py`)
  - Standardized event-bus stream naming to `heber:events:*` across stream enum values and registry helpers
  - Watch consumer now defaults to `settings.redis_stream_name` instead of hardcoded stream literals
  - Updated operations runbook/troubleshooting Redis commands to use aligned event and DLQ stream keys
  - Added regression coverage for stream naming consistency (`tests/test_stream_naming_conventions.py`)
- **Alert Label Pipeline Bar-Key + Intraday Wiring** (`heber/features/pipelines/alert_labels.py`, `heber/features/templates/alert_labels.py`)
  - Alert-label bar reads now canonicalize equity symbols (`equity:*`) and include legacy raw-key filters for compatibility
  - Intraday labeling now reads from `bars` and filters `timeframe` to 5-minute bars instead of querying stale `bars_5min`
  - Added fallback to daily bars when intraday data is unavailable or timeframe metadata is missing
  - Added regression tests for key normalization, intraday dataset selection, and fallback behavior (`tests/test_alert_labels_pipeline_keys.py`)
- **Intraday Label Window Unit Fix** (`heber/features/templates/alert_labels.py`)
  - Corrected intraday horizon window math to use 5-minute bar durations instead of day-based offsets
  - `ts_available` and SPY-relative return windows now share the same minute-based intraday horizon timing
  - Added regression tests for intraday/daily window duration behavior (`tests/test_alert_label_intraday_windows.py`)
- **Flow Feature Rolling Window Hardening** (`heber/features/templates/flow.py`)
  - Normalized flow `ts_event` values to UTC and dropped invalid timestamps before time-window rolling
  - Added regression checks that 24-hour aggregates are time-windowed (not row-count based)
  - Added regression checks for UTC normalization of string timestamps in flow feature outputs (`heber/features/templates/tests.py`)
- **Lifecycle Async Shutdown Wait Race Fix** (`heber/ops/lifecycle.py`)
  - `async_wait_for_shutdown` now returns immediately when shutdown is already signaled
  - Added race-safe async shutdown-event initialization to prevent hung waits
  - Added regression coverage for pre-signaled and late-signaled async shutdown waits (`tests/test_lifecycle_shutdown_wait.py`)
- **Lifecycle Shutdown Timeout Status Fix** (`heber/ops/lifecycle.py`)
  - Shutdown timeout paths now report `status="timeout"` instead of `status="success"` in lifecycle metrics
  - Sync/async shutdown methods now return `False` when drain timeout occurs and `True` only on successful drain
  - Added regression coverage for sync timeout, async timeout, and successful drain behavior (`tests/test_lifecycle_shutdown_timeout.py`)
- **Structured Logging Level Filtering** (`heber/ops/logging.py`)
  - `configure_logging(log_level=...)` now validates level names and fails fast on invalid values
  - Logging level now applies to both stdlib root logger configuration and structlog filtering wrappers
  - Added regression tests for INFO/DEBUG behavior in JSON and console render modes (`tests/test_logging_level_filtering.py`)
- **Dedupe Bloom Rotation Bounding** (`heber/ops/reliability.py`)
  - `EventDeduplicator` now rotates Bloom filters on a configured interval to bound long-lived false-positive buildup
  - Duplicate checks now include active and previous Bloom windows, so recent duplicates are still caught across a rotation boundary
  - Added regression tests covering in-window duplicate detection and post-rotation aging behavior (`tests/test_event_deduplicator_rotation.py`)
- **lakeFS Storage Namespace Configurability** (`heber/versioning/__init__.py`)
  - Added configurable storage namespace resolution via `LAKEFS_STORAGE_NAMESPACE_BASE` and `LAKEFS_STORAGE_NAMESPACE_TEMPLATE`
  - Repository creation now uses config-driven namespace resolution instead of hardcoded `s3://heber-lakehouse/{repo}`
  - Added regression tests for namespace resolution and repository create-path wiring (`tests/test_lakefs_namespace_config.py`)
- **lakeFS Operation Metrics Coverage** (`heber/versioning/__init__.py`)
  - Added success/error counter and duration histogram instrumentation for `create_tag`, `list_tags`, `merge`, and `diff`
  - Error paths now include repository-resolution and branch-resolution failures for these operations
  - Added regression tests for operation metrics coverage across success and error paths (`tests/test_lakefs_operation_metrics.py`)
- **Terraform Environment Region/Backend Parameterization** (`infrastructure/terraform/environments/*`)
  - Replaced hardcoded environment module region literals with `var.aws_region` in `dev`/`staging`/`prod` Terraform entrypoints
  - Converted environment S3 backend blocks to partial configuration and moved backend defaults into per-environment `backend.hcl` files
  - Removed hardcoded backend region keys and added regression checks for overrideable Terraform env wiring (`tests/test_terraform_environment_config.py`)
- **Backfill Bronze/Catalog Write Reliability** (`heber/backfill/__init__.py`)
  - Backfill writes now persist raw records into Bronze partitioned paths in addition to Silver temp parquet outputs
  - Backfill coordinator now performs catalog dataset + coverage metadata updates after successful chunk writes (best effort when catalog is unavailable)
  - Missing `pyarrow` in backfill parquet writes now raises a runtime failure instead of silently skipping writes
  - Added regression coverage for Bronze+Silver writes, pyarrow failure handling, and catalog metadata updater invocation (`tests/test_backfill_writer_reliability.py`)
- **Backfill Job Persistence and Resume** (`heber/backfill/__init__.py`)
  - Backfill job state now persists under storage-root job state files and reloads automatically when coordinator starts
  - Progress checkpoints are persisted during run, enabling resumed backfills to skip already completed dates after restart
  - Persisted stale `running` jobs are recovered into resume-safe status instead of remaining blocked forever
  - Added regression coverage for persisted job reload, fail+restart resume, and stale-running recovery (`tests/test_backfill_job_persistence.py`)
- **Backfill Gap Detection Layout Conformance** (`heber/backfill/__init__.py`)
  - Gap detection now scans both legacy backfill Silver roots and canonical Silver feed/instrument_type partition trees for `dt=*` coverage
  - Existing-date discovery now unions coverage across both layouts to avoid false full-gap reports
  - Added regression coverage for legacy-only, canonical-only, and mixed-layout date discovery (`tests/test_backfill_gap_detector_layout.py`)
- **Backtest Label-Version Pinning** (`heber/backtest/integration.py`)
  - `BacktestDataLoader` now accepts `label_version` and passes it to label `read_gold()` calls for train/test data loads
  - Default label version behavior is now explicit (`latest`) instead of implicitly unpinned
  - Added regression coverage for explicit and default label-version read behavior (`heber/backtest/tests.py`)
- **Backtest As-Of Reproducibility Metadata** (`heber/backtest/integration.py`)
  - `ExperimentConfig` now includes feature/label as-of timestamps and propagates them through config serialization/checklist output
  - `BacktestResult` now persists dataset as-of metadata, and `ExperimentTracker.log_fold()` now supports per-fold as-of timestamps
  - Added regression coverage for as-of metadata round-trip persistence and fold/result/checklist inclusion (`heber/backtest/tests.py`)
- **Gold Retention Layout + Version Pruning Alignment** (`heber/retention/__init__.py`)
  - Reaper Gold scans now discover canonical `dataset=.../(project|type)=.../version=...[/dt=...]` paths and capture version metadata for pruning
  - Gold version pruning now uses semantic-version-aware ordering with deterministic fallback instead of lexicographic-only sort
  - Added regression coverage for project-layout scans, label-layout scans without `dt=*`, and semver retention ordering (`tests/test_retention_gold_layout.py`)
- **Retention Layer Coverage + Config-Root Defaults** (`heber/retention/__init__.py`)
  - Reaper scheduler now evaluates retention policies for `HOT_STORE` and `DLQ` layers in addition to Bronze/Silver/Gold
  - `DatasetRetentionConfig` now includes explicit `hot_store` and `dlq` policy fields in serialized retention configs
  - Reaper default storage/archive paths now resolve from configured `HEBER_DATA_ROOT`/shared settings instead of hardcoded `/data/heber`
  - Added regression coverage for all-layer scheduler processing and default-root resolution (`tests/test_retention_gold_layout.py`)
- **Label Read Latest-Version + Point-In-Time Guard Hardening** (`heber/gold/labels.py`)
  - `read_label()` latest-version resolution now uses semantic-version-aware ordering instead of lexicographic `version=*` folder sort
  - `read_label()` now fails closed by default when `ts_available` is missing, preventing unfiltered future-label reads
  - Added regression coverage for semver latest selection and missing-`ts_available` fail-closed behavior (`heber/gold/label_tests.py`)
- **Persistent Dead-Letter Queue** (`heber/ops/reliability.py`)
  - `DeadLetterQueue` now supports optional persisted storage and startup reload so failed events survive process restarts
  - Queue add/retry/pop mutations now persist state atomically when persistence is configured
  - Added regression coverage for restart recovery, retry-attempt persistence, and persisted pop behavior (`tests/test_dead_letter_queue_persistence.py`)
- **Firewall SCD Join + Strict Validation Gate Hardening** (`heber/firewall/scd.py`, `heber/firewall/validation.py`)
  - `join_with_reference_asof()` now resolves reference validity columns from suffixed or unsuffixed names after join
  - `validate_gold_build(strict=True)` now raises only for hard leakage gates and keeps warning-only checks non-fatal
  - Added regression coverage for both SCD validity-column modes and strict warning-only validation behavior (`tests/test_firewall_scd_and_validation.py`)
- **Kubernetes HPA/Probe Runtime Conformance** (`k8s/base/hpa/*.yaml`, `k8s/base/deployments/*.yaml`)
  - Replaced stale custom HPA pod metrics with CPU/memory resource metrics for catalog/consumer/writer autoscalers
  - Replaced worker HTTP health probes with exec probes that verify expected runtime entrypoints
  - Added regression checks for HPA metric type and worker probe mode (`tests/test_k8s_hpa_probe_conformance.py`)
- **Tracing No-OTEL Decorator Safety** (`heber/ops/tracing.py`)
  - `traced()` now avoids unconditional `SpanKind` access when OpenTelemetry is unavailable
  - No-OpenTelemetry paths now pass `kind=None` to noop tracing context safely
  - Added regression coverage for `@traced` execution with `OTEL_AVAILABLE=False` (`tests/test_tracing_no_otel.py`)
- **Cross-Platform Volume Init Guarding** (`scripts/init_volume.sh`)
  - Added explicit OS/tool checks before invoking macOS-only `dot_clean`
  - Non-macOS and missing-tool paths now emit clear skip messages instead of implicit fallback
  - Added regression checks for explicit platform guards and removal of `dot_clean ... || true` behavior (`tests/test_init_volume_platform_guard.py`)
- **Worker Entrypoint Service Modes** (`heber/backfill/__main__.py`, `heber/writer/hotstore.py`)
  - Added executable `python -m heber.backfill` service entrypoint with backfill API and `/health`/`/ready` routes
  - Added real hotloader CLI runtime for `python -m heber.writer.hotstore` with continuous sync-loop mode and `--once` mode
  - Added regression coverage for entrypoint execution paths and runtime module availability (`tests/test_worker_entrypoint_services.py`, `tests/test_runtime_entrypoints.py`)
- **Metrics Exporter Wiring Alignment** (`heber/ops/metrics.py`, service entrypoints)
  - Added `start_metrics_server_from_env` helper and wired it into catalog, consumer/writer, compactor, hotloader, and backfill entrypoint paths
  - Kept deployment scrape annotations/ports aligned with runtime behavior by ensuring scraped entrypoints start metrics exporters
  - Added regression checks for deployment-to-entrypoint metrics alignment (`tests/test_metrics_exporter_alignment.py`)

\n\n#### SonarQube Code Quality Remediation\n\n- Replaced deprecated `datetime.utcnow()` with `datetime.now(UTC)` in `writer.py` and `writer/consumer.py`\n- Extracted constants for duplicate literals: `DEFAULT_GATEWAY_URL`, `DEFAULT_STORAGE_ROOT`\n- Refactored complex functions by extracting helpers in `consumer.py` and `alert_labels.py`\n- Removed async from functions without await in `hotstore/client.py`, `backfill`, `retention`\n- Removed unused parameters in `openmetadata_client.py` and `backfill/__init__.py`\n- Fixed asyncio.create_task GC issue in `backfill/__init__.py`\n\n### Added

#### Code Quality Pipeline

- **Pre-commit Hooks** (`.pre-commit-config.yaml`)
  - Ruff linter with auto-fix and formatting
  - Detect-secrets for secret leak prevention
  - Standard hooks: trailing whitespace, end-of-file, yaml, merge conflicts, debug statements
  - MyPy and Bandit documented for manual CI runs (deferred due to existing issues)

- **Security Scanning** (`pyproject.toml`)
  - Bandit configuration with test exclusions
  - Detect-secrets baseline generation

- **Dependency Management** (`.github/dependabot.yml`)
  - Weekly Python dependency updates
  - Weekly GitHub Actions updates
  - Weekly Docker dependency updates

- **SonarQube Integration** (`sonar-project.properties`)
  - Project configuration with Python 3.11 target
  - Coverage report integration
  - Source/test path configuration

- **Development Documentation** (`README.md`)
  - Prerequisites and setup instructions
  - Code quality tools usage guide
  - CI/CD pipeline overview

- **Test Infrastructure** (`tests/`)
  - Created tests directory with placeholder test
  - pytest configuration in pyproject.toml

#### Part VII: ML/Research Features (PRD §28-35)

- **Gold Dataset Versioning** (Phase 30, PRD §28)
  - `GoldDatasetVersion` dataclass with semantic versioning support
  - `GoldVersionRegistry` for version persistence and lineage tracking
  - `read_gold()` with version pinning and compatibility checks
  - `check_compatibility()` for safe version upgrades
  - 11/11 tests passing

- **Label Management** (Phase 31, PRD §29)
  - `LabelMetadata` with forward_window, label_horizon, availability_lag
  - `compute_availability_time()` for point-in-time correct labels
  - `write_label()` and `read_label()` with ts_available filtering
  - Zero-leakage guarantee through availability alignment
  - 15/15 tests passing

- **Train/Test Split Utilities** (Phase 32, PRD §30)
  - `walk_forward_splits()` for rolling train/test windows
  - `expanding_window_splits()` for growing training data
  - `HoldoutSet` with `check_holdout_access()` warnings
  - `purge_window()` for label-aware data purging
  - Embargo enforcement between train/test periods
  - 19/19 tests passing

- **Feast Integration** (Phase 33, PRD §31)
  - Feature views: `volatility.py`, `flow.py`, `microstructure.py`, `labels.py`
  - `materialize_features()` for incremental/full materialization
  - `get_historical_features()` for point-in-time training data
  - `get_online_features()` for low-latency inference
  - `search_features()` for feature discovery by owner/category
  - 6/6 tests passing

- **Feature Template Library** (Phase 34, PRD §32)
  - `heber/features/templates/momentum.py` - RSI, MACD, ROC, momentum returns
  - `heber/features/templates/volatility.py` - ATR, Parkinson vol, Bollinger, z-scores
  - `heber/features/templates/flow.py` - Options premium aggregates, call/put ratio
  - `heber/features/templates/microstructure.py` - Spread, depth, imbalance metrics
  - `heber/features/templates/cross_asset.py` - Beta, alpha, relative strength, correlation
  - `heber/features/templates/labels.py` - Forward return labels, classification labels
  - `heber/features/templates/alert_labels.py` - Flow alert return labels for underlying and option contracts
  - 7/7 tests passing

- **Data Quality Contracts** (Phase 35, PRD §33)
  - `QualityContract` for defining validation rules per dataset
  - `QualityViolation` for tracking failures with affected symbols/dates
  - `QualityReport` with pass/fail status and metrics
  - `DataQualityValidator` with checks:
    - `fill_rate` - % of expected trading days with data
    - `non_null_rate` - % non-null values for OHLCV columns
    - `max_lag_hours` - Data freshness from market close
    - `max_gap_seconds` - Maximum gap between data points
  - Default contracts for bars, trades, quotes datasets
  - 12/12 tests passing

- **Backtest Integration** (Phase 36, PRD §34)
  - `ExperimentConfig` for reproducibility metadata capture
  - `BacktestDataLoader` with point-in-time correct loading
  - `ExperimentTracker` for fold logging and result persistence
  - `generate_reproducibility_checklist()` per PRD §34.4
  - 9/9 tests passing

- **Survivor Bias Handling** (Phase 37, PRD §35)
  - `InstrumentLifecycle` with list_date, delist_date, delist_reason
  - `DelistReason` enum: bankruptcy, merger, acquisition, voluntary, regulatory
  - `UniverseManager` for point-in-time universe snapshots
  - `filter_dataframe()` with:
    - `exclude_future_delistings` - Strict survivor bias prevention
    - `mark_delistings` - Flag upcoming delistings
  - `get_delisted_instruments()`, `get_newly_listed_instruments()`
  - 13/13 tests passing

#### Part VIII: Reliability Engineering (PRD §37-38)

- **SLO Framework** (Phase 38, PRD §37)
  - `SLI` dataclass for Service Level Indicators with PromQL queries
  - `SLO` with target percentage and error budget ratio calculation
  - `BurnRateAlert` with Prometheus rule generation
  - `SLOManager` for status calculation and rule generation
  - Default SLOs: Ingestion (99.9%), Write (99.95%), Catalog (99.9%)
  - Burn rate alerts: 14x/1h (critical), 6x/6h (warning), 3x/1d, 1x/3d

- **Error Budget Policy** (Phase 39, PRD §38)
  - `ErrorBudget` with allowed/remaining calculation
  - `BudgetState` enum: healthy (>50%), warning (25-50%), critical (<25%), exhausted
  - `BudgetPolicy` with deploy gates by state
  - `DeployRisk` levels: standard, high_risk, breaking_change, infrastructure
  - `ErrorBudgetManager` for policy enforcement and reporting
  - 20/20 tests passing

- **Incident Runbooks** (Phase 40, PRD §39)
  - `Runbook` with symptoms, triage steps, resolutions
  - `RunbookRegistry` with lookup by key or alert name
  - 6 default runbooks: consumer lag, DLQ, Hot Store, Catalog, compaction, leakage
  - Markdown export for documentation

- **On-Call & Escalation** (Phase 41, PRD §40)
  - `OnCallSchedule` with active time checking
  - `EscalationPolicy` with P1-P4 response/escalation times
  - `Incident` lifecycle: create, acknowledge, resolve
  - `OnCallManager` with escalation logic and channel routing
  - 18/18 tests passing

- **Chaos Engineering** (Phase 42, PRD §41)
  - `ChaosExperiment` with hypothesis, procedure, success criteria
  - `ChaosRegistry` with scheduling by frequency (weekly/monthly/quarterly)
  - 7 default experiments: kill pods, throttle S3, block Catalog, bad events, etc.
  - `ExperimentRun` lifecycle: start, complete, pass/fail tracking
  - Markdown runbook export

- **Capacity Planning** (Phase 43, PRD §42)
  - `BaselineMetric` with 5 defaults: events/day, peak rate, storage
  - `ScalingTrigger` with 7 thresholds: CPU, lag, memory, connections
  - `CapacityForecast` for Q1-Q4 2026 projections
  - `BottleneckAnalysis` for 5 components
  - `CapacityPlanner` with cost projection (volume multiplier)
  - 18/18 tests passing

#### Part IX: Testing Framework (PRD §45-50)

- **Synthetic Data Generators** (Phase 49, PRD §50)
  - `SyntheticDataGenerator` for bars, trades, quotes
  - Deterministic generation with seed support
  - `TestDataConfig` for date ranges and symbols
  - `TestFixture` and `FixtureRegistry` for curated test data

- **Leakage Validation Suite** (Phase 46, PRD §49)
  - `LeakageValidator` with LK-001 through LK-007 test cases
  - `validate_no_future_data()` for zero-leakage assertion
  - `validate_backfill_ts_available()` for backfill validation
  - `validate_gold_lineage()` for feature/label integrity
  - Report generation with pass/fail summary
  - 17/17 tests passing

- **Unit/Integration/E2E Framework** (Phases 43-45, PRD §46-48)
  - `UnitTestSpec` with 7 module test areas
  - `MockStrategy` for S3, Redis, Postgres, ClickHouse
  - `IntegrationTestHarness` with 6 component suites
  - `E2ETestSuite` with 7 test flows and schedule

- **Performance Testing** (Phase 47, PRD §51)
  - `PerformanceSLO` with 5 targets (throughput, latency)
  - `LoadTestScenario` with 5 load profiles
  - `RegressionDetection` for baseline comparison
  - `PerformanceTester` with SLO checking

- **CI Gates** (Phase 48, PRD §53)
  - `CoverageRequirement` with 6 component thresholds
  - `CIGate` for PR, main, staging, prod gates
  - `FlakyTestPolicy` with quarantine logic
  - `CIGateEnforcer` with gate checking and reporting
  - 18/18 tests passing

#### Part X: Data Sources (PRD §52, §55-57)

- **Test Environments** (Phase 50, PRD §52)
  - `EnvironmentConfig` for local, CI, staging, production
  - `DockerComposeService` with 4 services (Postgres, Redis, MinIO, ClickHouse)
  - `StagingConfig` with AWS resource specs
  - `EnvironmentManager` with Docker Compose generation

- **Data Source Inventory** (Phase 51, PRD §55-57)
  - `DataProvider` with 7 providers (Alpaca, UW, Finnhub, etc.)
  - `DatasetSpec` with 25 dataset definitions
  - `StorageBoundary` enum (Heber vs Document Store)
  - `ProviderRegistry` and `DatasetCatalog`
  - 17/17 tests passing

- **Additional Dataset Schemas** (Phase 51, PRD §57)
  - `DailyBar` for daily OHLCV with adjusted close, dividends, splits
  - `OptionQuote` and `OptionTrade` with Greeks (delta, gamma, theta, vega)
  - `CongressTrade` and `LobbyingDisclosure` for alternative data
  - `CompanyInfo`, `IncomeStatement`, `BalanceSheet`, `CashFlow`, `FinancialRatios`
  - `EconomicIndicator`, `InterestRate`, `TreasuryYield`
  - `ForexRate`, `CryptoBar`, `CryptoQuote`
  - 16 schemas total, 14/14 tests passing

- **Event Bus Streams** (Phase 52, PRD §60)
  - `StreamConfig` with 15 streams across priorities
  - `ConsumerGroupConfig` with 6 consumer groups
  - `StreamRegistry` for stream/group management

- **Implementation Slices** (Phase 53, PRD §61)
  - `ImplementationSlice` with 8 ordered slices
  - `SliceManager` with dependency tracking and status

- **Gap Resolution Summaries** (Phase 57, PRD §17-62)
  - `DecisionRecord` for design decisions
  - `GapResolutionRegistry` with 18 decisions across 6 categories
  - 17/17 tests passing

- **Access Control** (Phase 56, PRD §11.9)
  - `Project` for project-based access control
  - `DatasetPermission` with layer-based access levels
  - `SDKToken` with scopes, expiry, and validation
  - `AccessControlManager` for permission checking
  - Silver shared by default, Gold requires explicit permission
  - 17/17 tests passing

#### Part VI: Final Infrastructure (PRD §21-29)

- **Backup & Disaster Recovery** (Phase 27, PRD §27)
  - Tiered backup strategy: Hot (1h), Warm (6h), Cold (24h)
  - Recovery procedures for Bronze/Silver/Gold/Catalog
  - RTO/RPO targets per data tier

- **Network Topology** (Phase 28, PRD §28)
  - Multi-tier VPC architecture
  - Security group configurations
  - Service mesh considerations

- **Cost Management** (Phase 29, PRD §29)
  - Monthly cost tracking per component
  - Optimization recommendations
  - Budget alerts

#### Part V: Production Infrastructure (PRD §17-20)

- **Secrets Management** (Phase 24, PRD §17)
  - External Secrets Operator integration
  - Secret rotation procedures

- **Infrastructure as Code** (Phase 25, PRD §18)
  - Terraform modules for AWS resources
  - State management configuration

- **CI/CD Pipeline** (Phase 26, PRD §19)
  - GitHub Actions workflows
  - Docker image builds with Trivy scanning
  - Automated testing gates

#### Part IV: Kubernetes Deployment (PRD §19-20)

- **Container Build** (Phase 22, PRD §19)
  - Multi-stage Dockerfile
  - Security hardening

- **Kubernetes Deployment** (Phase 23, PRD §20)
  - Helm charts for all services
  - HPA configurations
  - PDB policies

#### Part III: Data Lifecycle (PRD §14-16)

- **Compaction Protocol** (Phase 21, PRD §16)
  - Manifest-based commit protocol with crash recovery
  - Concurrent write prevention via distributed locks
  - Compaction scheduling with backoff

- **Retention & Lifecycle** (Phase 20, PRD §15)
  - Automated retention reaper
  - Tier-based retention policies

- **Schema Evolution** (Phase 19, PRD §14)
  - Backward/forward compatibility checks
  - Schema registry integration

## [0.1.0] - 2025-12-01

### Added

- Initial project structure
- Bronze layer ingestion from Redis Streams
- Silver layer canonical schema
- Gold layer feature datasets
- Hot Store integration (ClickHouse)
- Zero-Leakage Firewall
- SDK client library
- Catalog service (PostgreSQL)

---

_For earlier history, see git commit log._
