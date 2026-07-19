# CLAUDE.md

Heber is the data lakehouse for the Empire monorepo. It ingests normalized market data from Data-Gateway via Redis Streams, stores it through Bronze/Silver/Gold layers on `/Volumes/heber/data`, and exposes a catalog API and filesystem reader for downstream trading systems.

## Commands

```bash
# Install
uv sync

# Tests
uv run pytest                          # all tests
uv run pytest -m unit                  # fast, isolated
uv run pytest -m integration           # real DB/file I/O
uv run pytest tests/test_foo.py        # single file
uv run pytest -k "test_name"           # single test

# Lint & format
ruff check .
ruff format .
mypy .

# CLI
uv run heber info
uv run heber datasets --layer silver

# Services (run from Heber/)
uv run python -m heber.writer.consumer     # event consumer (Bronze+Silver)
uv run python -m heber.watch               # alert watch consumer
uv run python -m heber.gold_poller         # EOD Gold feature poller
uv run python -m heber.catalog.api         # catalog API (:8085)

# Docker
docker compose up -d
# Deploy code changes: images BAKE the source, so a restart re-runs stale code —
# you must rebuild. scripts/deploy.sh rebuilds + recreates app services, waits for
# health, and prints the deployed commit.
./scripts/deploy.sh                            # all app services
./scripts/deploy.sh heber-consumer heber-watch # just these
```

## Architecture

### Storage Layers

```
Bronze  → raw JSONL.gz, append-only, immutable
          Path: bronze/provider={}/feed={}/dt={}/hour={}/
Silver  → typed-flat Parquet, rename + type coerce only (no derived fields)
          Path: silver/feed={}/instrument_type={}/dt={}/[hour={}]/
Gold    → ML features, labels, enriched datasets
          Path: gold/dataset={}/project={}/version={}/dt={}/
```

### Services

| Service | Entry Point | Port | Purpose |
|---------|-------------|------|---------|
| heber-consumer | `heber.writer.consumer` | — | Redis Streams (`heber:events`) → Bronze + Silver writer |
| heber-backfill-consumer | `heber.writer.consumer` | — | Same writer on the dedicated `heber:events:backfill` stream (group `heber-backfill-writers`), isolated so bulk UW backfill can't evict live feeds |
| heber-watch | `heber.watch` | — | flow_alerts stream → watch creation, snapshot polling, Gold feature enrichment |
| heber-catalog | `heber.catalog.api` | 8085 | Dataset/instrument/coverage metadata API (Postgres-backed) |
| gold-poller | `heber.gold_poller` | — | EOD scheduled Gold feature pipeline (16:35 ET default) |

### Data Flow

```
Data-Gateway → Redis Stream (heber:events)
  → EventConsumer
    → parse EventEnvelope (Pydantic)
    → validate payload + instrument key
    → deduplicate (event_id SHA256)
    → BronzeWriter (JSONL.gz, all contracted feeds)
    → is_bronze_only_feed? → stop
    → resolve_silver_feed (alias mapping)
    → SilverWriter (typed Parquet with Arrow schemas)
    → XACK

  On failure: retry with backoff → DLQ stream (heber:events:dlq) after max retries
```

### Key Modules

| Module | Purpose |
|--------|---------|
| `heber/config.py` | Pydantic Settings, `HEBER_*` env vars, `get_settings()` |
| `heber/models/envelope.py` | `EventEnvelope` with `ts_available` (zero-leakage gate) |
| `heber/writer/ingest_contracts.py` | Feed aliases, contracted feeds, field mappings, payload normalization |
| `heber/writer/bronze.py` | `BronzeWriter` — buffered JSONL.gz append |
| `heber/writer/silver.py` | `SilverWriter` — typed Parquet with Arrow schema enforcement |
| `heber/writer/consumer.py` | `EventConsumer` — Redis XREADGROUP main loop |
| `heber/writer/normalizer.py` | `envelope_to_silver_row()`, required field enforcement |
| `heber/writer/key_normalization.py` | Instrument key validation/normalization for Silver |
| `heber/reader/core.py` | `HeberReader` — canonical filesystem reader (predicate pushdown) |
| `heber/catalog/service.py` | `CatalogService` — datasets, instruments, coverage (async Postgres) |
| `heber/schemas/silver.py` | `SILVER_SCHEMAS` — Arrow schemas per feed |
| `heber/gold/labels.py` | Triple-barrier label generation with availability tracking |
| `heber/watch/consumer.py` | Alert watch consumer (flow_alerts → watch → poll → enrich) |
| `heber/watch/poller.py` | `SnapshotPoller` — option quote polling for active watches |
| `heber/features/` | Feast feature views for Gold layer |

## Zero-Leakage Contract

Every record has `ts_available` set on write — the first time the record is safe to query. This prevents look-ahead bias in backtesting and ML training.

- `ts_available` is set by Heber on write (not by Data-Gateway)
- `ts_effective = ts_available + processing_delay_ms` accounts for realistic processing lag
- `HeberReader.read_asof(asof_time=...)` pushes `ts_available <= asof_time` into the pyarrow dataset scan as **predicate pushdown** — not a post-filter
- `HeberReader.asof_join()` enforces `ts_available` on the right table before merge
- Gold writes validate `ts_available >= ts_event` invariant

## Feed Routing

Incoming feeds from Data-Gateway are routed through `ingest_contracts.py`:

1. **Contracted feeds** (`CONTRACTED_RAW_FEEDS`): written to Bronze + Silver
2. **Bronze-only feeds** (`BRONZE_ONLY_SILVER_DATASETS`): `news`, `institution_holdings` — Bronze only, skip Silver
3. **Uncontracted feeds**: routed to DLQ with reason `uncontracted_feed`
4. **Feed aliases** (`FEED_ALIASES`): map gateway feed names to canonical Silver names (e.g., `flow` → `flow_alerts`, `greeks` → `greek_exposure`, `daily_bars` → `bars`)

Silver layer does rename + type coerce only. Derived/computed fields (moneyness, DTE, volume_oi_ratio) belong in Gold/Feature views.

## Gold Pipeline Result Shape

Pipelines return one of two `dict` shapes from `run()`:

- Nested: `{gold_dataset_name: {"status": ..., "rows": N, "path": ...}}`
  (used by `darkpool_features`, `oi_momentum_features`, `iv_surface_features`,
  `flow_toxicity_features`, `flow_normalization_features`, `market_intel_features`)
- Flat: `{"status": ..., "rows": N, "path": ...}` (used by `sector_flow_features`,
  `trend_scan_features`, `flow_context_features`, `straddle_momentum_features`,
  `ticker_base_rates`, `excursion_analytics`, `alert_labels`)

`heber.gold_poller.service` normalizes both before aggregation. New pipelines
may pick either; the orchestrator handles both. The flat shape was historically
counted as `total_rows=0` in poller logs — that's been fixed but worth knowing
if you read older log lines.

## Instrument Key Format

Per PRD section 6.2, validated by regex patterns in `models/envelope.py`:

| Type | Format | Example |
|------|--------|---------|
| equity | `equity:{SYMBOL}` | `equity:AAPL` |
| crypto | `crypto:{BASE}-{QUOTE}` | `crypto:BTC-USD` |
| forex | `forex:{FROM}-{TO}` | `forex:USD-EUR` |
| option | `option:OCC:{OCC_SYMBOL}` | `option:OCC:AAPL260116C00200000` |

**Per-underlying analytics with `expiry` fields are equity, not option.** Feeds like
`iv_term_structure` carry one row per expiry of the *same* underlying — they're
IV term structure across expiries for one ticker, not per-OCC-contract data.
Producers must emit `instrument_type=equity, instrument_key=equity:{symbol}`;
the bare `option:{symbol}` form (no OCC suffix) is rejected at the writer.

## HeberReader (Canonical Read Interface)

`heber.reader.HeberReader` is the only supported way to read lakehouse data. All predicates are pushed into pyarrow dataset scans for row-group pruning.

`_open_dataset_safe` silently filters three classes of files before passing
to `pyarrow.dataset` — required for bind-mounted volumes on macOS:
- `._*.parquet` AppleDouble sidecar files (stat() returns EPERM on Apple
  Silicon Docker bind mounts; pyarrow's auto-walk crashes if not pre-filtered)
- Files inside `._<name>` sidecar directories (`._dt=2026-03-11/...`)
- `*.tmp` files (typically `part-*.parquet.tmp` partial writes — pyarrow
  bails with `Parquet file size is X bytes` on these)

It also folds `large_string` / `dictionary<string>` into plain `pa.string()`
during manual schema unification so fragments written by different writers
(real-time vs. compactor) merge cleanly.

```python
from heber.reader import HeberReader
reader = HeberReader()  # defaults to /Volumes/heber/data

# Silver read with point-in-time correctness
bars = reader.read_asof("bars", asof_time="2026-01-15T09:30:00Z",
                        time_range=("2026-01-01", "2026-01-15"),
                        instrument_keys=["equity:AAPL"])

# Silver read without asof
df = reader.read_silver("flow_alerts", instrument_type="equity")

# Gold read (auto-resolves latest version)
feats = reader.read_gold("momentum_features", project="kairos")

# Gold write (validates ts_available >= ts_event)
reader.write_gold("momentum_features", df, project="kairos", version="v1")

# Point-in-time join (anti-leakage enforced on right table)
result = reader.asof_join(left_df, right_df, on_keys=["instrument_key"])
```

Lightweight reader-only install: `pip install heber[reader]` (no Postgres, Redis, or Feast deps).

## Configuration

All settings via `HEBER_*` env vars (Pydantic Settings in `heber/config.py`). Key variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `HEBER_DATA_ROOT` | `/Volumes/heber/data` | Bronze/Silver/Gold root |
| `HEBER_POSTGRES_URL` | `postgresql+asyncpg://...localhost:5433/heber_catalog` | Catalog DB |
| `HEBER_REDIS_URL` | `redis://localhost:6380` | Event stream |
| `HEBER_REDIS_STREAM_NAME` | `heber:events` | Ingest stream key |
| `HEBER_REDIS_READ_BATCH_SIZE` | `500` | Messages per XREADGROUP (10-5000) |
| `HEBER_SILVER_TARGET_FILE_SIZE_MB` | `256` | Target Parquet file size |
| `HEBER_ENVIRONMENT` | `dev` | dev/staging/prod |
| `HEBER_WATCH_GATEWAY_URL` | `http://localhost:8080` | Data-Gateway for watch polling |
| `HEBER_GOLD_POLLER_EOD_HOUR` | `16` | Gold refresh hour (ET) |
| `HEBER_GOLD_POLLER_DISABLED_PIPELINES` | `""` | Comma-separated pipelines to skip |

## Test Markers

Defined in `pyproject.toml`:
- `unit` — fast, isolated, no I/O or network
- `integration` — real DB, file I/O, or component interactions
- `e2e` — full system flow
- `slow` — tests >1s, excluded by default

`asyncio_mode = "auto"` — async tests auto-detected, no `@pytest.mark.asyncio` needed.

## Logging

Uses `structlog` via `heber/ops/logging.py`. JSON output in production, human-readable in dev. Configured by `HEBER_LOG_LEVEL` (default `INFO`). Prometheus metrics via `heber/ops/metrics.py`.

## Data Integrity Rules

- Never silently accept corrupted data — skip the row and log, never store bad data
- Bronze is append-only and immutable — never modify written files
- Silver does rename + type coerce only — no computed fields, no cross-event joins
- All timestamps must be timezone-aware UTC
- Event deduplication by `event_id` (SHA256) at consumer level
- Failed messages retry with exponential backoff, then DLQ after `HEBER_REDIS_PROCESS_MAX_RETRIES` (default 3)
- Dictionary-encoded vs plain string schema conflicts are auto-resolved by `HeberReader`

## Dependencies

- `empire-core` and `empire-schemas` are editable path deps from `../empire-core` and `../empire-schemas`
- Ruff config extends `ruff-base.toml` (local copy, not monorepo root)
- Python >= 3.12, mypy strict on `reader`, `config`, `core`, `universe`, `utils`

## Commit & Changelog Discipline

- Commit often with small, atomic changes
- Every behavior change, bug fix, or feature must have a `CHANGELOG.md` entry
- Format: `## [Unreleased]` at top, grouped by `Added`, `Changed`, `Fixed`, `Removed`
- Write entries from the user's perspective — describe what changed, not implementation details

---

## Karpathy Coding Guidelines

_Source: [andrej-karpathy-skills](https://github.com/multica-ai/andrej-karpathy-skills) — behavioral guidelines to reduce common LLM coding mistakes. Bias toward caution over speed; for trivial tasks, use judgment._

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Data Analysis Review

Any data analysis presented as a conclusion — backtest results, strategy performance claims, Optuna/WFO output, dataset QA findings, or other statistical/quantitative findings — must be adversarially reviewed before being presented to the user. Use one of:

- **An Opus subagent** (`Agent` with `model: "opus"`), explicitly instructed to challenge the methodology — look for overfitting, look-ahead/leakage, cherry-picked windows, confounds, and unsupported causal claims. Not a proofread pass.
- **A Codex adversarial review** (`/codex:adversarial-review`, or the `codex` skill run in review mode) using the strongest available GPT model (currently `gpt-5.6-terra`) at high/xhigh reasoning effort.

Report the adversarial review's findings alongside the analysis itself, not as a separate follow-up step.
