# Code Standards

Heber inherits Empire-wide conventions from the [monorepo CLAUDE.md](../../CLAUDE.md) and adds a small set of lakehouse-specific rules.

## Language & Tooling

- **Python 3.12+** (`requires-python = ">=3.12"` in `pyproject.toml`).
- **uv** for dependency management. `uv sync` to install, `uv run <cmd>` to execute.
- **Ruff** for lint + format (extends `ruff-base.toml`). `line-length = 120`, rules `E F I UP B`.
- **mypy** strict on `reader`, `config`, `core`, `backtest`, `universe`, `utils`.
- **pre-commit** (`.pre-commit-config.yaml`) runs ruff, mypy, bandit, detect-secrets.
- **bandit** + **detect-secrets** in CI.

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy .
pre-commit run --all-files
```

## File & Module Layout

- All runtime code lives under `heber/`.
- New service entry points go under `heber/<service>/` with `__main__.py` so they're runnable via `python -m heber.<service>`.
- Tests mirror the package layout under `tests/`.
- Never write working files or test data to the repo root.

## Imports

- Absolute imports rooted at `heber.` only — no relative imports across packages.
- Import `settings` from `heber.config` (or `get_settings()` if you need a fresh instance, e.g. during tests).
- Never re-implement logging setup — `from heber.ops.logging import configure_logging` and `import structlog; logger = structlog.get_logger(__name__)`.

## Logging

- `structlog` with JSON output in prod, human in dev.
- Configure once via `heber/ops/logging.py`. Do **not** call `structlog.configure()` anywhere else.
- Log keys are snake_case. Event names are verbs (`event_processed`, `silver_flush`, `dlq_emitted`).
- Use structured fields, not f-strings: `logger.info("silver_flush", feed=feed, rows=n)` ✓; `logger.info(f"flushed {n}")` ✗.
- Honor `HEBER_LOG_LEVEL` (default `INFO`).

## Error Handling

- Use specific exception types from the relevant module (`UnmappedFeedError`, `InvalidInstrumentKeyError`, `SilverNormalizationError`, `MissingRequiredFieldsError`).
- Never silently accept bad data — skip the row, log with full context, push to DLQ.
- Bronze is the durability anchor: persist the raw envelope **before** attempting Silver normalization.
- Never return `None` to signal failure — raise with details.
- Don't catch bare `Exception` unless you re-raise or DLQ; if you must, log with traceback (`logger.exception(...)`).

## Configuration

- Every setting goes through `heber/config.py` (`Settings` + `get_settings()`).
- Use `HEBER_` prefix for new env vars. Documented in [configuration guide](./configuration-guide.md).
- Don't read `os.environ` directly anywhere except `heber/config.py`.
- Add grouped `NamedTuple` accessors for related fields (see `StorageConfig`, `RedisConfig`, `WriterConfig`).

## Data Layer Rules

These rules are load-bearing for zero-leakage + reproducibility. Violations are blockers.

### Bronze

- **Append-only and immutable.** Never modify a written file.
- One JSONL.gz per `(provider, feed, dt, hour)` partition (buffered, flushed by writer).
- Every record is the raw envelope JSON-serialized. Do not pre-normalize.

### Silver

- **Rename + type coerce only.** No derived fields, no cross-event joins, no enrichment.
- Typed Parquet with Arrow schema from `heber/schemas/silver.py`. Adding a new feed = add a schema entry first.
- Path partitions: `feed=/instrument_type=/dt=[/hour=]`. Hour partition is optional and used for high-volume feeds.
- Every Silver row carries `ts_event`, `ts_ingest`, `ts_available`, `instrument_key`, `instrument_type`, `event_id`.

### Gold

- All derived fields (moneyness, DTE, volume/OI ratio, features, labels) live here.
- Every Gold write **must** set `ts_available >= ts_event`. `HeberReader.write_gold()` validates this invariant.
- Versioned by directory: `dataset=/project=/version=v{N}/dt=/`.

### Zero-Leakage

- `ts_available` is the *only* time-of-availability gate. Set it on write, never trust producer values.
- `HeberReader.read_asof(asof_time=...)` pushes `ts_available <= asof_time` into the pyarrow dataset scan via predicate pushdown — **not** a post-filter.
- `HeberReader.asof_join()` enforces `ts_available` on the right side before merge.
- Any new read path must use predicate pushdown (`ds.Expression`), not `df[df.ts_available <= t]` after `to_pandas()`.

### Instrument Keys

Validated by regex in `heber/models/envelope.py`. Producers must conform; the writer rejects non-conforming keys.

| Type | Format | Example |
|------|--------|---------|
| equity | `equity:{SYMBOL}` | `equity:AAPL` |
| crypto | `crypto:{BASE}-{QUOTE}` | `crypto:BTC-USD` |
| forex | `forex:{FROM}-{TO}` | `forex:USD-EUR` |
| option | `option:OCC:{OCC_SYMBOL}` | `option:OCC:AAPL260116C00200000` |

Per-underlying analytics with `expiry` (e.g. `iv_term_structure`) are **equity, not option** — one row per expiry of the same underlying. Bare `option:{symbol}` (no OCC suffix) is rejected.

### Feed Routing

`heber/writer/ingest_contracts.py` is the source-of-truth:

1. **Contracted** (`CONTRACTED_RAW_FEEDS`) → Bronze + Silver.
2. **Bronze-only** (`BRONZE_ONLY_SILVER_DATASETS = {"news", "institution_holdings"}`) → Bronze only.
3. **Uncontracted** → DLQ, reason `uncontracted_feed`.
4. **Contracted-but-unmapped** → DLQ, reason `unmapped_feed`.
5. **Feed aliases** (`FEED_ALIASES`) collapse gateway feed names to canonical Silver names (e.g. `flow` → `flow_alerts`, `greeks` → `greek_exposure`, `daily_bars` → `bars`).

### Timestamps

- **All timestamps timezone-aware**, UTC by default.
- Market-session times use America/New_York. Use `heber.calendar` helpers for session boundaries.
- `ts_event` = when the event happened upstream. `ts_ingest` = when Heber received it. `ts_available` = when it's safe to query (set by Heber on write).

### Dedup

- Consumer dedupes by `event_id` (SHA256 produced by Data-Gateway) before writing.
- If you produce events outside the gateway, you own the dedup-stable `event_id`.

## Database Patterns

- Catalog uses async SQLAlchemy 2.x with `asyncpg`.
- `async_sessionmaker(engine, expire_on_commit=False)`.
- `pool_pre_ping=True`, `pool_recycle=300`.
- Never `echo=True` on the async engine — synchronous logging blocks the event loop.
- Tables are auto-created in `dev` (`_should_auto_create_catalog_tables`); use Alembic in staging/prod.

## HTTP Patterns

- Use `httpx` for outbound HTTP (e.g. `heber.watch.gateway`).
- Wrap retries with `tenacity`. Exponential backoff. Don't reinvent retry loops.

## Tests

- `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed).
- Markers: `unit`, `integration`, `e2e`, `slow`.
- Reproduce bugs with a failing test before fixing.
- Assert structured log fields when error handling is the target behavior.
- Don't import `heber.config.settings` at module import time in test-imported modules — it eagerly reads env. Use `get_settings()` inside functions.

## Commits & Changelog

- Small, atomic commits.
- Every behavior change, bug fix, or feature gets an entry in `CHANGELOG.md` under `## [Unreleased]`, grouped by `Added` / `Changed` / `Fixed` / `Removed`.
- Write entries from the user's perspective (what changed), not implementation details.
- Never commit `.env`, credentials, or files containing live API keys. `.secrets.baseline` is maintained by `detect-secrets`.

## Cross-Repo Discipline

Heber's `EventEnvelope` and `NormalizedBar` / `NormalizedQuote` / `NormalizedTrade` are **shared contracts** with Data-Gateway and downstream trading systems. Before changing them:

1. Search every consumer with Sourcegraph (`empire_search`, `sg-search`) — at minimum Data-Gateway, Cerberus, Kairos, Orbit, Orion, 3Roses, Athena, EmpireUI.
2. Update producer + every consumer in the same logical change set.
3. Bump `schema_version` on the envelope and add a migration note in `docs/MIGRATION_GUIDE.md`.
