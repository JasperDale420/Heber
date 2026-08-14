# Testing Guide

How tests are organized, how to run them, and the quality gate that must pass before commit.

Sister docs: [code standards](./code-standards.md), [codebase summary](./codebase-summary.md).

## Quick Reference

```bash
# Full suite
uv run pytest

# Fast suite only
uv run pytest -m unit

# Integration only
uv run pytest -m integration

# E2E
uv run pytest -m e2e

# Single file
uv run pytest tests/test_writer_consumer_reliability.py

# Single test
uv run pytest -k "test_flow_alert_schema_allows_id_without_warning"

# With coverage
uv run pytest --cov=heber --cov-report=term-missing --cov-report=xml
```

## Markers

Defined in `pyproject.toml`. `asyncio_mode = "auto"` — you do **not** need `@pytest.mark.asyncio` on async tests.

| Marker | Meaning | When to use |
|--------|---------|-------------|
| `unit` | Fast, isolated, no I/O or network | Default for pure functions, normalization, contract logic |
| `integration` | Real DB / file I/O / component interactions | Catalog Postgres, Bronze/Silver writes to tmp dirs |
| `e2e` | Full system flow | Consumer → Bronze → Silver → reader round-trip |
| `slow` | Tests >1s; runs by default, deselect with `-m "not slow"` | Backfills, large Parquet round-trips |

## Layout

```
tests/
├── conftest.py                      # shared fixtures
├── test_writer_*.py                 # Bronze/Silver ingestion + contracts
├── test_watch_*.py                  # watch parsing, enrichment, labeling, retries
├── test_dataflow_*.py               # health/reporting paths
├── test_reader_*.py                 # HeberReader + zero-leakage
├── test_catalog_*.py                # Catalog API + service
├── test_gold_*.py                   # Gold pipelines + label generation
└── test_ops_*.py                    # logging, metrics, retry, dedupe
```

`tests/` mirrors the `heber/` package layout. Cross-cutting fixtures live in `conftest.py` at the repo root (`/Users/jacobmcmillan/Empire/Heber/conftest.py`).

## Writing Tests

Heber follows test-first development. Required practices:

1. **Reproduce bugs with a failing test first.** No fix lands without a regression test.
2. **Cover happy path and failure path.** Especially for the ingest pipeline — every DLQ branch (`uncontracted_feed`, `unmapped_feed`, `invalid_instrument_key`, `missing_required_field`, `validation_error`) must have a test.
3. **Assert structured log fields** when error handling is the target behavior. Use `caplog` plus a structlog-aware assert (`record.event` for the event name, `record.<field>` for keys).
4. **Use tmp paths**, never write to `/Volumes/heber`. Bronze/Silver writers accept a `data_root` override.
5. **Don't import `heber.config.settings` at module import time** in test-imported modules — it eagerly reads env vars. Call `get_settings()` inside functions/methods so tests can monkeypatch.

### Async tests

```python
async def test_consumer_dedupes_event_id(consumer_fixture):
    await consumer_fixture.process(envelope)
    # no @pytest.mark.asyncio needed (asyncio_mode = "auto")
```

### Catalog DB tests

The Catalog DB uses async SQLAlchemy. Use the provided session fixture from `conftest.py`. Auto-table-creation only fires when `HEBER_ENVIRONMENT=dev`; tests should set this via monkeypatch or rely on the fixture.

### HeberReader tests

Reader tests must verify **predicate pushdown**, not post-filter behavior. The canonical pattern: write rows with `ts_available` spanning a range, call `read_asof(asof_time=T)`, assert returned rows all satisfy `ts_available <= T`, and ideally inspect the pyarrow `Expression` was passed through (see existing `tests/test_reader_*.py` patterns).

## Quality Gate

The pre-commit and CI pipelines run:

```bash
uv run pytest -q                                # all tests
uv run ruff check .                             # lint
uv run ruff format --check .                    # format check
uv run mypy .                                   # type check (strict subset)
uv run bandit -c pyproject.toml -r heber/       # security scan
uv run detect-secrets scan --baseline .secrets.baseline
pre-commit run --all-files                      # everything wired
```

Coverage report (`coverage.xml`) is written at the repo root for SonarQube ingestion.

## Docker Verification

When runtime, config, or compose-level behavior changes, rebuild and tail:

```bash
docker compose build heber-watch heber-consumer heber-compactor heber-catalog
docker compose up -d heber-watch heber-consumer heber-compactor heber-catalog
docker compose logs --since 30m heber-consumer
docker compose logs --since 30m heber-watch
docker compose logs --since 30m heber-compactor
docker compose logs --since 30m heber-catalog
```

Confirm `curl -s http://localhost:8085/health` returns `{"status":"healthy","service":"heber-catalog"}` and that consumer `/metrics` (`http://localhost:9090/metrics`) shows non-zero event counters.

## CI

GitHub Actions (`.github/workflows/ci.yaml`):

1. **Build** — Docker image creation.
2. **Test** — `ruff`, `mypy`, `pytest` with coverage. Coverage XML is produced at repo root for SonarQube.
3. **Scan** — Trivy filesystem scan; fails on `HIGH`/`CRITICAL`.
4. **Push** — Container registry push (main only).
5. **Deploy** — Staging → production (main only).

**Dependabot** auto-creates weekly dependency-update PRs.

## Test Data Discipline

- Never check in real provider data. Use synthetic fixtures.
- Bronze/Silver test fixtures live in `tests/fixtures/` (gitignored beyond a small canonical set).
- Synthetic `EventEnvelope` builders should set `ts_available` explicitly so leakage tests are deterministic.
