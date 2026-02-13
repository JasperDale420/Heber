# AGENTS.md

Project-specific AI agent instructions for Heber.

## Project Overview

Heber is the Empire lakehouse service. It ingests `EventEnvelope` events from Redis, writes Bronze and Silver lake layers, builds Gold features/labels, and serves data through the Catalog API and SDK.

## Architecture

Core ingestion flow:

`Data Gateway -> Redis Stream (heber:events) -> heber-consumer -> Bronze + Silver -> Catalog metadata`

Watch flow for labels:

`flow_alerts -> heber-watch -> feature enrichment + polling -> Gold label writes`

Important directories:

- `heber/writer/` Bronze/Silver ingestion, normalization, compaction
- `heber/watch/` watch consumer, poller, feature extraction, label writing
- `heber/catalog/` catalog API and metadata persistence
- `heber/ops/` metrics and operational helpers
- `tests/` regression and behavior coverage

## Development Commands

```bash
# Install dependencies
uv pip install -e ".[dev]"

# Tests
uv run pytest -q

# Lint + types
uv run ruff check .
uv run mypy .

# Local stack
docker compose up -d

# Rebuild impacted services after runtime/config changes
docker compose build heber-watch heber-consumer heber-compactor
docker compose up -d heber-watch heber-consumer heber-compactor
```

## Key Patterns

- Follow vertical slice architecture; keep feature logic close to its boundary.
- Use TDD by default: write failing tests first, then implementation, then refactor.
- Use structured logging (`structlog`) with concrete context fields.
- Prefer fail-fast for startup/config/auth/data-corruption issues.
- For batch/item pipelines, continue processing valid items and log item-level failures.
- Keep Bronze as raw source of truth; Silver is strict normalized contract.

## Important Files

- `/Users/jacobmcmillan/Empire/Heber/heber/config.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/writer/consumer.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/watch/consumer.py`
- `/Users/jacobmcmillan/Empire/Heber/heber/writer/compactor.py`
- `/Users/jacobmcmillan/Empire/Heber/docs/ARCHITECTURE.md`
- `/Users/jacobmcmillan/Empire/Heber/PRD.md`

## Testing

Use this quality gate before finalizing:

```bash
uv run pytest -q
uv run ruff check .
uv run mypy .
```

For log/RCA fixes, add regression tests that fail before the fix and stay in the suite after the fix.

## Workflow Rules

- Commit often after meaningful additions.
- Update `CHANGELOG.md` in the same commit.
- If Docker runtime behavior changed, rebuild impacted containers before signoff.
- Do not paper over errors; resolve root causes and verify with logs/tests.

## Common Pitfalls

- Path mismatches between host and container (`/Volumes/...` vs `/data/...`).
- Feed payload drift causing noisy schema warnings.
- Silent fallback behavior that hides upstream API failures.
- Mixed historical Parquet schemas during compaction.
