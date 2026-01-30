# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Heber is a data lakehouse for market and intelligence data. It ingests events from a Data Gateway via Redis Streams, writes them to Bronze (raw JSONL.gz) and Silver (normalized Parquet) storage layers, and provides a catalog API + Python SDK for zero-leakage data access. A Watch service tracks flow alert outcomes for ML labeling in the Gold layer.

## Common Commands

```bash
# Install dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v

# Run a single test file
uv run pytest tests/test_foo.py -v

# Run tests with coverage
pytest tests/ -v --cov=heber --cov-report=term-missing

# Lint and format
ruff check heber/ --fix
ruff format heber/

# Run all pre-commit hooks
pre-commit run --all-files

# Start local dev stack
docker compose up -d

# Run services directly
python -m heber.catalog.api       # Catalog API (FastAPI, port 8085)
python -m heber.writer.consumer   # Event consumer
python -m heber.watch             # Watch service (--redis, --gateway flags)

# CLI
heber info --verbose
heber datasets --layer silver
```

## Architecture

**Data flow:** Data Gateway → Redis Streams (`heber:events`) → `heber-consumer` → Bronze + Silver → Catalog DB (Postgres)

**Storage layers:**
- **Bronze** (`/Volumes/heber/data/bronze/`): Raw provider payloads, JSONL.gz, partitioned by `provider/feed/dt/hour`
- **Silver** (`/Volumes/heber/data/silver/`): Normalized Parquet, partitioned by `feed/instrument_type/dt[/hour]`
- **Gold** (`/Volumes/heber/data/gold/`): Features/labels Parquet, partitioned by `dataset/project/version/dt`

**Core services:**
- `heber/catalog/` — FastAPI catalog API backed by Postgres (datasets, instruments, feed mappings)
- `heber/writer/` — Redis Streams consumer that validates `EventEnvelope`, stamps `ts_available`, writes Bronze + Silver
- `heber/watch/` — Tracks flow alert outcomes: consumer → manager (Redis) → poller (quotes) → checker (TP/SL barriers) → writer (Gold)
- `heber/sdk/` — `HeberClient` for reading data with point-in-time (`asof`) guarantees
- `heber/ml/` — Meta-labeling pipeline: dataset builder → LightGBM trainer → inference scorer

**Key concepts:**
- `EventEnvelope` (`heber/models/`) is the canonical event wrapper used throughout ingestion
- `ts_available` timestamp enforces zero-leakage for backtesting/ML — data is only visible after this time
- Config via Pydantic Settings (`heber/config.py`), env prefix `HEBER_`

## Code Style

- Python 3.11, line length 120 (ruff)
- Structured logging via structlog
- Type hints throughout (mypy strict mode configured but currently disabled due to existing errors)
- Security scanning: detect-secrets, bandit, trivy
- Data volume root configurable via `HEBER_VOLUME_ROOT` (default `/Volumes/heber`)
