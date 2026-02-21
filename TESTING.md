# Testing Guide

## Overview

Heber uses test-first development for ingestion, normalization, watch processing, and operational tooling.

## Running Tests

```bash
# Full suite
uv run pytest -q

# Focused file
uv run pytest tests/test_watch_consumer_reliability.py -q

# Focused test
uv run pytest tests/test_writer_consumer_reliability.py::test_flow_alert_schema_allows_id_without_warning -q
```

## Test Structure

- `tests/test_writer_*.py`: Bronze/Silver ingestion and contracts
- `tests/test_watch_*.py`: watch parsing, enrichment, labeling, retry behavior
- `tests/test_dataflow_*.py`: operational health/reporting paths

## Writing Tests

- Reproduce bugs with a failing test first.
- Verify both happy-path and failure-path behavior.
- Assert structured log fields when error handling is the target behavior.

## Quality Gate

```bash
uv run pytest -q
uv run ruff check .
uv run mypy .
```

## Docker Verification

When runtime or config behavior changes:

```bash
docker compose build heber-watch heber-consumer heber-compactor
docker compose up -d heber-watch heber-consumer heber-compactor
docker compose logs --since 30m heber-watch
docker compose logs --since 30m heber-consumer
docker compose logs --since 30m heber-compactor
```
