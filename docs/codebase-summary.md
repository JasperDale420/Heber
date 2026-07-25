# Codebase Summary

A package-by-package map of the `heber/` Python source tree. Sister docs: [system architecture](./ARCHITECTURE.md), [code standards](./code-standards.md), [API reference](./API_REFERENCE.md).

Repo root: `/Users/jacobmcmillan/Empire/Heber`. Source package: `heber/`. Tests: `tests/`. Operational runbooks: `docs/operations/`.

## Top-Level Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | uv project definition. Python ≥3.12. Entry points: `heber = heber.cli:main`. Optional extras: `dev`, `reader`. |
| `heber/__init__.py` | Exports `__version__`. |
| `heber/cli.py` | `heber` console script. Subcommands: `info`, `datasets`, `versions`, `backfill`, `health-dataflow`, `health-daily`. |
| `heber/config.py` | `Settings` (pydantic-settings) with `HEBER_*` env var loading; grouped `NamedTuple` accessors (StorageConfig, RedisConfig, WriterConfig, ...). `get_settings()` is cached. |
| `conftest.py` | Pytest fixtures shared across the suite. |
| `alembic/`, `alembic.ini` | Catalog DB migrations (Postgres). |
| `docker-compose.yml` | Local stack: Postgres, Redis, ClickHouse, lakeFS, MinIO, Apicurio, OpenMetadata, plus Heber services. |
| `Dockerfile` | Container image for all Heber services (different entry points per service). |
| `launchd/` | macOS launchd plists for running services natively (see [operations/native-launchd.md](./operations/native-launchd.md)). |

## Core Packages

### `heber/models/`

Pydantic data models exchanged on the wire and persisted into Silver.

- `envelope.py` — `EventEnvelope`: the canonical event format from Data-Gateway. Required fields: `event_id`, `provider`, `feed`, `source`, `instrument_type`, `instrument_key`, `symbol`, `ts_event`, `ts_ingest`, `payload`. Optional: `raw`, `ts_available`, `quality_flags`, `schema_version`. Instrument-key regex per type (equity / option / crypto / forex).
- `silver.py` — Silver-row Pydantic models per feed (where applicable).

### `heber/writer/`

The ingestion pipeline (Bronze + Silver writers, consumer, normalization contracts).

| Module | Purpose |
|--------|---------|
| `consumer.py` | `EventConsumer` — async `XREADGROUP` loop. Parses envelope → dedupes by `event_id` → writes Bronze → routes to Silver per contract → DLQ on failure. |
| `bronze.py` | `BronzeWriter` — buffered append into `provider/feed/dt/hour/` JSONL.gz. Flushes on time interval or batch size. |
| `silver.py` | `SilverWriter` — typed Parquet with Arrow schema enforcement (`heber/schemas/silver.py`). Flushes on row count / file size / time. |
| `ingest_contracts.py` | The contract source-of-truth: `CONTRACTED_RAW_FEEDS`, `BRONZE_ONLY_SILVER_DATASETS`, `FEED_ALIASES`, `FIELD_MAPPINGS`, `PAYLOAD_REQUIRED_FIELDS`, `PAYLOAD_ALLOWED_FIELDS`, `resolve_silver_feed`, `is_bronze_only_feed`, `is_contracted_feed`. |
| `normalizer.py` | `envelope_to_silver_row()` — converts a validated envelope into a Silver-shaped dict; enforces required-non-null fields. |
| `key_normalization.py` | Strict deterministic instrument-key synthesis & validation for Silver (`normalize_envelope_for_silver`). |
| `transformer.py` | `BronzeToSilverTransformer` — replay Bronze to (re)build Silver. Used by `heber backfill` CLI. |
| `compactor.py` | Periodic Parquet compaction service entry. |
| `dlq_fallback.py` | When Redis DLQ XADD fails, write JSON fallback files for later replay. |
| `dlq_reprocessor.py` | Pull from DLQ and retry through the pipeline. |
| `utils.py` | `build_silver_candidates`, `get_partition_key`. |

### `heber/reader/`

The canonical read interface.

- `core.py` — `HeberReader`. Methods: `read_silver`, `read_asof`, `read_gold`, `write_gold`, `list_gold_versions`, `asof_join`. Predicate pushdown via `pyarrow.dataset` (no post-filter). `_open_dataset_safe` filters AppleDouble (`._*`), tmp files (`*.tmp`), and folds `large_string` / `dictionary<string>` into plain `string` so writer + compactor fragments unify cleanly.

### `heber/catalog/`

Catalog REST service + storage layer.

| Module | Purpose |
|--------|---------|
| `api.py` | FastAPI app, lifespan setup (engine, periodic discovery loop), all routes. Auto-creates tables in `dev`, expects Alembic elsewhere. |
| `service.py` | `CatalogService` — async business logic over the catalog (datasets, instruments, coverage, feed mappings). |
| `db.py` | SQLAlchemy ORM models (`datasets`, `dataset_versions`, `instrument_registry`, `instrument_provider_map`, `feed_mappings`, `data_coverage`). |
| `seeds.py` | `discover_datasets_from_disk`, `seed_coverage_from_disk`, `seed_datasets`, `seed_feed_mappings`, `seed_schema_versions`. |
| `access_control.py` | RBAC checks (per-dataset). |
| `datasources.py` | Datasource registry (provider connection metadata). |
| `urn.py` | Canonical URN helpers for dataset / instrument identifiers. |
| `openmetadata_client.py` | Optional sync into OpenMetadata. |

### `heber/gold/`

Gold-layer ML primitives (label generation).

- `labels.py` — `TripleBarrierLabeler` + helpers. Generates ML labels with `ts_available` set so reads stay leakage-safe.
- `duration.py` — first-touch / barrier-hit timing helpers.
- `splits.py` — purged + embargoed CV splits.

### `heber/gold_poller/`

Scheduled Gold feature pipeline orchestrator.

- `service.py` — main loop. Fires at `HEBER_GOLD_POLLER_EOD_HOUR:HEBER_GOLD_POLLER_EOD_MINUTE` (ET). Normalizes both nested and flat pipeline result shapes (see [CLAUDE.md](../CLAUDE.md#gold-pipeline-result-shape)).

### `heber/watch/`

Real-time flow-alert outcome tracker. Feeds ML meta-labeling.

| Module | Purpose |
|--------|---------|
| `__main__.py` | `python -m heber.watch` entry. |
| `consumer.py` | Redis Streams consumer for `flow_alerts`. |
| `manager.py` | Watch lifecycle. |
| `poller.py` | `SnapshotPoller` — polls Data-Gateway for option quotes. |
| `gateway.py` | HTTP client for Data-Gateway. |
| `checker.py` | TP/SL barrier evaluation. |
| `features.py` | Feature capture at alert time (snapshot for ML). |
| `writer.py` | Persist watch + outcome to Gold. |
| `backfill_scanner.py` | Backfill historical alerts. |
| `models.py` | Watch / outcome / snapshot Pydantic models. |

### `heber/ml/`

Meta-labeling infrastructure (predict alert success).

- `MetaLabelDatasetBuilder` — join features + outcomes from Gold.
- `MetaModelTrainer` — LightGBM training, MLflow logging.
- `MetaLabelScorer` — async scoring of new alerts.
- `AlertGate` — optional consumer-side filter.

### `heber/ops/`

Operational concerns: logging, metrics, reliability, dataflow + daily health, retry policy.

- `logging.py` — `configure_logging()` (structlog). JSON in prod, human in dev. Configured by `HEBER_LOG_LEVEL`.
- `metrics.py` — Prometheus counters/histograms (event received/processed, batch latency, DLQ, dedupe drop). `start_metrics_server_from_env()`.
- `reliability.py` — `EventDeduplicator`.
- `runtime_retry.py` — retry classification + backoff.
- `dataflow_health.py` — Gateway → Ingest → Storage proof-of-flow (used by `heber health-dataflow`).
- `daily_health.py` — EOD 7-check report (used by `heber health-daily`).

### `heber/schemas/`

Arrow / Iceberg schemas per Silver feed (source-of-truth for Parquet typing).

- `silver.py` — `SILVER_SCHEMAS: dict[str, pa.Schema]`. Add a feed here to make Silver writes typed-flat.

### `heber/quality/`

Soda Core data-quality checks. Wired into `heber health-daily`.

### `heber/calendar/`

Trading calendar integration (`exchange-calendars`). Used by health checks and Gold pipelines for session-aware logic.

### `heber/universe/`

Instrument universe management — point-in-time membership snapshots to avoid survivor bias in backtests.

### `heber/backtest/`

Reproducible experiment scaffolding. `ExperimentConfig` (captures git SHA + params), `ExperimentTracker`, `BacktestDataLoader` (always reads through `HeberReader` so leakage rules apply).

### `heber/features/` & `heber/feast/`

Feature views consumed by the Gold layer; Feast feature-store integration (off by default in local dev).

### `heber/storage/`

Iceberg adapters (`iceberg_catalog.py`, `iceberg_writer.py`) — present but not wired into `HeberReader`. Migration is staged.

### `heber/schema_registry/`

Confluent-compatible schema-registry client. Optional; used when Apicurio is enabled.

### `heber/versioning/`

lakeFS integration scaffolding for Gold. Currently filesystem-discovery is the live mechanism.

### `heber/bus/`

Lower-level Redis Streams event-bus helpers. (Consumer uses `redis.asyncio` directly for `XREADGROUP` control — see header of `writer/consumer.py`.)

### `heber/backfill/`

Historical re-ingest service (Bronze → Silver replay + provider replay where applicable).

### `heber/health_monitor/`

Continuous health monitoring (stream/partition drift detectors, leakage spot-checks).

### `heber/retention/`

Data retention policy enforcement (TTL-based pruning).

### `heber/sre/`

Site-reliability scripts (cluster-level operations, oncall helpers).

### `heber/testing/`

Test-only helpers (fixtures, fakes) shared across `tests/`.

### `heber/utils/`

Misc utilities.

### `heber/cli.py`

Argparse-based CLI. See [API reference / CLI](./API_REFERENCE.md#cli) for subcommand details.

## Cross-Cutting Files

| File | Role |
|------|------|
| `heber/config.py` | Single source-of-truth for runtime configuration. Always import via `from heber.config import settings` or `get_settings()`. |
| `heber/ops/logging.py` | Sole entry point for logging setup. Don't call `structlog.configure()` elsewhere. |
| `heber/writer/ingest_contracts.py` | Sole source-of-truth for "what feeds become Silver and how". |
| `heber/schemas/silver.py` | Sole source-of-truth for Silver Parquet schemas. |
| `heber/reader/core.py` | Sole supported read interface for lake data. |

## Tests

- `tests/test_writer_*.py` — Bronze/Silver ingestion + contracts.
- `tests/test_watch_*.py` — watch parsing, enrichment, labeling, retries.
- `tests/test_dataflow_*.py` — operational health/reporting paths.
- `tests/test_reader_*.py` — `HeberReader` semantics + zero-leakage.
- `tests/test_catalog_*.py` — Catalog API + service.

See [testing guide](./testing-guide.md) for markers and commands.
