# Configuration Guide

All Heber runtime configuration goes through `heber/config.py` (pydantic-settings, `HEBER_` prefix, loads `.env`). Never read `os.environ` directly elsewhere in the codebase.

This doc lists every `HEBER_*` variable in use today and the host vs. container URL conventions. The source-of-truth defaults live in `heber/config.py` (~860 lines) and `.env.example`.

Sister docs: [code standards](./code-standards.md#configuration), [deployment guide](./deployment-guide.md).

## Quick Start

```bash
cp .env.example .env
# Optional: edit .env for API keys, paths, ports
./scripts/init_volume.sh        # one-time: prepare /Volumes/heber/data tree
docker compose up -d            # bring up the stack
```

## Core Runtime

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_DATA_ROOT` | `/Volumes/heber/data` | Root for `bronze/`, `silver/`, `gold/` |
| `HEBER_VOLUME_ROOT` | `/Volumes/heber` | External-volume root used by `scripts/init_volume.sh` and Docker bind mounts |
| `HEBER_ENVIRONMENT` | `dev` | `dev` / `staging` / `prod`. `dev` auto-creates Catalog tables; others require Alembic |
| `HEBER_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` (case-insensitive) |
| `HEBER_METRICS_PORT` | `9090` | Prometheus exporter port for service entry points |

## Postgres (Catalog)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_POSTGRES_URL` | `postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog` | Catalog DB connection (async) |

## Redis (Event Bus)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_REDIS_URL` | `redis://localhost:6379` | Redis endpoint |
| `HEBER_REDIS_STREAM_NAME` | `heber:events` | Ingest stream |
| `HEBER_REDIS_CONSUMER_GROUP` | `heber-writers` | Consumer group |
| `HEBER_REDIS_DLQ_STREAM_NAME` | `heber:events:dlq` | Dead-letter stream for failed messages |
| `HEBER_REDIS_CLAIM_IDLE_MS` | `60000` | Idle threshold before claiming pending messages |
| `HEBER_REDIS_CLAIM_BATCH_SIZE` | `100` | Max pending messages claimed per recovery cycle |
| `HEBER_REDIS_PROCESS_MAX_RETRIES` | `3` | Processing retries before DLQ |
| `HEBER_REDIS_RETRY_BACKOFF_SECONDS` | `0.25` | Base retry backoff |
| `HEBER_REDIS_READ_BATCH_SIZE` | `500` | Messages per XREADGROUP (range 10–5000) |
| `HEBER_REDIS_READ_BLOCK_MS` | (see `heber/config.py`) | Block ms on XREADGROUP |
| `HEBER_REDIS_PROCESS_CONCURRENCY` | (see `heber/config.py`) | Parallel envelope processing |

## Writer Tuning (Bronze + Silver)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_BRONZE_FLUSH_INTERVAL_SECONDS` | `30` | Max time before Bronze buffer flush |
| `HEBER_BRONZE_MAX_BATCH_SIZE` | `10000` | Max events per Bronze file before flush |
| `HEBER_SILVER_TARGET_FILE_SIZE_MB` | `256` | Target Parquet file size |
| `HEBER_SILVER_MAX_ROWS_PER_FILE` | `1000000` | Max rows per Silver file |
| `HEBER_SILVER_MAX_FLUSH_TIME_SECONDS` | `30` | Max time before Silver buffer flush |
| `HEBER_SILVER_ROW_GROUP_SIZE_MB` | `128` | Parquet row group size |

Bronze is buffered then atomically renamed; Silver writes are typed against `heber/schemas/silver.py`.

## Catalog API

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_API_HOST` | `0.0.0.0` | API bind host |
| `HEBER_API_PORT` | `8080` | Internal API port (mapped to host `8085` by Docker Compose) |
| `HEBER_CATALOG_URL` | `http://localhost:8085/api/v1` | Base URL used by SDK/CLI callers on the host |
| `HEBER_CATALOG_DISCOVER_INTERVAL_SECONDS` | (see `heber/config.py`) | Background re-scan interval for Silver coverage |

## ClickHouse (Hot Store)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_CLICKHOUSE_HOST` | `localhost` | Hostname |
| `HEBER_CLICKHOUSE_PORT` | `9000` | Native port (mapped to host `9002` by Docker Compose) |
| `HEBER_CLICKHOUSE_USER` | `default` | User |
| `HEBER_CLICKHOUSE_PASSWORD` | *(empty)* | Password |
| `HEBER_CLICKHOUSE_DATABASE` | `heber` | Database |

ClickHouse helpers are available; the sync writer is not deployed by default in `docker-compose.yml`.

## Watch Service

Tracks flow-alert outcomes via option-quote polling. Writes Gold labels for ML meta-labeling.

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_WATCH_GATEWAY_URL` | `http://localhost:8080` | Data-Gateway endpoint for option-quote polling |
| `HEBER_WATCH_GATEWAY_API_KEY` | *(unset)* | Optional API key |
| `HEBER_WATCH_GATEWAY_LEGACY_FALLBACK_ENABLED` | (see `heber/config.py`) | Allow fallback to legacy endpoints |
| `HEBER_WATCH_ENRICHMENT_TIMEOUT_SECONDS` | (see `heber/config.py`) | Per-call timeout for enrichment |
| `HEBER_WATCH_ENRICHMENT_OPTION_CHAIN_TIMEOUT_SECONDS` | (see `heber/config.py`) | Per-call timeout for option chain |
| `HEBER_WATCH_ENRICHMENT_BACKFILL_ENABLED` | (see `heber/config.py`) | Run enrichment backfill loop |
| `HEBER_WATCH_ENRICHMENT_BACKFILL_INTERVAL` | (see `heber/config.py`) | Backfill loop cadence (seconds) |
| `HEBER_WATCH_ENRICHMENT_BACKFILL_LOOKBACK_DAYS` | (see `heber/config.py`) | Days back to backfill |
| `HEBER_WATCH_ENRICHMENT_BACKFILL_BATCH_SIZE` | (see `heber/config.py`) | Watches per backfill batch |
| `DATA_GATEWAY_URL` | `http://localhost:8000` | Legacy alias still honored by some watch entry points |
| `HEBER_GOLD_PATH` | *(derived from `HEBER_DATA_ROOT`)* | Gold output for watch labels |

## Gold Poller (Scheduled EOD Features)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_GOLD_POLLER_ENABLED` | (see `heber/config.py`) | Master enable flag |
| `HEBER_GOLD_POLLER_EOD_HOUR` | `16` | Run hour (ET) |
| `HEBER_GOLD_POLLER_EOD_MINUTE` | (see `heber/config.py`) | Run minute (ET; default `35`) |
| `HEBER_GOLD_POLLER_CHECK_INTERVAL_SECONDS` | (see `heber/config.py`) | Wake-up interval |
| `HEBER_GOLD_POLLER_RETRY_MAX` | (see `heber/config.py`) | Per-pipeline retries |
| `HEBER_GOLD_POLLER_RETRY_BACKOFF_SECONDS` | (see `heber/config.py`) | Retry backoff base |
| `HEBER_GOLD_POLLER_PROJECT` | (see `heber/config.py`) | Default Gold `project` partition |
| `HEBER_GOLD_POLLER_VERSION` | (see `heber/config.py`) | Default Gold `version` partition |
| `HEBER_GOLD_POLLER_LOOKBACK_DAYS` | (see `heber/config.py`) | Days of Silver to scan |
| `HEBER_GOLD_POLLER_DISABLED_PIPELINES` | `""` | Comma-separated pipeline names to skip |

## Health Monitor (Continuous)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_HEALTH_MONITOR_ENABLED` | (see `heber/config.py`) | Enable continuous monitor |
| `HEBER_HEALTH_MONITOR_STREAM_CHECK_INTERVAL_SECONDS` | (see `heber/config.py`) | Stream lag check cadence |
| `HEBER_HEALTH_MONITOR_PARTITION_CHECK_INTERVAL_SECONDS` | (see `heber/config.py`) | Partition freshness cadence |
| `HEBER_HEALTH_MONITOR_VOLUME_BASELINE_DAYS` | (see `heber/config.py`) | Days used for baseline |
| `HEBER_HEALTH_MONITOR_STATS_BASELINE_DAYS` | (see `heber/config.py`) | Days for stats baseline (null rate, PSI) |
| `HEBER_HEALTH_MONITOR_VOLUME_WARN_RATIO` | (see `heber/config.py`) | Warn at this ratio drop |
| `HEBER_HEALTH_MONITOR_VOLUME_CRITICAL_RATIO` | (see `heber/config.py`) | Critical at this ratio drop |
| `HEBER_HEALTH_MONITOR_NULL_RATE_THRESHOLD` | (see `heber/config.py`) | Null-rate warn threshold |
| `HEBER_HEALTH_MONITOR_PSI_THRESHOLD` | (see `heber/config.py`) | PSI drift threshold |
| `HEBER_HEALTH_MONITOR_LEAKAGE_SAMPLE_SIZE` | (see `heber/config.py`) | Rows sampled per leakage check |

## Dataflow Health (`heber health-dataflow`)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_HEALTH_CONSUMER_METRICS_URL` | `http://localhost:9090/metrics` | Consumer Prometheus endpoint |
| `HEBER_HEALTH_WATCH_METRICS_URL` | `http://localhost:9091/metrics` | Watch Prometheus endpoint |
| `HEBER_HEALTH_FRESHNESS_SECONDS` | `900` | Feed freshness window |
| `HEBER_HEALTH_REPORT_DIR` | `/data/ops/dataflow-health` | JSON report output |
| `HEBER_HEALTH_INTERVAL_SECONDS` | `300` | Scheduled run cadence |

## Daily Health (`heber health-daily`)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_DAILY_HEALTH_REPORT_DIR` | `/Volumes/heber/data/ops/daily-health` | JSON report output |
| `HEBER_DAILY_HEALTH_EXPECTED_SYMBOL_COUNT` | `500` | Minimum distinct symbols in `bars` |
| `HEBER_DAILY_HEALTH_EXPECTED_FEEDS` | `["bars","quotes","trades","flow_alerts"]` | Feeds expected to have partitions on each trading day |

## LLM Provider (Optional)

Used by analysis / labeling helpers that call an OpenAI-compatible API.

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_LLM_PROVIDER` | `openai` | `openai` or `qwen` |
| `HEBER_LLM_MODEL` | `gpt-4o-mini` | Default model |
| `HEBER_LLM_BASE_URL` | *(unset)* | Explicit endpoint override |
| `HEBER_LLM_QWEN_REGION` | `intl` | `intl` / `us` / `cn` (only when provider=qwen) |
| `HEBER_LLM_API_KEY` | *(unset)* | Generic LLM API key override |
| `OPENAI_API_KEY` | *(unset)* | OpenAI alias (used when provider=openai) |
| `DASHSCOPE_API_KEY` | *(unset)* | Qwen alias (used when provider=qwen) |

## Feast (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `HEBER_FEAST_REPO_PATH` | `features` | Repo root for Feast materialization/search helpers |

## OSS Migration (Optional)

Read by modules in `heber/storage/`, `heber/versioning/`, and `heber/schema_registry/`. Not on the default hot path.

- `ICEBERG_*` — Iceberg catalog + warehouse config
- `LAKEFS_*` — Gold versioning
  - `LAKEFS_STORAGE_NAMESPACE_BASE` (default `s3://heber-lakehouse`)
  - `LAKEFS_STORAGE_NAMESPACE_TEMPLATE` (optional, supports `{repo}` placeholder)
- `SCHEMA_REGISTRY_*` — schema registry
- `MINIO_*` — S3-compatible storage

See `.env.example` for the canonical list.

## Host vs Container URLs

`docker-compose.yml` exposes different host ports than internal container ports.

**From the host** (running CLI, tests, SDK):

| Service | URL |
|---------|-----|
| Postgres | `postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog` |
| Redis | `redis://localhost:6379` |
| Catalog API | `http://localhost:8085` |
| ClickHouse HTTP | `localhost:8124` |
| ClickHouse native | `localhost:9002` |
| lakeFS | `http://localhost:8000` |
| MinIO S3 | `http://localhost:19000` |
| MinIO console | `http://localhost:19001` |
| Apicurio Registry | `http://localhost:18081` |
| OpenMetadata | `http://localhost:8585` |
| Consumer metrics | `http://localhost:9090/metrics` |
| Watch metrics | `http://localhost:9091/metrics` |

**From inside containers**, use the service names: `postgres`, `redis`, `clickhouse`, `heber-catalog`, etc., with their internal ports.

> The consumer reads Data-Gateway's Redis (`host.docker.internal:6379` in dev) by default, not Heber's own Redis. The stream is always `heber:events` with consumer group `heber-writers`.

## One-Time Volume Initialization

```bash
./scripts/init_volume.sh
```

Uses `HEBER_VOLUME_ROOT` and creates: `data/bronze`, `data/silver`, `data/gold`, `postgres/data`, `clickhouse/data`, `clickhouse/logs`, `redis/data`.

On macOS hosts the script also runs `dot_clean` to clear AppleDouble (`._*`) sidecar files after permissions are set. On non-macOS the step is skipped explicitly.

`.env.example` ships with `HEBER_VOLUME_ROOT=/Volumes/HeberDocker` to avoid clashing with an existing `/Volumes/heber` mount — adjust as needed.

## API Keys

Add API keys in `/Users/jacobmcmillan/Empire/Heber/.env` (local) or via environment variables in your deploy system. Never commit secrets — `detect-secrets` enforces this in `pre-commit` and CI via `.secrets.baseline`.
