# Configuration

Heber uses `pydantic-settings` with the `HEBER_` prefix and loads `.env` by default. See `heber/config.py` for authoritative defaults.

Note: `.env.example` sets `HEBER_VOLUME_ROOT=/Volumes/HeberDocker` to avoid clashing with existing mounts; adjust as needed.

## Core Runtime Settings

| Variable | Default | Description |
|---|---|---|
| `HEBER_DATA_ROOT` | `/Volumes/heber/data` | Root path for Bronze/Silver/Gold data |
| `HEBER_VOLUME_ROOT` | `/Volumes/heber` | External volume root used by scripts/docker |
| `HEBER_POSTGRES_URL` | `postgresql+asyncpg://heber:heber_dev_password@localhost:5432/heber_catalog` | Catalog DB connection string |
| `HEBER_REDIS_URL` | `redis://localhost:6379` | Redis Streams endpoint |
| `HEBER_REDIS_STREAM_NAME` | `heber:events` | Redis stream name |
| `HEBER_REDIS_CONSUMER_GROUP` | `heber-writers` | Redis consumer group |
| `HEBER_REDIS_DLQ_STREAM_NAME` | `heber:events:dlq` | Dead-letter stream for failed consumer messages |
| `HEBER_REDIS_CLAIM_IDLE_MS` | `60000` | Idle threshold before claiming pending messages |
| `HEBER_REDIS_CLAIM_BATCH_SIZE` | `100` | Max pending messages claimed per recovery cycle |
| `HEBER_REDIS_PROCESS_MAX_RETRIES` | `3` | Processing retries before DLQ |
| `HEBER_REDIS_RETRY_BACKOFF_SECONDS` | `0.25` | Base retry backoff delay |
| `HEBER_CLICKHOUSE_HOST` | `localhost` | ClickHouse hostname |
| `HEBER_CLICKHOUSE_PORT` | `9000` | ClickHouse native port |
| `HEBER_CLICKHOUSE_USER` | `default` | ClickHouse user |
| `HEBER_CLICKHOUSE_PASSWORD` | *(empty)* | ClickHouse password |
| `HEBER_CLICKHOUSE_DATABASE` | `heber` | ClickHouse database |
| `HEBER_API_HOST` | `0.0.0.0` | Catalog API bind host |
| `HEBER_API_PORT` | `8080` | Catalog API port |
| `HEBER_CATALOG_URL` | `http://localhost:8085/api/v1` | SDK Catalog API base URL |
| `HEBER_ENVIRONMENT` | `dev` | `dev`, `staging`, or `prod` |

## Writer Tuning

| Variable | Default | Description |
|---|---|---|
| `HEBER_BRONZE_FLUSH_INTERVAL_SECONDS` | `30` | Max time before Bronze flush |
| `HEBER_BRONZE_MAX_BATCH_SIZE` | `10000` | Max events per Bronze file |
| `HEBER_SILVER_TARGET_FILE_SIZE_MB` | `256` | Target Parquet file size |
| `HEBER_SILVER_MAX_ROWS_PER_FILE` | `1000000` | Max rows per Silver file |
| `HEBER_SILVER_MAX_FLUSH_TIME_SECONDS` | `30` | Max time before Silver flush |
| `HEBER_SILVER_ROW_GROUP_SIZE_MB` | `128` | Parquet row group size |

## Local vs Container URLs

`docker-compose.yml` exposes different host ports. If you run SDK/CLI on the host, use:

- Postgres: `postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog`
- Redis: `redis://localhost:6380`
- ClickHouse: host `localhost`, port `9002`

Inside containers, use the service names (`postgres`, `redis`, `clickhouse`) and their internal ports.

## Docker Compose Ports (Host)

From `docker-compose.yml`:

- Postgres: `localhost:5433` (internal 5432)
- Redis: `localhost:6380` (internal 6379)
- ClickHouse HTTP: `localhost:8124` (internal 8123)
- ClickHouse native: `localhost:9002` (internal 9000)
- Catalog API: `http://localhost:8085` (internal 8080)
- lakeFS: `http://localhost:8000`
- MinIO: `http://localhost:19000` (S3), `http://localhost:19001` (console)
- Apicurio Registry: `http://localhost:18081`
- OpenMetadata: `http://localhost:8585`

## One-Time Volume Init

`scripts/init_volume.sh` uses `HEBER_VOLUME_ROOT` and creates:

- `data/bronze`, `data/silver`, `data/gold`
- `postgres/data`, `clickhouse/data`, `clickhouse/logs`, `redis/data`

## OSS Migration Settings (Optional)

These are read by modules in `heber/storage/`, `heber/versioning/`, and `heber/schema/`:

- `ICEBERG_*` for Iceberg catalog + warehouse
- `LAKEFS_*` for Gold versioning
- `SCHEMA_REGISTRY_*` for schema registry
- `MINIO_*` for S3-compatible storage

See `.env.example` for the complete list.
