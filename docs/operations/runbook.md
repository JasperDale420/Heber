# Heber Operational Runbook

Day-to-day operational guide for the Heber Data Lakehouse.

---

## System Overview

```
                 ┌──────────────────────────────────────────────────┐
                 │               Data Gateway                       │
                 │   (Alpaca WS, Unusual Whales polling)           │
                 └──────────────┬───────────────────────────────────┘
                                │  XADD heber:events
                                ▼
                 ┌──────────────────────────────────────────────────┐
                 │        Redis (data-gateway-redis:6379)           │
                 │   Stream: heber:events   DLQ: heber:events:dlq  │
                 └──────┬──────────────────┬────────────────────────┘
                        │                  │
            ┌───────────▼──────┐   ┌───────▼───────────┐
            │  heber-consumer  │   │   heber-watch      │
            │  Bronze + Silver │   │  Flow alert labels │
            │  writer pipeline │   │  (Gold labels)     │
            └────────┬─────────┘   └───────────────────┘
                     │
        ┌────────────┼────────────────┐
        ▼            ▼                ▼
   ┌─────────┐ ┌──────────┐  ┌──────────────┐
   │ Bronze  │ │  Silver  │  │  Hot Store   │
   │ JSONL.gz│ │  Parquet │  │ (ClickHouse) │
   └─────────┘ └──────────┘  └──────────────┘
        │            │
        │     ┌──────▼──────┐
        │     │  Compactor  │  (merges small files)
        │     └─────────────┘
        │
        └──────────────────────────────────────────┐
                                                    ▼
                                           ┌──────────────┐
                                           │  Gold Layer  │
                                           │ (lakeFS ver) │
                                           └──────────────┘
```

### Service Inventory

| Container           | Port(s)          | Role                           |
| ------------------- | ---------------- | ------------------------------ |
| heber-postgres      | 5433 → 5432      | Catalog DB, lakeFS DB          |
| heber-redis         | 6380 → 6379      | Local event bus (optional)     |
| heber-clickhouse    | 8124/9002        | Hot Store (recent data cache)  |
| heber-minio         | 19000/19001      | S3-compatible object storage   |
| heber-lakefs        | 8000             | Data versioning (Gold layer)   |
| heber-apicurio      | 18081            | Schema registry                |
| heber-openmetadata  | 8585/8586        | Data catalog                   |
| heber-elasticsearch | 9200             | Search index (for OpenMetadata)|
| heber-catalog       | 8085 → 8080      | Catalog REST API               |
| heber-consumer      | —                | Bronze/Silver writer           |
| heber-compactor     | —                | Parquet file merger            |
| heber-watch         | —                | Flow alert outcome tracker     |

> [!NOTE]
> The consumer reads from Data Gateway's Redis (`host.docker.internal:6379`) by default, not Heber's own Redis. The stream name is `heber:events` with consumer group `heber-writers`.

---

## Startup / Shutdown

### Start All

```bash
cd /Users/jacobmcmillan/Empire/Heber
docker compose up -d
```

Services start in dependency order automatically (Postgres → Redis/MinIO → lakeFS/Apicurio → app services).

### Stop All

```bash
docker compose down
```

### Restart a Single Service

```bash
docker compose restart heber-consumer
```

### Verify Health

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep heber
```

All services should show `(healthy)` or `Up`. The consumer, compactor, and watch services have healthchecks disabled (they're long-running workers).

---

## Daily Operations

### Health Check Script

```bash
# 1. Container status
docker ps --format "table {{.Names}}\t{{.Status}}" | grep heber

# 2. Catalog API health
curl -s http://localhost:8085/health | python3 -m json.tool

# 3. Consumer lag (check for stalled processing)
docker logs heber-consumer --since 1h 2>&1 | grep -E "(lag|stall|ERROR)" | tail -5

# 4. DLQ size (should be zero or near-zero)
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq

# 5. Recent errors across services
for svc in heber-catalog heber-consumer heber-compactor heber-watch; do
  count=$(docker logs "$svc" --since 24h 2>&1 | grep -c ERROR)
  echo "$svc: $count errors"
done
```

### Log Review

```bash
# Consumer processing events
docker logs heber-consumer --tail 20

# Compactor cycles
docker logs heber-compactor --since 1h 2>&1 | grep -E "(compacted|skipped|error)"

# Watch service flow tracking
docker logs heber-watch --tail 20
```

### Dataflow Proof (JSON)

```bash
# Host/manual
heber health-dataflow --mode manual --window-seconds 900

# Docker/manual (inside scheduler container)
docker compose exec heber-dataflow-health \
  python -m heber.ops.dataflow_health --mode manual --window-seconds 900

# Scheduled service logs (one JSON line per cycle)
docker compose logs --since 30m heber-dataflow-health

# Latest persisted report
cat /Volumes/HeberDocker/data/ops/dataflow-health/latest.json
```

---

## Common Operations

### Backfill Silver from Bronze

```bash
# All feeds
heber backfill

# Specific feed with date range
heber backfill --feed flow_alerts --since 2026-01-01 --until 2026-02-01

# Programmatic
python -c "
import asyncio
from heber.writer.transformer import backfill_silver
stats = asyncio.run(backfill_silver())
print(stats)
"
```

### Force Compaction

Compaction runs automatically. To trigger manually:

```bash
docker exec heber-compactor python -c "
import asyncio
from heber.writer.compactor import run_compactor
asyncio.run(run_compactor(once=True))
"
```

### Query Hot Store

```bash
docker exec heber-clickhouse clickhouse-client --query "
  SELECT count() FROM heber.bars WHERE dt >= today() - 7
"
```

### Drain the Dead Letter Queue

```bash
# Inspect DLQ entries
docker exec data-gateway-redis redis-cli XRANGE heber:events:dlq - + COUNT 5

# Clear DLQ after investigation
docker exec data-gateway-redis redis-cli XTRIM heber:events:dlq MAXLEN 0
```

### SDK Usage

```python
from heber.sdk.client import HeberClient

client = HeberClient()

# Read Silver data as-of a point in time
df = client.read_asof("bars", as_of="2026-02-01T09:30:00Z")

# List datasets
datasets = client.list_datasets(layer="silver")

# Read Gold with version pinning
df = client.read_gold("features", version="v3")
```

### Schema Changes

1. Update the Pydantic model in `heber/models/`
2. Update the Silver schema in `heber/schemas/silver.py`
3. Update field mappings in `heber/writer/transformer.py` (if needed)
4. Rebuild containers: `docker compose build && docker compose up -d`
5. Re-backfill affected feeds if historical data needs the new schema

---

## Incident Response

### Decision Tree

```
Is a container down?
├── Yes → docker compose restart <service>
│         └── Still down? → docker logs <service> --tail 50
└── No
    ├── Consumer lag high?
    │   ├── Check Redis connectivity: docker exec data-gateway-redis redis-cli PING
    │   ├── Check stream length: docker exec data-gateway-redis redis-cli XLEN heber:events
    │   └── Check for slow processing: docker logs heber-consumer --tail 50
    │       └── See troubleshooting.md §1
    ├── DLQ growing?
    │   ├── Inspect entries: XRANGE heber:events:dlq - + COUNT 5
    │   ├── Check for schema errors (most common cause)
    │   └── See troubleshooting.md §3
    ├── Data quality alert?
    │   ├── Run quality scan: docker exec heber-catalog python -m heber.quality.soda_scanner
    │   └── See troubleshooting.md §4
    └── Disk space?
        └── df -h ${HEBER_VOLUME_ROOT}
            └── If low: trigger compaction, then check Bronze retention
```

### Checklist for Any Incident

1. Check container health: `docker ps | grep heber`
2. Check recent errors: `docker logs <service> --since 10m 2>&1 | grep ERROR`
3. Check DLQ: `docker exec data-gateway-redis redis-cli XLEN heber:events:dlq`
4. Check disk: `df -h /Volumes/heber`
5. Cross-reference with [troubleshooting.md](troubleshooting.md)

---

## Data Recovery

### Re-ingest Bronze → Silver

If Silver data is corrupted or needs reprocessing:

```bash
# Remove bad Silver partitions
rm -rf /Volumes/heber/data/silver/feed=<feed>/dt=<date>

# Re-transform from Bronze
heber backfill --feed <feed> --since <date> --until <date>
```

### Gold Version Rollback (lakeFS)

```bash
# List Gold commits
lakectl log lakefs://heber-gold/main

# Revert to a prior commit
lakectl revert lakefs://heber-gold/main <commit-id>
```

### Catalog Database Recovery

```bash
# Postgres backup (manual)
docker exec heber-postgres pg_dump -U heber heber_catalog > backup.sql

# Restore
docker exec -i heber-postgres psql -U heber heber_catalog < backup.sql
```

---

## Configuration Reference

| Variable                          | Default                 | Description                        |
| --------------------------------- | ----------------------- | ---------------------------------- |
| `HEBER_DATA_ROOT`                 | `/Volumes/heber/data`   | Root path for all data layers      |
| `HEBER_VOLUME_ROOT`               | `/Volumes/HeberDocker`  | Docker volume mount root           |
| `HEBER_POSTGRES_URL`              | (see `.env.example`)    | Catalog database connection        |
| `HEBER_REDIS_URL`                 | `redis://localhost:6380`| Redis connection URL               |
| `HEBER_REDIS_STREAM_NAME`         | `heber:events`          | Redis stream for event ingestion   |
| `HEBER_REDIS_CONSUMER_GROUP`      | `heber-writers`         | Consumer group name                |
| `HEBER_REDIS_DLQ_STREAM_NAME`     | `heber:events:dlq`      | Dead letter queue stream           |
| `HEBER_REDIS_CLAIM_IDLE_MS`       | `60000`                 | Idle time before claiming messages |
| `HEBER_REDIS_PROCESS_MAX_RETRIES` | `3`                     | Max retries before DLQ             |
| `HEBER_CLICKHOUSE_HOST`           | `clickhouse`            | ClickHouse hostname                |
| `LAKEFS_ENDPOINT`                 | `http://localhost:8000`  | lakeFS API endpoint               |
| `SCHEMA_REGISTRY_URL`             | `http://localhost:8081`  | Apicurio schema registry          |

See [.env.example](../../.env.example) for the complete list.

---

## Related Docs

- [troubleshooting.md](troubleshooting.md) — Quick fixes for common issues
- [monitoring.md](monitoring.md) — Metrics and alerting guide
- [deployment.md](deployment.md) — Deployment procedures
- [backup-dr-runbook.md](backup-dr-runbook.md) — Backup & DR (aspirational production)
- [network-topology.md](network-topology.md) — Network architecture
- [cost-estimates.md](cost-estimates.md) — Infrastructure cost estimates
