# Deployment Guide

How to bring Heber up locally (Docker Compose), run it natively (launchd), and execute a controlled rollout / rollback. Operational runbooks live under [`docs/operations/`](./operations/).

Sister docs: [system architecture](./system-architecture.md), [configuration guide](./configuration-guide.md), [API reference](./api-reference.md).

## Prerequisites

- Docker + Docker Compose
- Python 3.12+ and [uv](https://github.com/astral-sh/uv)
- An accessible Data-Gateway instance (default `host.docker.internal:6379` for Redis ingest)
- External volume mountable at `/Volumes/heber` (or override via `HEBER_VOLUME_ROOT`)
- `.env` configured from `.env.example`

```bash
cp .env.example .env
./scripts/init_volume.sh        # one-time directory layout
docker compose up -d            # bring up the stack
```

## What Comes Up

`docker-compose.yml` ships:

| Container | Role | Host port |
|-----------|------|-----------|
| `heber-postgres` | Catalog + lakeFS DB | `5433` → `5432` |
| `heber-redis` | Local event bus (optional; consumer can use Gateway's Redis instead) | `6380` → `6379` |
| `heber-clickhouse` | Hot store | `8124` (HTTP), `9002` (native) |
| `heber-minio` | S3-compatible object storage | `19000` (S3), `19001` (console) |
| `heber-lakefs` | Gold versioning (staged, optional) | `8000` |
| `heber-apicurio` | Schema registry | `18081` |
| `heber-openmetadata` | Data catalog UI | `8585` |
| `heber-elasticsearch` | OpenMetadata search index | `9200` |
| `heber-catalog` | Catalog REST API | `8085` → `8080` |
| `heber-consumer` | Bronze + Silver writer | metrics `9090` |
| `heber-compactor` | Parquet compaction | — |
| `heber-watch` | Flow-alert outcome tracker | metrics `9091` |

The consumer reads from Data-Gateway's Redis (`host.docker.internal:6379`) by default. The stream is `heber:events` with consumer group `heber-writers`. Override via [`HEBER_REDIS_URL` and friends](./configuration-guide.md#redis-event-bus).

## Health Checks

```bash
curl -s http://localhost:8085/health
# {"status":"healthy","service":"heber-catalog"}

heber health-dataflow --mode manual --window-seconds 900
heber health-daily
heber health-daily --date 2026-03-09 --verbose
heber health-daily --force                    # run on non-trading days

curl -s http://localhost:9090/metrics | head -20    # consumer
curl -s http://localhost:9091/metrics | head -20    # watch
```

Reports land in:

- `HEBER_HEALTH_REPORT_DIR` (default `/data/ops/dataflow-health`) — dataflow JSON
- `HEBER_DAILY_HEALTH_REPORT_DIR` (default `/Volumes/heber/data/ops/daily-health`) — daily JSON

The daily report covers seven checks: partition freshness, cross-feed completeness, Soda quality, fill rate, zero-leakage spot-check, DLQ size, Gold freshness.

## Build & Rollout

```bash
# Build all images
docker compose build heber-catalog heber-consumer heber-watch heber-compactor

# Rolling restart
docker compose up -d heber-catalog heber-consumer heber-watch heber-compactor

# Tail post-rollout
docker compose logs --since 5m heber-consumer
docker compose logs --since 5m heber-watch
```

Containers are stateless aside from their backing services (Postgres, Redis, Parquet on `/Volumes/heber/data`). Restarts are safe; in-flight messages are claimed via Redis Streams `XCLAIM` (idle threshold `HEBER_REDIS_CLAIM_IDLE_MS`, default 60s).

## Native (macOS launchd)

Long-running native execution uses launchd plists in `launchd/`. Full procedure: [`docs/operations/native-launchd.md`](./operations/native-launchd.md). Quick reference:

```bash
launchctl load -w ~/Library/LaunchAgents/com.empire.heber-consumer.plist
launchctl load -w ~/Library/LaunchAgents/com.empire.heber-watch.plist
launchctl load -w ~/Library/LaunchAgents/com.empire.heber-gold-poller.plist

launchctl list | grep heber
launchctl unload -w ~/Library/LaunchAgents/com.empire.heber-consumer.plist
```

Logs land in the path configured by the plist (typically `~/Library/Logs/Heber/<service>.{out,err}.log`).

## Database Migrations (Catalog)

`HEBER_ENVIRONMENT=dev` auto-creates Catalog tables via SQLAlchemy `create_all` (`_should_auto_create_catalog_tables` in `heber/catalog/api.py`).

For `staging` / `prod`:

```bash
# Generate a migration after changing heber/catalog/db.py
uv run alembic revision --autogenerate -m "describe change"

# Apply
uv run alembic upgrade head

# Rollback one revision
uv run alembic downgrade -1
```

`alembic.ini` lives at the repo root; migration scripts under `alembic/versions/`.

## Rollback Procedure

1. Identify the last-known-good commit.
2. `git checkout <sha>` (or pin the container tag in `docker-compose.yml`).
3. Rebuild affected services: `docker compose build heber-consumer heber-watch heber-catalog`.
4. Restart: `docker compose up -d heber-consumer heber-watch heber-catalog`.
5. Verify health endpoints + metrics counters resume increasing.
6. If ingestion continuity was affected, verify Bronze persistence is intact, then replay missing windows to Silver:

   ```bash
   uv run heber backfill --feed flow_alerts --since 2026-06-07 --until 2026-06-07
   ```

   `heber backfill` uses `BronzeToSilverTransformer` (`heber/writer/transformer.py`) which applies the same `ingest_contracts.py` rules as the live consumer — Bronze is the durability anchor.

## Disaster Recovery

Bronze is the recovery anchor. Silver and Gold are derivable.

1. **Confirm Bronze integrity** — list expected partitions, validate file counts and sizes.
2. **Rebuild Silver from Bronze** via `heber backfill` (full or per-feed).
3. **Re-run compaction** (`heber-compactor`) to merge small post-replay Parquet files.
4. **Regenerate Gold** by triggering the relevant pipelines (`heber-gold-poller` next cycle, or by directly invoking the pipeline if urgent).
5. **Verify** with `heber health-daily --date <YYYY-MM-DD>` — confirm partition counts, cross-feed completeness, zero-leakage spot-check pass.

Detailed scenarios: [`docs/operations/backup-dr-runbook.md`](./operations/backup-dr-runbook.md).

## Monitoring

| Channel | URL / Path | Purpose |
|---------|------------|---------|
| Consumer Prometheus | `http://localhost:9090/metrics` | Event throughput, dedupe drops, DLQ counts, batch latency |
| Watch Prometheus | `http://localhost:9091/metrics` | Watch creation/poll/enrichment counters, gateway timing |
| Catalog health | `GET http://localhost:8085/health` | Liveness for the API |
| Dataflow reports | `HEBER_HEALTH_REPORT_DIR` | Periodic JSON proof-of-flow (Gateway → Ingest → Storage) |
| Daily reports | `HEBER_DAILY_HEALTH_REPORT_DIR` | EOD 7-check summary |
| Container logs | `docker compose logs <service>` | structlog JSON on stdout |
| Native logs | `~/Library/Logs/Heber/` | Same logs when running under launchd |

Detailed wiring + alert thresholds: [`docs/operations/monitoring.md`](./operations/monitoring.md).

## Common Troubleshooting

- **Volume permission errors** in containers — re-run `./scripts/init_volume.sh`; verify `chown` matches the container UID.
- **Provider auth/rate-limit 401/429** — surfaced from Data-Gateway, not Heber; check Gateway logs first.
- **Schema drift warnings** in Silver writer — usually a producer-side field rename; bump the relevant Arrow schema in `heber/schemas/silver.py` and rebuild affected partitions via `heber backfill`.
- **DLQ growth** — `redis-cli -p 6380 XLEN heber:events:dlq` and inspect reasons; `heber/writer/dlq_reprocessor.py` can replay after the underlying contract is fixed.
- **AppleDouble (`._*`) Parquet files** on macOS bind mounts — `HeberReader._open_dataset_safe` filters them silently, but you can also `dot_clean /Volumes/heber/data` to scrub them.

Step-by-step recovery: [`docs/operations/troubleshooting.md`](./operations/troubleshooting.md).

## Infrastructure & Cost

- Deployment is docker-compose + launchd on a single macOS host. The former Terraform/Kubernetes templates were removed 2026-06-10 (never deployed); recover from git history if a cloud migration ever starts.
- Capacity / cost notes: [`docs/operations/cost-estimates.md`](./operations/cost-estimates.md).
- Network topology: [`docs/operations/network-topology.md`](./operations/network-topology.md).
