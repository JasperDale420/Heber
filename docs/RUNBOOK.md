# Runbook

Operational entry point for the Heber Data Lakehouse. This is the canonical, current runbook — for deeper procedures on a specific topic, follow the links in each section out to `docs/operations/`.

Companion docs: [architecture](./ARCHITECTURE.md), [configuration guide](./configuration-guide.md), [API reference](./API_REFERENCE.md).

## Service Overview

Heber runs as a set of Docker Compose services (`docker-compose.yml`, run from the repo root):

| Container | Host port(s) | Role |
|---|---|---|
| `heber-postgres` | `127.0.0.1:5433` → `5432` | Catalog database |
| `heber-catalog` | `127.0.0.1:8085` → `8080` | Catalog REST API (datasets, instruments, coverage) |
| `heber-consumer` | `127.0.0.1:9090` → `9090` | Redis Streams (`heber:events`) → Bronze + Silver writer |
| `heber-backfill-consumer` | `127.0.0.1:9095` → `9090` | Same writer on the isolated `heber:events:backfill` stream, so bulk UW backfill can't evict live feeds |
| `heber-compactor` | — (mem-capped 2g) | Parquet file compaction (Silver/Gold) |
| `heber-watch` | `127.0.0.1:9091` → `9090` | Flow-alert outcome tracking → Gold labels |
| `heber-gold-poller` | `127.0.0.1:9092` → `9091` | EOD scheduled Gold feature pipelines (`16:35` ET default) |
| `heber-dataflow-health` | — | Scheduled Gateway → Ingest → Storage proof-of-flow checks |
| `heber-health-monitor` | `127.0.0.1:9093` → `9093` | Tiered data-quality monitoring (mem-capped 1500m) |

There is **no Redis, ClickHouse, MinIO, lakeFS, Apicurio, or OpenMetadata container in this docker-compose.yml** — those were removed as unwired subsystems (see `git log` on `docker-compose.yml`, June 2026) or were never wired in. All `HEBER_REDIS_URL` values point at `redis://host.docker.internal:6379` — the Data-Gateway repo's own Redis (often named `data-gateway-redis` in that stack), not something Heber runs itself. If you see docs elsewhere in this repo describing those services as running containers, they predate this cleanup — see the note at the top of `docs/operations/runbook.md`.

## Startup / Shutdown

```bash
cd /Users/jacobmcmillan/Empire/Heber

# One-time: create the external volume directories
./scripts/init_volume.sh

# Start everything (Postgres starts first and must pass its healthcheck
# before heber-catalog starts; other services depend on heber-catalog)
docker compose up -d

# Stop everything
docker compose down

# Restart one service
docker compose restart heber-consumer
```

**Code changes require a rebuild, not just a restart.** Images bake the source at build time (`Dockerfile` `COPY`s `heber/`); only `/data` is a live-mounted volume. Use `scripts/deploy.sh` instead of `docker compose restart` after any code change:

```bash
./scripts/deploy.sh                              # rebuild + redeploy all app services
./scripts/deploy.sh heber-consumer heber-watch   # just these
```

`deploy.sh` refuses to run between 16:25–16:45 ET by default (`HEBER_DEPLOY_FORCE=1` or `--force` overrides) — that window is the daily UW EOD publish, and dropping `heber-consumer` mid-burst lets the capped Redis stream evict unread feeds (this is exactly how the 2026-06-25 data loss happened).

## Health Checks

```bash
# Container status — everything should show (healthy) or Up
docker ps --format "table {{.Names}}\t{{.Status}}" | grep heber

# Catalog API liveness
curl -s http://localhost:8085/health

# Catalog health-check summary (recent Soda/quality results)
curl -s "http://localhost:8085/api/v1/health/summary?days=1"

# Real-time Gateway -> Ingest -> Storage proof of flow
heber health-dataflow --mode manual --window-seconds 900

# End-of-day 7-check report: partition freshness, cross-feed completeness,
# Soda quality, fill rate, zero-leakage audit, DLQ status, Gold freshness.
# Skips non-trading days unless --force.
heber health-daily
heber health-daily --date 2026-03-09 --verbose
heber health-daily --force
```

`heber-consumer` and `heber-backfill-consumer` have an active Docker healthcheck that fails if their run-loop heartbeat gauge (on port 9090, path `/`) hasn't ticked in 180 seconds — this catches a running-but-stalled process, not just a crashed one. `heber-compactor`, `heber-watch`, `heber-gold-poller`, `heber-dataflow-health`, and `heber-health-monitor` have Docker healthchecks disabled (long-running workers); use the CLI/report checks above for those instead.

## Common Issues & Troubleshooting

- **Container stopped but not restarting**: `docker compose restart: always` only recovers a container that *crashed* (non-zero exit) — it will not bring back one that was simply left stopped (e.g. across a deploy). `scripts/heber_docker_watchdog.sh`, run on a `launchd` interval (`launchd/com.empire.heber.docker-watchdog.plist`), checks and restarts any of `heber-consumer heber-watch heber-catalog heber-gold-poller heber-compactor heber-dataflow-health heber-health-monitor` that is down.
- **Consumer lag / stalled processing**: check `docker logs heber-consumer --since 1h` for errors, and confirm the Data-Gateway Redis is reachable (`redis://host.docker.internal:6379` from inside the container).
- **DLQ growing** (`heber:events:dlq` stream on the Data-Gateway Redis): inspect entries with `XRANGE heber:events:dlq - + COUNT 5` against that Redis instance; common reasons are `validation_error`, `uncontracted_feed`, `unmapped_feed`, and `invalid_instrument_key` (see [architecture — Bronze → Silver contract](./ARCHITECTURE.md#bronze--silver-normalization-contract)).
- **Path / permission errors on `/Volumes/...`**: usually a host-vs-container path mismatch, or macOS AppleDouble (`._*`) sidecar files on the external volume — `HeberReader` already filters these on read; see [architecture](./ARCHITECTURE.md#zero-leakage).
- Deeper troubleshooting steps: [`docs/operations/troubleshooting.md`](operations/troubleshooting.md).

## Monitoring & Alerting

- Consumer metrics: `http://localhost:9090/metrics` (also backs the Docker healthcheck heartbeat).
- Watch metrics: `http://localhost:9091/metrics`.
- Catalog `/health` and `/api/v1/health/summary` (see above).
- Discord alerting for critical data-quality failures is available (`HEBER_ALERT_DISCORD_ENABLED`, `HEBER_ALERT_DISCORD_WEBHOOK_URL`) with debounce/cooldown controls (`HEBER_ALERT_DEBOUNCE_CYCLES`, `HEBER_ALERT_COOLDOWN_SECONDS`) — off by default.
- An optional off-machine dead-man heartbeat (`HEBER_HEARTBEAT_URL`, e.g. a healthchecks.io check) is pinged after each dataflow-health cycle — the only monitoring that survives the host itself going down.
- Metrics/alerting detail: [`docs/operations/monitoring.md`](operations/monitoring.md).

## Disaster Recovery

The lakehouse's data volume (default `/Volumes/heber`) is backed up nightly by `scripts/backup_lakehouse.sh`:

- Bronze is bundled per `provider/feed/dt` day-partition into a single `.tar` (re-archiving only days that changed) — copying millions of tiny `.jsonl.gz` files one at a time over the external drive never completes in a reasonable window.
- Silver/Gold (large Parquet files, comparatively few) are mirrored with `rsync`, no `--delete` — a source-side deletion or a faulted/empty mount can never propagate into the backup.
- The catalog Postgres database is captured with `pg_dump` for a consistent logical backup, not a raw filesystem copy.
- A `.last-backup-ok` marker under the backup root records the last successful run; the dataflow-health `backup_freshness` check (`HEBER_BACKUP_FRESHNESS_HOURS`, default `30`) alerts if that marker goes stale.

```bash
# Run the backup manually
./scripts/backup_lakehouse.sh

# Recover a corrupted/missing Silver partition by re-deriving it from Bronze
heber backfill --feed <feed> --since <date> --until <date>
```

Confirm your own scheduler (cron or `launchd`) actually invokes `backup_lakehouse.sh` nightly — verify recent runs via `logs/heber-backup_*.log` and the `backup_freshness` health check rather than assuming a schedule.

Deeper DR procedures (including aspirational cloud/production steps not applicable to the current local Docker Compose deployment): [`docs/operations/backup-dr-runbook.md`](operations/backup-dr-runbook.md). Postmortem for the most recent real incident: [`docs/operations/postmortem-2026-07-19-power-outage.md`](operations/postmortem-2026-07-19-power-outage.md).

## Maintenance Tasks

- **Schema changes**: update the Pydantic model in `heber/models/`, the Arrow schema in `heber/schemas/silver.py`, and `heber/writer/ingest_contracts.py` field mappings if needed, then `./scripts/deploy.sh` and re-backfill affected feeds with `heber backfill`.
- **Compaction**: runs automatically in `heber-compactor`; there is no documented manual one-shot trigger beyond restarting that service.
- **Log rotation**: services log JSON to `logs/` (see `EMPIRE_LOG_DIR`/rotation conventions in `AGENTS.md`); Docker's own `json-file` logging driver is capped at `50m` × `5` files per container in `docker-compose.yml`.
