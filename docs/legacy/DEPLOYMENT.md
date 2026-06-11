> **Legacy doc — superseded by [`../deployment-guide.md`](../deployment-guide.md). Kept for historical reference only.**

# DEPLOYMENT

## Prerequisites

- Docker and Docker Compose
- `.env` configured from `.env.example`
- Access to required external dependencies (Redis/Data Gateway as configured)

## Local Deployment

```bash
docker compose up -d
```

## Build and Rollout

```bash
docker compose build heber-catalog heber-consumer heber-watch heber-compactor
docker compose up -d heber-catalog heber-consumer heber-watch heber-compactor
```

## Infrastructure Dependencies

- Postgres (catalog)
- Redis streams
- ClickHouse (hot store)
- MinIO/lakeFS/Apicurio/OpenMetadata (optional OSS stack)

## Configuration

Key runtime files:

- `/Users/jacobmcmillan/Empire/Heber/.env.example`
- `/Users/jacobmcmillan/Empire/Heber/docs/configuration.md`

## Rollback Procedure

1. Identify last known good commit.
2. Redeploy pinned commit.
3. Verify health endpoints and worker logs.
4. If ingestion continuity is affected, validate Bronze persistence and replay to Silver.

## Detailed Runbook

See `/Users/jacobmcmillan/Empire/Heber/docs/operations/deployment.md` for service-specific deployment commands.
