# Heber Deployment Runbook

Procedures for deploying and updating Heber services.

---

## Prerequisites

- Docker and Docker Compose installed
- Access to Heber repository
- `.env` file configured

---

## Local Development Deployment

### Initial Setup

```bash
cd /Users/jacobmcmillan/Empire/Heber

# Copy environment template
cp .env.example .env
# Edit .env with your values

# Start all services
docker compose up -d

# Verify health
docker ps --format "table {{.Names}}\t{{.Status}}" | grep heber
```

### Rebuild After Code Changes

```bash
# Rebuild and restart Heber services
docker compose up -d --build heber-catalog heber-consumer heber-compactor

# Wait for health checks
sleep 30
docker ps | grep heber
```

---

## Service-Specific Deployment

### Deploy Catalog API Only

```bash
docker compose up -d --build heber-catalog
# Verify
curl http://localhost:8085/health
```

### Deploy Consumer Only

```bash
docker compose up -d --build heber-consumer
# Verify logs
docker logs heber-consumer --tail 20
```

---

## Infrastructure Updates

### Update OSS Components

```bash
# Pull latest images
docker compose pull lakefs apicurio openmetadata

# Recreate with new images
docker compose up -d lakefs apicurio openmetadata
```

### Database Migrations

```bash
# Run Alembic migrations
docker exec heber-catalog alembic upgrade head
```

---

## Pre-Deployment Checklist

- [ ] All tests passing locally
- [ ] Pre-commit hooks passing
- [ ] No breaking schema changes (or migration plan ready)
- [ ] Changelog updated
- [ ] `.env` values correct

---

## Rollback Procedure

### Quick Rollback

```bash
# Get previous working commit
git log --oneline -5

# Checkout and redeploy
git checkout <previous-commit>
docker compose up -d --build
```

### Rollback Specific Service

```bash
# Tag current state
docker tag heber-heber-catalog:latest heber-heber-catalog:rollback

# Rollback
git checkout <commit>
docker compose up -d --build heber-catalog
```

---

## Health Verification

After any deployment:

```bash
# Check all services
curl -s http://localhost:8085/health | jq
curl -s http://localhost:8000/api/v1/healthcheck
curl -s http://localhost:18081/health/ready | jq

# Verify SDK works
PYTHONPATH=. python -c "from heber.sdk.client import HeberClient; print('SDK OK')"
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.2.0 | 2026-01-21 | OSS migration, SDK packaging |
| 0.1.0 | 2026-01-18 | Initial release |
