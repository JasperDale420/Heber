# Heber Troubleshooting Runbook

Quick reference for diagnosing and resolving common Heber issues.

---

## Quick Diagnostics

```bash
# Check all container health
docker ps --format "table {{.Names}}\t{{.Status}}" | grep heber

# Check consumer lag
docker logs heber-consumer --tail 50 2>&1 | grep -i lag

# Check for errors
docker logs heber-catalog --since 5m 2>&1 | grep -E "(ERROR|Exception)"
```

---

## Common Issues

### 1. Consumer Lag High (> 60s)

**Symptoms**: `HeberConsumerLagHigh` alert, data freshness issues

**Diagnosis**:

```bash
# Check consumer logs
docker logs heber-consumer --tail 100

# Check Redis stream length
docker exec data-gateway-redis redis-cli XLEN heber:events
```

**Resolution**:

1. Check if consumer is running: `docker ps | grep consumer`
2. Restart if stuck: `docker restart heber-consumer`
3. If backlog too large, scale consumers or increase batch size

---

### 2. Catalog API Unhealthy

**Symptoms**: `HeberCatalogDown` alert, SDK connection errors

**Diagnosis**:

```bash
curl -s http://localhost:8085/health | jq
docker logs heber-catalog --tail 50
```

**Resolution**:

1. Check Postgres connectivity: `docker exec heber-catalog nc -zv postgres 5432`
2. Check Redis connectivity: `docker exec heber-catalog nc -zv redis 6379`
3. Restart: `docker restart heber-catalog`

---

### 3. lakeFS Connection Failed

**Symptoms**: SDK versioning methods fail, tags not listing

**Diagnosis**:

```bash
# Check lakeFS health
curl -s http://localhost:8000/api/v1/healthcheck

# Check logs
docker logs heber-lakefs --tail 50
```

**Resolution**:

1. Verify lakeFS credentials in `.env`
2. Restart: `docker restart heber-lakefs`
3. Re-run setup if needed: <http://localhost:8000/setup>

---

### 4. Hot Store Lag (> 5 min)

**Symptoms**: `HeberHotStoreLagHigh` alert, stale ClickHouse data

**Diagnosis**:

```bash
# Check ClickHouse health
curl -s "http://localhost:8124/ping"

# Check hot store sync logs
docker logs heber-consumer --tail 50 | grep -i hotstore
```

**Resolution**:

1. Check ClickHouse connection
2. Verify quotes_hot/trades_hot/bars_hot tables exist
3. Restart consumer: `docker restart heber-consumer`

---

### 5. Data Quality Issues

**Symptoms**: Missing data, duplicates, schema errors

**Diagnosis**:

```bash
# Run the daily health report (includes the Silver invariant scan)
docker exec heber-catalog python -m heber.cli health-daily

# Check dead letter queue
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq
```

**Resolution**:

1. Check DLQ for failed events
2. Review schema registry for mismatches
3. Re-ingest from Bronze if needed

---

### 6. OpenMetadata Not Starting

**Symptoms**: Container restarting, migration errors

**Diagnosis**:

```bash
docker logs heber-openmetadata --tail 50
```

**Resolution**:

1. Verify `heber_openmetadata` database exists
2. Run migrations if needed
3. Check Elasticsearch is healthy: `curl http://localhost:9200/_cluster/health`

---

## Emergency Procedures

### Full System Restart

```bash
cd /Users/jacobmcmillan/Empire/Heber
docker compose down
docker compose up -d
docker compose logs -f
```

### Rollback to Previous Version

```bash
# List recent commits
git log --oneline -10

# Rollback
git checkout <commit-hash>
docker compose up -d --build
```

### Data Recovery

See [Backup & DR Runbook](backup-dr-runbook.md)

---

## Support Contacts

- **On-call**: Check #heber-alerts
- **Escalation**: @data-platform-team
