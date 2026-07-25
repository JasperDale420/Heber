# Heber Monitoring Guide

Guide to monitoring Heber services and responding to alerts.

> **Note:** this predates the metrics exporter being wired up. Consumer and watch metrics are now exposed at `http://localhost:9090/metrics` and `http://localhost:9091/metrics` respectively (see [`docs/RUNBOOK.md`](../RUNBOOK.md#monitoring--alerting)) and back the `heber-consumer` / `heber-backfill-consumer` Docker healthchecks. The alerting guidance below is still useful context.

---

## Metrics Endpoints

Prometheus metrics are instrumented in code and exposed over HTTP for `heber-consumer` (`:9090`) and `heber-watch` (`:9091`); see the note above. The rest of this section (added before that exporter existed) is kept for the alerting-threshold guidance further down.

Current state:

- Catalog: no `/metrics` endpoint.
- Consumer: metrics counters exist in modules but are not exposed.
- Compactor: logs only.

---

## Key Metrics to Watch (When Exposed)

### Data Freshness

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| `heber_consumer_lag_seconds` | > 60s | > 300s | Scale consumers |
| `heber_availability_lag_seconds` | > 10s | > 30s | Check processing |
| `heber_hotstore_sync_lag_seconds` | > 60s | > 300s | Check ClickHouse |

### Throughput

| Metric | Expected | Alert Condition |
|--------|----------|-----------------|
| `heber_consumer_events_processed_total` | Steady growth | Flat for > 5m |
| `heber_writer_rows_written_total` | Steady growth | Drop > 50% |

### Errors

| Metric | Threshold | Action |
|--------|-----------|--------|
| `heber_consumer_events_processed_total{status="error"}` | > 1% | Check DLQ |
| `heber_writer_errors_total` | > 0.1% | Check storage |

---

## Alert Responses

### HeberConsumerLagHigh

**Severity**: Warning
**Threshold**: lag > 60s for 5m

1. Check consumer logs: `docker logs heber-consumer --tail 50`
2. Check Redis stream length
3. Consider scaling consumers or increasing batch size

### HeberConsumerLagCritical

**Severity**: Critical
**Threshold**: lag > 300s for 5m

1. **Page on-call immediately**
2. Check for consumer crashes
3. Check Redis connectivity
4. Consider emergency restart

### HeberWriteErrorRateHigh

**Severity**: Warning
**Threshold**: error rate > 1% for 5m

1. Check storage health (MinIO, local disk)
2. Check Iceberg catalog connectivity
3. Review error logs: `docker logs heber-consumer | grep ERROR`

### HeberAvailabilityLagSpike

**Severity**: Warning
**Threshold**: p99 availability lag > 30s

1. Check if late-arriving data spike
2. Review data source delays
3. Check processing pipeline

### HeberDLQGrowing

**Severity**: Warning
**Threshold**: DLQ size increasing

1. Check DLQ contents: `docker exec heber-redis redis-cli LRANGE heber:dlq 0 10`
2. Identify pattern of failures
3. Fix root cause and reprocess

---

## Dashboard Panels

### Overview Dashboard

- Total events processed (counter)
- Processing rate (events/sec)
- Error rate (%)
- Consumer lag (seconds)

### Latency Dashboard

- Ingest lag (ts_ingest - ts_event)
- Availability lag (ts_available - ts_event)
- P50/P95/P99 latencies

### Health Dashboard

- Service up/down status
- Memory/CPU usage
- Disk usage
- Connection pool status

---

## Daily Checks

```bash
# End-of-day health report (partition freshness, cross-feed, Soda, fill rate, zero-leakage, DLQ, Gold)
heber health-daily
heber health-daily --verbose    # full JSON output
# Reports are written to /Volumes/heber/data/ops/daily-health/{date}.json

# Quick health check
curl -s http://localhost:8085/health | jq '.status'

# Check for recent errors
docker logs heber-catalog --since 24h 2>&1 | grep -c ERROR

# Check DLQ size
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq
```

## Logging Signals (Available Now)

- `heber-consumer` logs show per-event processing failures and flush events.
- `heber-compactor` logs show compaction cycles and failures.
- `heber-catalog` logs show request failures and startup.

---

## Weekly Checks

1. Review error trends
2. Check disk usage growth
3. Review compaction metrics
4. Validate data quality scans
