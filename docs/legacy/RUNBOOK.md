> **Legacy doc — superseded by [`../RUNBOOK.md`](../RUNBOOK.md) (canonical), with deeper procedures in [`../operations/runbook.md`](../operations/runbook.md) and [`../deployment-guide.md`](../deployment-guide.md). Kept for historical reference only.**

# RUNBOOK

## Service Overview

Heber ingests market events from Redis streams, writes Bronze/Silver lake data, and runs watch + compactor workers.

For full operational procedures, see:

- `/Users/jacobmcmillan/Empire/Heber/docs/operations/runbook.md`
- `/Users/jacobmcmillan/Empire/Heber/docs/operations/troubleshooting.md`
- `/Users/jacobmcmillan/Empire/Heber/docs/operations/monitoring.md`

## Startup / Shutdown

```bash
docker compose up -d
docker compose down
```

## Health Checks

```bash
# Real-time dataflow verification
curl -s http://localhost:8085/health
heber health-dataflow --mode manual --window-seconds 900

# End-of-day health report (7 checks: partitions, cross-feed, Soda, fill rate, zero-leakage, DLQ, Gold)
heber health-daily
heber health-daily --date 2026-03-09 --verbose  # specific date, full JSON
heber health-daily --force                       # run on non-trading days
```

## Monitoring & Alerting

- Consumer metrics: `http://localhost:9090/metrics`
- Watch metrics: `http://localhost:9091/metrics`
- Dataflow JSON reports: `/data/ops/dataflow-health`
- Daily health reports: `/Volumes/heber/data/ops/daily-health`

## Common Issues & Troubleshooting

- Path permission failures (`/Volumes/...`) in container contexts.
- Upstream API auth/rate-limit failures (`401/429`).
- Schema drift causing Silver or compactor warnings.

Use the operations troubleshooting guide for step-by-step recovery procedures.

## Disaster Recovery

- Validate Bronze data first.
- Rebuild Silver from Bronze using backfill tooling.
- Re-run compaction after recovery.
