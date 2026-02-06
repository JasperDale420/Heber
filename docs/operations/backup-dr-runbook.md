# Heber Backup & Disaster Recovery Runbook

## RTO/RPO Targets (PRD §24.1)

| Component | RPO | RTO | Priority |
|-----------|-----|-----|----------|
| Catalog (Postgres) | 1 hour | 4 hours | Critical |
| Silver (S3) | 0 (durable) | N/A | Critical |
| Bronze (S3) | 0 (durable) | N/A | High |
| Hot Store (ClickHouse) | 24 hours | 8 hours | Medium |
| Redis (event bus) | 0 (ephemeral OK) | 1 hour | Medium |

## Backup Strategy (PRD §24.2)

### Catalog (Postgres RDS)

| Backup Type | Frequency | Retention |
|-------------|-----------|-----------|
| Automated snapshots | Daily | 30 days |
| Point-in-time recovery | Continuous | 7 days |
| Cross-region replica | Async | Warm standby |

**Commands:**

```bash
# List available snapshots
aws rds describe-db-snapshots --db-instance-identifier heber-catalog-prod

# Restore from snapshot
aws rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier heber-catalog-restored \
  --db-snapshot-identifier rds:heber-catalog-prod-2026-01-15

# Point-in-time recovery
aws rds restore-db-instance-to-point-in-time \
  --source-db-instance-identifier heber-catalog-prod \
  --target-db-instance-identifier heber-catalog-pitr \
  --restore-time 2026-01-15T12:00:00Z
```

### Object Storage (S3)

| Feature | Configuration |
|---------|---------------|
| Versioning | Enabled |
| Cross-region replication | prod only, to DR region |
| Lifecycle rules | Bronze: IA after 30d, delete after 90d |

**Commands:**

```bash
# Restore previous version
aws s3api get-object \
  --bucket heber-data-prod \
  --key silver/dataset/dt=2026-01-15/file.parquet \
  --version-id <version-id> \
  restored-file.parquet

# List versions
aws s3api list-object-versions \
  --bucket heber-data-prod \
  --prefix silver/dataset/dt=2026-01-15/
```

### ClickHouse (Hot Store)

- **Backup tool:** `clickhouse-backup`
- **Frequency:** Daily at 02:00 UTC
- **Storage:** Remote destination configured in `clickhouse-backup` config (production target: S3)
- **Retention:** 7 days

**Commands:**

```bash
# Create backup
clickhouse-backup create daily-$(date +%Y%m%d)

# Upload to S3
clickhouse-backup upload daily-$(date +%Y%m%d)

# List backups
clickhouse-backup list

# Restore
clickhouse-backup download <backup-name>
clickhouse-backup restore <backup-name>
```

---

## Disaster Recovery Runbook (PRD §24.3)

### Scenario: Primary Region Failure

**Estimated RTO:** 2-4 hours

#### Step 1: Assess (5 min)

```bash
# Check AWS status
open https://status.aws.amazon.com/

# Check PagerDuty alerts
pd incident list

# Confirm region is unavailable
aws ec2 describe-availability-zones --region us-east-1
```

#### Step 2: Failover Postgres (30-60 min)

```bash
# Promote cross-region replica (DR region)
aws rds promote-read-replica \
  --db-instance-identifier heber-catalog-replica-dr

# Wait for promotion
aws rds wait db-instance-available \
  --db-instance-identifier heber-catalog-replica-dr

# Update connection string in DR cluster
kubectl -n heber-prod set env deployment/heber-catalog \
  HEBER_CATALOG_DSN="postgresql://...@heber-catalog-replica-dr..."
```

#### Step 3: Update DNS (5 min)

```bash
# Update Route53 failover record
aws route53 change-resource-record-sets \
  --hosted-zone-id Z1234567890ABC \
  --change-batch file://dns-failover.json
```

#### Step 4: Deploy Services in DR (30-60 min)

```bash
# Configure kubectl for DR cluster
aws eks update-kubeconfig --name heber-dr --region us-west-2

# Apply Kustomize overlay for DR
cd k8s/overlays/prod
kustomize edit set image heber=ghcr.io/jacobmcmillan/heber:<latest-sha>
kubectl apply -k .

# Wait for rollout
kubectl -n heber-prod rollout status deployment/heber-consumer
kubectl -n heber-prod rollout status deployment/heber-writer
kubectl -n heber-prod rollout status deployment/heber-catalog
```

#### Step 5: Verify (15 min)

```bash
# Run smoke tests
./scripts/smoke-test.sh https://heber-dr.example.com

# Check metrics
open https://grafana.example.com/d/heber-overview

# Verify data integrity
curl -s https://heber-dr.example.com/health | jq
```

#### Step 6: Notify (5 min)

```bash
# Alert Slack channel
./scripts/notify-slack.sh "#heber-alerts" "DR activated for Heber. Primary region: us-east-1 → DR region: us-west-2"

# Email stakeholders
./scripts/notify-email.sh "heber-stakeholders@example.com" "Heber Failover Complete"
```

---

## Backup Validation Schedule (PRD §24.4)

| Frequency | Validation |
|-----------|------------|
| Monthly | Restore Catalog backup to test environment |
| Quarterly | Full DR drill (failover to secondary region) |

### Monthly Catalog Restore Test

```bash
./scripts/backup/validate-catalog-backup.sh
```

### Quarterly DR Drill Checklist

- [ ] Alert stakeholders of planned drill
- [ ] Promote read replica in DR region
- [ ] Deploy services to DR cluster
- [ ] Run smoke tests
- [ ] Validate data integrity
- [ ] Measure actual RTO
- [ ] Fail back to primary
- [ ] Document lessons learned
