# Heber Cost Estimates (Monthly, Production)

## Overview (PRD §26)

**Total Estimated Monthly Cost: ~$1,600 - $1,800**

This estimate assumes:

- Production workload with 6 EKS nodes
- 1-2 TB total storage across S3 tiers
- Standard data transfer patterns

---

## Compute (PRD §26.1)

| Resource | Specification | Quantity | Est. Monthly Cost |
|----------|---------------|----------|-------------------|
| EKS cluster | Control plane | 1 | $72 |
| EKS nodes | m5.large (2 vCPU, 8 GB) | 6 | $540 |
| ClickHouse | r6g.large (2 vCPU, 16 GB) | 3 | $330 |

**Compute Subtotal:** ~$950/month

---

## Storage (PRD §26.2)

| Resource | Specification | Est. Monthly Cost |
|----------|---------------|-------------------|
| S3 (Silver tier) | 1 TB Standard | $23 |
| S3 (Bronze tier) | 500 GB Standard | $12 |
| S3 (Gold tier) | 200 GB Standard | $5 |
| S3 Cross-region replication | 1 TB | $20 |
| RDS (Postgres) | db.r6g.large, 100 GB gp3 | $200 |
| ElastiCache (Redis) | r6g.large cluster mode | $200 |

**Storage Subtotal:** ~$460/month

---

## Networking & Other (PRD §26.3)

| Resource | Est. Monthly Cost |
|----------|-------------------|
| NAT Gateway (2x, data transfer) | $100 |
| ALB (Application Load Balancer) | $25 |
| VPC Endpoints (4x Interface) | $30 |
| CloudWatch Logs (50 GB/month) | $25 |
| Secrets Manager (6 secrets) | $2 |
| ECR (image storage + transfer) | $10 |

**Networking Subtotal:** ~$190/month

---

## Environment Comparison

| Environment | EKS Nodes | RDS | Redis | Est. Monthly |
|-------------|-----------|-----|-------|--------------|
| **Dev** | 2x m5.large | t3.small | t3.micro | ~$400 |
| **Staging** | 3x m5.large | t3.medium | t3.small | ~$650 |
| **Production** | 6x m5.large | r6g.large | r6g.large | ~$1,600 |

---

## Cost Optimization Opportunities

### Immediate Savings

- **Reserved Instances:** 30-40% savings on EC2/RDS with 1-year commitment
- **Spot Instances:** Use for non-critical EKS workloads (compactor, backfill)
- **S3 Intelligent Tiering:** Auto-transitions infrequent data (~15% savings)

### Medium-term

- **Graviton instances:** 20% cost reduction (ARM-based m6g, r6g)
- **Right-sizing:** Monitor and adjust after 30 days of production data
- **Savings Plans:** Commit to compute spend for additional discounts

### Long-term

- **Multi-AZ consolidation:** If HA requirements allow, reduce redundancy
- **Data lifecycle automation:** Aggressive Bronze tier cleanup

---

## Monitoring Costs

Use AWS Cost Explorer and billing alerts:

```bash
# Set billing alert at 80% of budget
aws budgets create-budget \
  --account-id 123456789012 \
  --budget file://budget.json \
  --notifications-with-subscribers file://notifications.json
```

**Recommended alerts:**

- 50% of monthly budget reached
- 80% of monthly budget reached
- Forecasted to exceed budget
