# Heber Network Topology

## VPC Architecture (PRD §25.1)

```
┌─────────────────────────────────────────────────────────────────┐
│ VPC: 10.0.0.0/16                                                │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Public Subnet           │   │ Public Subnet           │     │
│  │ 10.0.1.0/24 (AZ-a)      │   │ 10.0.2.0/24 (AZ-b)      │     │
│  │                         │   │                         │     │
│  │  ┌─────────────────┐    │   │  ┌─────────────────┐    │     │
│  │  │ Load Balancer   │    │   │  │ Load Balancer   │    │     │
│  │  └─────────────────┘    │   │  └─────────────────┘    │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Private Subnet          │   │ Private Subnet          │     │
│  │ 10.0.10.0/24 (AZ-a)     │   │ 10.0.11.0/24 (AZ-b)     │     │
│  │                         │   │                         │     │
│  │  ┌─────────────────┐    │   │  ┌─────────────────┐    │     │
│  │  │ EKS Nodes       │    │   │  │ EKS Nodes       │    │     │
│  │  │ (Heber services)│    │   │  │ (Heber services)│    │     │
│  │  └─────────────────┘    │   │  └─────────────────┘    │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
│                                                                 │
│  ┌─────────────────────────┐   ┌─────────────────────────┐     │
│  │ Data Subnet             │   │ Data Subnet             │     │
│  │ 10.0.20.0/24 (AZ-a)     │   │ 10.0.21.0/24 (AZ-b)     │     │
│  │                         │   │                         │     │
│  │  ┌────────┐ ┌────────┐  │   │  ┌────────┐ ┌────────┐  │     │
│  │  │Postgres│ │ Redis  │  │   │  │Postgres│ │ Redis  │  │     │
│  │  │ (RDS)  │ │(Elasti)│  │   │  │(standby)││(replica)│  │     │
│  │  └────────┘ └────────┘  │   │  └────────┘ └────────┘  │     │
│  └─────────────────────────┘   └─────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## Subnet Purpose (PRD §25.2)

| Subnet Type | CIDR Range | Contains | Internet Access |
|-------------|------------|----------|-----------------|
| Public | 10.0.1-2.0/24 | Load balancers, NAT gateways | Yes (IGW) |
| Private | 10.0.10-11.0/24 | EKS worker nodes, services | Outbound only (NAT) |
| Data | 10.0.20-21.0/24 | RDS, ElastiCache, ClickHouse | None |

## Security Groups (PRD §25.3)

| Security Group | Inbound Rules | Outbound Rules |
|----------------|---------------|----------------|
| `heber-alb` | 443 from 0.0.0.0/0 | All to VPC |
| `heber-services` | All from `heber-alb` | All to VPC, 443 to 0.0.0.0/0 |
| `heber-postgres` | 5432 from `heber-services` | None |
| `heber-redis` | 6379 from `heber-services` | None |
| `heber-clickhouse` | 8123, 9000 from `heber-services` | None |
| `heber-s3-endpoint` | 443 from VPC | N/A (VPC endpoint) |

## VPC Endpoints (PRD §25.4)

For private access to AWS services without traversing public internet:

| Service | Endpoint Type | Purpose |
|---------|---------------|---------|
| S3 | Gateway endpoint | Parquet file storage |
| ECR | Interface endpoint | Container image pulls |
| Secrets Manager | Interface endpoint | Secret retrieval |
| CloudWatch Logs | Interface endpoint | Log shipping |

## Network Flow

### Ingestion Path

```
Data Gateway → Redis (via public NAT) → Consumer → Writer → S3 (via VPC endpoint)
```

### Query Path

```
Client → ALB → Catalog API → RDS (via private subnet)
                         → S3 (via VPC endpoint)
```

### Hot Store Sync

```
Writer → Parquet (S3) → Hot Loader → ClickHouse (data subnet)
```

## mTLS Roadmap (PRD §25.5)

When service mesh is adopted:

- All service-to-service traffic encrypted
- Certificates managed by cert-manager + Linkerd/Istio
- Automatic rotation every 24 hours

**Current status:** Not implemented (recommended after MVP stabilizes)
