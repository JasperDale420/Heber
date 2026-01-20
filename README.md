# Heber Data Lakehouse

Centralized storage for market and intelligence data across all trading projects.

## Quick Start

```bash
# Initialize external volume directories
./scripts/init_volume.sh

# Start infrastructure
docker-compose up -d

# Run tests
uv run pytest tests/ -v
```

## Architecture

```
Data Gateway → Redis Streams → Heber Writer → Bronze/Silver/Gold (Parquet)
                                    ↓
                              Heber Catalog (Postgres)
                                    ↓
                              Hot Store (ClickHouse)
```

## Storage

All data is stored on the external volume `/Volumes/heber`:

- `data/` - Bronze/Silver/Gold Parquet files
- `postgres/` - Catalog database
- `clickhouse/` - Hot Store
- `redis/` - Event bus streams

## Services

- **heber-catalog**: REST API for dataset/instrument discovery
- **heber-consumer**: Redis → Lake writer
- **heber-compactor**: Periodic file compaction

## SDK Usage

```python
from heber.sdk import HeberClient

client = HeberClient()

# Read Silver data (point-in-time correct)
bars = client.read_asof("bars", asof_time="2025-01-15", instrument_keys=["equity:AAPL"])

# Write Gold features
client.write_gold("momentum_features", df=features, project="kairos", version="v1")
```
