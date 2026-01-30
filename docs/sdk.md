# Heber SDK

The Heber SDK is the main Python client for accessing the Heber Data Lakehouse. It provides **safe, point-in-time correct** access to Silver/Gold data and Catalog metadata.

## Installation

```bash
# Full installation (all dependencies)
pip install heber

# Lightweight SDK-only (minimal dependencies)
pip install heber[sdk]
```

### From Source

```python
from heber.sdk.client import HeberClient
```

## Quick Start

```python
from datetime import datetime
from heber.sdk.client import HeberClient

client = HeberClient()

# Read market data with zero-leakage guarantee
bars = client.read_asof(
    dataset="bars",
    asof_time=datetime(2025, 1, 15),
    instrument_keys=["equity:AAPL"],
)
```

### Local Docker Compose Note

When the Catalog API is running via `docker compose`, it is exposed on port `8085` (host). Either:

- Set `HEBER_API_PORT=8085` in your environment, or
- Pass `catalog_url="http://localhost:8085/api/v1"` when constructing `HeberClient`.

## Core Features

### Zero-Leakage Data Access

The `read_asof()` method ensures you only get data that was **available** at the specified time:

```python
# Only returns data where ts_available <= asof_time
bars = client.read_asof(
    dataset="bars",
    asof_time=datetime(2025, 1, 15),
    instrument_keys=["equity:AAPL", "equity:TSLA"],
    time_range=("2025-01-01", "2025-01-15"),
)
```

### Silver Layer (Market Data)

```python
# Read market data from local Parquet partitions
quotes = client.read_silver(
    dataset="quotes",
    time_range=("2025-01-01", "2025-01-15"),
    instrument_keys=["equity:TSLA"],
    columns=["ts_event", "bid_px", "ask_px", "bid_sz", "ask_sz"],
)
```

### Gold Layer (Features & Labels)

```python
# Write computed features
client.write_gold(
    dataset="momentum_features",
    df=features,  # Must include: instrument_key, ts_event, ts_available
    project="kairos",
    version="v1",
)

# Read with version resolution
features = client.read_gold_versioned(
    dataset="momentum_features",
    version="v3.*",  # Latest v3.x version
)
```

### Version Management

If lakeFS is configured, the SDK uses it for Git-like data versioning. If lakeFS is not reachable, it falls back to filesystem discovery.

```python
# List all versions
versions = client.list_gold_versions("momentum_features")
# ["v3.5.0", "v3.2.1", "v1.0.0"]

# Check compatibility between versions
compat = client.check_version_compatibility(
    dataset="momentum_features",
    from_version="v3.2.1",
    to_version="v3.5.0",
)
# {"compatible": True, "breaking": [], "changes": ["added momentum_20d"]}

# Get version lineage (commit metadata)
lineage = client.get_version_lineage("momentum_features", "v3.5.0")
# {"commit_id": "abc123", "created_at": "2025-01-15T...", "parents": [...]}
```

### As-Of Joins

Point-in-time correct joins that prevent lookahead bias:

```python
# Join trades with earnings, ensuring no future data leaks
result = client.asof_join(
    left=trades,
    right=earnings,
    on_keys=["instrument_key"],
    left_time="ts_event",
    right_time="ts_event",
    right_available="ts_available",
    tolerance="1h",
)
```

### Dataset Discovery

```python
# List available datasets
datasets = client.list_datasets(layer="silver")

# Get dataset metadata
info = client.get_dataset("bars")

# Discover partitions and schema
discovery = client.discover("bars", layer="silver")
```

## When to Update the SDK

### Updates Required

- Adding new dataset types (options, crypto, etc.)
- New read patterns (streaming, incremental)
- Catalog API changes
- Schema contract changes
- New helper methods

### No Updates Needed

- Adding new instruments (just data)
- New Gold datasets (existing `write_gold()` works)
- Version changes (lakeFS tags resolve automatically)
- New data sources (Bronze -> Silver pipeline handles it)

## Architecture

The SDK is a thin wrapper over:

| Layer | Implementation |
|-------|----------------|
| Silver reads | Parquet partitions on local filesystem |
| Gold reads/writes | Parquet partitions on local filesystem |
| Catalog API | HTTP client to `heber-catalog` |
| Versioning | lakeFS API (optional; fallback to filesystem) |
| Schema registry | Confluent-compatible registry (optional, via `heber.schema`) |

Iceberg and other OSS migration components live in `heber/storage/` and are not yet wired into `HeberClient`.
