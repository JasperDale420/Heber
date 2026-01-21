# Heber SDK

The Heber SDK is the main Python client for accessing the Heber Data Lakehouse. It provides **safe, point-in-time correct** access to financial data, preventing future information from leaking into past queries.

## Installation

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
# Read raw market data
quotes = client.read_silver(
    dataset="quotes",
    time_range=("2025-01-01", "2025-01-15"),
    instrument_keys=["equity:TSLA"],
    columns=["ts_event", "bid", "ask", "bid_size", "ask_size"],
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

Powered by lakeFS for Git-like data versioning:

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
- New helper methods

### No Updates Needed

- Adding new instruments (just data)
- Schema changes (Apicurio handles evolution)
- New Gold datasets (existing `write_gold()` works)
- Version changes (lakeFS tags work automatically)
- New data sources (Bronze → Silver pipeline handles it)

## Architecture

The SDK is a thin wrapper over:

| Layer | Implementation |
|-------|----------------|
| Silver reads | Apache Iceberg (via PyIceberg) |
| Gold reads/writes | Parquet + lakeFS tags |
| Catalog API | HTTP client to heber-catalog |
| Versioning | lakeFS API |
| Schema registry | Apicurio Registry |

The OSS migration makes the SDK more stable by replacing custom implementations with well-tested open source APIs.
