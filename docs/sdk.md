# Heber Reader

The `HeberReader` is the canonical thin filesystem reader for the Heber Data Lakehouse. It provides **safe, point-in-time correct** access to Silver and Gold data via direct pyarrow.dataset reads with predicate pushdown — no HTTP, no lakeFS, no Catalog API required.

## Installation

```bash
# Full installation (all dependencies)
pip install heber

# Or from source
uv sync
```

## Quick Start

```python
from heber.reader import HeberReader

reader = HeberReader()

# Read market data with zero-leakage guarantee
bars = reader.read_asof(
    "bars",
    asof_time="2025-01-15",
    instrument_keys=["equity:AAPL"],
)
```

### Data Root

By default, `HeberReader()` uses `settings.data_root` (`/Volumes/heber/data`), which is the mounted Heber volume.

To override:

```python
from pathlib import Path

reader = HeberReader(data_root=Path("/custom/data/path"))
```

## Core Features

### Zero-Leakage Data Access

The `read_asof()` method ensures you only get data that was **available** at the specified time. The `ts_available <= asof_time` predicate is pushed into the pyarrow dataset scan — not applied as a post-filter — so Parquet row-group pruning eliminates unnecessary I/O before data reaches memory.

```python
# Only returns data where ts_available <= asof_time
bars = reader.read_asof(
    "bars",
    asof_time="2025-01-15",
    instrument_keys=["equity:AAPL", "equity:TSLA"],
    time_range=("2025-01-01", "2025-01-15"),
)
```

### Silver Layer (Market Data)

```python
# Read market data from Parquet partitions
quotes = reader.read_silver(
    "quotes",
    time_range=("2025-01-01", "2025-01-15"),
    instrument_keys=["equity:TSLA"],
    columns=["ts_event", "bid_px", "ask_px", "bid_sz", "ask_sz"],
)
```

Path layout: `silver/feed={dataset}/instrument_type={type}/dt={date}/`

### Gold Layer (Features & Labels)

```python
# Write computed features
reader.write_gold(
    "momentum_features",
    df=features,  # Must include: instrument_key, ts_event, ts_available
    project="kairos",
    version="v1",
)

# Read Gold features (latest version auto-resolved)
features = reader.read_gold(
    "momentum_features",
    project="kairos",
)

# Read a specific version
features = reader.read_gold(
    "momentum_features",
    project="kairos",
    version="v3",
)
```

Path layout: `gold/dataset={dataset}/project={project}/version={version}/dt={date}/`

### Version Discovery

```python
# List all versions for a Gold dataset
versions = reader.list_gold_versions("momentum_features")
# ["v3", "v2", "v1"]

# Filter by project
versions = reader.list_gold_versions("momentum_features", project="kairos")
```

### As-Of Joins

Point-in-time correct joins that prevent lookahead bias:

```python
# Join trades with earnings, ensuring no future data leaks
result = reader.asof_join(
    left=trades,
    right=earnings,
    on_keys=["instrument_key"],
    left_time="ts_event",
    right_time="ts_event",
    right_available="ts_available",
    tolerance="1h",
)
```

### Context Manager

```python
with HeberReader() as reader:
    bars = reader.read_asof("bars", asof_time="2025-01-15")
```

## When to Update the Reader

### Updates Required

- Adding new dataset types (options, crypto, etc.)
- New read patterns (streaming, incremental)
- Schema contract changes
- New helper methods

### No Updates Needed

- Adding new instruments (just data)
- New Gold datasets (existing `write_gold()` works)
- New data sources (Bronze -> Silver pipeline handles it)

## Architecture

The reader is a thin wrapper over pyarrow.dataset:

| Layer | Implementation |
|-------|----------------|
| Silver reads | pyarrow.dataset with hive partitioning + predicate pushdown |
| Gold reads/writes | pyarrow.dataset with hive partitioning + predicate pushdown |
| Versioning | Filesystem discovery (sorted `version=*` directories) |

All time and availability predicates are pushed into `ds.dataset(...).to_table(filter=...)` via `ds.Expression`, enabling Parquet row-group pruning before data reaches memory.

Iceberg and other OSS migration components live in `heber/storage/` and are not yet wired into `HeberReader`.
