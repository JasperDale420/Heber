# API Reference

Heber exposes three programmatic surfaces:

1. **`HeberReader`** — Python filesystem reader (canonical lake read interface).
2. **Catalog REST API** — FastAPI service at `http://localhost:8085`.
3. **`heber` CLI** — operator-facing console script.

Sister docs: [system architecture](./system-architecture.md), [code standards](./code-standards.md), [configuration guide](./configuration-guide.md).

---

## HeberReader (Python)

`from heber.reader import HeberReader`

The only sanctioned way to read lake data. Thin wrapper around `pyarrow.dataset` with predicate pushdown — no HTTP, no Catalog, no lakeFS required.

Source: `heber/reader/core.py` (~935 LOC).

### Lightweight install

For external consumers that only need the reader (no Postgres/Redis/Feast):

```bash
pip install heber[reader]
```

### Construction

```python
from heber.reader import HeberReader
from pathlib import Path

reader = HeberReader()                          # defaults to settings.data_root
reader = HeberReader(data_root=Path("/custom"))

with HeberReader() as r:                        # supports context manager
    bars = r.read_asof("bars", asof_time="2026-01-15")
```

### Methods

#### `read_asof(dataset, asof_time, *, time_range=None, instrument_keys=None, columns=None) -> pd.DataFrame`

Point-in-time-correct Silver read. Pushes `ts_available <= asof_time` into the pyarrow scan as predicate pushdown (Parquet row-group pruning, *not* a post-filter).

```python
bars = reader.read_asof(
    "bars",
    asof_time="2026-01-15T09:30:00Z",
    time_range=("2026-01-01", "2026-01-15"),
    instrument_keys=["equity:AAPL", "equity:TSLA"],
    columns=["ts_event", "open", "high", "low", "close", "volume"],
)
```

#### `read_silver(dataset, *, time_range=None, instrument_type=None, instrument_keys=None, columns=None) -> pd.DataFrame`

Plain Silver read, no zero-leakage gate. Use when you genuinely want all available rows (e.g. operational audits) — not for backtests or model training.

```python
flow = reader.read_silver("flow_alerts", instrument_type="equity")
```

#### `read_gold(dataset, *, project=None, version=None, time_range=None, instrument_keys=None, columns=None) -> pd.DataFrame`

Gold read. If `version` is omitted, the latest `version=v*` directory under the matching `dataset/project` path wins.

```python
features = reader.read_gold("momentum_features", project="kairos")
features_v3 = reader.read_gold("momentum_features", project="kairos", version="v3")
```

#### `write_gold(dataset, df, *, project, version) -> Path`

Atomically writes Gold partitions. **Validates** `ts_available >= ts_event` row-wise; raises on violation. `df` must include `instrument_key`, `ts_event`, `ts_available`.

```python
reader.write_gold("momentum_features", df=features, project="kairos", version="v1")
```

#### `list_gold_versions(dataset, *, project=None) -> list[str]`

Sorted version directories under the dataset path (newest first).

```python
versions = reader.list_gold_versions("momentum_features", project="kairos")
# ["v3", "v2", "v1"]
```

#### `asof_join(left, right, *, on_keys, left_time, right_time, right_available, tolerance=None) -> pd.DataFrame`

Point-in-time-correct merge. The right side is filtered on `ts_available <= left[left_time]` (with optional `tolerance`) **before** merging — no lookahead bias.

```python
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

### Safety behaviors

`HeberReader._open_dataset_safe` (used internally) silently:

- Filters `._*.parquet` AppleDouble files (macOS Apple Silicon Docker bind mounts return EPERM on `stat()` for them; pyarrow's auto-walk crashes otherwise).
- Filters files inside `._<name>` sidecar directories (e.g. `._dt=2026-03-11/...`).
- Filters `*.tmp` files (typically `part-*.parquet.tmp` partial writes — pyarrow errors with `Parquet file size is X bytes`).
- Folds `large_string` and `dictionary<string>` into plain `pa.string()` during manual schema unification so fragments written by the real-time writer and the compactor merge cleanly.

You don't need to think about any of this in normal use — it's documented because it explains why your `/Volumes/heber/data` directory can have `._*` files without breaking reads.

---

## Catalog REST API

FastAPI service at `http://localhost:8085`. Source: `heber/catalog/api.py` (~620 LOC). Service layer: `heber/catalog/service.py`. Schema (Postgres): `heber/catalog/db.py`.

Auth is not enforced in local Docker — restrict network exposure in shared deploys.

All responses follow:

```json
{
  "data": <payload>,
  "meta": { "ts": "2026-06-07T12:00:00Z" }
}
```

Errors follow:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Dataset 'bars' not found"
  },
  "meta": { "ts": "2026-06-07T12:00:00Z" }
}
```

Known error codes: `INVALID_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `CONFLICT`, `RATE_LIMITED`, `INTERNAL_ERROR`.

### Health

#### `GET /health`

```json
{ "status": "healthy", "service": "heber-catalog" }
```

### Datasets

#### `GET /api/v1/datasets?layer={bronze|silver|gold}`

List datasets. Optional `layer` query param.

```json
{
  "data": [
    {
      "dataset_name": "bars",
      "layer": "silver",
      "owner": "shared",
      "description": "Bars data",
      "storage_root": "/Volumes/heber/data",
      "path_template": "silver/feed={dataset}/instrument_type={instrument_type}/dt={dt}",
      "partition_cols": ["feed", "instrument_type", "dt"],
      "is_active": true
    }
  ],
  "meta": { "ts": "2026-06-07T12:00:00Z" }
}
```

#### `GET /api/v1/datasets/{name}`

Single dataset definition (same shape as a list item).

#### `POST /api/v1/datasets`

Create dataset metadata.

```json
{
  "dataset_name": "bars",
  "layer": "silver",
  "owner": "shared",
  "description": "Bars data",
  "storage_root": "/Volumes/heber/data",
  "path_template": "silver/feed={dataset}/instrument_type={instrument_type}/dt={dt}",
  "partition_cols": ["feed", "instrument_type", "dt"],
  "primary_keys": ["event_id"]
}
```

Response:

```json
{ "data": { "dataset_name": "bars" }, "meta": { "ts": "..." } }
```

#### `GET /api/v1/datasets/{name}/versions`

```json
{
  "data": [
    {
      "schema_version": "v1",
      "schema_json": { "fields": [] },
      "is_current": true,
      "created_at": "2026-01-20T00:00:00Z"
    }
  ],
  "meta": { "ts": "..." }
}
```

#### `GET /api/v1/datasets/{name}/versions/{version}`

Single schema version (same shape as a list item).

#### `GET /api/v1/datasets/{name}/coverage`

```json
{
  "data": [
    {
      "instrument_key": "equity:AAPL",
      "dt_min": "2025-01-01",
      "dt_max": "2025-01-31",
      "approx_row_count": 123456
    }
  ],
  "meta": { "ts": "..." }
}
```

### Instruments

#### `GET /api/v1/instruments/{key}`

```json
{
  "data": {
    "instrument_key": "equity:AAPL",
    "instrument_type": "equity",
    "canonical_symbol": "AAPL",
    "underlying_key": null,
    "occ_symbol": null,
    "expiry": null,
    "strike": null,
    "put_call": null
  },
  "meta": { "ts": "..." }
}
```

#### `POST /api/v1/instruments/lookup`

```json
{ "symbols": ["AAPL", "TSLA"] }
```

Response: array of `InstrumentResponse` under `data`.

#### `GET /api/v1/instruments/search?instrument_type={type}&symbol_prefix={prefix}&limit={n}`

`limit` default `100`, max `1000`.

#### `PUT /api/v1/instruments/{key}`

Upsert. Request body matches the `GET` response shape (without `meta`).

### Feeds

#### `GET /api/v1/feeds`

List all provider → Silver feed mappings.

#### `GET /api/v1/feeds/resolve?provider={p}&feed={f}`

```json
{
  "data": { "silver_dataset_name": "bars" },
  "meta": { "ts": "..." }
}
```

### Backfill (in-memory)

Backfill jobs are tracked in process memory only; they reset on restart. For production-grade backfill use the `heber backfill` CLI directly.

#### `POST /api/v1/backfill`

```json
{
  "provider": "alpaca",
  "feed": "bars",
  "instrument_keys": ["equity:AAPL"],
  "start_date": "2025-01-01",
  "end_date": "2025-01-31",
  "project": "kairos"
}
```

Response: `{"data": {"backfill_id": "uuid", "status": "pending"}, "meta": {...}}`.

#### `GET /api/v1/backfill/{id}`

#### `GET /api/v1/backfill?status={s}&limit={n}`

### Instrument Key Format

Validated by regex in `heber/models/envelope.py`.

| Type | Format | Example |
|------|--------|---------|
| equity | `equity:{SYMBOL}` | `equity:AAPL` |
| crypto | `crypto:{BASE}-{QUOTE}` | `crypto:BTC-USD` |
| forex | `forex:{FROM}-{TO}` | `forex:USD-EUR` |
| option | `option:OCC:{OCC_SYMBOL}` | `option:OCC:AAPL260116C00200000` |

Per-underlying analytics with `expiry` fields (e.g. `iv_term_structure`) are **equity, not option** — one row per expiry of the same underlying. The bare `option:{symbol}` form is rejected at the writer.

---

## CLI

`heber` is registered as a console script by `pyproject.toml` (entry: `heber.cli:main`). Source: `heber/cli.py` (~228 LOC).

```bash
uv run heber --help
```

### `heber info [--verbose]`

Print Heber version. With `--verbose`, also lists optional OSS components (Iceberg, lakeFS, Apicurio, OpenMetadata).

### `heber datasets [--layer {bronze|silver|gold}]`

List dataset names under `HEBER_DATA_ROOT/<layer>/feed=*`. Defaults to `silver`. Filters out Atlas hypothesis materializations (paths containing `_atlas_materialization_meta.json` or starting with `feed=atlas_features_`).

### `heber versions <dataset>`

List Gold version directories for a dataset (uses `HeberReader.list_gold_versions`).

### `heber backfill [--feed <name>] [--since YYYY-MM-DD] [--until YYYY-MM-DD]`

Replay Bronze → Silver using `BronzeToSilverTransformer`. Applies the same `ingest_contracts.py` rules as the live consumer. With `--feed`, runs one feed; without, runs all contracted feeds and prints per-feed row counts.

### `heber health-dataflow [--mode {manual|scheduled}] [--window-seconds N] [--loop] [--interval-seconds N] [--consumer-metrics-url ...] [--watch-metrics-url ...] [--report-dir ...]`

Run the Gateway → Ingest → Storage proof-of-flow check. Prints JSON to stdout. With `--loop`, runs on a fixed interval until interrupted.

### `heber health-daily [--date YYYY-MM-DD] [--force] [--verbose]`

Run the end-of-day 7-check report (partition freshness, cross-feed completeness, Soda quality, fill rate, zero-leakage, DLQ, Gold freshness). With `--verbose`, prints full JSON. Exit code is non-zero on `fail` status.

---

## Service Entry Points

Each long-running service is a module with `__main__.py`:

| Command | Service |
|---------|---------|
| `python -m heber.writer.consumer` | `heber-consumer` — Bronze + Silver writer |
| `python -m heber.writer.compactor` | `heber-compactor` — Parquet compaction |
| `python -m heber.watch` | `heber-watch` — flow-alert outcome tracker |
| `python -m heber.gold_poller` | `heber-gold-poller` — EOD Gold pipelines |
| `python -m heber.catalog.api` | `heber-catalog` — REST API on `:8085` |

All read configuration from environment variables (`HEBER_*`) — see [configuration guide](./configuration-guide.md).
