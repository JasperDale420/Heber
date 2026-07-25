> **Legacy doc — superseded by the Catalog REST section of [`../API_REFERENCE.md`](../API_REFERENCE.md). Kept for historical reference only.**

# Catalog API

Reference for the Heber Catalog service (`heber/catalog/api.py`).

Base URL (local docker): `http://localhost:8085`

## Health

`GET /health`

Response:

```json
{ "status": "healthy", "service": "heber-catalog" }
```

## Datasets

### List datasets

`GET /api/v1/datasets`

Query params:

- `layer` (optional): `bronze|silver|gold`

Response:

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
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

### Get dataset

`GET /api/v1/datasets/{name}`

Response shape matches list item under `data`.

### List dataset versions

`GET /api/v1/datasets/{name}/versions`

Response:

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
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

### Get specific schema version

`GET /api/v1/datasets/{name}/versions/{version}`

Response shape matches list item under `data`.

### Dataset coverage

`GET /api/v1/datasets/{name}/coverage`

Response:

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
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

### Create dataset

`POST /api/v1/datasets`

Request:

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
{
  "data": { "dataset_name": "bars" },
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

## Instruments

### Get instrument

`GET /api/v1/instruments/{key}`

Response:

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
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

### Lookup instruments

`POST /api/v1/instruments/lookup`

Request:

```json
{ "symbols": ["AAPL", "TSLA"] }
```

Response: array of `InstrumentResponse` objects under `data`.

### Search instruments

`GET /api/v1/instruments/search`

Query params:

- `instrument_type` (optional)
- `symbol_prefix` (optional)
- `limit` (optional, default 100, max 1000)

### Upsert instrument

`PUT /api/v1/instruments/{key}`

Request:

```json
{
  "instrument_key": "equity:AAPL",
  "instrument_type": "equity",
  "canonical_symbol": "AAPL",
  "underlying_key": null,
  "occ_symbol": null,
  "expiry": null,
  "strike": null,
  "put_call": null
}
```

Response:

```json
{
  "data": { "instrument_key": "equity:AAPL" },
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

## Feeds

### List feed mappings

`GET /api/v1/feeds`

### Resolve feed mapping

`GET /api/v1/feeds/resolve?provider={provider}&feed={feed}`

Response:

```json
{
  "data": { "silver_dataset_name": "bars" },
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

## Backfill (in-memory)

Backfill endpoints are in-memory only; they reset on process restart.

### Create backfill job

`POST /api/v1/backfill`

Request:

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

Response:

```json
{
  "data": { "backfill_id": "uuid", "status": "pending" },
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

### Get backfill job

`GET /api/v1/backfill/{id}`

### List backfill jobs

`GET /api/v1/backfill?status={status}&limit={limit}`

## Error Envelope

Errors conform to:

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "Dataset 'bars' not found"
  },
  "meta": { "ts": "2026-01-27T12:00:00Z" }
}
```

Known error codes: `INVALID_REQUEST`, `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `CONFLICT`, `RATE_LIMITED`, `INTERNAL_ERROR`.
