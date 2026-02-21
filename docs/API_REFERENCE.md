# API_REFERENCE

## Authentication

Catalog API currently runs without auth in local Docker development. Restrict network exposure in shared environments.

## Base URL

- Local Docker: `http://localhost:8085`
- API prefix: `/api/v1`

## Endpoints

### Health

#### `GET /health`

**Description**: Service health status.

### Datasets

#### `GET /api/v1/datasets`

**Description**: List datasets, optionally filtered by layer.

#### `GET /api/v1/datasets/{name}`

**Description**: Get one dataset definition.

#### `GET /api/v1/datasets/{name}/versions`

**Description**: List schema versions.

#### `GET /api/v1/datasets/{name}/coverage`

**Description**: Dataset instrument/date coverage metadata.

#### `POST /api/v1/datasets`

**Description**: Create dataset metadata.

### Instruments

#### `GET /api/v1/instruments/{key}`

**Description**: Read canonical instrument metadata.

#### `POST /api/v1/instruments/lookup`

**Description**: Bulk lookup by symbol list.

#### `GET /api/v1/instruments/search`

**Description**: Search instruments by type/prefix.

#### `PUT /api/v1/instruments/{key}`

**Description**: Upsert instrument metadata.

### Feeds

#### `GET /api/v1/feeds`

**Description**: List provider feed mappings.

#### `GET /api/v1/feeds/resolve`

**Description**: Resolve provider feed to Silver dataset mapping.

### Backfill

#### `POST /api/v1/backfill`

**Description**: Create an in-memory backfill job.

#### `GET /api/v1/backfill/{id}`

**Description**: Read one backfill job.

#### `GET /api/v1/backfill`

**Description**: List backfill jobs.

## Rate Limiting

No built-in API rate limiting is configured by default for local Docker use.

## Error Format

Errors follow FastAPI/JSON response conventions. Client calls should capture `status_code`, `detail`, and request context.

## Detailed Contract

For full request/response examples, see `/Users/jacobmcmillan/Empire/Heber/docs/catalog_api.md`.
