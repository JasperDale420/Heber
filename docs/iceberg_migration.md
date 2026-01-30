# Iceberg Migration

Iceberg support is present but not wired into the default writer/SDK paths.

## Current State

- Iceberg catalog + schemas live in `heber/storage/iceberg_catalog.py`.
- Iceberg write/read helper lives in `heber/storage/iceberg_writer.py`.
- `heber/writer/silver.py` still writes Parquet directly.
- `HeberClient` reads from local Parquet partitions.

## Configuration

Iceberg settings are read from environment variables:

- `ICEBERG_CATALOG_TYPE` (default `sql`)
- `ICEBERG_CATALOG_URI`
- `ICEBERG_WAREHOUSE`
- `ICEBERG_S3_ENDPOINT`
- `ICEBERG_S3_ACCESS_KEY`
- `ICEBERG_S3_SECRET_KEY`

## Enabling (Future Work)

To migrate Silver writes to Iceberg:

1) Initialize catalog storage and warehouse (S3 or MinIO).
2) Ensure Iceberg catalog database exists (default: `heber_iceberg`).
3) Replace calls in `heber/writer/consumer.py` to use `IcebergSilverWriter` for Silver writes.
4) Update SDK reads to use Iceberg scans rather than Parquet filesystem.

## Risks / Considerations

- Schema compatibility between Parquet and Iceberg definitions.
- Data migration strategy from existing Parquet partitions.
- Operational monitoring for Iceberg snapshots and compaction.
