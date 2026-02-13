# MIGRATION_GUIDE

## Parquet Schema Evolution (Silver/Gold)

### Breaking Changes

Schema drift across historical partitions can break compaction when fields are added or types diverge.

### Migration Steps

1. Build a unified schema per partition.
2. Cast each source table to the unified schema.
3. Fill missing optional columns with null values.
4. Skip and log only truly incompatible type-conflict partitions.

### Verification

- Run compactor and confirm compatible partitions compact successfully.
- Confirm no schema-mismatch errors for nullable-column additions.

### Rollback

- Keep original source files until compaction output validation completes.
- Re-run compactor for failed partitions after schema fix.

## Iceberg Migration Status

Iceberg support exists but is not the default writer path yet. See `/Users/jacobmcmillan/Empire/Heber/docs/iceberg_migration.md` for current status.
