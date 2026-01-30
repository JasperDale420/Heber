# Schema Registry

Heber includes a Confluent-compatible schema registry client in `heber/schema/registry_client.py`. This is optional and currently used by tools/services that want centralized schema evolution control.

## Configuration

Environment variables:

- `SCHEMA_REGISTRY_URL` (default: `http://localhost:8081`)
- `SCHEMA_REGISTRY_USER` (optional)
- `SCHEMA_REGISTRY_PASSWORD` (optional)

## Supported Schema Types

- AVRO
- JSON

## Example Usage

```python
from heber.schema.registry_client import SchemaRegistryClient, SchemaType

client = SchemaRegistryClient()
schema_id = client.register_schema(
    "silver-bars-value",
    {"type": "record", "name": "bars", "fields": []},
    schema_type=SchemaType.AVRO,
)
```

## Compatibility Checks

`SchemaRegistryClient.check_compatibility()` should be used in CI or deployment pipelines to prevent breaking changes. Compatibility levels are defined in `CompatibilityLevel` and can be set with `set_compatibility()`.

## Current State

The registry client is available, but ingestion and SDK paths do not enforce registry usage by default. You should wire it in if you want strict schema governance.
