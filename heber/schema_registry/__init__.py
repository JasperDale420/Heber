"""Heber Schema Registry.

Provides Karapace-compatible schema registry integration for
centralized schema management (registration, compatibility checks).

Renamed from ``schema/`` to ``schema_registry/`` to avoid confusion
with ``schemas/`` (Arrow schema definitions).
"""

from heber.schema_registry.registry_client import (
    CompatibilityLevel,
    RegisteredSchema,
    SchemaRegistryClient,
    SchemaRegistryConfig,
    SchemaType,
)

__all__ = [
    "CompatibilityLevel",
    "RegisteredSchema",
    "SchemaRegistryClient",
    "SchemaRegistryConfig",
    "SchemaType",
]
