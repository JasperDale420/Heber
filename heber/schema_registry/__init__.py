"""Heber Schema Registry.

Provides Apicurio/Karapace-based schema registry integration for
centralized schema management (registration, compatibility checks).

Renamed from ``schema/`` to ``schema_registry/`` to avoid confusion
with ``schemas/`` (Arrow schema definitions).
"""

from heber.schema_registry.registry_client import ApicurioSchemaRegistry, get_registry

__all__ = ["ApicurioSchemaRegistry", "get_registry"]
