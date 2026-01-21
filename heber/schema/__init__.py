"""Heber Schema Registry.

Provides Apicurio-based schema registry integration.
"""

from heber.schema.registry_client import ApicurioSchemaRegistry, get_registry

__all__ = ["ApicurioSchemaRegistry", "get_registry"]
