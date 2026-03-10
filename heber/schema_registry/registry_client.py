"""Schema Registry Client for Karapace.

This module provides schema registry integration using Karapace-compatible API,
replacing custom schema versioning with centralized schema management.

Phase 4 of OSS Migration Roadmap.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast

import structlog
from prometheus_client import Counter

logger = structlog.get_logger(__name__)

# Metrics
schema_operations = Counter(
    "heber_schema_operations_total",
    "Schema registry operations",
    ["operation", "subject", "status"],
)


class CompatibilityLevel(StrEnum):
    """Schema compatibility levels for evolution."""

    BACKWARD = "BACKWARD"  # New schema can read old data
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"  # All previous versions
    FORWARD = "FORWARD"  # Old schema can read new data
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"  # All previous versions
    FULL = "FULL"  # Both backward and forward
    FULL_TRANSITIVE = "FULL_TRANSITIVE"  # All previous versions
    NONE = "NONE"  # No compatibility checking


class SchemaType(StrEnum):
    """Supported schema types."""

    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


@dataclass
class SchemaRegistryConfig:
    """Schema registry connection configuration.

    Environment Variables:
        SCHEMA_REGISTRY_URL: Registry endpoint (default: http://localhost:8081)
        SCHEMA_REGISTRY_USER: Optional username
        SCHEMA_REGISTRY_PASSWORD: Optional password
    """

    url: str = "http://localhost:8081"
    username: str | None = None
    password: str | None = None

    @classmethod
    def from_env(cls) -> SchemaRegistryConfig:
        """Load from Heber settings."""
        from heber.config import get_settings

        settings = get_settings()
        return cls(
            url=settings.schema_registry_url,
            username=settings.schema_registry_user,
            password=settings.schema_registry_password,
        )


@dataclass
class RegisteredSchema:
    """A schema registered in the registry."""

    subject: str
    schema_id: int
    version: int
    schema_type: SchemaType
    schema: dict[str, Any]


class SchemaRegistryClient:
    """Client for Karapace/Confluent-compatible schema registry.

    This replaces custom SchemaRegistry with centralized management.

    Example:
        client = SchemaRegistryClient()

        # Register a schema
        schema_id = client.register_schema(
            "silver-bars-value",
            bars_avro_schema,
            schema_type=SchemaType.AVRO,
        )

        # Check compatibility before deploy
        is_compatible = client.check_compatibility(
            "silver-bars-value",
            new_schema,
        )
    """

    def __init__(self, config: SchemaRegistryConfig | None = None) -> None:
        """Initialize the client.

        Args:
            config: Optional configuration. If None, loads from environment.
        """
        if config is None:
            config = SchemaRegistryConfig.from_env()

        self.config = config
        self._client: Any = None

    def _get_client(self) -> Any:
        """Lazily initialize the schema registry client."""
        if self._client is None:
            from schema_registry.client import SchemaRegistryClient as SRClient

            extra_headers = None
            if self.config.username and self.config.password:
                import base64

                credentials = f"{self.config.username}:{self.config.password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                extra_headers = {"Authorization": f"Basic {encoded}"}

            self._client = SRClient(
                url=self.config.url,
                extra_headers=extra_headers,
            )
            logger.info("schema_registry_client_initialized", url=self.config.url)

        return self._client

    def register_schema(
        self,
        subject: str,
        schema: dict[str, Any] | str,
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> int:
        """Register a schema for a subject.

        Args:
            subject: Subject name (e.g., "silver-bars-value")
            schema: Schema definition (dict or JSON string)
            schema_type: Schema format

        Returns:
            Schema ID
        """
        client = self._get_client()

        if isinstance(schema, dict):
            schema_str = json.dumps(schema)
        else:
            schema_str = schema

        try:
            if schema_type == SchemaType.AVRO:
                from schema_registry.serializers import AvroSchema  # type: ignore[attr-defined]

                avro_schema = AvroSchema(schema_str)
                schema_id = client.register(subject, avro_schema)
            elif schema_type == SchemaType.JSON:
                from schema_registry.serializers import JsonSchema  # type: ignore[attr-defined]

                json_schema = JsonSchema(schema_str)
                schema_id = client.register(subject, json_schema)
            else:
                raise ValueError(f"Unsupported schema type: {schema_type}")

            schema_operations.labels(
                operation="register",
                subject=subject,
                status="success",
            ).inc()

            logger.info(
                "schema_registered",
                subject=subject,
                schema_id=schema_id,
                schema_type=schema_type.value,
            )

            return cast(int, schema_id)

        except Exception as e:
            schema_operations.labels(
                operation="register",
                subject=subject,
                status="error",
            ).inc()
            logger.error("schema_register_failed", subject=subject, error=str(e))
            raise

    def get_schema(self, subject: str, version: str = "latest") -> RegisteredSchema:
        """Get a schema by subject and version.

        Args:
            subject: Subject name
            version: Version number or "latest"

        Returns:
            RegisteredSchema with full details
        """
        client = self._get_client()
        result = client.get_schema(subject, version)

        return RegisteredSchema(
            subject=subject,
            schema_id=result.schema_id,
            version=int(version) if version != "latest" else result.version,
            schema_type=SchemaType(result.schema_type) if result.schema_type else SchemaType.AVRO,
            schema=json.loads(result.schema.raw_schema) if hasattr(result.schema, "raw_schema") else result.schema,
        )

    def get_schema_by_id(self, schema_id: int) -> dict[str, Any]:
        """Get a schema by its global ID.

        Args:
            schema_id: Global schema ID

        Returns:
            Schema definition
        """
        client = self._get_client()
        result = client.get_by_id(schema_id)
        return cast(dict[str, Any], json.loads(result.raw_schema) if hasattr(result, "raw_schema") else result)

    def list_subjects(self) -> list[str]:
        """List all registered subjects.

        Returns:
            List of subject names
        """
        client = self._get_client()
        return cast(list[str], client.get_subjects())

    def list_versions(self, subject: str) -> list[int]:
        """List all versions for a subject.

        Args:
            subject: Subject name

        Returns:
            List of version numbers
        """
        client = self._get_client()
        return cast(list[int], client.get_versions(subject))

    def check_compatibility(
        self,
        subject: str,
        schema: dict[str, Any] | str,
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> bool:
        """Check if a schema is compatible with the latest version.

        Args:
            subject: Subject name
            schema: New schema to check
            schema_type: Schema format

        Returns:
            True if compatible
        """
        client = self._get_client()

        if isinstance(schema, dict):
            schema_str = json.dumps(schema)
        else:
            schema_str = schema

        try:
            if schema_type == SchemaType.AVRO:
                from schema_registry.serializers import AvroSchema  # type: ignore[attr-defined]

                avro_schema = AvroSchema(schema_str)
                return cast(bool, client.test_compatibility(subject, avro_schema))
            elif schema_type == SchemaType.JSON:
                from schema_registry.serializers import JsonSchema  # type: ignore[attr-defined]

                json_schema = JsonSchema(schema_str)
                return cast(bool, client.test_compatibility(subject, json_schema))
            else:
                raise ValueError(f"Unsupported schema type: {schema_type}")

        except Exception as e:
            logger.warning(
                "compatibility_check_failed",
                subject=subject,
                error=str(e),
            )
            return False

    def set_compatibility(
        self,
        subject: str,
        level: CompatibilityLevel,
    ) -> None:
        """Set compatibility level for a subject.

        Args:
            subject: Subject name
            level: Compatibility level
        """
        client = self._get_client()
        client.update_compatibility(level.value, subject)
        logger.info(
            "compatibility_updated",
            subject=subject,
            level=level.value,
        )

    def delete_subject(self, subject: str) -> list[int]:
        """Delete a subject and all its versions.

        Args:
            subject: Subject name

        Returns:
            List of deleted version numbers
        """
        client = self._get_client()
        return cast(list[int], client.delete_subject(subject))


# =============================================================================
# Pre-defined Schemas for Silver Tables
# =============================================================================

SILVER_BARS_AVRO_SCHEMA = {
    "type": "record",
    "name": "SilverBar",
    "namespace": "io.heber.silver",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "instrument_key", "type": "string"},
        {"name": "instrument_type", "type": "string"},
        {"name": "provider", "type": "string"},
        {"name": "feed", "type": "string"},
        {"name": "ts_event", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "ts_ingest", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "ts_available", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "processing_delay_ms", "type": ["null", "long"], "default": None},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "quality_flags", "type": ["null", "string"], "default": None},
        {"name": "bar_start_ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "bar_end_ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "bar_duration_seconds", "type": "long"},
        {"name": "open", "type": "double"},
        {"name": "high", "type": "double"},
        {"name": "low", "type": "double"},
        {"name": "close", "type": "double"},
        {"name": "volume", "type": "long"},
        {"name": "vwap", "type": ["null", "double"], "default": None},
        {"name": "trade_count", "type": ["null", "long"], "default": None},
    ],
}

SILVER_QUOTES_AVRO_SCHEMA = {
    "type": "record",
    "name": "SilverQuote",
    "namespace": "io.heber.silver",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "instrument_key", "type": "string"},
        {"name": "instrument_type", "type": "string"},
        {"name": "provider", "type": "string"},
        {"name": "feed", "type": "string"},
        {"name": "ts_event", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "ts_ingest", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "ts_available", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "processing_delay_ms", "type": ["null", "long"], "default": None},
        {"name": "source", "type": ["null", "string"], "default": None},
        {"name": "quality_flags", "type": ["null", "string"], "default": None},
        {"name": "bid_price", "type": "double"},
        {"name": "bid_size", "type": "long"},
        {"name": "ask_price", "type": "double"},
        {"name": "ask_size", "type": "long"},
        {"name": "exchange", "type": ["null", "string"], "default": None},
        {"name": "conditions", "type": ["null", "string"], "default": None},
    ],
}
