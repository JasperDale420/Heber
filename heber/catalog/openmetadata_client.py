"""OpenMetadata Catalog Client.

This module provides integration with OpenMetadata for data discovery,
lineage tracking, and governance, replacing the custom catalog implementation.

Phase 5 of OSS Migration Roadmap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from functools import lru_cache
from typing import Any

import structlog
from prometheus_client import Counter, Histogram

from heber.catalog.access_control import DataLayer
from heber.models.envelope import ENVELOPE_SCHEMA_VERSION

logger = structlog.get_logger(__name__)

# Metrics
catalog_operations = Counter(
    "heber_catalog_operations_total",
    "OpenMetadata catalog operations",
    ["operation", "status"],
)

catalog_operation_duration = Histogram(
    "heber_catalog_operation_duration_seconds",
    "Time for catalog operations",
    ["operation"],
    buckets=[0.1, 0.5, 1, 2, 5, 10],
)


class TableType(str, Enum):
    """Table type classification."""

    REGULAR = "Regular"
    VIEW = "View"
    MATERIALIZED_VIEW = "MaterializedView"
    ICEBERG = "Iceberg"
    EXTERNAL = "External"


@dataclass
class OpenMetadataConfig:
    """OpenMetadata connection configuration.

    Environment Variables:
        OPENMETADATA_HOST: Server URL (default: http://localhost:8585)
        OPENMETADATA_API_KEY: JWT token or API key
    """

    host: str = "http://localhost:8585"
    api_key: str = ""

    @classmethod
    def from_env(cls) -> OpenMetadataConfig:
        """Load from environment."""
        from heber.config import get_settings

        s = get_settings()
        return cls(
            host=s.openmetadata_host,
            api_key=s.openmetadata_api_key,
        )


@dataclass
class TableMetadata:
    """Metadata for a table in the catalog."""

    name: str
    layer: DataLayer
    description: str = ""
    schema_version: str = ENVELOPE_SCHEMA_VERSION
    owner: str = ""
    tags: list[str] = field(default_factory=list)
    custom_properties: dict[str, str] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # Zero-leakage specific
    zero_leakage_enabled: bool = True
    freshness_sla_hours: int = 1


@dataclass
class ColumnMetadata:
    """Metadata for a table column."""

    name: str
    data_type: str
    description: str = ""
    is_nullable: bool = True
    tags: list[str] = field(default_factory=list)


@dataclass
class LineageEdge:
    """Represents a lineage relationship between tables."""

    upstream_table: str
    downstream_table: str
    column_mapping: dict[str, str] = field(default_factory=dict)
    transformation: str | None = None


class OpenMetadataCatalog:
    """Client for OpenMetadata data catalog.

    This replaces the custom heber/catalog module with OpenMetadata integration.

    Example:
        catalog = OpenMetadataCatalog()

        # Register a table
        catalog.register_table(
            TableMetadata(
                name="bars",
                layer=DataLayer.SILVER,
                description="OHLCV market bars",
            )
        )

        # Add lineage
        catalog.add_lineage(
            LineageEdge(
                upstream_table="silver.bars",
                downstream_table="gold.momentum_features",
            )
        )
    """

    def __init__(self, config: OpenMetadataConfig | None = None) -> None:
        """Initialize the catalog client.

        Args:
            config: Optional configuration. If None, loads from environment.
        """
        if config is None:
            config = OpenMetadataConfig.from_env()

        self.config = config
        self._client: Any = None
        self._initialized = False

    def _get_client(self) -> Any:
        """Lazily initialize the OpenMetadata client."""
        if self._client is None:
            try:
                from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
                    AuthProvider,
                    OpenMetadataConnection,
                )
                from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
                    OpenMetadataJWTClientConfig,
                )
                from metadata.ingestion.ometa.ometa_api import OpenMetadata

                config = OpenMetadataConnection(
                    hostPort=self.config.host,
                    authProvider=AuthProvider.openmetadata,
                    securityConfig=OpenMetadataJWTClientConfig(
                        jwtToken=self.config.api_key,
                    ),
                )
                self._client = OpenMetadata(config)
                logger.info("openmetadata_client_initialized", host=self.config.host)

            except ImportError:
                logger.warning(
                    "openmetadata_import_failed",
                    msg="OpenMetadata SDK not installed, using mock client",
                )
                self._client = MockOpenMetadataClient()

        return self._client

    def register_table(
        self,
        table: TableMetadata,
    ) -> str:
        """Register or update a table in the catalog.

        Args:
            table: Table metadata
            columns: Optional column metadata

        Returns:
            Table FQN (fully qualified name)
        """
        start_time = datetime.now(UTC)
        client = self._get_client()

        try:
            fqn = f"heber.{table.layer.value}.{table.name}"

            # Use OpenMetadata API to create/update table
            if hasattr(client, "create_or_update_table"):
                client.create_or_update_table(
                    name=table.name,
                    database_schema=f"heber.{table.layer.value}",
                    description=table.description,
                    tags=table.tags,
                    custom_properties={
                        **table.custom_properties,
                        "zero_leakage_enabled": str(table.zero_leakage_enabled),
                        "freshness_sla_hours": str(table.freshness_sla_hours),
                        "schema_version": table.schema_version,
                    },
                )
            else:
                # Mock or fallback implementation
                logger.info(
                    "table_registered_mock",
                    fqn=fqn,
                    layer=table.layer.value,
                )

            duration = (datetime.now(UTC) - start_time).total_seconds()
            catalog_operations.labels(operation="register_table", status="success").inc()
            catalog_operation_duration.labels(operation="register_table").observe(duration)

            logger.info("table_registered", fqn=fqn, layer=table.layer.value)
            return fqn

        except Exception as e:
            catalog_operations.labels(operation="register_table", status="error").inc()
            logger.error("table_registration_failed", error=str(e))
            raise

    def get_table(self, fqn: str) -> TableMetadata | None:
        """Get table metadata by FQN.

        Args:
            fqn: Fully qualified name (e.g., "heber.silver.bars")

        Returns:
            TableMetadata or None if not found
        """
        client = self._get_client()

        try:
            if hasattr(client, "get_table_by_name"):
                table = client.get_table_by_name(fqn)
                if table:
                    parts = fqn.split(".")
                    layer = DataLayer(parts[1]) if len(parts) > 1 else DataLayer.SILVER

                    return TableMetadata(
                        name=table.name,
                        layer=layer,
                        description=table.description or "",
                        owner=str(table.owner) if table.owner else "",
                        tags=[tag.tagFQN for tag in (table.tags or [])],
                        custom_properties=dict(table.extension or {}),
                    )
            return None

        except Exception as e:
            logger.warning("table_get_failed", fqn=fqn, error=str(e))
            return None

    def list_tables(
        self,
        layer: DataLayer | None = None,
        limit: int = 100,
    ) -> list[str]:
        """List tables in the catalog.

        Args:
            layer: Optional filter by layer
            limit: Maximum results

        Returns:
            List of table FQNs
        """
        client = self._get_client()

        try:
            if hasattr(client, "list_tables"):
                schema_filter = f"heber.{layer.value}" if layer else None
                tables = client.list_tables(database_schema=schema_filter, limit=limit)
                return [t.fullyQualifiedName for t in tables]

            # Fallback for mock
            return []

        except Exception as e:
            logger.warning("table_list_failed", error=str(e))
            return []

    def add_lineage(self, edge: LineageEdge) -> None:
        """Add a lineage relationship between tables.

        Args:
            edge: Lineage edge definition
        """
        client = self._get_client()

        try:
            if hasattr(client, "add_lineage"):
                client.add_lineage(
                    from_entity=edge.upstream_table,
                    to_entity=edge.downstream_table,
                    lineage_details={
                        "column_mapping": edge.column_mapping,
                        "transformation": edge.transformation,
                    },
                )
            else:
                logger.info(
                    "lineage_added_mock",
                    upstream=edge.upstream_table,
                    downstream=edge.downstream_table,
                )

            catalog_operations.labels(operation="add_lineage", status="success").inc()
            logger.info(
                "lineage_added",
                upstream=edge.upstream_table,
                downstream=edge.downstream_table,
            )

        except Exception as e:
            catalog_operations.labels(operation="add_lineage", status="error").inc()
            logger.error("lineage_add_failed", error=str(e))
            raise

    def get_lineage(self, table_fqn: str, direction: str = "both") -> list[LineageEdge]:
        """Get lineage for a table.

        Args:
            table_fqn: Table FQN
            direction: "upstream", "downstream", or "both"

        Returns:
            List of lineage edges
        """
        client = self._get_client()
        edges: list[LineageEdge] = []

        try:
            if hasattr(client, "get_lineage"):
                lineage = client.get_lineage(table_fqn, direction=direction)
                for edge in lineage.edges or []:
                    edges.append(
                        LineageEdge(
                            upstream_table=edge.fromEntity,
                            downstream_table=edge.toEntity,
                        )
                    )

            return edges

        except Exception as e:
            logger.warning("lineage_get_failed", fqn=table_fqn, error=str(e))
            return []

    def search(
        self,
        query: str,
        layer: DataLayer | None = None,
        limit: int = 20,
    ) -> list[TableMetadata]:
        """Search for tables by name or description.

        Args:
            query: Search query
            layer: Optional filter by layer
            limit: Maximum results

        Returns:
            List of matching tables
        """
        client = self._get_client()
        results: list[TableMetadata] = []

        try:
            if hasattr(client, "search"):
                search_results = client.search(
                    entity_type="table",
                    query=query,
                    limit=limit,
                )
                for hit in search_results:
                    parts = hit.fullyQualifiedName.split(".")
                    table_layer = DataLayer(parts[1]) if len(parts) > 1 else DataLayer.SILVER

                    if layer is None or table_layer == layer:
                        results.append(
                            TableMetadata(
                                name=hit.name,
                                layer=table_layer,
                                description=hit.description or "",
                            )
                        )

            return results

        except Exception as e:
            logger.warning("search_failed", query=query, error=str(e))
            return []


class MockOpenMetadataClient:
    """Mock client for local development without OpenMetadata server."""

    def __init__(self) -> None:
        self._tables: dict[str, dict[str, Any]] = {}
        self._lineage: list[dict[str, Any]] = []

    def create_or_update_table(self, **kwargs: Any) -> None:
        """Mock table creation."""
        name = kwargs.get("name", "")
        schema = kwargs.get("database_schema", "")
        self._tables[f"{schema}.{name}"] = kwargs

    def get_table_by_name(self, fqn: str) -> Any:
        """Mock table retrieval."""
        return self._tables.get(fqn)

    def list_tables(self, **kwargs: Any) -> list[Any]:
        """Mock table listing."""
        return []

    def add_lineage(self, **kwargs: Any) -> None:
        """Mock lineage addition."""
        self._lineage.append(kwargs)


# Singleton
_catalog: OpenMetadataCatalog | None = None


@lru_cache(maxsize=1)
def get_catalog() -> OpenMetadataCatalog:
    """Get the singleton catalog instance."""
    global _catalog
    if _catalog is None:
        _catalog = OpenMetadataCatalog()
    return _catalog
