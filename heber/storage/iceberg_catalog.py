"""Apache Iceberg Catalog Configuration for Heber.

This module provides the Iceberg catalog setup for the Silver/Gold layers,
replacing custom Parquet management with Iceberg's ACID transactions,
schema evolution, and time-travel capabilities.

Phase 1 of OSS Migration Roadmap.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any

import structlog
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

logger = structlog.get_logger(__name__)


class IcebergCatalogType(str, Enum):
    """Supported Iceberg catalog backends."""

    SQL = "sql"  # PostgreSQL-based (recommended for production)
    REST = "rest"  # REST catalog (e.g., Tabular, AWS Glue)
    SQLITE = "sqlite"  # Local development
    IN_MEMORY = "in-memory"  # Testing only


@dataclass
class IcebergConfig:
    """Iceberg catalog configuration.

    Environment Variables:
        ICEBERG_CATALOG_TYPE: Catalog backend type (default: sql)
        ICEBERG_CATALOG_URI: Database connection string
        ICEBERG_WAREHOUSE: S3/local path for data files
        ICEBERG_S3_ENDPOINT: S3-compatible endpoint (for MinIO)
        ICEBERG_S3_ACCESS_KEY: S3 access key
        ICEBERG_S3_SECRET_KEY: S3 secret key
    """

    catalog_type: IcebergCatalogType = IcebergCatalogType.SQL
    catalog_uri: str = "postgresql://heber:heber@localhost:5432/heber_iceberg"  # pragma: allowlist secret
    warehouse: str = "s3://heber-lakehouse/warehouse"
    s3_endpoint: str | None = None  # For MinIO: http://localhost:9000
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    @classmethod
    def from_env(cls) -> IcebergConfig:
        """Load configuration from environment variables."""
        return cls(
            catalog_type=IcebergCatalogType(os.getenv("ICEBERG_CATALOG_TYPE", "sql")),
            catalog_uri=os.getenv(
                "ICEBERG_CATALOG_URI",
                "postgresql://heber:heber@localhost:5432/heber_iceberg",  # pragma: allowlist secret
            ),
            warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://heber-lakehouse/warehouse"),
            s3_endpoint=os.getenv("ICEBERG_S3_ENDPOINT"),
            s3_access_key=os.getenv("ICEBERG_S3_ACCESS_KEY"),
            s3_secret_key=os.getenv("ICEBERG_S3_SECRET_KEY"),
        )


@lru_cache(maxsize=1)
def get_iceberg_catalog(config: IcebergConfig | None = None) -> Catalog:
    """Get or create the Iceberg catalog singleton.

    Args:
        config: Optional configuration. If None, loads from environment.

    Returns:
        Configured Iceberg Catalog instance.
    """
    if config is None:
        config = IcebergConfig.from_env()

    catalog_properties: dict[str, Any] = {
        "type": config.catalog_type.value,
        "warehouse": config.warehouse,
    }

    # Add catalog-specific properties
    if config.catalog_type == IcebergCatalogType.SQL:
        catalog_properties["uri"] = config.catalog_uri

    # Add S3 properties if configured (for MinIO or custom S3)
    if config.s3_endpoint:
        catalog_properties["s3.endpoint"] = config.s3_endpoint
    if config.s3_access_key:
        catalog_properties["s3.access-key-id"] = config.s3_access_key
    if config.s3_secret_key:
        catalog_properties["s3.secret-access-key"] = config.s3_secret_key

    logger.info(
        "initializing_iceberg_catalog",
        catalog_type=config.catalog_type.value,
        warehouse=config.warehouse,
    )

    return load_catalog("heber", **catalog_properties)


# =============================================================================
# Silver Table Schemas (Iceberg format)
# =============================================================================

# Base columns present in all Silver tables
SILVER_BASE_FIELDS = [
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "instrument_key", StringType(), required=True),
    NestedField(3, "instrument_type", StringType(), required=True),
    NestedField(4, "provider", StringType(), required=True),
    NestedField(5, "feed", StringType(), required=True),
    NestedField(6, "ts_event", TimestamptzType(), required=True),
    NestedField(7, "ts_ingest", TimestamptzType(), required=True),
    NestedField(8, "ts_available", TimestamptzType(), required=True),
    NestedField(9, "processing_delay_ms", LongType(), required=False),
    NestedField(10, "source", StringType(), required=False),
    NestedField(11, "quality_flags", StringType(), required=False),
]


def get_silver_bars_schema() -> Schema:
    """Iceberg schema for Silver bars table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "bar_start_ts", TimestamptzType(), required=True),
        NestedField(21, "bar_end_ts", TimestamptzType(), required=True),
        NestedField(22, "bar_duration_seconds", LongType(), required=True),
        NestedField(23, "open", DoubleType(), required=True),
        NestedField(24, "high", DoubleType(), required=True),
        NestedField(25, "low", DoubleType(), required=True),
        NestedField(26, "close", DoubleType(), required=True),
        NestedField(27, "volume", LongType(), required=True),
        NestedField(28, "vwap", DoubleType(), required=False),
        NestedField(29, "trade_count", LongType(), required=False),
    )


def get_silver_quotes_schema() -> Schema:
    """Iceberg schema for Silver quotes table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "bid_price", DoubleType(), required=True),
        NestedField(21, "bid_size", LongType(), required=True),
        NestedField(22, "ask_price", DoubleType(), required=True),
        NestedField(23, "ask_size", LongType(), required=True),
        NestedField(24, "exchange", StringType(), required=False),
        NestedField(25, "conditions", StringType(), required=False),
    )


def get_silver_trades_schema() -> Schema:
    """Iceberg schema for Silver trades table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "price", DoubleType(), required=True),
        NestedField(21, "size", LongType(), required=True),
        NestedField(22, "exchange", StringType(), required=False),
        NestedField(23, "conditions", StringType(), required=False),
        NestedField(24, "tape", StringType(), required=False),
        NestedField(25, "trade_id", StringType(), required=False),
    )


def get_silver_flow_alerts_schema() -> Schema:
    """Iceberg schema for Silver flow_alerts table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "alert_type", StringType(), required=True),
        NestedField(21, "sentiment", StringType(), required=True),
        NestedField(22, "premium", DoubleType(), required=True),
        NestedField(23, "volume", LongType(), required=True),
        NestedField(24, "open_interest", LongType(), required=False),
        NestedField(25, "strike", DoubleType(), required=False),
        NestedField(26, "expiry", TimestampType(), required=False),
        NestedField(27, "option_type", StringType(), required=False),
        # Additional fields from Gateway NormalizedFlowAlert
        NestedField(28, "side", StringType(), required=False),  # bid, ask, mid
        NestedField(29, "is_sweep", BooleanType(), required=False),
        NestedField(30, "is_unusual", BooleanType(), required=False),
        NestedField(31, "symbol", StringType(), required=False),  # Ticker symbol
    )


def get_silver_darkpool_schema() -> Schema:
    """Iceberg schema for Silver darkpool trades table.

    Matches UW darkpool API response.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "symbol", StringType(), required=True),
        NestedField(21, "price", DoubleType(), required=True),
        NestedField(22, "size", LongType(), required=True),
        NestedField(23, "notional", DoubleType(), required=True),  # premium in UW
        NestedField(24, "exchange", StringType(), required=False),  # market_center
        NestedField(25, "tracking_id", StringType(), required=False),  # Unique trade ID
        NestedField(26, "nbbo_bid", DoubleType(), required=False),  # NBBO bid at trade
        NestedField(27, "nbbo_ask", DoubleType(), required=False),  # NBBO ask at trade
        NestedField(28, "ext_hours", StringType(), required=False),  # extended_hours_trade
        NestedField(29, "trade_settlement", StringType(), required=False),  # regular_settlement
        NestedField(30, "canceled", BooleanType(), required=False),  # Was trade cancelled
    )


def get_silver_congress_trades_schema() -> Schema:
    """Iceberg schema for Silver congressional trades table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "symbol", StringType(), required=True),
        NestedField(21, "politician", StringType(), required=True),
        NestedField(22, "transaction_type", StringType(), required=True),  # buy/sell
        NestedField(23, "amount", StringType(), required=False),  # Range like "$1M-$5M"
        NestedField(24, "transaction_date", TimestamptzType(), required=False),
        NestedField(25, "disclosure_date", TimestamptzType(), required=False),
        NestedField(26, "party", StringType(), required=False),  # D/R
        NestedField(27, "chamber", StringType(), required=False),  # House/Senate
    )


def get_silver_insider_trades_schema() -> Schema:
    """Iceberg schema for Silver insider trades table."""
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "symbol", StringType(), required=True),
        NestedField(21, "insider", StringType(), required=True),
        NestedField(22, "title", StringType(), required=False),  # CEO, CFO, etc.
        NestedField(23, "transaction_type", StringType(), required=True),
        NestedField(24, "shares", LongType(), required=False),
        NestedField(25, "price", DoubleType(), required=False),
        NestedField(26, "value", DoubleType(), required=False),
        NestedField(27, "transaction_date", TimestamptzType(), required=False),
    )


def get_silver_market_tide_schema() -> Schema:
    """Iceberg schema for Silver market tide/sentiment table.

    Matches UW market/tide API response.
    """
    return Schema(
        *SILVER_BASE_FIELDS,
        NestedField(20, "date", StringType(), required=False),  # Trading date YYYY-MM-DD
        NestedField(21, "net_call_premium", DoubleType(), required=True),
        NestedField(22, "net_put_premium", DoubleType(), required=True),
        NestedField(23, "net_volume", LongType(), required=False),  # Net volume (call - put)
        NestedField(24, "sentiment", StringType(), required=True),  # bullish/bearish/neutral
    )


# Schema registry for table creation
SILVER_SCHEMAS: dict[str, Schema] = {
    "bars": get_silver_bars_schema(),
    "quotes": get_silver_quotes_schema(),
    "trades": get_silver_trades_schema(),
    "flow_alerts": get_silver_flow_alerts_schema(),
    "darkpool": get_silver_darkpool_schema(),
    "congress_trades": get_silver_congress_trades_schema(),
    "insider_trades": get_silver_insider_trades_schema(),
    "market_tide": get_silver_market_tide_schema(),
}


# =============================================================================
# Table Management
# =============================================================================


def create_silver_table(
    catalog: Catalog,
    table_name: str,
    namespace: str = "silver",
) -> Table:
    """Create a Silver table in Iceberg if it doesn't exist.

    Args:
        catalog: Iceberg catalog instance
        table_name: Name of the table (e.g., "bars", "quotes")
        namespace: Iceberg namespace (default: "silver")

    Returns:
        The created or existing Table
    """
    full_name = f"{namespace}.{table_name}"

    # Check if table exists
    try:
        return catalog.load_table(full_name)
    except Exception:
        pass  # Table doesn't exist, create it

    # Get schema
    if table_name not in SILVER_SCHEMAS:
        raise ValueError(f"Unknown Silver table: {table_name}")

    schema = SILVER_SCHEMAS[table_name]

    logger.info("creating_iceberg_table", table=full_name)

    # Create namespace if needed
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # Namespace already exists

    # Create table with partitioning by date
    return catalog.create_table(
        identifier=full_name,
        schema=schema,
        partition_spec=[
            # Partition by day extracted from ts_event
            ("day", "ts_event"),
        ],
    )


def initialize_silver_tables(catalog: Catalog | None = None) -> dict[str, Table]:
    """Initialize all Silver tables.

    Args:
        catalog: Optional catalog instance. If None, uses default.

    Returns:
        Dictionary of table_name -> Table
    """
    if catalog is None:
        catalog = get_iceberg_catalog()

    tables = {}
    for table_name in SILVER_SCHEMAS:
        tables[table_name] = create_silver_table(catalog, table_name)
        logger.info("silver_table_ready", table=table_name)

    return tables
