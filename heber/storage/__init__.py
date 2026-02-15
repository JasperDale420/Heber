"""Heber Storage Module.

This module provides the storage layer abstractions, including:
- Apache Iceberg table management (Phase 1 OSS Migration)
- Bronze JSONL writers
- Silver Parquet/Iceberg writers
"""

from heber.storage.iceberg_catalog import (
    ICEBERG_SILVER_SCHEMAS,
    IcebergCatalogType,
    IcebergConfig,
    create_silver_table,
    get_iceberg_catalog,
    get_silver_bars_schema,
    get_silver_flow_alerts_schema,
    get_silver_quotes_schema,
    get_silver_trades_schema,
    initialize_silver_tables,
)
from heber.storage.iceberg_writer import (
    IcebergSilverWriter,
    get_iceberg_writer,
)

__all__ = [
    # Iceberg catalog
    "IcebergConfig",
    "IcebergCatalogType",
    "get_iceberg_catalog",
    # Table schemas
    "ICEBERG_SILVER_SCHEMAS",
    "get_silver_bars_schema",
    "get_silver_quotes_schema",
    "get_silver_trades_schema",
    "get_silver_flow_alerts_schema",
    # Table management
    "create_silver_table",
    "initialize_silver_tables",
    # Writer
    "IcebergSilverWriter",
    "get_iceberg_writer",
]
