"""Iceberg-based Silver Writer.

This module provides the Iceberg implementation of the Silver writer,
replacing custom Parquet management with Iceberg's ACID transactions.

Key benefits over custom Parquet:
- Automatic compaction (no manual compaction scheduler)
- Schema evolution without rewriting data
- Time-travel queries via snapshot IDs
- ACID transactions (no partial writes)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow as pa
import structlog
from prometheus_client import Counter, Histogram

from heber.storage.iceberg_catalog import (
    ICEBERG_SILVER_SCHEMAS,
    get_iceberg_catalog,
    initialize_silver_tables,
)

if TYPE_CHECKING:
    from pyiceberg.table import Table

logger = structlog.get_logger(__name__)

# Metrics
iceberg_writes = Counter(
    "heber_iceberg_writes_total",
    "Total Iceberg write operations",
    ["table", "status"],
)

iceberg_rows_written = Counter(
    "heber_iceberg_rows_written_total",
    "Total rows written to Iceberg",
    ["table"],
)

iceberg_write_duration = Histogram(
    "heber_iceberg_write_duration_seconds",
    "Time to write batch to Iceberg",
    ["table"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30],
)


class IcebergSilverWriter:
    """Writer for Silver layer using Apache Iceberg.

    This replaces the custom Parquet writer with Iceberg's managed storage.

    Example:
        writer = IcebergSilverWriter()
        await writer.write_batch("bars", df)
    """

    def __init__(self) -> None:
        """Initialize the Iceberg writer."""
        self._catalog = get_iceberg_catalog()
        self._tables: dict[str, Table] = {}
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Lazily initialize tables on first write."""
        if not self._initialized:
            self._tables = initialize_silver_tables(self._catalog)
            self._initialized = True

    def _get_table(self, table_name: str) -> Table:
        """Get or load an Iceberg table.

        Args:
            table_name: Name of the Silver table

        Returns:
            The Iceberg Table instance

        Raises:
            ValueError: If table name is not recognized
        """
        self._ensure_initialized()

        if table_name not in self._tables:
            if table_name not in ICEBERG_SILVER_SCHEMAS:
                raise ValueError(f"Unknown Silver table: {table_name}")
            # Try to load from catalog
            full_name = f"silver.{table_name}"
            self._tables[table_name] = self._catalog.load_table(full_name)

        return self._tables[table_name]

    def write_batch(
        self,
        table_name: str,
        df: pd.DataFrame,
    ) -> int:
        """Write a batch of records to a Silver table.

        Args:
            table_name: Name of the Silver table (e.g., "bars", "quotes")
            df: DataFrame with records to write

        Returns:
            Number of rows written

        Raises:
            ValueError: If table name is not recognized
        """
        if df.empty:
            logger.debug("skipping_empty_batch", table=table_name)
            return 0

        start_time = datetime.now(UTC)

        try:
            table = self._get_table(table_name)

            # Convert pandas to PyArrow
            arrow_table = pa.Table.from_pandas(df, preserve_index=False)

            # Append to Iceberg table (ACID transaction)
            table.append(arrow_table)

            rows_written = len(df)

            # Update metrics
            duration = (datetime.now(UTC) - start_time).total_seconds()
            iceberg_writes.labels(table=table_name, status="success").inc()
            iceberg_rows_written.labels(table=table_name).inc(rows_written)
            iceberg_write_duration.labels(table=table_name).observe(duration)

            logger.info(
                "iceberg_batch_written",
                table=table_name,
                rows=rows_written,
                duration_ms=int(duration * 1000),
            )

            return rows_written

        except Exception as e:
            iceberg_writes.labels(table=table_name, status="error").inc()
            logger.error(
                "iceberg_write_failed",
                table=table_name,
                error=str(e),
                exc_info=True,
            )
            raise

    def read_asof(
        self,
        table_name: str,
        asof_time: datetime,
        filters: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Read data as of a specific time (zero-leakage query).

        This uses Iceberg's time-travel to find the snapshot that was
        current at asof_time, then filters for ts_available <= asof_time.

        Args:
            table_name: Name of the Silver table
            asof_time: Point-in-time for the query
            filters: Optional additional filters (e.g., {"instrument_key": "equity:AAPL"})

        Returns:
            DataFrame with records available as of the given time
        """
        table = self._get_table(table_name)

        # Build filter expression
        # ts_available <= asof_time is the zero-leakage constraint
        filter_expr = f"ts_available <= timestamp '{asof_time.isoformat()}'"

        if filters:
            for key, value in filters.items():
                filter_expr += f" AND {key} = '{value}'"

        # Read with filter pushdown
        scan = table.scan(row_filter=filter_expr)
        arrow_table = scan.to_arrow()

        return arrow_table.to_pandas()

    def read_snapshot(
        self,
        table_name: str,
        snapshot_id: int,
    ) -> pd.DataFrame:
        """Read data from a specific Iceberg snapshot.

        This enables exact reproducibility for ML experiments.

        Args:
            table_name: Name of the Silver table
            snapshot_id: Iceberg snapshot ID

        Returns:
            DataFrame with records from that snapshot
        """
        table = self._get_table(table_name)
        scan = table.scan(snapshot_id=snapshot_id)
        arrow_table = scan.to_arrow()

        return arrow_table.to_pandas()

    def get_current_snapshot_id(self, table_name: str) -> int | None:
        """Get the current snapshot ID for a table.

        Useful for recording which snapshot was used for training.

        Args:
            table_name: Name of the Silver table

        Returns:
            Current snapshot ID, or None if table has no data
        """
        table = self._get_table(table_name)
        current = table.current_snapshot()
        return current.snapshot_id if current else None

    def list_snapshots(self, table_name: str) -> list[dict]:
        """List all snapshots for a table (time-travel history).

        Args:
            table_name: Name of the Silver table

        Returns:
            List of snapshot metadata dicts
        """
        table = self._get_table(table_name)
        return [
            {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "manifest_list": s.manifest_list,
            }
            for s in table.snapshots()
        ]


# Singleton instance for convenience
_writer: IcebergSilverWriter | None = None


def get_iceberg_writer() -> IcebergSilverWriter:
    """Get the singleton Iceberg writer instance."""
    global _writer
    if _writer is None:
        _writer = IcebergSilverWriter()
    return _writer
