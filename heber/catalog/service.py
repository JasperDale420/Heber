"""Catalog service business logic."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from heber.catalog.db import (
    DataCoverage,
    Dataset,
    DatasetVersion,
    FeedMapping,
    InstrumentRegistry,
)


class CatalogService:
    """Business logic for Catalog operations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # Dataset operations
    async def list_datasets(self, layer: str | None = None) -> list[Dataset]:
        """List all datasets, optionally filtered by layer."""
        query = select(Dataset).where(Dataset.is_active)
        if layer:
            query = query.where(Dataset.layer == layer)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_dataset(self, name: str) -> Dataset | None:
        """Get dataset by name."""
        result = await self.session.execute(select(Dataset).where(Dataset.dataset_name == name))
        return result.scalar_one_or_none()

    async def get_dataset_versions(self, dataset_name: str) -> list[DatasetVersion]:
        """Get all schema versions for a dataset."""
        result = await self.session.execute(
            select(DatasetVersion)
            .where(DatasetVersion.dataset_name == dataset_name)
            .order_by(DatasetVersion.created_at.desc())
        )
        return list(result.scalars().all())

    async def get_current_schema(self, dataset_name: str) -> DatasetVersion | None:
        """Get current schema version for a dataset."""
        result = await self.session.execute(
            select(DatasetVersion).where(DatasetVersion.dataset_name == dataset_name).where(DatasetVersion.is_current)
        )
        return result.scalar_one_or_none()

    async def create_dataset(
        self,
        name: str,
        layer: str,
        owner: str,
        storage_root: str,
        description: str | None = None,
        path_template: str | None = None,
        partition_cols: list[str] | None = None,
        primary_keys: list[str] | None = None,
    ) -> Dataset:
        """Create a new dataset."""
        dataset = Dataset(
            dataset_name=name,
            layer=layer,
            owner=owner,
            storage_root=storage_root,
            description=description,
            path_template=path_template,
            partition_cols=partition_cols,
            primary_keys=primary_keys,
        )
        self.session.add(dataset)
        await self.session.commit()
        await self.session.refresh(dataset)
        return dataset

    # Instrument operations
    async def get_instrument(self, key: str) -> InstrumentRegistry | None:
        """Get instrument by key."""
        result = await self.session.execute(select(InstrumentRegistry).where(InstrumentRegistry.instrument_key == key))
        return result.scalar_one_or_none()

    async def lookup_instruments(self, symbols: list[str]) -> list[InstrumentRegistry]:
        """Batch lookup instruments by symbol."""
        result = await self.session.execute(
            select(InstrumentRegistry).where(InstrumentRegistry.canonical_symbol.in_(symbols))
        )
        return list(result.scalars().all())

    async def search_instruments(
        self,
        instrument_type: str | None = None,
        symbol_prefix: str | None = None,
        limit: int = 100,
    ) -> list[InstrumentRegistry]:
        """Search instruments with filters."""
        query = select(InstrumentRegistry)
        if instrument_type:
            query = query.where(InstrumentRegistry.instrument_type == instrument_type)
        if symbol_prefix:
            query = query.where(InstrumentRegistry.canonical_symbol.ilike(f"{symbol_prefix}%"))
        query = query.limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def upsert_instrument(
        self,
        instrument_key: str,
        instrument_type: str,
        canonical_symbol: str,
        **kwargs: Any,
    ) -> InstrumentRegistry:
        """Create or update instrument."""
        existing = await self.get_instrument(instrument_key)
        if existing:
            for key, value in kwargs.items():
                if hasattr(existing, key) and value is not None:
                    setattr(existing, key, value)
            existing.updated_at = datetime.now(UTC)
        else:
            existing = InstrumentRegistry(
                instrument_key=instrument_key,
                instrument_type=instrument_type,
                canonical_symbol=canonical_symbol,
                **kwargs,
            )
            self.session.add(existing)
        await self.session.commit()
        await self.session.refresh(existing)
        return existing

    # Feed mapping operations
    async def resolve_feed(self, provider: str, gateway_feed: str) -> str | None:
        """Resolve gateway feed to Silver dataset name."""
        result = await self.session.execute(
            select(FeedMapping).where(FeedMapping.provider == provider).where(FeedMapping.gateway_feed == gateway_feed)
        )
        mapping = result.scalar_one_or_none()
        return mapping.silver_dataset_name if mapping else None

    async def list_feed_mappings(self) -> list[FeedMapping]:
        """List all feed mappings."""
        result = await self.session.execute(select(FeedMapping))
        return list(result.scalars().all())

    # Coverage operations
    async def get_coverage(self, dataset_name: str, instrument_key: str | None = None) -> list[DataCoverage]:
        """Get data coverage for a dataset."""
        query = select(DataCoverage).where(DataCoverage.dataset_name == dataset_name)
        if instrument_key:
            query = query.where(DataCoverage.instrument_key == instrument_key)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def update_coverage(
        self,
        dataset_name: str,
        instrument_key: str,
        dt_min: datetime,
        dt_max: datetime,
        approx_row_count: int | None = None,
    ) -> DataCoverage:
        """Update data coverage for an instrument in a dataset."""
        result = await self.session.execute(
            select(DataCoverage)
            .where(DataCoverage.dataset_name == dataset_name)
            .where(DataCoverage.instrument_key == instrument_key)
        )
        coverage = result.scalar_one_or_none()

        if coverage:
            coverage.dt_min = min(coverage.dt_min, dt_min)
            coverage.dt_max = max(coverage.dt_max, dt_max)
            if approx_row_count:
                coverage.approx_row_count = (coverage.approx_row_count or 0) + approx_row_count
            coverage.last_updated_ts = datetime.now(UTC)
        else:
            coverage = DataCoverage(
                dataset_name=dataset_name,
                instrument_key=instrument_key,
                dt_min=dt_min,
                dt_max=dt_max,
                approx_row_count=approx_row_count,
            )
            self.session.add(coverage)

        await self.session.commit()
        await self.session.refresh(coverage)
        return coverage
