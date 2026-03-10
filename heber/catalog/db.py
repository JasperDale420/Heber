"""SQLAlchemy models for Heber Catalog.

See PRD Section 11.2 for table specifications.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Dataset(Base):
    """Dataset registry - what datasets exist in the lake."""

    __tablename__ = "datasets"

    dataset_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_name: str = Column(String(255), unique=True, nullable=False, index=True)
    layer: str = Column(String(50), nullable=False)  # bronze | silver | gold
    owner: str = Column(String(100), nullable=False)  # shared | project name
    description: str = Column(Text, nullable=True)
    storage_root: str = Column(String(500), nullable=False)
    path_template: str = Column(String(500), nullable=True)
    partition_cols: list[str] | None = Column(JSONB, nullable=True)
    primary_keys: list[str] | None = Column(JSONB, nullable=True)
    retention_policy: dict[str, Any] | None = Column(JSONB, nullable=True)
    is_active: bool = Column(Boolean, default=True, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: datetime = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    versions = relationship("DatasetVersion", back_populates="dataset", lazy="dynamic")


class DatasetVersion(Base):
    """Schema versions for datasets."""

    __tablename__ = "dataset_versions"

    dataset_version_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    dataset_name: str = Column(String(255), ForeignKey("datasets.dataset_name"), nullable=False, index=True)
    schema_version: str = Column(String(50), nullable=False)
    schema_json: dict[str, Any] = Column(JSONB, nullable=False)
    writer_min_version: str = Column(String(50), nullable=True)
    reader_min_version: str = Column(String(50), nullable=True)
    is_current: bool = Column(Boolean, default=True, nullable=False)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    dataset = relationship("Dataset", back_populates="versions")

    __table_args__ = (UniqueConstraint("dataset_name", "schema_version", name="uq_dataset_schema_version"),)


class FeedMapping(Base):
    """Maps provider feed names to canonical Silver dataset names."""

    __tablename__ = "feed_mappings"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    provider: str = Column(String(100), nullable=False, index=True)
    gateway_feed: str = Column(String(100), nullable=False)
    silver_dataset_name: str = Column(String(255), nullable=False)
    notes: str = Column(Text, nullable=True)

    __table_args__ = (UniqueConstraint("provider", "gateway_feed", name="uq_provider_feed"),)


class InstrumentRegistry(Base):
    """Canonical instrument registry."""

    __tablename__ = "instrument_registry"

    instrument_key: str = Column(String(255), primary_key=True)
    instrument_type: str = Column(String(50), nullable=False, index=True)
    canonical_symbol: str = Column(String(50), nullable=False, index=True)
    underlying_key: str = Column(String(255), nullable=True)  # For options
    occ_symbol: str = Column(String(50), nullable=True)
    expiry: datetime = Column(Date, nullable=True)
    strike: float = Column(Float, nullable=True)
    put_call: str = Column(String(1), nullable=True)  # P or C
    multiplier: int = Column(Integer, nullable=True, default=100)
    currency: str = Column(String(10), nullable=True, default="USD")
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: datetime = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    provider_mappings = relationship("InstrumentProviderMap", back_populates="instrument", lazy="dynamic")


class InstrumentProviderMap(Base):
    """Maps provider-specific symbols to canonical instrument keys."""

    __tablename__ = "instrument_provider_map"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    instrument_key: str = Column(
        String(255), ForeignKey("instrument_registry.instrument_key"), nullable=False, index=True
    )
    provider: str = Column(String(100), nullable=False, index=True)
    provider_symbol: str = Column(String(100), nullable=False, index=True)
    provider_id: str = Column(String(255), nullable=True)
    is_primary: bool = Column(Boolean, default=False, nullable=False)

    # Relationships
    instrument = relationship("InstrumentRegistry", back_populates="provider_mappings")

    __table_args__ = (UniqueConstraint("provider", "provider_symbol", name="uq_provider_symbol"),)


class DataCoverage(Base):
    """Fast lookup for what data exists per instrument."""

    __tablename__ = "data_coverage"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    dataset_name: str = Column(String(255), nullable=False, index=True)
    instrument_key: str = Column(String(255), nullable=False, index=True)
    dt_min: datetime = Column(Date, nullable=False)
    dt_max: datetime = Column(Date, nullable=False)
    last_updated_ts: datetime = Column(DateTime(timezone=True), server_default=func.now())
    approx_row_count: int = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("dataset_name", "instrument_key", name="uq_dataset_instrument"),
        Index("ix_dataset_instrument", "dataset_name", "instrument_key"),
    )


class Project(Base):
    """Project registry for tracking consumers."""

    __tablename__ = "projects"

    project_id: str = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    project_name: str = Column(String(100), unique=True, nullable=False, index=True)
    description: str = Column(Text, nullable=True)
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now())


class Request(Base):
    """Request tracking for REST pulls (PRD §11.2.2)."""

    __tablename__ = "requests"

    request_id: str = Column(String(255), primary_key=True)
    project_name: str = Column(String(100), ForeignKey("projects.project_name"), nullable=True, index=True)
    provider: str = Column(String(100), nullable=False, index=True)
    feed: str = Column(String(100), nullable=False, index=True)
    params_json: dict[str, Any] | None = Column(JSONB, nullable=True)
    status: str = Column(String(50), default="pending")  # pending, completed, failed
    created_at: datetime = Column(DateTime(timezone=True), server_default=func.now(), index=True)

    __table_args__ = (
        # Index for audit queries (PRD §11.3)
        # CREATE INDEX idx_requests_project_provider_feed ON requests(project_name, provider, feed, created_at)
    )


class Subscription(Base):
    """Subscription tracking for WebSocket streams (PRD §11.2.2)."""

    __tablename__ = "subscriptions"

    subscription_id: str = Column(String(255), primary_key=True)
    project_name: str = Column(String(100), ForeignKey("projects.project_name"), nullable=True, index=True)
    provider: str = Column(String(100), nullable=False, index=True)
    feed: str = Column(String(100), nullable=False, index=True)
    instrument_keys: list[str] | None = Column(JSONB, nullable=True)  # List of instrument_keys
    started_at: datetime = Column(DateTime(timezone=True), server_default=func.now())
    ended_at: datetime = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Index for audit queries (PRD §11.3)
        # CREATE INDEX idx_subscriptions_project ON subscriptions(project_name, provider, feed)
    )
