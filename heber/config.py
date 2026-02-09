"""Heber configuration using Pydantic Settings."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Heber application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_prefix="HEBER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Storage
    data_root: Path = Field(
        default=Path("/Volumes/heber/data"),
        description="Root path for Bronze/Silver/Gold data",
    )
    volume_root: Path = Field(
        default=Path("/Volumes/heber"),
        description="Root path for external volume",
    )

    # Postgres (Catalog)
    postgres_url: str = Field(
        default="postgresql+asyncpg://heber:heber_dev_password@localhost:5433/heber_catalog",
        description="PostgreSQL connection URL for Catalog DB",
    )

    # Redis (Event Bus)
    redis_url: str = Field(
        default="redis://localhost:6380",
        description="Redis connection URL for event streams",
    )
    redis_stream_name: str = Field(
        default="heber:events",
        description="Redis stream name for incoming events",
    )
    redis_consumer_group: str = Field(
        default="heber-writers",
        description="Redis consumer group name",
    )
    redis_dlq_stream_name: str = Field(
        default="heber:events:dlq",
        description="Redis stream for failed consumer messages",
    )
    redis_claim_idle_ms: int = Field(
        default=60_000,
        description="Minimum idle time before claiming pending stream messages",
    )
    redis_claim_batch_size: int = Field(
        default=100,
        description="Max pending messages to claim per recovery cycle",
    )
    redis_process_max_retries: int = Field(
        default=3,
        description="Retry attempts for a stream message before DLQ",
    )
    redis_retry_backoff_seconds: float = Field(
        default=0.25,
        description="Base backoff delay between processing retries",
    )

    # ClickHouse (Hot Store)
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=9000)
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")
    clickhouse_database: str = Field(default="heber")

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8080)
    catalog_url: str = Field(
        default="http://localhost:8085/api/v1",
        description="Catalog API base URL used by SDK clients",
    )
    feast_repo_path: Path = Field(
        default=Path("features"),
        validation_alias=AliasChoices("HEBER_FEAST_REPO_PATH", "FEAST_REPO_PATH"),
        description="Path to Feast feature repository",
    )

    # Writer settings (PRD §7.5 - File sizing, batching, compaction)
    bronze_flush_interval_seconds: int = Field(default=30, description="Max time before flushing Bronze")
    bronze_max_batch_size: int = Field(default=10000, description="Max events per Bronze file")

    # Silver file sizing targets (PRD §7.5)
    silver_target_file_size_mb: int = Field(default=256, description="Target Parquet file size (128-512 MB)")
    silver_max_rows_per_file: int = Field(default=1_000_000, description="Max rows per file (250k-2M)")
    silver_max_flush_time_seconds: int = Field(default=30, description="Max seconds before flush (5-30s)")
    silver_row_group_size_mb: int = Field(default=128, description="Parquet row group size (64-256 MB)")

    # Environment
    environment: Literal["dev", "staging", "prod"] = Field(default="dev")

    # Gold layer paths (used by feature_views/_paths.py)
    gold_root: Path | None = Field(default=None, description="Override for gold data root (defaults to data_root/gold)")
    gold_project: str = Field(default="*", description="Glob pattern for gold project dirs")
    gold_version: str = Field(default="*", description="Glob pattern for gold version dirs")

    # Soda quality checks
    soda_checks_dir: Path = Field(
        default=Path("soda/checks"),
        validation_alias=AliasChoices("HEBER_SODA_CHECKS_DIR", "SODA_CHECKS_DIR"),
    )

    # Backfill service
    backfill_host: str = Field(default="0.0.0.0")
    backfill_port: int = Field(default=8080)
    backfill_log_level: str = Field(default="info")

    # Hot store writer
    hotloader_datasets: str = Field(
        default="quotes,trades,bars",
        description="Comma-separated datasets for hot loading",
    )
    hotloader_silver_base_path: str | None = Field(default=None, description="Override silver base path for hot loader")

    # Watch consumer
    watch_redis_url: str = Field(
        default="redis://localhost:6379",
        validation_alias=AliasChoices("HEBER_WATCH_REDIS_URL", "HEBER_REDIS_URL"),
    )
    watch_gateway_url: str = Field(
        default="http://localhost:8000",
        validation_alias=AliasChoices("HEBER_WATCH_GATEWAY_URL", "DATA_GATEWAY_URL"),
    )

    # Quarantine
    quarantine_path: str = Field(default="quarantine")

    # Metrics
    metrics_port: int | None = Field(default=None, description="Prometheus metrics port")

    # Ops
    service_name: str = Field(
        default="heber",
        validation_alias=AliasChoices("HEBER_SERVICE_NAME", "SERVICE_NAME"),
    )
    instance_id: str = Field(
        default="unknown",
        validation_alias=AliasChoices("HEBER_INSTANCE_ID", "INSTANCE_ID", "HOSTNAME"),
    )
    service_version: str = Field(
        default="0.1.0",
        validation_alias=AliasChoices("HEBER_SERVICE_VERSION", "SERVICE_VERSION"),
    )
    shutdown_timeout_seconds: int = Field(default=30)

    # OpenTelemetry
    otel_endpoint: str = Field(
        default="http://localhost:4317",
        validation_alias=AliasChoices("HEBER_OTEL_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"),
    )

    # OpenMetadata catalog
    openmetadata_host: str = Field(
        default="http://localhost:8585",
        validation_alias=AliasChoices("HEBER_OPENMETADATA_HOST", "OPENMETADATA_HOST"),
    )
    openmetadata_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("HEBER_OPENMETADATA_API_KEY", "OPENMETADATA_API_KEY"),
    )

    # Schema Registry
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        validation_alias=AliasChoices("HEBER_SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_URL"),
    )
    schema_registry_user: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_SCHEMA_REGISTRY_USER", "SCHEMA_REGISTRY_USER"),
    )
    schema_registry_password: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_SCHEMA_REGISTRY_PASSWORD", "SCHEMA_REGISTRY_PASSWORD"),
    )

    # Iceberg catalog
    iceberg_catalog_type: str = Field(
        default="sql",
        validation_alias=AliasChoices("HEBER_ICEBERG_CATALOG_TYPE", "ICEBERG_CATALOG_TYPE"),
    )
    iceberg_catalog_uri: str = Field(
        default="sqlite:///iceberg_catalog.db",
        validation_alias=AliasChoices("HEBER_ICEBERG_CATALOG_URI", "ICEBERG_CATALOG_URI"),
    )
    iceberg_warehouse: str = Field(
        default="s3://heber-lakehouse/warehouse",
        validation_alias=AliasChoices("HEBER_ICEBERG_WAREHOUSE", "ICEBERG_WAREHOUSE"),
    )
    iceberg_s3_endpoint: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_ICEBERG_S3_ENDPOINT", "ICEBERG_S3_ENDPOINT"),
    )
    iceberg_s3_access_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_ICEBERG_S3_ACCESS_KEY", "ICEBERG_S3_ACCESS_KEY"),
    )
    iceberg_s3_secret_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_ICEBERG_S3_SECRET_KEY", "ICEBERG_S3_SECRET_KEY"),
    )

    # LakeFS versioning
    lakefs_endpoint: str = Field(
        default="http://localhost:8000",
        validation_alias=AliasChoices("HEBER_LAKEFS_ENDPOINT", "LAKEFS_ENDPOINT"),
    )
    lakefs_access_key: str = Field(
        default="",
        validation_alias=AliasChoices("HEBER_LAKEFS_ACCESS_KEY", "LAKEFS_ACCESS_KEY"),
    )
    lakefs_secret_key: str = Field(
        default="",
        validation_alias=AliasChoices("HEBER_LAKEFS_SECRET_KEY", "LAKEFS_SECRET_KEY"),
    )
    lakefs_default_repo: str = Field(
        default="heber-gold",
        validation_alias=AliasChoices("HEBER_LAKEFS_DEFAULT_REPO", "LAKEFS_DEFAULT_REPO"),
    )
    lakefs_storage_namespace_base: str = Field(
        default="s3://heber-lakehouse",
        validation_alias=AliasChoices("HEBER_LAKEFS_STORAGE_NAMESPACE_BASE", "LAKEFS_STORAGE_NAMESPACE_BASE"),
    )
    lakefs_storage_namespace_template: str | None = Field(
        default=None,
        validation_alias=AliasChoices("HEBER_LAKEFS_STORAGE_NAMESPACE_TEMPLATE", "LAKEFS_STORAGE_NAMESPACE_TEMPLATE"),
    )

    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        if self.gold_root:
            return self.gold_root
        return self.data_root / "gold"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


# Convenience alias
settings = get_settings()
