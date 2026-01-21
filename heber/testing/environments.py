"""Test Environments (PRD §52).

Environment configurations for local, CI, staging, and production.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class EnvironmentType(str, Enum):
    """Test environment types (PRD §52.1)."""

    LOCAL = "local"
    CI = "ci"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class EnvironmentConfig:
    """Environment configuration (PRD §52.1)."""

    env_type: EnvironmentType
    purpose: str
    data_source: str
    isolation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "env_type": self.env_type.value,
            "purpose": self.purpose,
            "data_source": self.data_source,
            "isolation": self.isolation,
        }


# Default environments from PRD §52.1
DEFAULT_ENVIRONMENTS: list[EnvironmentConfig] = [
    EnvironmentConfig(
        env_type=EnvironmentType.LOCAL,
        purpose="Developer testing",
        data_source="Synthetic",
        isolation="Full (Docker Compose)",
    ),
    EnvironmentConfig(
        env_type=EnvironmentType.CI,
        purpose="Automated tests",
        data_source="Synthetic + Golden",
        isolation="Ephemeral containers",
    ),
    EnvironmentConfig(
        env_type=EnvironmentType.STAGING,
        purpose="Pre-production validation",
        data_source="Sampled production",
        isolation="Shared, refreshed weekly",
    ),
    EnvironmentConfig(
        env_type=EnvironmentType.PRODUCTION,
        purpose="Live system",
        data_source="Real data",
        isolation="N/A",
    ),
]


@dataclass
class StagingConfig:
    """Staging environment configuration (PRD §52.4)."""

    component: str
    config: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component,
            "config": self.config,
        }


# Staging config from PRD §52.4
DEFAULT_STAGING_CONFIG: list[StagingConfig] = [
    StagingConfig("EKS cluster", "3 nodes (smaller than prod)"),
    StagingConfig("RDS", "db.t3.small (Postgres)"),
    StagingConfig("Redis", "t3.micro"),
    StagingConfig("S3", "Separate bucket (heber-staging)"),
    StagingConfig("ClickHouse", "Single node"),
]


@dataclass
class DockerComposeService:
    """Docker Compose service definition."""

    name: str
    image: str
    ports: list[str] = field(default_factory=list)
    environment: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "image": self.image,
            "ports": self.ports,
            "environment": self.environment,
        }


# Local dev services from PRD §47.2
DEFAULT_LOCAL_SERVICES: list[DockerComposeService] = [
    DockerComposeService(
        name="postgres",
        image="postgres:15",
        ports=["5432:5432"],
        environment={"POSTGRES_DB": "heber_test", "POSTGRES_PASSWORD": "test"},
    ),
    DockerComposeService(
        name="redis",
        image="redis:7",
        ports=["6379:6379"],
    ),
    DockerComposeService(
        name="minio",
        image="minio/minio",
        ports=["9000:9000", "9001:9001"],
        environment={"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"},
    ),
    DockerComposeService(
        name="clickhouse",
        image="clickhouse/clickhouse-server",
        ports=["8123:8123", "9000:9000"],
    ),
]


class EnvironmentManager:
    """Manage test environments."""

    def __init__(
        self,
        environments: list[EnvironmentConfig] | None = None,
        staging_config: list[StagingConfig] | None = None,
        local_services: list[DockerComposeService] | None = None,
    ):
        self.environments = {e.env_type: e for e in (environments or DEFAULT_ENVIRONMENTS)}
        self.staging_config = staging_config or DEFAULT_STAGING_CONFIG
        self.local_services = local_services or DEFAULT_LOCAL_SERVICES

    def get_environment(self, env_type: EnvironmentType) -> EnvironmentConfig | None:
        """Get environment configuration."""
        return self.environments.get(env_type)

    def list_all(self) -> list[dict[str, Any]]:
        """List all environments."""
        return [e.to_dict() for e in self.environments.values()]

    def get_local_services(self) -> list[dict[str, Any]]:
        """Get local Docker Compose services."""
        return [s.to_dict() for s in self.local_services]

    def get_staging_config(self) -> list[dict[str, Any]]:
        """Get staging environment configuration."""
        return [s.to_dict() for s in self.staging_config]

    def generate_docker_compose(self) -> str:
        """Generate Docker Compose YAML for local testing."""
        lines = ["version: '3.8'", "services:"]

        for service in self.local_services:
            lines.append(f"  {service.name}:")
            lines.append(f"    image: {service.image}")

            if service.ports:
                lines.append("    ports:")
                for port in service.ports:
                    lines.append(f'      - "{port}"')

            if service.environment:
                lines.append("    environment:")
                for key, value in service.environment.items():
                    lines.append(f"      {key}: {value}")

        return "\n".join(lines)

    def generate_report(self) -> dict[str, Any]:
        """Generate environment report."""
        return {
            "environments": [e.to_dict() for e in self.environments.values()],
            "staging_config": [s.to_dict() for s in self.staging_config],
            "local_services": [s.to_dict() for s in self.local_services],
            "generated_at": datetime.now(UTC).isoformat(),
        }
