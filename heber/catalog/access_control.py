"""Access Control (PRD §11.9).

Project-based access control for Gold datasets, shared Silver datasets,
and SDK token enforcement.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


# DataLayer is the canonical enum from heber.retention (superset with HOT_STORE, DLQ).
# Access control only uses BRONZE/SILVER/GOLD but importing the canonical avoids duplication.
from heber.retention import DataLayer  # noqa: E402


class AccessLevel(str, Enum):
    """Access permission levels."""

    NONE = "none"
    READ = "read"
    WRITE = "write"
    ADMIN = "admin"


@dataclass
class Project:
    """Project definition for access control."""

    project_id: str
    name: str
    description: str = ""
    owner: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "name": self.name,
            "description": self.description,
            "owner": self.owner,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class DatasetPermission:
    """Permission for a dataset within a project."""

    project_id: str
    dataset: str
    layer: DataLayer
    access_level: AccessLevel

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_id": self.project_id,
            "dataset": self.dataset,
            "layer": self.layer.value,
            "access_level": self.access_level.value,
        }


@dataclass
class SDKToken:
    """SDK access token for authentication."""

    token_id: str
    project_id: str
    token_hash: str  # Hashed token value
    name: str
    scopes: list[str] = field(default_factory=list)
    expires_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    revoked: bool = False

    def is_valid(self) -> bool:
        """Check if token is valid."""
        if self.revoked:
            return False
        return not (self.expires_at and datetime.now(UTC) > self.expires_at)

    def has_scope(self, scope: str) -> bool:
        """Check if token has a scope."""
        return scope in self.scopes or "*" in self.scopes

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "project_id": self.project_id,
            "name": self.name,
            "scopes": self.scopes,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "created_at": self.created_at.isoformat(),
            "revoked": self.revoked,
            "is_valid": self.is_valid(),
        }


class AccessControlManager:
    """Manage access control for Heber datasets.

    Design principles (PRD §11.9):
    - Silver datasets are shared (read-only for all projects)
    - Gold datasets are restricted per project
    - SDK tokens enforce access policies
    """

    def __init__(self):
        self.projects: dict[str, Project] = {}
        self.permissions: dict[str, list[DatasetPermission]] = {}  # project_id -> permissions
        self.tokens: dict[str, SDKToken] = {}  # token_id -> token

        # Default: Silver is shared
        self.shared_layers = {DataLayer.SILVER}

    def create_project(self, project_id: str, name: str, owner: str = "") -> Project:
        """Create a new project."""
        project = Project(
            project_id=project_id,
            name=name,
            owner=owner,
        )
        self.projects[project_id] = project
        self.permissions[project_id] = []
        logger.info("Created project", project_id=project_id, name=name)
        return project

    def get_project(self, project_id: str) -> Project | None:
        """Get project by ID."""
        return self.projects.get(project_id)

    def grant_permission(
        self,
        project_id: str,
        dataset: str,
        layer: DataLayer,
        access_level: AccessLevel,
    ) -> DatasetPermission:
        """Grant dataset permission to a project."""
        if project_id not in self.permissions:
            self.permissions[project_id] = []

        permission = DatasetPermission(
            project_id=project_id,
            dataset=dataset,
            layer=layer,
            access_level=access_level,
        )

        # Update or add permission
        existing = [p for p in self.permissions[project_id] if p.dataset == dataset and p.layer == layer]
        if existing:
            existing[0].access_level = access_level
        else:
            self.permissions[project_id].append(permission)

        logger.info(
            "Granted permission",
            project_id=project_id,
            dataset=dataset,
            layer=layer.value,
            access_level=access_level.value,
        )
        return permission

    def check_access(
        self,
        project_id: str,
        dataset: str,
        layer: DataLayer,
        required_level: AccessLevel = AccessLevel.READ,
    ) -> bool:
        """Check if a project has access to a dataset.

        Returns True if:
        - Layer is in shared_layers (Silver by default)
        - Project has explicit permission >= required_level
        """
        # Shared layers allow read access for all
        if layer in self.shared_layers and required_level == AccessLevel.READ:
            return True

        # Check explicit permissions
        permissions = self.permissions.get(project_id, [])
        for perm in permissions:
            if perm.dataset == dataset and perm.layer == layer:
                return self._access_level_gte(perm.access_level, required_level)

        # Check wildcard permission for the layer
        for perm in permissions:
            if perm.dataset == "*" and perm.layer == layer:
                return self._access_level_gte(perm.access_level, required_level)

        return False

    def _access_level_gte(self, actual: AccessLevel, required: AccessLevel) -> bool:
        """Check if actual access level is >= required."""
        levels = [AccessLevel.NONE, AccessLevel.READ, AccessLevel.WRITE, AccessLevel.ADMIN]
        return levels.index(actual) >= levels.index(required)

    def create_token(
        self,
        project_id: str,
        name: str,
        scopes: list[str] | None = None,
        expires_in_days: int | None = None,
    ) -> tuple[str, SDKToken]:
        """Create an SDK token.

        Returns:
            Tuple of (raw_token, SDKToken)
        """
        raw_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        token_id = f"tok_{secrets.token_hex(8)}"

        expires_at = None
        if expires_in_days:
            expires_at = datetime.now(UTC) + timedelta(days=expires_in_days)

        token = SDKToken(
            token_id=token_id,
            project_id=project_id,
            token_hash=token_hash,
            name=name,
            scopes=scopes or ["read"],
            expires_at=expires_at,
        )

        self.tokens[token_id] = token
        logger.info("Created SDK token", token_id=token_id, project_id=project_id)

        return raw_token, token

    def validate_token(self, raw_token: str) -> SDKToken | None:
        """Validate a raw token and return the SDKToken if valid."""
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()

        for token in self.tokens.values():
            if token.token_hash == token_hash and token.is_valid():
                return token

        return None

    def revoke_token(self, token_id: str) -> bool:
        """Revoke a token."""
        token = self.tokens.get(token_id)
        if token:
            token.revoked = True
            logger.info("Revoked token", token_id=token_id)
            return True
        return False

    def list_project_permissions(self, project_id: str) -> list[dict[str, Any]]:
        """List all permissions for a project."""
        permissions = self.permissions.get(project_id, [])
        return [p.to_dict() for p in permissions]

    def list_project_tokens(self, project_id: str) -> list[dict[str, Any]]:
        """List all tokens for a project."""
        return [t.to_dict() for t in self.tokens.values() if t.project_id == project_id]

    def generate_report(self) -> dict[str, Any]:
        """Generate access control report."""
        return {
            "summary": {
                "total_projects": len(self.projects),
                "total_tokens": len(self.tokens),
                "active_tokens": sum(1 for t in self.tokens.values() if t.is_valid()),
            },
            "shared_layers": [layer.value for layer in self.shared_layers],
            "projects": [p.to_dict() for p in self.projects.values()],
            "generated_at": datetime.now(UTC).isoformat(),
        }


# Default scopes for SDK tokens
DEFAULT_SCOPES = {
    "read": "Read access to permitted datasets",
    "write": "Write access to permitted datasets",
    "admin": "Administrative access",
    "*": "Full access",
}
