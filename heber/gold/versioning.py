"""Gold Dataset Versioning Module (PRD §28).

Provides semantic versioning, lineage tracking, and compatibility checking
for Gold layer datasets.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Literal

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class GoldVersion:
    """Semantic version for Gold datasets (PRD §28.2).
    
    Format: v{major}.{minor}.{patch}
    
    - Major: Breaking schema changes
    - Minor: New columns (backward compatible)
    - Patch: Bug fixes, recomputation
    """
    
    major: int
    minor: int
    patch: int
    
    def __str__(self) -> str:
        return f"v{self.major}.{self.minor}.{self.patch}"
    
    def __lt__(self, other: GoldVersion) -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __le__(self, other: GoldVersion) -> bool:
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GoldVersion):
            return False
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)
    
    def __hash__(self) -> int:
        return hash((self.major, self.minor, self.patch))
    
    @classmethod
    def parse(cls, version_str: str) -> GoldVersion:
        """Parse version string like 'v3.2.1' or '3.2.1'.
        
        Args:
            version_str: Version string
            
        Returns:
            GoldVersion instance
            
        Raises:
            ValueError: If version string is invalid
        """
        pattern = r"v?(\d+)\.(\d+)\.(\d+)"
        match = re.match(pattern, version_str)
        if not match:
            raise ValueError(f"Invalid version string: {version_str}")
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
        )
    
    def matches_wildcard(self, pattern: str) -> bool:
        """Check if this version matches a wildcard pattern.
        
        Patterns:
        - "v3.*" matches any v3.x.y
        - "v3.2.*" matches any v3.2.x
        - "v3.2.1" matches exactly v3.2.1
        
        Args:
            pattern: Wildcard pattern
            
        Returns:
            True if this version matches the pattern
        """
        pattern = pattern.lstrip("v")
        parts = pattern.split(".")
        version_parts = [self.major, self.minor, self.patch]
        
        for i, part in enumerate(parts):
            if part == "*":
                return True
            if int(part) != version_parts[i]:
                return False
        return True


@dataclass
class UpstreamDependency:
    """Upstream dataset dependency in lineage tracking."""
    
    dataset: str
    layer: str
    version: str


@dataclass
class VersionLineage:
    """Lineage metadata for a Gold version (PRD §28.4).
    
    Tracks:
    - Upstream Silver dataset dependencies
    - Code commit that produced this version
    - Config hash for reproducibility
    """
    
    upstream_deps: list[UpstreamDependency] = field(default_factory=list)
    code_commit: str | None = None
    config_hash: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "upstream_deps": [
                {"dataset": d.dataset, "layer": d.layer, "version": d.version}
                for d in self.upstream_deps
            ],
            "code_commit": self.code_commit,
            "config_hash": self.config_hash,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VersionLineage:
        """Create from dictionary."""
        return cls(
            upstream_deps=[
                UpstreamDependency(**d) for d in data.get("upstream_deps", [])
            ],
            code_commit=data.get("code_commit"),
            config_hash=data.get("config_hash"),
        )


@dataclass
class SchemaChange:
    """Records a schema change between versions."""
    
    change_type: Literal["added_column", "removed_column", "deprecated_column", "type_change"]
    column: str
    details: str | None = None


@dataclass
class CompatibilityResult:
    """Result of version compatibility check (PRD §28.3)."""
    
    compatible: bool
    breaking: bool
    changes: list[SchemaChange] = field(default_factory=list)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            "compatible": self.compatible,
            "breaking": self.breaking,
            "changes": [
                {"type": c.change_type, "column": c.column, "details": c.details}
                for c in self.changes
            ],
        }


@dataclass
class VersionManifest:
    """Version manifest stored with each Gold version (PRD §28.4-28.5).
    
    Stored at: gold/dataset={name}/version={version}/_manifest.json
    
    Enforces immutability: once published, contents cannot change.
    """
    
    version: GoldVersion
    created_at: datetime
    created_by: str
    lineage: VersionLineage
    immutable: bool = True
    schema_columns: list[str] = field(default_factory=list)
    row_count: int | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON storage."""
        return {
            "version": str(self.version),
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "immutable": self.immutable,
            "lineage": self.lineage.to_dict(),
            "schema_columns": self.schema_columns,
            "row_count": self.row_count,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VersionManifest:
        """Create from dictionary."""
        return cls(
            version=GoldVersion.parse(data["version"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            created_by=data["created_by"],
            lineage=VersionLineage.from_dict(data.get("lineage", {})),
            immutable=data.get("immutable", True),
            schema_columns=data.get("schema_columns", []),
            row_count=data.get("row_count"),
        )
    
    def save(self, manifest_path: Path) -> None:
        """Save manifest to JSON file.
        
        Args:
            manifest_path: Path to _manifest.json
            
        Raises:
            ValueError: If trying to overwrite an immutable manifest
        """
        if manifest_path.exists():
            existing = VersionManifest.load(manifest_path)
            if existing.immutable:
                raise ValueError(
                    f"Cannot overwrite immutable version {self.version}. "
                    "Create a new patch version instead."
                )
        
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manifest_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        
        logger.info(
            "Saved version manifest",
            version=str(self.version),
            path=str(manifest_path),
        )
    
    @classmethod
    def load(cls, manifest_path: Path) -> VersionManifest:
        """Load manifest from JSON file.
        
        Args:
            manifest_path: Path to _manifest.json
            
        Returns:
            VersionManifest instance
            
        Raises:
            FileNotFoundError: If manifest doesn't exist
        """
        with open(manifest_path) as f:
            data = json.load(f)
        return cls.from_dict(data)


def resolve_version(
    available_versions: list[GoldVersion],
    pattern: str | None,
) -> GoldVersion | None:
    """Resolve version pattern to specific version (PRD §28.2).
    
    Args:
        available_versions: List of available versions
        pattern: Version pattern (exact, wildcard, or None for latest)
        
    Returns:
        Resolved GoldVersion or None if no match
        
    Examples:
        resolve_version([v1.0.0, v3.2.1, v3.5.0], "v3.*")  -> v3.5.0
        resolve_version([v1.0.0, v3.2.1], "v3.2.1")        -> v3.2.1
        resolve_version([v1.0.0, v3.2.1], None)            -> v3.2.1 (latest)
    """
    if not available_versions:
        return None
    
    sorted_versions = sorted(available_versions, reverse=True)
    
    if pattern is None:
        return sorted_versions[0]
    
    if "*" in pattern:
        for version in sorted_versions:
            if version.matches_wildcard(pattern):
                return version
        return None
    
    try:
        target = GoldVersion.parse(pattern)
        return target if target in available_versions else None
    except ValueError:
        logger.warning("Invalid version pattern", pattern=pattern)
        return None


def check_compatibility(
    from_manifest: VersionManifest,
    to_manifest: VersionManifest,
) -> CompatibilityResult:
    """Check compatibility between two versions (PRD §28.3).
    
    Detects:
    - Added columns (compatible)
    - Removed columns (breaking)
    - Deprecated columns (warning)
    
    Args:
        from_manifest: Source version manifest
        to_manifest: Target version manifest
        
    Returns:
        CompatibilityResult with details
    """
    from_cols = set(from_manifest.schema_columns)
    to_cols = set(to_manifest.schema_columns)
    
    changes: list[SchemaChange] = []
    breaking = False
    
    added = to_cols - from_cols
    for col in added:
        changes.append(SchemaChange(
            change_type="added_column",
            column=col,
        ))
    
    removed = from_cols - to_cols
    for col in removed:
        changes.append(SchemaChange(
            change_type="removed_column",
            column=col,
        ))
        breaking = True
    
    compatible = (
        from_manifest.version.major == to_manifest.version.major
        and not breaking
    )
    
    logger.info(
        "Version compatibility check",
        from_version=str(from_manifest.version),
        to_version=str(to_manifest.version),
        compatible=compatible,
        breaking=breaking,
        num_changes=len(changes),
    )
    
    return CompatibilityResult(
        compatible=compatible,
        breaking=breaking,
        changes=changes,
    )


def list_available_versions(gold_root: Path, dataset: str) -> list[GoldVersion]:
    """List all available versions for a Gold dataset.
    
    Args:
        gold_root: Root path to gold layer (e.g., /data/gold)
        dataset: Dataset name
        
    Returns:
        List of available GoldVersion instances, sorted newest first
    """
    dataset_path = gold_root / f"dataset={dataset}"
    if not dataset_path.exists():
        return []
    
    versions: list[GoldVersion] = []
    for version_dir in dataset_path.iterdir():
        if version_dir.is_dir() and version_dir.name.startswith("version="):
            version_str = version_dir.name.replace("version=", "")
            try:
                versions.append(GoldVersion.parse(version_str))
            except ValueError:
                logger.warning(
                    "Invalid version directory",
                    path=str(version_dir),
                )
                continue
    
    return sorted(versions, reverse=True)


def get_manifest_path(gold_root: Path, dataset: str, version: GoldVersion) -> Path:
    """Get path to version manifest file.
    
    Args:
        gold_root: Root path to gold layer
        dataset: Dataset name
        version: Version
        
    Returns:
        Path to _manifest.json
    """
    return (
        gold_root 
        / f"dataset={dataset}" 
        / f"version={version}" 
        / "_manifest.json"
    )
