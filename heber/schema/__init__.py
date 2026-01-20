"""Schema evolution for Heber per PRD §14.

Provides:
- Schema registry with versioning
- Backward/forward compatibility checking
- Migration utilities
- Reader/writer version checks
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, UTC
from enum import Enum
from typing import Any

import structlog
from prometheus_client import Counter, Gauge

logger = structlog.get_logger(__name__)


# Prometheus metrics
schema_version_checks = Counter(
    "heber_schema_version_checks_total",
    "Schema version compatibility checks",
    ["dataset", "result"],
)

schema_migrations = Counter(
    "heber_schema_migrations_total",
    "Schema migrations performed",
    ["dataset", "from_version", "to_version"],
)

active_schema_versions = Gauge(
    "heber_active_schema_versions",
    "Number of active schema versions",
    ["dataset"],
)


class SchemaChangeType(str, Enum):
    """Types of schema changes per PRD §14.2."""
    ADD_OPTIONAL_COLUMN = "add_optional_column"      # ✅ Allowed
    ADD_REQUIRED_COLUMN = "add_required_column"      # ❌ Not allowed
    REMOVE_COLUMN = "remove_column"                  # ⚠️ Deprecate first
    RENAME_COLUMN = "rename_column"                  # ❌ Not allowed
    WIDEN_TYPE = "widen_type"                        # ✅ Allowed (int32→int64)
    NARROW_TYPE = "narrow_type"                      # ❌ Not allowed
    INCOMPATIBLE_TYPE = "incompatible_type"          # ❌ Not allowed


class CompatibilityResult(str, Enum):
    """Result of compatibility check."""
    COMPATIBLE = "compatible"
    BACKWARD_INCOMPATIBLE = "backward_incompatible"
    FORWARD_INCOMPATIBLE = "forward_incompatible"
    DEPRECATED = "deprecated"


@dataclass
class ColumnSchema:
    """Schema for a single column."""
    name: str
    dtype: str  # e.g., "int64", "string", "float64", "timestamp"
    nullable: bool = True
    default: Any = None
    description: str = ""
    deprecated_at: datetime | None = None


@dataclass
class SchemaVersion:
    """A versioned schema per PRD §14.3.
    
    Version format: v<major>.<minor> (e.g., v1.0, v1.1, v2.0)
    - Minor bump: Backward-compatible changes
    - Major bump: Breaking changes
    """
    dataset: str
    version: str  # e.g., "v1.0"
    columns: list[ColumnSchema]
    is_current: bool = False
    writer_min_version: str = "0.0.0"  # Minimum SDK version to write
    reader_min_version: str = "0.0.0"  # Minimum SDK version to read
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    deprecated_at: datetime | None = None
    
    @property
    def major(self) -> int:
        """Get major version number."""
        match = re.match(r"v(\d+)\.(\d+)", self.version)
        return int(match.group(1)) if match else 0
    
    @property
    def minor(self) -> int:
        """Get minor version number."""
        match = re.match(r"v(\d+)\.(\d+)", self.version)
        return int(match.group(2)) if match else 0
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "version": self.version,
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "nullable": c.nullable,
                    "default": c.default,
                    "description": c.description,
                }
                for c in self.columns
            ],
            "is_current": self.is_current,
            "writer_min_version": self.writer_min_version,
            "reader_min_version": self.reader_min_version,
            "created_at": self.created_at.isoformat(),
            "deprecated_at": self.deprecated_at.isoformat() if self.deprecated_at else None,
        }
    
    def to_json_schema(self) -> str:
        """Export as JSON schema string."""
        return json.dumps(self.to_dict(), indent=2)


# Type widening rules per PRD §14.2
TYPE_WIDENING_ALLOWED = {
    ("int8", "int16"): True,
    ("int8", "int32"): True,
    ("int8", "int64"): True,
    ("int16", "int32"): True,
    ("int16", "int64"): True,
    ("int32", "int64"): True,
    ("float32", "float64"): True,
}


class CompatibilityChecker:
    """Checks schema compatibility per PRD §14.1-14.2."""
    
    def check_backward_compatible(
        self,
        old_schema: SchemaVersion,
        new_schema: SchemaVersion,
    ) -> tuple[bool, list[str]]:
        """Check if new schema is backward compatible with old.
        
        Backward compatible = new readers can read old data.
        """
        issues = []
        
        old_columns = {c.name: c for c in old_schema.columns}
        new_columns = {c.name: c for c in new_schema.columns}
        
        # Check for removed columns (must be deprecated first)
        for name in old_columns:
            if name not in new_columns:
                old_col = old_columns[name]
                if old_col.deprecated_at is None:
                    issues.append(f"Column '{name}' removed without deprecation")
        
        # Check for type changes
        for name, old_col in old_columns.items():
            if name in new_columns:
                new_col = new_columns[name]
                if old_col.dtype != new_col.dtype:
                    # Check if it's a valid widening
                    if not TYPE_WIDENING_ALLOWED.get((old_col.dtype, new_col.dtype), False):
                        issues.append(
                            f"Column '{name}' type change from {old_col.dtype} to "
                            f"{new_col.dtype} is not backward compatible"
                        )
        
        # Check new required columns
        for name, new_col in new_columns.items():
            if name not in old_columns:
                if not new_col.nullable and new_col.default is None:
                    issues.append(
                        f"New required column '{name}' without default breaks backward compat"
                    )
        
        return len(issues) == 0, issues
    
    def check_forward_compatible(
        self,
        old_schema: SchemaVersion,
        new_schema: SchemaVersion,
    ) -> tuple[bool, list[str]]:
        """Check if old readers can handle new data.
        
        Forward compatible = old readers gracefully ignore unknown columns.
        """
        issues = []
        
        # Forward compatibility mainly requires:
        # - New columns should be optional (so old readers can ignore)
        # - No type changes that would break old readers
        
        old_columns = {c.name: c for c in old_schema.columns}
        new_columns = {c.name: c for c in new_schema.columns}
        
        for name, new_col in new_columns.items():
            if name not in old_columns:
                if not new_col.nullable:
                    issues.append(
                        f"New required column '{name}' may break old readers"
                    )
        
        return len(issues) == 0, issues
    
    def validate_change(
        self,
        old_schema: SchemaVersion,
        new_schema: SchemaVersion,
    ) -> CompatibilityResult:
        """Validate a schema change and return compatibility result."""
        backward_ok, backward_issues = self.check_backward_compatible(old_schema, new_schema)
        forward_ok, forward_issues = self.check_forward_compatible(old_schema, new_schema)
        
        if backward_ok and forward_ok:
            return CompatibilityResult.COMPATIBLE
        elif not backward_ok:
            return CompatibilityResult.BACKWARD_INCOMPATIBLE
        else:
            return CompatibilityResult.FORWARD_INCOMPATIBLE


class SchemaRegistry:
    """Schema registry backed by Catalog per PRD §14.4.
    
    dataset_versions table responsibilities:
    - Store JSON schema per version
    - Track is_current flag
    - Record writer_min_version and reader_min_version
    """
    
    def __init__(self):
        self._versions: dict[str, list[SchemaVersion]] = {}
        self._checker = CompatibilityChecker()
    
    def register_version(
        self,
        schema: SchemaVersion,
        force: bool = False,
    ) -> tuple[bool, list[str]]:
        """Register a new schema version.
        
        Args:
            schema: The schema version to register
            force: Skip compatibility checks (use for major version bumps)
            
        Returns:
            Tuple of (success, issues)
        """
        dataset = schema.dataset
        
        if dataset not in self._versions:
            self._versions[dataset] = []
        
        versions = self._versions[dataset]
        
        # Check compatibility with current version
        current = self.get_current_version(dataset)
        if current and not force:
            result = self._checker.validate_change(current, schema)
            
            if result == CompatibilityResult.BACKWARD_INCOMPATIBLE:
                # Major version bump required
                if schema.major <= current.major:
                    return False, [
                        f"Breaking change requires major version bump. "
                        f"Current: {current.version}, New: {schema.version}"
                    ]
        
        # Add version
        versions.append(schema)
        
        # Update current flag
        if schema.is_current:
            for v in versions:
                if v.version != schema.version:
                    v.is_current = False
        
        active_schema_versions.labels(dataset=dataset).set(
            len([v for v in versions if v.deprecated_at is None])
        )
        
        logger.info(
            "schema_version_registered",
            dataset=dataset,
            version=schema.version,
            is_current=schema.is_current,
        )
        
        return True, []
    
    def get_current_version(self, dataset: str) -> SchemaVersion | None:
        """Get the current schema version for a dataset."""
        versions = self._versions.get(dataset, [])
        for v in versions:
            if v.is_current:
                return v
        return versions[-1] if versions else None
    
    def get_version(self, dataset: str, version: str) -> SchemaVersion | None:
        """Get a specific schema version."""
        versions = self._versions.get(dataset, [])
        for v in versions:
            if v.version == version:
                return v
        return None
    
    def list_versions(self, dataset: str) -> list[SchemaVersion]:
        """List all versions for a dataset."""
        return self._versions.get(dataset, [])
    
    def deprecate_version(
        self,
        dataset: str,
        version: str,
    ) -> bool:
        """Mark a version as deprecated."""
        schema = self.get_version(dataset, version)
        if schema:
            schema.deprecated_at = datetime.now(UTC)
            logger.info(
                "schema_version_deprecated",
                dataset=dataset,
                version=version,
            )
            return True
        return False
    
    def check_reader_compatibility(
        self,
        dataset: str,
        version: str,
        sdk_version: str,
    ) -> tuple[bool, str | None]:
        """Check if SDK version can read this schema per PRD §14.4."""
        schema = self.get_version(dataset, version)
        if not schema:
            return False, f"Version {version} not found"
        
        if self._version_lt(sdk_version, schema.reader_min_version):
            schema_version_checks.labels(dataset=dataset, result="incompatible").inc()
            return False, (
                f"SDK version {sdk_version} is too old to read {version}. "
                f"Minimum required: {schema.reader_min_version}"
            )
        
        schema_version_checks.labels(dataset=dataset, result="compatible").inc()
        return True, None
    
    def check_writer_compatibility(
        self,
        dataset: str,
        version: str,
        sdk_version: str,
    ) -> tuple[bool, str | None]:
        """Check if SDK version can write this schema per PRD §14.4."""
        schema = self.get_version(dataset, version)
        if not schema:
            return False, f"Version {version} not found"
        
        if self._version_lt(sdk_version, schema.writer_min_version):
            schema_version_checks.labels(dataset=dataset, result="incompatible").inc()
            return False, (
                f"SDK version {sdk_version} is too old to write {version}. "
                f"Minimum required: {schema.writer_min_version}"
            )
        
        schema_version_checks.labels(dataset=dataset, result="compatible").inc()
        return True, None
    
    def _version_lt(self, v1: str, v2: str) -> bool:
        """Check if version v1 < v2."""
        def parse(v: str) -> tuple:
            parts = v.split(".")
            return tuple(int(p) for p in parts if p.isdigit())
        return parse(v1) < parse(v2)


class SchemaMigrator:
    """Schema migration utilities per PRD §14.5."""
    
    def __init__(self, registry: SchemaRegistry):
        self.registry = registry
    
    def migrate_workflow(
        self,
        dataset: str,
        new_schema: SchemaVersion,
        run_backfill: bool = False,
    ) -> list[str]:
        """Execute schema migration workflow per PRD §14.5.
        
        Steps:
        1. Add new version with is_current = false
        2. (External) Deploy new writers
        3. Set is_current = true
        4. (Optional) Run backfill
        5. Deprecate old version
        6. (Later) Remove old version after grace period
        """
        steps_completed = []
        
        # Step 1: Add new version (not current yet)
        new_schema.is_current = False
        success, issues = self.registry.register_version(new_schema)
        if not success:
            raise ValueError(f"Failed to register version: {issues}")
        steps_completed.append(f"Added version {new_schema.version}")
        
        # Step 3: Set as current
        new_schema.is_current = True
        for v in self.registry.list_versions(dataset):
            v.is_current = (v.version == new_schema.version)
        steps_completed.append(f"Set {new_schema.version} as current")
        
        # Step 5: Deprecate old versions
        for v in self.registry.list_versions(dataset):
            if v.version != new_schema.version and v.deprecated_at is None:
                self.registry.deprecate_version(dataset, v.version)
                steps_completed.append(f"Deprecated version {v.version}")
        
        schema_migrations.labels(
            dataset=dataset,
            from_version="any",
            to_version=new_schema.version,
        ).inc()
        
        return steps_completed


def normalize_schema(
    data: list[dict[str, Any]],
    source_version: SchemaVersion,
    target_version: SchemaVersion,
) -> list[dict[str, Any]]:
    """Normalize data from source schema to target schema per PRD §14.6.
    
    Fill missing columns, cast types, apply defaults.
    """
    source_cols = {c.name: c for c in source_version.columns}
    target_cols = {c.name: c for c in target_version.columns}
    
    normalized = []
    for row in data:
        new_row = {}
        
        for col_name, col_schema in target_cols.items():
            if col_name in row:
                # Column exists - may need type casting
                new_row[col_name] = row[col_name]
            else:
                # Column missing - use default
                if col_schema.default is not None:
                    new_row[col_name] = col_schema.default
                elif col_schema.nullable:
                    new_row[col_name] = None
                else:
                    raise ValueError(
                        f"Missing required column '{col_name}' with no default"
                    )
        
        normalized.append(new_row)
    
    return normalized


# Global registry singleton
_registry: SchemaRegistry | None = None


def get_schema_registry() -> SchemaRegistry:
    """Get the global schema registry."""
    global _registry
    if _registry is None:
        _registry = SchemaRegistry()
    return _registry
