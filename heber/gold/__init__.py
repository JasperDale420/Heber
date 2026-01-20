"""Gold Dataset Versioning & Reproducibility (PRD §28).

This package provides versioning, lineage tracking, and compatibility checks
for Gold layer datasets, enabling reproducible ML experiments and backtests.
"""

from heber.gold.versioning import (
    GoldVersion,
    VersionLineage,
    VersionManifest,
    resolve_version,
    check_compatibility,
    CompatibilityResult,
)

__all__ = [
    "GoldVersion",
    "VersionLineage",
    "VersionManifest",
    "resolve_version",
    "check_compatibility",
    "CompatibilityResult",
]
