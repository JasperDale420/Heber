"""Label Management Module (PRD §29).

Provides utilities for managing forward-looking labels with proper
availability tracking to prevent leakage in ML training.
"""

from heber.gold.labels import (
    LabelDataset,
    LabelMetadata,
    compute_availability_time,
    read_label,
    write_label,
)

__all__ = [
    "LabelDataset",
    "LabelMetadata",
    "write_label",
    "read_label",
    "compute_availability_time",
]
