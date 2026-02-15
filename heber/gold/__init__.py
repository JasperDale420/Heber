"""Gold Layer Module (PRD §28-29).

Provides utilities for managing forward-looking labels with proper
availability tracking, train/test splits, and duration parsing.
"""

from heber.gold.duration import parse_duration
from heber.gold.labels import (
    LabelDataset,
    LabelMetadata,
    compute_availability_time,
    read_label,
    write_label,
)

__all__ = [
    "parse_duration",
    "LabelDataset",
    "LabelMetadata",
    "write_label",
    "read_label",
    "compute_availability_time",
]
