"""Zero-Leakage Firewall - Core utilities for point-in-time correct queries.

Per PRD §10, this module provides the fundamental building blocks to prevent:
1. Transport/arrival leakage - Using data before it was available
2. Revision leakage - Using corrected data that wasn't available at the time
3. Enrichment leakage - Mixing outcomes into features
4. Label/target leakage - Future data in training features
5. Split leakage - Information bleed across train/test

CRITICAL RULE: All reads for research/backtest/ML must use ts_available <= T
"""

from heber.firewall.asof import read_asof, asof_join, read_asof_range
from heber.firewall.validation import (
    validate_asof_read,
    validate_gold_build,
    validate_train_test_split,
    LeakageError,
    GoldBuildMetadata,
)
from heber.firewall.scd import read_reference_asof, join_with_reference_asof
from heber.firewall.tests import run_all_leakage_tests, monitor_availability_lag

__all__ = [
    "read_asof",
    "asof_join",
    "read_asof_range",
    "validate_asof_read",
    "validate_gold_build",
    "validate_train_test_split",
    "read_reference_asof",
    "join_with_reference_asof",
    "LeakageError",
    "GoldBuildMetadata",
    "run_all_leakage_tests",
    "monitor_availability_lag",
]
