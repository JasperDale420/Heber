"""Feast Integration Module (PRD §31).

Provides SDK wrappers for Feast feature store operations.
"""

from heber.feast.materialization import (
    get_historical_features,
    get_online_features,
    list_feature_views,
    materialize_features,
    search_features,
)

__all__ = [
    "materialize_features",
    "get_historical_features",
    "get_online_features",
    "list_feature_views",
    "search_features",
]
