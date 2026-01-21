"""Feast Materialization Pipeline (PRD §31.9).

Materializes features from offline (Parquet) to online (ClickHouse) store.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

import structlog

logger = structlog.get_logger(__name__)


def materialize_features(
    repo_path: str | Path = "features/",
    feature_views: list[str] | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    mode: Literal["incremental", "full"] = "incremental",
) -> dict[str, int]:
    """Materialize features from offline to online store.

    Args:
        repo_path: Path to Feast feature repository
        feature_views: Specific feature views to materialize (None = all)
        start_date: Start date for materialization window
        end_date: End date (default: now)
        mode: "incremental" or "full" materialization

    Returns:
        Dict mapping feature view name to rows materialized
    """
    try:
        from feast import FeatureStore
    except ImportError:
        logger.warning("Feast not installed, skipping materialization")
        return {}

    store = FeatureStore(repo_path=str(repo_path))
    end_date = end_date or datetime.now(UTC)

    results: dict[str, int] = {}

    if mode == "incremental":
        logger.info(
            "Running incremental materialization",
            end_date=end_date.isoformat(),
            feature_views=feature_views,
        )
        store.materialize_incremental(
            end_date=end_date,
            feature_views=feature_views,
        )
    else:
        start_date = start_date or (end_date - timedelta(days=90))
        logger.info(
            "Running full materialization",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            feature_views=feature_views,
        )
        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views,
        )

    all_views = feature_views or [fv.name for fv in store.list_feature_views()]
    for view_name in all_views:
        results[view_name] = -1

    logger.info("Materialization complete", results=results)
    return results


def get_historical_features(
    repo_path: str | Path,
    entity_df,
    features: list[str],
    full_feature_names: bool = True,
):
    """Get historical features for training (PRD §31.6).

    Wraps Feast's get_historical_features with Heber conventions.

    Args:
        repo_path: Path to Feast feature repository
        entity_df: DataFrame with (instrument_key, event_timestamp) columns
        features: List of feature references like "momentum_features:momentum_10d"
        full_feature_names: Whether to include feature view name in column names

    Returns:
        DataFrame with requested features joined to entity_df
    """
    try:
        from feast import FeatureStore
    except ImportError:
        raise ImportError("Feast is required for get_historical_features")

    store = FeatureStore(repo_path=str(repo_path))

    logger.info(
        "Getting historical features",
        num_entities=len(entity_df),
        num_features=len(features),
    )

    feature_vector = store.get_historical_features(
        entity_df=entity_df,
        features=features,
        full_feature_names=full_feature_names,
    )

    return feature_vector.to_df()


def get_online_features(
    repo_path: str | Path,
    features: list[str],
    entity_rows: list[dict[str, str]],
) -> dict[str, list]:
    """Get online features for inference (PRD §31.7).

    Low-latency feature lookup from online store.

    Args:
        repo_path: Path to Feast feature repository
        features: List of feature references
        entity_rows: List of entity key dicts like {"instrument_key": "equity:AAPL"}

    Returns:
        Dict mapping feature names to values
    """
    try:
        from feast import FeatureStore
    except ImportError:
        raise ImportError("Feast is required for get_online_features")

    store = FeatureStore(repo_path=str(repo_path))

    logger.info(
        "Getting online features",
        num_entities=len(entity_rows),
        num_features=len(features),
    )

    online_response = store.get_online_features(
        features=features,
        entity_rows=entity_rows,
    )

    return online_response.to_dict()


def list_feature_views(repo_path: str | Path = "features/") -> list[dict]:
    """List all registered feature views.

    Returns:
        List of feature view metadata dicts
    """
    try:
        from feast import FeatureStore
    except ImportError:
        return []

    store = FeatureStore(repo_path=str(repo_path))
    views = []

    for fv in store.list_feature_views():
        views.append(
            {
                "name": fv.name,
                "entities": [e.name for e in fv.entities],
                "features": [f.name for f in fv.features],
                "ttl_days": fv.ttl.days if fv.ttl else None,
                "online": fv.online,
                "tags": fv.tags,
            }
        )

    return views


def search_features(
    repo_path: str | Path = "features/",
    tags: list[str] | None = None,
    owner: str | None = None,
    category: str | None = None,
) -> list[dict]:
    """Search features by tags and metadata (PRD §31.10).

    Args:
        repo_path: Path to Feast repository
        tags: Filter by feature view tags
        owner: Filter by owner
        category: Filter by category

    Returns:
        List of matching feature metadata
    """
    views = list_feature_views(repo_path)
    results = []

    for view in views:
        view_tags = view.get("tags", {})

        if owner and view_tags.get("owner") != owner:
            continue
        if category and view_tags.get("category") != category:
            continue
        if tags:
            if not any(t in view_tags for t in tags):
                continue

        for feature in view.get("features", []):
            results.append(
                {
                    "feature_id": feature,
                    "feature_view": view["name"],
                    "owner": view_tags.get("owner"),
                    "category": view_tags.get("category"),
                    "tags": view_tags,
                }
            )

    return results
