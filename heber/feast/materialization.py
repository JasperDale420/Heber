"""Feast Materialization Pipeline (PRD §31.9).

Materializes features from offline (Parquet) to online (ClickHouse) store.
"""

from __future__ import annotations

import glob
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import structlog

from heber.config import settings

logger = structlog.get_logger(__name__)

# Default path to Feast feature repository
DEFAULT_REPO_PATH = settings.feast_repo_path


def _extract_materialization_counts(materialize_result: object, view_names: list[str]) -> dict[str, int]:
    """Extract per-view row counts from Feast materialization responses when available."""
    if isinstance(materialize_result, dict):
        counts: dict[str, int] = {}
        for view_name in view_names:
            value = materialize_result.get(view_name)
            if isinstance(value, int):
                counts[view_name] = value
            elif isinstance(value, dict):
                nested = value.get("rows") or value.get("row_count") or value.get("count")
                if isinstance(nested, int):
                    counts[view_name] = nested
        if counts:
            return counts

    return {}


def _resolve_feature_view(store: object, view_name: str) -> object | None:
    """Resolve a feature view object by name from a Feast store."""
    getter = getattr(store, "get_feature_view", None)
    if callable(getter):
        try:
            return getter(view_name)  # type: ignore[no-any-return]
        except Exception:
            pass

    lister = getattr(store, "list_feature_views", None)
    if callable(lister):
        try:
            for feature_view in lister():
                if getattr(feature_view, "name", None) == view_name:
                    return feature_view  # type: ignore[no-any-return]
        except Exception:
            pass

    return None


def _count_view_rows_from_offline_source(
    feature_view: object,
    start_date: datetime | None,
    end_date: datetime,
) -> int:
    """Best-effort row count from file-backed offline source metadata."""
    source = getattr(feature_view, "source", None)
    source_path = getattr(source, "path", None)
    if not source_path:
        return 0

    try:
        import pyarrow.dataset as ds
    except ImportError:
        return 0

    path_pattern = str(source_path)
    if any(char in path_pattern for char in "*?[]"):
        matching_files = glob.glob(path_pattern)
        if not matching_files:
            return 0
        dataset = ds.dataset(matching_files, format="parquet")
    else:
        path = Path(path_pattern)
        if not path.exists():
            return 0
        dataset = ds.dataset(path_pattern, format="parquet")
    timestamp_field = getattr(source, "timestamp_field", None)
    if timestamp_field and timestamp_field in dataset.schema.names:
        end_expr = ds.field(timestamp_field) <= end_date
        if start_date:
            start_expr = ds.field(timestamp_field) >= start_date
            return int(dataset.count_rows(filter=start_expr & end_expr))
        return int(dataset.count_rows(filter=end_expr))

    return int(dataset.count_rows())


def _estimate_materialization_counts(
    store: object,
    view_names: list[str],
    start_date: datetime | None,
    end_date: datetime,
) -> dict[str, int]:
    """Estimate per-view row counts from offline sources when Feast doesn't report counts."""
    counts: dict[str, int] = {}
    for view_name in view_names:
        feature_view = _resolve_feature_view(store, view_name)
        if not feature_view:
            counts[view_name] = 0
            continue
        try:
            counts[view_name] = _count_view_rows_from_offline_source(feature_view, start_date, end_date)
        except Exception as exc:
            logger.warning("failed_to_estimate_materialization_count", feature_view=view_name, error=str(exc))
            counts[view_name] = 0
    return counts


def _tag_matches(view_tags: dict[str, str], tag_filter: str) -> bool:
    """Match tag filters against key and value, including key:value expressions."""
    if ":" in tag_filter:
        key, value = tag_filter.split(":", 1)
        return str(view_tags.get(key, "")).lower() == value.lower()

    needle = tag_filter.lower()
    if needle in (str(k).lower() for k in view_tags):
        return True
    return needle in (str(v).lower() for v in view_tags.values())


def materialize_features(
    repo_path: str | Path = DEFAULT_REPO_PATH,
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

    repo_path = repo_path or DEFAULT_REPO_PATH
    store = FeatureStore(repo_path=str(repo_path))
    end_date = end_date or datetime.now(UTC)

    # Feast type stubs declare materialize/materialize_incremental as returning
    # None, but the runtime implementation can return count dicts. We capture
    # the return value defensively for _extract_materialization_counts.
    materialize_result: object = None

    if mode == "incremental":
        logger.info(
            "Running incremental materialization",
            end_date=end_date.isoformat(),
            feature_views=feature_views,
        )
        materialize_result = store.materialize_incremental(  # type: ignore[func-returns-value]
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
        materialize_result = store.materialize(  # type: ignore[func-returns-value]
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views,
        )

    all_views = feature_views or [fv.name for fv in store.list_feature_views()]
    results = _extract_materialization_counts(materialize_result, all_views)
    if len(results) != len(all_views):
        estimated = _estimate_materialization_counts(store, all_views, start_date, end_date)
        for view_name in all_views:
            results.setdefault(view_name, estimated.get(view_name, 0))

    logger.info("Materialization complete", results=results)
    return results


def get_historical_features(
    repo_path: str | Path,
    entity_df: Any,
    features: list[str],
    full_feature_names: bool = True,
) -> Any:
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
) -> dict[str, list[object]]:
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


def list_feature_views(repo_path: str | Path = DEFAULT_REPO_PATH) -> list[dict[str, Any]]:
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
                "entities": [str(e) for e in (fv.entities or [])],
                "features": [f.name for f in (fv.features or [])],
                "ttl_days": fv.ttl.days if fv.ttl else None,
                "online": fv.online,
                "tags": fv.tags,
            }
        )

    return views


def search_features(
    repo_path: str | Path = DEFAULT_REPO_PATH,
    tags: list[str] | None = None,
    owner: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
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
        if tags and not all(_tag_matches(view_tags, tag_filter) for tag_filter in tags):
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
