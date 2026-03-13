"""Regression tests for Feast materialization/search technical debt fixes."""

from __future__ import annotations

import sys
import types
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from heber.config import settings
from heber.feast.materialization import DEFAULT_REPO_PATH, materialize_features, search_features


def _install_fake_feast(monkeypatch, store: MagicMock) -> None:
    feast_module = types.ModuleType("feast")
    # Mark as package so downstream imports of ``feast.types`` are valid.
    feast_module.__path__ = []  # type: ignore[attr-defined]

    feast_types = types.ModuleType("feast.types")
    feast_types.Float32 = type("Float32", (), {})
    feast_types.Int64 = type("Int64", (), {})
    feast_types.String = type("String", (), {})

    def _feature_store(*, repo_path: str):
        store._repo_path = repo_path
        return store

    feast_module.FeatureStore = _feature_store
    feast_module.types = feast_types
    monkeypatch.setitem(sys.modules, "feast", feast_module)
    monkeypatch.setitem(sys.modules, "feast.types", feast_types)


def test_materialize_uses_settings_default_repo_path(monkeypatch) -> None:
    mock_store = MagicMock()
    mock_store.materialize_incremental.return_value = {"flow_features": 3}
    mock_store.list_feature_views.return_value = [SimpleNamespace(name="flow_features")]
    _install_fake_feast(monkeypatch, mock_store)

    results = materialize_features(repo_path=None, feature_views=["flow_features"], mode="incremental")

    assert mock_store._repo_path == str(DEFAULT_REPO_PATH)
    assert settings.feast_repo_path == DEFAULT_REPO_PATH
    assert results["flow_features"] == 3


def test_materialize_uses_reported_counts_when_available(monkeypatch) -> None:
    mock_store = MagicMock()
    mock_store.materialize.return_value = {"flow_features": {"rows": 7}}
    mock_store.list_feature_views.return_value = [SimpleNamespace(name="flow_features")]
    _install_fake_feast(monkeypatch, mock_store)

    results = materialize_features(
        repo_path="features/",
        feature_views=["flow_features"],
        mode="full",
        start_date=datetime(2026, 2, 1, tzinfo=UTC),
        end_date=datetime(2026, 2, 7, tzinfo=UTC),
    )

    assert results == {"flow_features": 7}


def test_materialize_estimates_counts_from_offline_sources(monkeypatch, tmp_path: Path) -> None:
    table = pa.table(
        {
            "ts_event": [
                datetime(2026, 2, 6, 10, 0, tzinfo=UTC),
                datetime(2026, 2, 6, 11, 0, tzinfo=UTC),
                datetime(2026, 2, 8, 11, 0, tzinfo=UTC),
            ],
            "value": [1.0, 2.0, 3.0],
        }
    )
    data_file = tmp_path / "part-1.parquet"
    pq.write_table(table, data_file)

    mock_store = MagicMock()
    mock_store.materialize_incremental.return_value = None
    mock_store.get_feature_view.return_value = SimpleNamespace(
        name="flow_features",
        source=SimpleNamespace(path=str(tmp_path / "*.parquet"), timestamp_field="ts_event"),
    )
    mock_store.list_feature_views.return_value = [SimpleNamespace(name="flow_features")]
    _install_fake_feast(monkeypatch, mock_store)

    results = materialize_features(
        repo_path="features/",
        feature_views=["flow_features"],
        mode="incremental",
        start_date=datetime(2026, 2, 6, tzinfo=UTC),
        end_date=datetime(2026, 2, 7, 23, 59, tzinfo=UTC),
    )

    assert results == {"flow_features": 2}


def test_search_features_matches_tag_values_and_key_value_filters(monkeypatch) -> None:
    mock_views = [
        {
            "name": "flow_features",
            "features": ["net_premium_24h"],
            "tags": {"owner": "quant_team", "category": "flow", "data_source": "unusual_whales"},
        },
        {
            "name": "momentum_features",
            "features": ["momentum_10d"],
            "tags": {"owner": "research", "category": "technical"},
        },
    ]
    monkeypatch.setattr("heber.feast.materialization.list_feature_views", lambda _: mock_views)

    by_value = search_features(tags=["quant_team"])
    by_key_value = search_features(tags=["owner:quant_team", "category:flow"])
    non_match = search_features(tags=["owner:data_team"])

    assert len(by_value) == 1
    assert by_value[0]["feature_view"] == "flow_features"
    assert len(by_key_value) == 1
    assert by_key_value[0]["feature_view"] == "flow_features"
    assert non_match == []
