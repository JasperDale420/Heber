"""Tests for Feast Integration (PRD §31)."""

import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

# Mock feast module before importing materialization
mock_feast = MagicMock()
sys.modules["feast"] = mock_feast

# Now import after mocking
from heber.feast.materialization import (
    search_features,
)


class TestMaterializeFunctions:
    """Test materialization utilities."""
    
    def test_materialize_incremental(self):
        mock_store = MagicMock()
        mock_store.list_feature_views.return_value = [
            MagicMock(name="momentum_features"),
        ]
        mock_feast.FeatureStore.return_value = mock_store
        
        from heber.feast.materialization import materialize_features
        
        result = materialize_features(
            repo_path="features/",
            mode="incremental",
        )
        
        mock_store.materialize_incremental.assert_called_once()
        assert isinstance(result, dict)


class TestHistoricalFeatures:
    """Test historical feature retrieval."""
    
    def test_get_historical_features(self):
        import pandas as pd
        
        mock_store = MagicMock()
        mock_feature_vector = MagicMock()
        mock_feature_vector.to_df.return_value = pd.DataFrame({
            "instrument_key": ["equity:AAPL"],
            "momentum_10d": [0.05],
        })
        mock_store.get_historical_features.return_value = mock_feature_vector
        mock_feast.FeatureStore.return_value = mock_store
        
        from heber.feast.materialization import get_historical_features
        
        entity_df = pd.DataFrame({
            "instrument_key": ["equity:AAPL"],
            "event_timestamp": [datetime.now()],
        })
        
        result = get_historical_features(
            repo_path="features/",
            entity_df=entity_df,
            features=["momentum_features:momentum_10d"],
        )
        
        assert "momentum_10d" in result.columns


class TestOnlineFeatures:
    """Test online feature retrieval."""
    
    def test_get_online_features(self):
        mock_store = MagicMock()
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {
            "instrument_key": ["equity:AAPL"],
            "momentum_10d": [0.05],
        }
        mock_store.get_online_features.return_value = mock_response
        mock_feast.FeatureStore.return_value = mock_store
        
        from heber.feast.materialization import get_online_features
        
        result = get_online_features(
            repo_path="features/",
            features=["momentum_features:momentum_10d"],
            entity_rows=[{"instrument_key": "equity:AAPL"}],
        )
        
        assert "instrument_key" in result
        assert "momentum_10d" in result


class TestSearchFeatures:
    """Test feature search functionality."""
    
    def test_search_by_owner(self):
        with patch("heber.feast.materialization.list_feature_views") as mock_list:
            mock_list.return_value = [
                {
                    "name": "momentum_features",
                    "features": ["momentum_10d"],
                    "tags": {"owner": "quant_team", "category": "technical"},
                },
                {
                    "name": "other_features",
                    "features": ["other"],
                    "tags": {"owner": "data_team"},
                },
            ]
            
            results = search_features(
                repo_path="features/",
                owner="quant_team",
            )
            
            assert len(results) == 1
            assert results[0]["feature_view"] == "momentum_features"
    
    def test_search_by_category(self):
        with patch("heber.feast.materialization.list_feature_views") as mock_list:
            mock_list.return_value = [
                {
                    "name": "momentum_features",
                    "features": ["momentum_10d", "rsi_14"],
                    "tags": {"category": "technical"},
                },
            ]
            
            results = search_features(
                repo_path="features/",
                category="technical",
            )
            
            assert len(results) == 2


class TestListFeatureViews:
    """Test feature view listing."""
    
    def test_list_views_returns_metadata(self):
        mock_fv = MagicMock()
        mock_fv.name = "momentum_features"
        mock_fv.entities = [MagicMock(name="equity")]
        mock_fv.features = [MagicMock(name="momentum_10d")]
        mock_fv.ttl = timedelta(days=90)
        mock_fv.online = True
        mock_fv.tags = {"owner": "quant_team"}
        
        mock_store = MagicMock()
        mock_store.list_feature_views.return_value = [mock_fv]
        mock_feast.FeatureStore.return_value = mock_store
        
        from heber.feast.materialization import list_feature_views
        
        views = list_feature_views()
        
        assert len(views) == 1
        assert views[0]["name"] == "momentum_features"


def run_all_feast_tests() -> dict[str, bool]:
    """Run all Feast integration tests."""
    results = {}
    
    test_classes = [
        TestMaterializeFunctions,
        TestHistoricalFeatures,
        TestOnlineFeatures,
        TestSearchFeatures,
        TestListFeatureViews,
    ]
    
    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nFeast Integration Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_feast_tests()
