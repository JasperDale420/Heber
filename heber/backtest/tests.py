"""Tests for Backtest Integration (PRD §34)."""

import tempfile
from datetime import datetime, timedelta, UTC
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from heber.backtest.integration import (
    ExperimentConfig,
    BacktestResult,
    BacktestDataLoader,
    ExperimentTracker,
    generate_reproducibility_checklist,
)


class TestExperimentConfig:
    """Test ExperimentConfig dataclass."""
    
    def test_to_dict_roundtrip(self):
        config = ExperimentConfig(
            feature_dataset="momentum_features",
            feature_version="v3.2.1",
            label_dataset="returns_5d",
            train_period="12M",
            test_period="3M",
        )
        
        d = config.to_dict()
        restored = ExperimentConfig.from_dict(d)
        
        assert restored.feature_dataset == "momentum_features"
        assert restored.feature_version == "v3.2.1"
    
    def test_config_hash_deterministic(self):
        config1 = ExperimentConfig(
            feature_dataset="momentum",
            feature_version="v1",
            label_dataset="returns",
        )
        config2 = ExperimentConfig(
            feature_dataset="momentum",
            feature_version="v1",
            label_dataset="returns",
        )
        
        assert config1.config_hash() == config2.config_hash()
    
    def test_config_hash_different_for_different_configs(self):
        config1 = ExperimentConfig(
            feature_dataset="momentum",
            feature_version="v1",
            label_dataset="returns",
        )
        config2 = ExperimentConfig(
            feature_dataset="momentum",
            feature_version="v2",
            label_dataset="returns",
        )
        
        assert config1.config_hash() != config2.config_hash()


class TestBacktestResult:
    """Test BacktestResult persistence."""
    
    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = ExperimentConfig(
                feature_dataset="momentum",
                feature_version="v1",
                label_dataset="returns",
            )
            
            result = BacktestResult(
                experiment_id="test_123",
                config=config,
                metrics={"sharpe": 1.5, "accuracy": 0.65},
                started_at=datetime.now(UTC),
                completed_at=datetime.now(UTC) + timedelta(hours=1),
                fold_results=[{"fold": 0, "metrics": {"sharpe": 1.4}}],
            )
            
            path = Path(tmpdir) / "result.json"
            result.save(path)
            
            loaded = BacktestResult.load(path)
            
            assert loaded.experiment_id == "test_123"
            assert loaded.metrics["sharpe"] == 1.5
            assert len(loaded.fold_results) == 1


class TestBacktestDataLoader:
    """Test BacktestDataLoader."""
    
    def test_load_train_data(self):
        mock_client = MagicMock()
        mock_client.read_gold.return_value = pd.DataFrame({
            "instrument_key": ["AAPL"],
            "momentum_10d": [0.05],
        })
        
        loader = BacktestDataLoader(
            client=mock_client,
            feature_dataset="momentum_features",
            feature_version="v3",
            label_dataset="returns_5d",
        )
        
        features, labels = loader.load_train_data(
            train_start="2024-01-01",
            train_end="2024-06-01",
        )
        
        assert mock_client.read_gold.call_count == 2
        assert len(features) == 1
    
    def test_load_without_labels(self):
        mock_client = MagicMock()
        mock_client.read_gold.return_value = pd.DataFrame({
            "instrument_key": ["AAPL"],
            "momentum_10d": [0.05],
        })
        
        loader = BacktestDataLoader(
            client=mock_client,
            feature_dataset="momentum_features",
            feature_version="v3",
            label_dataset=None,
        )
        
        features, labels = loader.load_train_data(
            train_start="2024-01-01",
            train_end="2024-06-01",
        )
        
        assert mock_client.read_gold.call_count == 1
        assert labels is None


class TestExperimentTracker:
    """Test experiment tracking workflow."""
    
    def test_full_experiment_workflow(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = ExperimentTracker(results_dir=tmpdir)
            
            config = ExperimentConfig(
                feature_dataset="momentum",
                feature_version="v1",
                label_dataset="returns",
            )
            
            exp_id = tracker.start_experiment(config)
            assert exp_id is not None
            
            # Log some folds
            tracker.log_fold(0, {"sharpe": 1.2})
            tracker.log_fold(1, {"sharpe": 1.4})
            tracker.log_fold(2, {"sharpe": 1.3})
            
            result = tracker.end_experiment({"sharpe": 1.3, "accuracy": 0.6})
            
            assert result.experiment_id == exp_id
            assert len(result.fold_results) == 3
            assert result.metrics["sharpe"] == 1.3
    
    def test_list_and_load_experiments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tracker = ExperimentTracker(results_dir=tmpdir)
            
            config = ExperimentConfig(
                feature_dataset="momentum",
                feature_version="v1",
                label_dataset="returns",
            )
            
            tracker.start_experiment(config)
            result = tracker.end_experiment({"sharpe": 1.0})
            
            experiments = tracker.list_experiments()
            assert len(experiments) == 1
            
            loaded = tracker.load_experiment(experiments[0])
            assert loaded.metrics["sharpe"] == 1.0


class TestReproducibilityChecklist:
    """Test reproducibility checklist generation."""
    
    def test_generate_checklist(self):
        config = ExperimentConfig(
            feature_dataset="momentum",
            feature_version="v3.2.1",
            label_dataset="returns_5d",
            train_period="12M",
            test_period="3M",
            embargo="5d",
            model_params={"learning_rate": 0.01},
            random_seed=42,
            code_commit="abc123",
        )
        
        checklist = generate_reproducibility_checklist(config)
        
        assert checklist["feature_dataset"] == "momentum"
        assert checklist["feature_version"] == "v3.2.1"
        assert checklist["random_seed"] == 42
        assert checklist["code_commit"] == "abc123"
        assert "config_hash" in checklist


def run_all_backtest_tests() -> dict[str, bool]:
    """Run all backtest tests."""
    results = {}
    
    test_classes = [
        TestExperimentConfig,
        TestBacktestResult,
        TestBacktestDataLoader,
        TestExperimentTracker,
        TestReproducibilityChecklist,
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
    print(f"\nBacktest Integration Tests: {passed}/{total} passed")
    
    return results


if __name__ == "__main__":
    run_all_backtest_tests()
