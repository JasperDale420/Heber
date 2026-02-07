from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from heber.ml.inference import MetaLabelScorer
from heber.ml.trainer import MetaModelTrainer, TrainingConfig


class _EstimatorStub:
    """Minimal estimator that mimics sklearn predict_proba output."""

    def predict_proba(self, X: np.ndarray) -> np.ndarray:  # noqa: N803
        assert X.shape[1] == 2
        return np.array([[0.2, 0.8]])


class _PersistableModel:
    def predict_proba(self, X: np.ndarray) -> np.ndarray:  # noqa: N803
        return np.array([[0.4, 0.6]])


class _FeatureStub:
    def __init__(self) -> None:
        self.called_with: list[str] | None = None

    def to_feature_array(self, feature_names: list[str] | None = None) -> list[float]:
        self.called_with = feature_names
        return [2.0, 1.0]


@pytest.mark.asyncio
async def test_scorer_uses_trained_feature_order_when_available() -> None:
    trainer = MetaModelTrainer(config=TrainingConfig(feature_names=["volume", "premium"]))
    trainer._model = _EstimatorStub()  # type: ignore[attr-defined]

    scorer = MetaLabelScorer(model=trainer)
    feature_stub = _FeatureStub()
    scorer._feature_extractor = SimpleNamespace(extract=lambda _alert: feature_stub)

    async def _extract(_alert):  # noqa: ANN001
        return feature_stub

    scorer._feature_extractor.extract = _extract  # type: ignore[method-assign]

    alert = SimpleNamespace(event_id="a1", underlying="AAPL")
    score = await scorer.score(alert)

    assert score == 0.8
    assert feature_stub.called_with == ["volume", "premium"]


def test_trainer_save_load_preserves_feature_names(tmp_path: Path) -> None:
    trainer = MetaModelTrainer(config=TrainingConfig(feature_names=["f1", "f2", "f3"]))
    trainer._model = _PersistableModel()  # type: ignore[attr-defined]

    model_path = tmp_path / "meta_model"
    trainer.save(model_path)

    loaded = MetaModelTrainer.load(model_path)
    assert loaded.config.feature_names == ["f1", "f2", "f3"]
