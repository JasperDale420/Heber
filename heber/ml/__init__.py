"""Heber ML Package - Machine learning utilities for meta-labeling.

This package provides:
- Dataset builders for training meta-models
- Training pipelines with MLflow integration
- Inference services for scoring alerts
"""

from heber.ml.datasets import DatasetConfig, MetaLabelDatasetBuilder
from heber.ml.inference import AlertGate, InferenceConfig, MetaLabelScorer
from heber.ml.trainer import MetaModelTrainer, TrainingConfig, train_meta_model

__all__ = [
    # Datasets
    "MetaLabelDatasetBuilder",
    "DatasetConfig",
    # Training
    "MetaModelTrainer",
    "TrainingConfig",
    "train_meta_model",
    # Inference
    "MetaLabelScorer",
    "InferenceConfig",
    "AlertGate",
]
