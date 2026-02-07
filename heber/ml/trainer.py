"""Meta-Model Training Pipeline.

Trains LightGBM models to predict which flow alerts will succeed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TrainingConfig:
    """Configuration for model training."""

    # Model hyperparameters
    n_estimators: int = 100
    max_depth: int = 6
    learning_rate: float = 0.1
    num_leaves: int = 31
    min_child_samples: int = 20
    subsample: float = 0.8
    colsample_bytree: float = 0.8

    # Training settings
    early_stopping_rounds: int = 10
    random_state: int = 42

    # Evaluation thresholds
    score_thresholds: list[float] = field(default_factory=lambda: [0.5, 0.55, 0.6, 0.65, 0.7])
    feature_names: list[str] = field(default_factory=list)

    # Output
    model_name: str = "meta_model"
    experiment_name: str = "heber_meta_labeling"


class MetaModelTrainer:
    """Trains meta-models for alert outcome prediction.

    Uses LightGBM for fast training with good tabular performance.
    Optionally logs to MLflow for experiment tracking.
    """

    def __init__(
        self,
        config: TrainingConfig | None = None,
        use_mlflow: bool = False,
        mlflow_tracking_uri: str | None = None,
    ):
        """Initialize trainer.

        Args:
            config: Training configuration
            use_mlflow: Whether to log to MLflow
            mlflow_tracking_uri: MLflow tracking server URI
        """
        self.config = config or TrainingConfig()
        self.use_mlflow = use_mlflow
        self.mlflow_tracking_uri = mlflow_tracking_uri
        self._model: Any = None

    def train(
        self,
        x_train: pl.DataFrame | np.ndarray,
        y_train: pl.Series | np.ndarray,
        x_val: pl.DataFrame | np.ndarray | None = None,
        y_val: pl.Series | np.ndarray | None = None,
        feature_names: list[str] | None = None,
    ) -> dict[str, float]:
        """Train the meta-model.

        Args:
            x_train: Training features
            y_train: Training labels (0/1)
            x_val: Validation features (optional)
            y_val: Validation labels (optional)
            feature_names: Feature column names

        Returns:
            Dict of evaluation metrics
        """
        try:
            import lightgbm as lgb
        except ImportError:
            logger.error("LightGBM not installed. Run: pip install lightgbm")
            raise

        # Convert to numpy if polars
        train_features = x_train.to_numpy() if hasattr(x_train, "to_numpy") else x_train
        train_labels = y_train.to_numpy() if hasattr(y_train, "to_numpy") else y_train

        # Handle validation set
        eval_set = None
        callbacks = None
        if x_val is not None and y_val is not None:
            val_features = x_val.to_numpy() if hasattr(x_val, "to_numpy") else x_val
            val_labels = y_val.to_numpy() if hasattr(y_val, "to_numpy") else y_val
            eval_set = [(val_features, val_labels)]
            callbacks = [lgb.early_stopping(self.config.early_stopping_rounds)]

        # Create and train model
        self._model = lgb.LGBMClassifier(
            n_estimators=self.config.n_estimators,
            max_depth=self.config.max_depth,
            learning_rate=self.config.learning_rate,
            num_leaves=self.config.num_leaves,
            min_child_samples=self.config.min_child_samples,
            subsample=self.config.subsample,
            colsample_bytree=self.config.colsample_bytree,
            random_state=self.config.random_state,
            n_jobs=-1,
            verbose=-1,
        )

        self._model.fit(
            train_features,
            train_labels,
            eval_set=eval_set,
            callbacks=callbacks,
        )

        # Compute metrics
        metrics = self._compute_metrics(train_features, train_labels, prefix="train")
        if x_val is not None and y_val is not None:
            val_metrics = self._compute_metrics(val_features, val_labels, prefix="val")
            metrics.update(val_metrics)

        # Persist training feature order for inference-time alignment.
        if feature_names:
            self.config.feature_names = list(feature_names)

        # Log to MLflow if enabled
        if self.use_mlflow:
            self._log_to_mlflow(metrics, feature_names)

        logger.info("Model training complete", **metrics)
        return metrics

    def _compute_metrics(
        self,
        X: np.ndarray,
        y: np.ndarray,
        prefix: str = "",
    ) -> dict[str, float]:
        """Compute classification metrics.

        Args:
            X: Features
            y: True labels
            prefix: Metric name prefix

        Returns:
            Dict of metric name to value
        """
        from sklearn.metrics import (
            accuracy_score,
            precision_score,
            recall_score,
            roc_auc_score,
        )

        y_pred = self._model.predict(X)
        y_proba = self._model.predict_proba(X)[:, 1]

        metrics = {
            f"{prefix}_accuracy": accuracy_score(y, y_pred),
            f"{prefix}_precision": precision_score(y, y_pred, zero_division=0),
            f"{prefix}_recall": recall_score(y, y_pred, zero_division=0),
            f"{prefix}_auc": roc_auc_score(y, y_proba) if len(np.unique(y)) > 1 else 0.5,
        }

        # Precision at various thresholds
        for thresh in self.config.score_thresholds:
            y_pred_thresh = (y_proba >= thresh).astype(int)
            prec = precision_score(y, y_pred_thresh, zero_division=0)
            metrics[f"{prefix}_precision_at_{int(thresh * 100)}"] = prec

        return metrics

    def _log_to_mlflow(
        self,
        metrics: dict[str, float],
        feature_names: list[str] | None = None,
    ) -> None:
        """Log training run to MLflow."""
        try:
            import mlflow
            import mlflow.sklearn
        except ImportError:
            logger.warning("MLflow not installed, skipping logging")
            return

        if self.mlflow_tracking_uri:
            mlflow.set_tracking_uri(self.mlflow_tracking_uri)

        mlflow.set_experiment(self.config.experiment_name)

        with mlflow.start_run():
            # Log parameters
            mlflow.log_params(
                {
                    "n_estimators": self.config.n_estimators,
                    "max_depth": self.config.max_depth,
                    "learning_rate": self.config.learning_rate,
                    "num_leaves": self.config.num_leaves,
                }
            )

            # Log metrics
            mlflow.log_metrics(metrics)

            # Log model
            mlflow.sklearn.log_model(
                self._model,
                self.config.model_name,
                registered_model_name=self.config.model_name,
            )

            # Log feature importance
            if feature_names and self._model is not None:
                importance = dict(zip(feature_names, self._model.feature_importances_, strict=False))
                mlflow.log_dict(importance, "feature_importance.json")

            logger.info("Logged run to MLflow")

    def predict_proba(self, X: pl.DataFrame | np.ndarray) -> np.ndarray:
        """Predict probability of TP hit.

        Args:
            X: Features

        Returns:
            Array of probabilities [0, 1]
        """
        if self._model is None:
            raise ValueError("Model not trained. Call train() first.")

        features = X.to_numpy() if hasattr(X, "to_numpy") else X
        return self._model.predict_proba(features)[:, 1]

    def predict(
        self,
        X: pl.DataFrame | np.ndarray,
        threshold: float = 0.5,
    ) -> np.ndarray:
        """Predict binary outcome.

        Args:
            X: Features
            threshold: Probability threshold for positive class

        Returns:
            Array of 0/1 predictions
        """
        proba = self.predict_proba(X)
        return (proba >= threshold).astype(int)

    def save(self, path: Path) -> None:
        """Save model to disk.

        Args:
            path: Output path (without extension)
        """
        if self._model is None:
            raise ValueError("No model to save")

        import joblib

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save model
        joblib.dump(self._model, path.with_suffix(".joblib"))

        # Save config
        config_dict = {
            "n_estimators": self.config.n_estimators,
            "max_depth": self.config.max_depth,
            "learning_rate": self.config.learning_rate,
            "num_leaves": self.config.num_leaves,
            "score_thresholds": self.config.score_thresholds,
            "feature_names": self.config.feature_names,
        }
        with open(path.with_suffix(".json"), "w") as f:
            json.dump(config_dict, f, indent=2)

        logger.info("Model saved", path=str(path))

    @classmethod
    def load(cls, path: Path) -> MetaModelTrainer:
        """Load model from disk.

        Args:
            path: Model path (without extension)

        Returns:
            MetaModelTrainer with loaded model
        """
        import joblib

        path = Path(path)
        trainer = cls()
        trainer._model = joblib.load(path.with_suffix(".joblib"))

        # Load config if exists
        config_path = path.with_suffix(".json")
        if config_path.exists():
            with open(config_path) as f:
                config_dict = json.load(f)
            trainer.config = TrainingConfig(**config_dict)

        logger.info("Model loaded", path=str(path))
        return trainer

    def get_feature_importance(
        self,
        feature_names: list[str],
    ) -> dict[str, float]:
        """Get feature importance scores.

        Args:
            feature_names: Feature column names

        Returns:
            Dict of feature name to importance score
        """
        if self._model is None:
            raise ValueError("Model not trained")

        return dict(zip(feature_names, self._model.feature_importances_, strict=False))


def train_meta_model(
    start_date: date,
    end_date: date,
    output_path: Path | None = None,
    use_mlflow: bool = False,
) -> MetaModelTrainer:
    """Convenience function to train a meta-model.

    Args:
        start_date: Start of training data
        end_date: End of training data
        output_path: Path to save model
        use_mlflow: Whether to log to MLflow

    Returns:
        Trained MetaModelTrainer
    """
    from heber.ml.datasets import MetaLabelDatasetBuilder

    # Build dataset
    builder = MetaLabelDatasetBuilder()
    df = builder.build_from_parquet(start_date, end_date)

    if df.is_empty():
        raise ValueError("No data found in date range")

    # Split
    train_df, val_df = builder.train_test_split(df)

    # Get features
    feature_cols = builder.get_feature_columns(df)
    x_train, y_train = builder.to_xy(train_df, feature_cols)
    x_val, y_val = builder.to_xy(val_df, feature_cols)

    # Train
    trainer = MetaModelTrainer(use_mlflow=use_mlflow)
    trainer.train(x_train, y_train, x_val, y_val, feature_cols)

    # Save if path provided
    if output_path:
        trainer.save(output_path)

    return trainer
