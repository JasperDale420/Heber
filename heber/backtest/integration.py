"""Backtest Integration Utilities (PRD §34).

Provides data loading helpers, experiment tracking, and result storage
for ML backtesting workflows.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ExperimentConfig:
    """Configuration for a backtest experiment (PRD §34.4).

    Captures all metadata needed for reproducibility.
    """

    feature_dataset: str
    feature_version: str
    label_dataset: str
    label_version: str = "latest"
    feature_asof_time: str | None = None
    label_asof_time: str | None = None
    train_period: str = "12M"
    test_period: str = "3M"
    embargo: str = "5d"
    model_params: dict[str, Any] = field(default_factory=dict)
    random_seed: int = 42
    code_commit: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_dataset": self.feature_dataset,
            "feature_version": self.feature_version,
            "label_dataset": self.label_dataset,
            "label_version": self.label_version,
            "feature_asof_time": self.feature_asof_time,
            "label_asof_time": self.label_asof_time,
            "train_period": self.train_period,
            "test_period": self.test_period,
            "embargo": self.embargo,
            "model_params": self.model_params,
            "random_seed": self.random_seed,
            "code_commit": self.code_commit,
        }

    def config_hash(self) -> str:
        """Generate a hash of the config for deduplication."""
        config_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:12]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExperimentConfig:
        return cls(**data)


@dataclass
class BacktestResult:
    """Result of a backtest run."""

    experiment_id: str
    config: ExperimentConfig
    metrics: dict[str, float]
    started_at: datetime
    completed_at: datetime
    fold_results: list[dict[str, Any]] = field(default_factory=list)
    dataset_asof_times: dict[str, str | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "config": self.config.to_dict(),
            "metrics": self.metrics,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "fold_results": self.fold_results,
            "dataset_asof_times": self.dataset_asof_times,
        }

    def save(self, path: Path) -> None:
        """Save result to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> BacktestResult:
        """Load result from JSON file."""
        with open(path) as f:
            data = json.load(f)
        return cls(
            experiment_id=data["experiment_id"],
            config=ExperimentConfig.from_dict(data["config"]),
            metrics=data["metrics"],
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"]),
            fold_results=data.get("fold_results", []),
            dataset_asof_times=data.get("dataset_asof_times", {}),
        )


class BacktestDataLoader:
    """Helper for loading data in backtesting workflows (PRD §34.2).

    Provides point-in-time correct data loading with consistent
    asof_time handling across features and labels.
    """

    def __init__(
        self,
        client: Any,  # HeberClient
        feature_dataset: str,
        feature_version: str = "latest",
        label_dataset: str | None = None,
        label_version: str = "latest",
    ):
        """Initialize data loader.

        Args:
            client: HeberClient instance
            feature_dataset: Name of feature dataset
            feature_version: Feature version (default: latest)
            label_dataset: Optional label dataset name
            label_version: Label version (default: latest)
        """
        self.client = client
        self.feature_dataset = feature_dataset
        self.feature_version = feature_version
        self.label_dataset = label_dataset
        self.label_version = label_version

    def load_train_data(
        self,
        train_start: datetime | str,
        train_end: datetime | str,
        asof_time: datetime | str | None = None,
        instrument_keys: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """Load training data for a split.

        Args:
            train_start: Start of training period
            train_end: End of training period
            asof_time: Point-in-time cutoff (defaults to train_end)
            instrument_keys: Optional filter for specific symbols

        Returns:
            Tuple of (features_df, labels_df or None)
        """
        return self._load_data_split(
            start=train_start,
            end=train_end,
            split_name="training",
            asof_time=asof_time,
            instrument_keys=instrument_keys,
        )

    def load_test_data(
        self,
        test_start: datetime | str,
        test_end: datetime | str,
        asof_time: datetime | str | None = None,
        instrument_keys: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """Load test data for a split.

        Args:
            test_start: Start of test period
            test_end: End of test period
            asof_time: Point-in-time cutoff (defaults to test_end)
            instrument_keys: Optional filter for specific symbols

        Returns:
            Tuple of (features_df, labels_df or None)
        """
        return self._load_data_split(
            start=test_start,
            end=test_end,
            split_name="test",
            asof_time=asof_time,
            instrument_keys=instrument_keys,
        )

    def _load_data_split(
        self,
        start: datetime | str,
        end: datetime | str,
        split_name: str,
        asof_time: datetime | str | None = None,
        instrument_keys: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """Load data for a specific split (internal helper)."""
        if asof_time is None:
            asof_time = end

        logger.info(
            f"Loading {split_name} data",
            start=str(start),
            end=str(end),
            asof_time=str(asof_time),
        )

        # Load features
        features = self.client.read_gold(
            dataset=self.feature_dataset,
            version=self.feature_version,
            time_range=(start, end),
            asof_time=asof_time,
            instrument_keys=instrument_keys,
        )

        # Load labels if configured
        labels = None
        if self.label_dataset:
            labels = self.client.read_gold(
                dataset=self.label_dataset,
                version=self.label_version,
                time_range=(start, end),
                asof_time=asof_time,
                instrument_keys=instrument_keys,
            )

        return features, labels


class ExperimentTracker:
    """Simple experiment tracker for backtest results (PRD §34.3).

    For full ML lifecycle, use MLflow or W&B.
    """

    def __init__(self, results_dir: Path | str):
        """Initialize tracker.

        Args:
            results_dir: Directory to store experiment results
        """
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self._current_experiment: str | None = None
        self._start_time: datetime | None = None
        self._config: ExperimentConfig | None = None
        self._fold_results: list[dict[str, Any]] = []

    def start_experiment(self, config: ExperimentConfig) -> str:
        """Start a new experiment.

        Args:
            config: Experiment configuration

        Returns:
            Experiment ID
        """
        self._config = config
        self._start_time = datetime.now(UTC)
        self._fold_results = []

        # Generate experiment ID from config hash + timestamp
        timestamp = self._start_time.strftime("%Y%m%d_%H%M%S")
        self._current_experiment = f"{config.config_hash()}_{timestamp}"

        logger.info(
            "Started experiment",
            experiment_id=self._current_experiment,
            config=config.to_dict(),
        )

        return self._current_experiment

    def log_fold(
        self,
        fold_idx: int,
        metrics: dict[str, float],
        asof_time: datetime | str | None = None,
    ) -> None:
        """Log metrics for a single fold.

        Args:
            fold_idx: Fold index
            metrics: Fold metrics
            asof_time: Optional split as-of timestamp for reproducibility
        """
        asof_value: str | None = None
        if isinstance(asof_time, datetime):
            asof_value = asof_time.isoformat()
        elif isinstance(asof_time, str):
            asof_value = asof_time

        self._fold_results.append(
            {
                "fold": fold_idx,
                "metrics": metrics,
                "timestamp": datetime.now(UTC).isoformat(),
                "asof_time": asof_value,
            }
        )

        logger.info(
            "Logged fold results",
            fold=fold_idx,
            metrics=metrics,
        )

    def end_experiment(self, final_metrics: dict[str, float]) -> BacktestResult:
        """End experiment and save results.

        Args:
            final_metrics: Aggregated metrics across all folds

        Returns:
            BacktestResult object
        """
        if not self._current_experiment or not self._config or not self._start_time:
            raise RuntimeError("No experiment in progress")

        result = BacktestResult(
            experiment_id=self._current_experiment,
            config=self._config,
            metrics=final_metrics,
            started_at=self._start_time,
            completed_at=datetime.now(UTC),
            fold_results=self._fold_results,
            dataset_asof_times={
                "feature_asof_time": self._config.feature_asof_time,
                "label_asof_time": self._config.label_asof_time,
            },
        )

        # Save to disk
        result_path = self.results_dir / f"{self._current_experiment}.json"
        result.save(result_path)

        logger.info(
            "Experiment complete",
            experiment_id=self._current_experiment,
            duration_seconds=(result.completed_at - result.started_at).total_seconds(),
            metrics=final_metrics,
        )

        self._current_experiment = None
        self._config = None
        self._start_time = None
        self._fold_results = []

        return result

    def list_experiments(self) -> list[str]:
        """List all experiment IDs."""
        return [p.stem for p in self.results_dir.glob("*.json")]

    def load_experiment(self, experiment_id: str) -> BacktestResult:
        """Load a previous experiment result."""
        return BacktestResult.load(self.results_dir / f"{experiment_id}.json")


def generate_reproducibility_checklist(config: ExperimentConfig) -> dict[str, Any]:
    """Generate reproducibility checklist for a backtest (PRD §34.4).

    Args:
        config: Experiment configuration

    Returns:
        Checklist with all reproducibility requirements
    """
    return {
        "feature_dataset": config.feature_dataset,
        "feature_version": config.feature_version,
        "feature_asof_time": config.feature_asof_time,
        "label_dataset": config.label_dataset,
        "label_version": config.label_version,
        "label_asof_time": config.label_asof_time,
        "train_period": config.train_period,
        "test_period": config.test_period,
        "embargo": config.embargo,
        "model_hyperparameters": config.model_params,
        "random_seed": config.random_seed,
        "code_commit": config.code_commit,
        "config_hash": config.config_hash(),
    }
