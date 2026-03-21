"""Meta-Label Dataset Builder.

Builds training datasets by joining captured features with outcomes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import numpy as np
import pandas as pd
import structlog

from heber.config import settings
from heber.reader import HeberReader

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

# Default path segments
_PROJECT_WATCH = "project=watch"
_VERSION_V1 = "version=v1"


# Default paths
DEFAULT_GOLD_PATH = settings.gold_path
DEFAULT_FEATURES_PATH = DEFAULT_GOLD_PATH / "dataset=meta_label_features" / _PROJECT_WATCH / _VERSION_V1
DEFAULT_OUTCOMES_PATH = DEFAULT_GOLD_PATH / "dataset=labels_alert_barriers" / _PROJECT_WATCH / _VERSION_V1


def _read_existing_partition_or_quarantine(out_file: Path) -> pd.DataFrame | None:
    """Read existing parquet partition; quarantine unreadable files and continue."""
    if not out_file.exists():
        return None

    try:
        return pd.read_parquet(out_file)
    except Exception as exc:
        quarantine_path = out_file.with_name(f"{out_file.name}.corrupt-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}")
        try:
            out_file.replace(quarantine_path)
            logger.warning(
                "Unreadable features partition quarantined",
                path=str(out_file),
                quarantine_path=str(quarantine_path),
                error_type=f"{exc.__class__.__module__}.{exc.__class__.__name__}",
                exc_info=True,
            )
        except Exception as move_error:
            logger.warning(
                "Failed to quarantine unreadable features partition",
                path=str(out_file),
                quarantine_path=str(quarantine_path),
                error=str(move_error),
                exc_info=True,
            )
        return None


def _atomic_write_parquet(df: pd.DataFrame, out_file: Path) -> None:
    """Write parquet via temp file then atomic rename to avoid partial reads."""
    temp_path = out_file.with_name(f".{out_file.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        df.to_parquet(temp_path, index=False)
        os.replace(temp_path, out_file)
    finally:
        temp_path.unlink(missing_ok=True)


@dataclass
class DatasetConfig:
    """Configuration for dataset building."""

    # Paths
    outcomes_path: Path = field(default_factory=lambda: DEFAULT_OUTCOMES_PATH)
    features_path: Path = field(default_factory=lambda: DEFAULT_FEATURES_PATH)
    output_path: Path | None = None

    # Filtering
    min_outcomes_per_symbol: int = 5
    exclude_expired: bool = False  # Whether to exclude EXPIRED outcomes

    # Train/validation split
    train_ratio: float = 0.8
    purge_days: int = 5  # Days to purge around split boundary
    embargo_days: int = 2  # Additional embargo after test start


class MetaLabelDatasetBuilder:
    """Builds training datasets for meta-model.

    Joins features captured at alert time with outcomes from the watch service.
    """

    def __init__(
        self,
        config: DatasetConfig | None = None,
        redis: Redis | None = None,
    ):
        """Initialize dataset builder.

        Args:
            config: Dataset configuration
            redis: Optional Redis client for feature lookup
        """
        self.config = config or DatasetConfig()
        self.redis = redis
        self.client = HeberReader()

    def build_from_parquet(
        self,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Build dataset from Parquet files.

        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            DataFrame with features and meta-labels joined
        """
        # Load outcomes
        outcomes = self._load_outcomes(start_date, end_date)
        if outcomes.empty:
            logger.warning("No outcomes found in date range")
            return pd.DataFrame()

        # Load features
        features = self._load_features(start_date, end_date)
        if features.empty:
            logger.warning("No features found in date range")
            return pd.DataFrame()

        # Join on alert_id
        dataset = self._join_features_outcomes(features, outcomes)

        # Add meta-label
        dataset = self._add_meta_label(dataset)

        # Apply filters
        dataset = self._apply_filters(dataset)

        logger.info(
            "Built meta-label dataset",
            rows=len(dataset),
            start_date=str(start_date),
            end_date=str(end_date),
        )

        return dataset

    async def build_from_redis(
        self,
        alert_ids: list[str],
    ) -> pd.DataFrame:
        """Build dataset from Redis feature cache and outcome lookups.

        Args:
            alert_ids: List of alert IDs to include

        Returns:
            DataFrame with features and meta-labels
        """
        if not self.redis:
            raise ValueError("Redis client required for build_from_redis")

        from heber.watch.features import get_features

        rows = []
        for alert_id in alert_ids:
            features = await get_features(self.redis, alert_id)
            if features:
                rows.append(features.to_dict())

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def _load_outcomes(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load outcomes from Gold layer."""
        # Partition filter
        filters = [
            ("dt", ">=", start_date.strftime("%Y-%m-%d")),
            ("dt", "<=", end_date.strftime("%Y-%m-%d")),
        ]

        try:
            # Support arbitrary path config via read_parquet_dataset
            df = self.client.read_parquet_dataset(
                path=self.config.outcomes_path,
                filters=filters,
            )

            if df.empty:
                logger.warning(
                    "Outcomes path does not exist or contains no data",
                    configured_path=str(self.config.outcomes_path),
                )
                return pd.DataFrame()

            return df

        except Exception as e:
            logger.error("Failed to load outcomes", path=str(self.config.outcomes_path), error=str(e))
            return pd.DataFrame()

    def _load_features(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load features from Gold layer."""
        # Partition filter
        filters = [
            ("dt", ">=", start_date.strftime("%Y-%m-%d")),
            ("dt", "<=", end_date.strftime("%Y-%m-%d")),
        ]

        try:
            # Support arbitrary path config via read_parquet_dataset
            df = self.client.read_parquet_dataset(
                path=self.config.features_path,
                filters=filters,
            )

            if df.empty:
                logger.warning(
                    "Features path does not exist or contains no data",
                    configured_path=str(self.config.features_path),
                )
                return pd.DataFrame()

            return df

        except Exception as e:
            logger.error("Failed to load features", path=str(self.config.features_path), error=str(e))
            return pd.DataFrame()

    def _join_features_outcomes(
        self,
        features: pd.DataFrame,
        outcomes: pd.DataFrame,
    ) -> pd.DataFrame:
        """Join features with outcomes on alert_id."""
        outcomes = self._normalize_outcomes(outcomes)

        # Ensure alert_id column exists in both
        if "alert_id" not in features.columns:
            logger.error("Features missing alert_id column")
            return pd.DataFrame()

        if "alert_id" not in outcomes.columns:
            logger.error("Outcomes missing alert_id column")
            return pd.DataFrame()

        # Select outcome columns for join
        outcome_cols = [
            "alert_id",
            "outcome",
            "outcome_return",
            "mfe",
            "mae",
            "bars_to_hit",
            "trading_minutes_to_hit",
            "hit_tp_first",
        ]
        available_outcome_cols = [c for c in outcome_cols if c in outcomes.columns]

        # Inner join - only keep alerts with both features and outcomes
        return pd.merge(
            features,
            outcomes[available_outcome_cols],
            on="alert_id",
            how="inner",
        )

    def _normalize_outcomes(self, outcomes: pd.DataFrame) -> pd.DataFrame:
        """Normalize outcome columns for backward compatibility."""
        df = outcomes.copy()

        if "outcome" not in df.columns and "outcome_reason" in df.columns:
            df["outcome"] = df["outcome_reason"]

        if "hit_tp_first" not in df.columns and "contract_hit_tp_first" in df.columns:
            df["hit_tp_first"] = df["contract_hit_tp_first"]

        if "mfe" not in df.columns and "contract_mfe" in df.columns:
            df["mfe"] = df["contract_mfe"]

        if "mae" not in df.columns and "contract_mae" in df.columns:
            df["mae"] = df["contract_mae"]

        if "bars_to_hit" not in df.columns and "contract_bars_to_hit" in df.columns:
            df["bars_to_hit"] = df["contract_bars_to_hit"]

        return df

    def _add_meta_label(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add meta-label column (1 if TP hit, 0 otherwise)."""
        if "outcome" not in df.columns:
            return df

        df = df.copy()
        df["meta_label"] = np.where(df["outcome"].astype(str).str.lower() == "hit_tp", 1, 0)
        return df

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply configured filters to dataset."""
        if df.empty:
            return df

        # Exclude expired if configured
        if self.config.exclude_expired and "outcome" in df.columns:
            df = df[df["outcome"].astype(str).str.lower() != "expired"].reset_index(drop=True)

        # Min samples per symbol
        if "symbol" in df.columns and self.config.min_outcomes_per_symbol > 1:
            symbol_counts = df.groupby("symbol").size()
            valid_symbols = symbol_counts[symbol_counts >= self.config.min_outcomes_per_symbol].index
            df = df[df["symbol"].isin(valid_symbols)].reset_index(drop=True)

        return df

    def train_test_split(
        self,
        df: pd.DataFrame,
        split_date: date | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split dataset into train and test with purge/embargo.

        Uses temporal split to avoid leakage.

        Args:
            df: Full dataset
            split_date: Date to split on (train < split_date, test >= split_date)
                       If None, uses train_ratio to determine split point.

        Returns:
            (train_df, test_df) tuple
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        if "alert_time" not in df.columns:
            logger.error("Cannot split: missing alert_time column")
            return df, pd.DataFrame()

        # Ensure datetime type
        df = df.copy()
        df["alert_time"] = pd.to_datetime(df["alert_time"])

        # Determine split date
        if split_date is None:
            sorted_df = df.sort_values("alert_time").reset_index(drop=True)
            split_idx = int(len(sorted_df) * self.config.train_ratio)
            split_date = sorted_df.iloc[split_idx]["alert_time"].date()

        # Calculate purge and embargo boundaries
        from datetime import timedelta

        purge_start = split_date - timedelta(days=self.config.purge_days)
        embargo_end = split_date + timedelta(days=self.config.embargo_days)

        # Train: before purge_start
        train_df = df[df["alert_time"] < datetime.combine(purge_start, datetime.min.time())].reset_index(drop=True)

        # Test: after embargo_end
        test_df = df[df["alert_time"] >= datetime.combine(embargo_end, datetime.min.time())].reset_index(drop=True)

        logger.info(
            "Train/test split completed",
            train_size=len(train_df),
            test_size=len(test_df),
            split_date=str(split_date),
            purge_days=self.config.purge_days,
            embargo_days=self.config.embargo_days,
        )

        return train_df, test_df

    def get_feature_columns(self, df: pd.DataFrame) -> list[str]:
        """Get list of feature columns (excluding identifiers and targets).

        Args:
            df: Dataset DataFrame

        Returns:
            List of feature column names
        """
        exclude = {
            # Identifiers
            "alert_id",
            "watch_id",
            "symbol",
            "underlying",
            "occ_symbol",
            # Timestamps
            "alert_time",
            "outcome_time",
            "expiry",
            # Targets/outcomes
            "outcome",
            "outcome_return",
            "meta_label",
            "hit_tp_first",
            "mfe",
            "mae",
            "bars_to_hit",
            "trading_minutes_to_hit",
            # Non-numeric
            "alert_type",
            "side",
            "aggressor",
            "put_call",
        }

        return [c for c in df.columns if c not in exclude]

    def to_xy(
        self,
        df: pd.DataFrame,
        feature_cols: list[str] | None = None,
        target_col: str = "meta_label",
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Convert to X (features) and y (target) for model training.

        Args:
            df: Dataset DataFrame
            feature_cols: Specific feature columns (or auto-detect)
            target_col: Target column name

        Returns:
            (X, y) tuple as pandas DataFrame/Series
        """
        if feature_cols is None:
            feature_cols = self.get_feature_columns(df)

        X = df[feature_cols]  # noqa: N806
        y = df[target_col]

        return X, y


def persist_features_to_gold(
    features_df: pd.DataFrame,
    output_path: Path,
    partition_col: str = "alert_time",
) -> None:
    """Persist features DataFrame to Gold layer with date partitioning.

    Args:
        features_df: DataFrame with feature rows
        output_path: Base path for features
        partition_col: Column to partition by (must be datetime)
    """
    if features_df.empty:
        return

    # Add date partition column
    df = features_df.copy()
    df["dt"] = pd.to_datetime(df[partition_col]).dt.date

    # Write partitioned by date
    for dt_val in df["dt"].unique():
        partition_df = df[df["dt"] == dt_val].drop(columns=["dt"]).reset_index(drop=True)
        dt_str = pd.Timestamp(dt_val).strftime("%Y-%m-%d")
        partition_path = output_path / f"dt={dt_str}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Audit for unexpected null values before writing
        from heber.quality.write_audit import audit_null_fields

        audit_null_fields(
            partition_df,
            layer="gold",
            dataset="meta_label_features",
            context={"date": dt_str, "path": str(partition_path)},
        )

        out_file = partition_path / "data.parquet"
        lock_file = out_file.with_suffix(".parquet.lock")
        try:
            from filelock import FileLock, Timeout

            with FileLock(lock_file, timeout=10):
                existing = _read_existing_partition_or_quarantine(out_file)
                if existing is not None:
                    partition_df = pd.concat(
                        [existing, partition_df],
                        ignore_index=True,
                    )
                    if "alert_id" in partition_df.columns:
                        partition_df = partition_df.drop_duplicates(subset=["alert_id"], keep="last").reset_index(
                            drop=True
                        )

                _atomic_write_parquet(partition_df, out_file)
        except Timeout:
            logger.warning(
                "Could not acquire partition lock, skipping write",
                path=str(out_file),
                lock_file=str(lock_file),
            )
            continue

        logger.info(
            "Persisted features partition",
            date=dt_str,
            rows=len(partition_df),
            path=str(out_file),
        )
