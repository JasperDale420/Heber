"""Meta-Label Dataset Builder.

Builds training datasets by joining captured features with outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

logger = structlog.get_logger(__name__)

# Default paths
DEFAULT_GOLD_PATH = Path("/tmp/heber/gold")
DEFAULT_FEATURES_PATH = DEFAULT_GOLD_PATH / "meta_labels" / "features"
DEFAULT_OUTCOMES_PATH = DEFAULT_GOLD_PATH / "labels_alert_barriers"


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

    def build_from_parquet(
        self,
        start_date: date,
        end_date: date,
    ) -> pl.DataFrame:
        """Build dataset from Parquet files.

        Args:
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)

        Returns:
            DataFrame with features and meta-labels joined
        """
        # Load outcomes
        outcomes = self._load_outcomes(start_date, end_date)
        if outcomes.is_empty():
            logger.warning("No outcomes found in date range")
            return pl.DataFrame()

        # Load features
        features = self._load_features(start_date, end_date)
        if features.is_empty():
            logger.warning("No features found in date range")
            return pl.DataFrame()

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
    ) -> pl.DataFrame:
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
            return pl.DataFrame()

        return pl.DataFrame(rows)

    def _load_outcomes(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Load outcomes from Gold layer."""
        outcomes_path = self.config.outcomes_path

        if not outcomes_path.exists():
            logger.warning("Outcomes path does not exist", path=str(outcomes_path))
            return pl.DataFrame()

        # Scan for Parquet files in date range
        dfs = []
        for dt_dir in outcomes_path.glob("dt=*"):
            dt_str = dt_dir.name.replace("dt=", "")
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
                if start_date <= dt <= end_date:
                    for pq_file in dt_dir.glob("*.parquet"):
                        dfs.append(pl.read_parquet(pq_file))
            except ValueError:
                continue

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs)

    def _load_features(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Load features from Gold layer."""
        features_path = self.config.features_path

        if not features_path.exists():
            logger.warning("Features path does not exist", path=str(features_path))
            return pl.DataFrame()

        # Scan for Parquet files in date range
        dfs = []
        for dt_dir in features_path.glob("dt=*"):
            dt_str = dt_dir.name.replace("dt=", "")
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
                if start_date <= dt <= end_date:
                    for pq_file in dt_dir.glob("*.parquet"):
                        dfs.append(pl.read_parquet(pq_file))
            except ValueError:
                continue

        if not dfs:
            return pl.DataFrame()

        return pl.concat(dfs)

    def _join_features_outcomes(
        self,
        features: pl.DataFrame,
        outcomes: pl.DataFrame,
    ) -> pl.DataFrame:
        """Join features with outcomes on alert_id."""
        # Ensure alert_id column exists in both
        if "alert_id" not in features.columns:
            logger.error("Features missing alert_id column")
            return pl.DataFrame()

        if "alert_id" not in outcomes.columns:
            logger.error("Outcomes missing alert_id column")
            return pl.DataFrame()

        # Inner join - only keep alerts with both features and outcomes
        return features.join(
            outcomes.select(
                [
                    "alert_id",
                    "outcome",
                    "outcome_return",
                    "mfe",
                    "mae",
                    "bars_to_hit",
                    "trading_minutes_to_hit",
                    "hit_tp_first",
                ]
            ),
            on="alert_id",
            how="inner",
        )

    def _add_meta_label(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add meta-label column (1 if TP hit, 0 otherwise)."""
        if "outcome" not in df.columns:
            return df

        return df.with_columns(pl.when(pl.col("outcome") == "HIT_TP").then(1).otherwise(0).alias("meta_label"))

    def _apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply configured filters to dataset."""
        if df.is_empty():
            return df

        # Exclude expired if configured
        if self.config.exclude_expired and "outcome" in df.columns:
            df = df.filter(pl.col("outcome") != "EXPIRED")

        # Min samples per symbol
        if "symbol" in df.columns and self.config.min_outcomes_per_symbol > 1:
            symbol_counts = df.group_by("symbol").count()
            valid_symbols = symbol_counts.filter(pl.col("count") >= self.config.min_outcomes_per_symbol).select(
                "symbol"
            )
            df = df.join(valid_symbols, on="symbol", how="inner")

        return df

    def train_test_split(
        self,
        df: pl.DataFrame,
        split_date: date | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Split dataset into train and test with purge/embargo.

        Uses temporal split to avoid leakage.

        Args:
            df: Full dataset
            split_date: Date to split on (train < split_date, test >= split_date)
                       If None, uses train_ratio to determine split point.

        Returns:
            (train_df, test_df) tuple
        """
        if df.is_empty():
            return pl.DataFrame(), pl.DataFrame()

        if "alert_time" not in df.columns:
            logger.error("Cannot split: missing alert_time column")
            return df, pl.DataFrame()

        # Ensure datetime type
        df = df.with_columns(pl.col("alert_time").cast(pl.Datetime).alias("alert_time"))

        # Determine split date
        if split_date is None:
            sorted_df = df.sort("alert_time")
            split_idx = int(len(sorted_df) * self.config.train_ratio)
            split_date = sorted_df.row(split_idx)[sorted_df.columns.index("alert_time")].date()

        # Calculate purge and embargo boundaries
        from datetime import timedelta

        purge_start = split_date - timedelta(days=self.config.purge_days)
        embargo_end = split_date + timedelta(days=self.config.embargo_days)

        # Train: before purge_start
        train_df = df.filter(pl.col("alert_time") < datetime.combine(purge_start, datetime.min.time()))

        # Test: after embargo_end
        test_df = df.filter(pl.col("alert_time") >= datetime.combine(embargo_end, datetime.min.time()))

        logger.info(
            "Train/test split completed",
            train_size=len(train_df),
            test_size=len(test_df),
            split_date=str(split_date),
            purge_days=self.config.purge_days,
            embargo_days=self.config.embargo_days,
        )

        return train_df, test_df

    def get_feature_columns(self, df: pl.DataFrame) -> list[str]:
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
        df: pl.DataFrame,
        feature_cols: list[str] | None = None,
        target_col: str = "meta_label",
    ) -> tuple[pl.DataFrame, pl.Series]:
        """Convert to X (features) and y (target) for model training.

        Args:
            df: Dataset DataFrame
            feature_cols: Specific feature columns (or auto-detect)
            target_col: Target column name

        Returns:
            (X, y) tuple as polars DataFrame/Series
        """
        if feature_cols is None:
            feature_cols = self.get_feature_columns(df)

        X = df.select(feature_cols)
        y = df.get_column(target_col)

        return X, y


def persist_features_to_gold(
    features_df: pl.DataFrame,
    output_path: Path,
    partition_col: str = "alert_time",
) -> None:
    """Persist features DataFrame to Gold layer with date partitioning.

    Args:
        features_df: DataFrame with feature rows
        output_path: Base path for features
        partition_col: Column to partition by (must be datetime)
    """
    if features_df.is_empty():
        return

    # Add date partition column
    df = features_df.with_columns(pl.col(partition_col).dt.date().alias("dt"))

    # Write partitioned by date
    for dt_val in df.get_column("dt").unique():
        partition_df = df.filter(pl.col("dt") == dt_val).drop("dt")
        dt_str = dt_val.strftime("%Y-%m-%d")
        partition_path = output_path / f"dt={dt_str}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Append to existing or create new
        out_file = partition_path / "data.parquet"
        partition_df.write_parquet(out_file)

        logger.info(
            "Persisted features partition",
            date=dt_str,
            rows=len(partition_df),
            path=str(out_file),
        )
