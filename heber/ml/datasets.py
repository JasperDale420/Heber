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
import pyarrow as pa
import pyarrow.parquet as pq
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


def normalize_expiry(val: object) -> date | None:
    """Coerce an option ``expiry`` value to a ``datetime.date`` (Arrow ``date32``).

    Accepts ``date``/``datetime``/``datetime64``, ISO ``YYYY-MM-DD``, compact
    ``YYYYMMDD`` as ``str`` or ``int``, and ISO datetime strings. Everything
    else — trailing garbage, non-integral floats, partial dates — returns
    ``None`` rather than being guessed at.

    Strictness matters because pyarrow reads a bare integer in a ``date32``
    column as a raw day count: ``20260819`` becomes year 57442, which makes the
    entire partition unreadable by pandas rather than just that one row.
    """
    if val is None or val is pd.NaT:
        return None
    if isinstance(val, float):
        if pd.isna(val) or not val.is_integer():
            return None
        val = int(val)
    # datetime is a subclass of date — check it first.
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, np.datetime64):
        if pd.isna(val):
            return None
        timestamp = pd.Timestamp(val)
        return date(timestamp.year, timestamp.month, timestamp.day)
    if isinstance(val, np.integer):
        val = int(val)
    if isinstance(val, int) and not isinstance(val, bool):
        # Only a full YYYYMMDD is unambiguous; 202609 or 2026 are not.
        val = str(val) if len(str(val)) == 8 else None
    if not isinstance(val, str):
        return None
    text = val.strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _coerce_expiry_column(df: pd.DataFrame, context: dict[str, str]) -> pd.DataFrame:
    """Normalize ``expiry`` to ``datetime.date`` before write, logging in aggregate.

    Runs on the merged frame (new rows plus whatever was read back off disk) so
    legacy string/int partitions are repaired on their next append instead of
    compounding the type drift.
    """
    if "expiry" not in df.columns:
        return df

    original = df["expiry"]
    coerced = original.map(normalize_expiry)

    # Non-null values that failed to parse — real data loss, not an empty field.
    rejected = original.notna() & coerced.isna()
    if rejected.any():
        logger.error(
            "meta_label_features expiry values rejected — could not parse to a date",
            rows=int(rejected.sum()),
            sample_values=[str(v) for v in original[rejected].head(5)],
            **context,
        )

    changed = original.notna() & coerced.notna() & (original != coerced)
    if changed.any():
        logger.warning(
            "meta_label_features expiry values coerced to date32",
            rows=int(changed.sum()),
            sample_values=[str(v) for v in original[changed].head(5)],
            **context,
        )

    df = df.copy()
    df["expiry"] = coerced
    return df


def _atomic_write_parquet(
    df: pd.DataFrame,
    out_file: Path,
    schema_overrides: dict[str, pa.DataType] | None = None,
) -> None:
    """Write parquet via temp file then atomic rename to avoid partial reads.

    ``schema_overrides`` pins a column's Arrow type instead of leaving it to
    pandas inference, which silently drifts between partitions (an all-null
    column infers as ``null``, a mixed column picks whichever type it sees
    first) and produces datasets that cannot be read as a whole.
    """
    temp_path = out_file.with_name(f".{out_file.name}.tmp-{os.getpid()}-{uuid4().hex}")
    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        for column, arrow_type in (schema_overrides or {}).items():
            if column not in table.column_names:
                continue
            index = table.schema.get_field_index(column)
            if table.schema.field(index).type == arrow_type:
                continue
            table = table.set_column(
                index,
                pa.field(column, arrow_type),
                table.column(column).cast(arrow_type),
            )
        pq.write_table(table, temp_path, compression="snappy")  # type: ignore[no-untyped-call]
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
        df["alert_time"] = pd.to_datetime(df["alert_time"], utc=True)

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
        purge_boundary = datetime.combine(purge_start, datetime.min.time(), tzinfo=UTC)
        train_df = df[df["alert_time"] < purge_boundary].reset_index(drop=True)

        # Test: after embargo_end
        embargo_boundary = datetime.combine(embargo_end, datetime.min.time(), tzinfo=UTC)
        test_df = df[df["alert_time"] >= embargo_boundary].reset_index(drop=True)

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


# Greek enrichment columns. If every one of these is null on a row, the
# enrichment path (gateway → features extractor) failed for that alert and
# the row would silently corrupt downstream ML training. We quarantine those
# rows to a sibling partition rather than writing them to the canonical path.
_REQUIRED_GREEK_COLUMNS: tuple[str, ...] = ("delta", "gamma", "theta", "vega", "iv")


def _split_greek_corrupted_rows(
    partition_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split a partition into (clean, corrupted) where corrupted = all Greeks null.

    A row is considered corrupted only when every Greek column listed in
    ``_REQUIRED_GREEK_COLUMNS`` is null/NA. Rows missing one or two values are
    kept (some upstream feeds legitimately omit single Greeks).
    """
    present_cols = [c for c in _REQUIRED_GREEK_COLUMNS if c in partition_df.columns]
    if not present_cols:
        # No Greek columns at all — nothing to validate. Pass through.
        return partition_df, partition_df.iloc[0:0]
    all_null_mask = partition_df[present_cols].isna().all(axis=1)
    clean = partition_df.loc[~all_null_mask].reset_index(drop=True)
    corrupted = partition_df.loc[all_null_mask].reset_index(drop=True)
    return clean, corrupted


def _write_quarantine_partition(
    corrupted_df: pd.DataFrame,
    output_path: Path,
    dt_str: str,
) -> Path:
    """Write quarantined rows under a sibling `_quarantine` partition.

    Quarantined files live at
    ``<output_path>/_quarantine/all_greeks_null/dt=<dt>/quarantine-<ts>.parquet``
    so downstream readers (which only read ``dt=`` partitions) never load them.
    """
    quarantine_root = output_path / "_quarantine" / "all_greeks_null" / f"dt={dt_str}"
    quarantine_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    quarantine_file = quarantine_root / f"quarantine-{ts}-{uuid4().hex[:8]}.parquet"
    corrupted_df.to_parquet(quarantine_file, index=False, compression="snappy")
    return quarantine_file


def persist_features_to_gold(
    features_df: pd.DataFrame,
    output_path: Path,
    partition_col: str = "alert_time",
) -> None:
    """Persist features DataFrame to Gold layer with date partitioning.

    Rows where ALL Greek enrichment columns (delta/gamma/theta/vega/iv) are
    null are diverted to a quarantine partition rather than written to the
    canonical dt=<date>/ path. This is the "fail loud" safety net for the
    gateway 401 cascade: the write_audit warning was the only previous
    detector, and it logged but did not block — letting null-Greek rows
    pollute ML training inputs.

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

        # Fail-loud guard: divert rows whose Greek enrichment is fully null
        # to a quarantine partition. Prevents silent ML corruption when the
        # gateway returns 401/5xx for an extended window.
        partition_df, corrupted = _split_greek_corrupted_rows(partition_df)
        if not corrupted.empty:
            try:
                qpath = _write_quarantine_partition(corrupted, output_path, dt_str)
                logger.error(
                    "meta_label_features rows quarantined — all Greek columns null",
                    rows=len(corrupted),
                    date=dt_str,
                    quarantine_path=str(qpath),
                    affected_columns=list(_REQUIRED_GREEK_COLUMNS),
                    hint=(
                        "This usually means gateway feature enrichment failed "
                        "(check heber-watch logs for 401/5xx around this time)."
                    ),
                )
            except Exception as q_exc:
                # If quarantine fails, drop the rows but never let them reach
                # the canonical partition.
                logger.error(
                    "meta_label_features quarantine write failed — dropping corrupted rows",
                    rows=len(corrupted),
                    date=dt_str,
                    error=str(q_exc),
                    exc_info=True,
                )

        if partition_df.empty:
            # Whole partition was corrupted — nothing to write.
            logger.warning(
                "meta_label_features partition fully quarantined",
                date=dt_str,
                path=str(partition_path),
            )
            continue

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

                partition_df = _coerce_expiry_column(
                    partition_df,
                    context={"date": dt_str, "path": str(out_file)},
                )
                _atomic_write_parquet(
                    partition_df,
                    out_file,
                    schema_overrides={"expiry": pa.date32()},
                )
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
