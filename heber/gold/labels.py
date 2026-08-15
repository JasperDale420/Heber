"""Label Management (PRD §29).

Handles forward-looking labels with proper availability tracking.
Labels are Gold datasets with special metadata indicating:
- forward_window: How far forward the label looks
- label_horizon: What the label measures (e.g., close_to_close)
- availability_lag: When the label becomes observable after forward_window
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import structlog

from heber.calendar.market import get_calendar
from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

# Constants
LABEL_TYPE_DIR = "type=label"
DEFAULT_VERSION = "v1.0.0"
DATA_PARQUET = "data.parquet"


from heber.gold.duration import parse_duration
from heber.utils.versioning import version_sort_key as _version_sort_key


@dataclass
class LabelMetadata:
    """Metadata for a label dataset (PRD §29.2).

    Attributes:
        dataset_type: Always "label" for label datasets
        forward_window: How far forward the label looks (e.g., "5d")
        label_horizon: What the label measures (e.g., "close_to_close")
        availability_lag: Delay after forward_window before label is observable
    """

    dataset_type: Literal["label"] = "label"
    forward_window: str = "1d"
    label_horizon: str = "close_to_close"
    availability_lag: str = "0s"

    def to_dict(self) -> dict[str, str]:
        return {
            "dataset_type": self.dataset_type,
            "forward_window": self.forward_window,
            "label_horizon": self.label_horizon,
            "availability_lag": self.availability_lag,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LabelMetadata:
        return cls(
            dataset_type=data.get("dataset_type", "label"),
            forward_window=data.get("forward_window", "1d"),
            label_horizon=data.get("label_horizon", "close_to_close"),
            availability_lag=data.get("availability_lag", "0s"),
        )

    def get_forward_window_delta(self) -> timedelta:
        """Get forward_window as timedelta."""
        return parse_duration(self.forward_window)

    def get_availability_lag_delta(self) -> timedelta:
        """Get availability_lag as timedelta."""
        return parse_duration(self.availability_lag)


@dataclass
class LabelDataset:
    """A label dataset with its metadata.

    Stored at: gold/dataset={name}/type=label/_metadata.json
    """

    name: str
    metadata: LabelMetadata
    version: str = DEFAULT_VERSION
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    created_by: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "metadata": self.metadata.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LabelDataset:
        return cls(
            name=data["name"],
            version=data.get("version", DEFAULT_VERSION),
            created_at=datetime.fromisoformat(data["created_at"]),
            created_by=data.get("created_by", "system"),
            metadata=LabelMetadata.from_dict(data.get("metadata", {})),
        )

    def save(self, gold_root: Path) -> Path:
        """Save metadata to gold layer."""
        metadata_path = gold_root / f"dataset={self.name}" / LABEL_TYPE_DIR / "_metadata.json"
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        with open(metadata_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

        logger.info("Saved label metadata", dataset=self.name, path=str(metadata_path))
        return metadata_path

    @classmethod
    def load(cls, gold_root: Path, name: str) -> LabelDataset:
        """Load metadata from gold layer."""
        metadata_path = gold_root / f"dataset={name}" / LABEL_TYPE_DIR / "_metadata.json"
        with open(metadata_path) as f:
            data = json.load(f)
        return cls.from_dict(data)


def compute_availability_time(
    label_time: datetime,
    forward_window: str,
    availability_lag: str = "0s",
) -> datetime:
    """Compute when a label becomes available (PRD §29.3).

    Daily windows resolve on TRADING sessions, not calendar days. A daily
    ``forward_window`` advances N trading sessions from the label's own session
    and snaps to that session's real close (``MarketCalendar.session_close``),
    which accounts for weekends, holidays, and early-close half-days (13:00 ET).
    That close is when a ``close_to_close`` label actually becomes observable —
    calendar-day arithmetic marked a Friday 1d label available on Saturday, days
    before the next session's close resolves it, violating the zero-leakage
    contract (``HeberReader.read_asof`` pushes ``ts_available <= asof`` into the
    scan).

    Intraday and longer wall-clock windows ("Nh", "Nm", "Nw", "NM") use plain
    forward arithmetic with no session snap.

        ts_available = <Nth trading-session close after ts_label> + availability_lag  # daily
        ts_available = ts_label + forward_window + availability_lag                   # other

    Args:
        label_time: The feature cutoff time (T)
        forward_window: How far forward the label looks (e.g. "5d", "1h")
        availability_lag: Additional processing delay added after resolution

    Returns:
        datetime when label becomes available, in label_time's timezone
    """
    forward_delta = parse_duration(forward_window)
    lag_delta = parse_duration(availability_lag)

    if label_time.tzinfo is None:
        label_time = label_time.replace(tzinfo=UTC)

    output_tz = label_time.tzinfo

    if "d" in forward_window:
        # Anchor on the label's own session close (session_close rolls a
        # non-trading label_time forward to the next session), then step N
        # trading sessions ahead, snapping to each real close along the way.
        cal = get_calendar()
        availability = cal.session_close(label_time)
        for _ in range(forward_delta.days):
            availability = cal.next_close(availability + timedelta(minutes=1))
        availability = availability.astimezone(output_tz)
    else:
        availability = label_time + forward_delta

    availability += lag_delta

    return availability


def write_label(
    gold_root: Path,
    dataset: str,
    df: pd.DataFrame,
    forward_window: str,
    label_horizon: str = "close_to_close",
    availability_lag: str = "0s",
    instrument_key_col: str = "instrument_key",
    label_time_col: str = "ts_label",
    label_value_col: str = "label",
    version: str = DEFAULT_VERSION,
    created_by: str = "system",
) -> Path:
    """Write labels to Gold layer with proper availability tracking (PRD §29.3).

    Automatically computes ts_available based on forward_window.

    Args:
        gold_root: Root path to gold layer
        dataset: Dataset name (e.g., "returns_5d")
        df: DataFrame with labels
        forward_window: How far forward the label looks (e.g., "5d")
        label_horizon: What the label measures
        availability_lag: Additional delay after forward_window
        instrument_key_col: Column name for instrument key
        label_time_col: Column name for label time (feature cutoff)
        label_value_col: Column name for label value
        version: Version string
        created_by: Creator identifier

    Returns:
        Path to written data
    """
    required_cols = {instrument_key_col, label_time_col, label_value_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.copy()

    df["ts_event"] = df[label_time_col]
    df["ts_available"] = df[label_time_col].apply(
        lambda t: compute_availability_time(t, forward_window, availability_lag)
    )

    label_dataset = LabelDataset(
        name=dataset,
        metadata=LabelMetadata(
            forward_window=forward_window,
            label_horizon=label_horizon,
            availability_lag=availability_lag,
        ),
        version=version,
        created_by=created_by,
    )
    label_dataset.save(gold_root)

    output_path = gold_root / f"dataset={dataset}" / LABEL_TYPE_DIR / f"version={version}"
    output_path.mkdir(parents=True, exist_ok=True)

    parquet_path = output_path / DATA_PARQUET
    df.to_parquet(parquet_path, index=False, compression="snappy")

    logger.info(
        "Wrote label dataset",
        dataset=dataset,
        rows=len(df),
        forward_window=forward_window,
        path=str(parquet_path),
    )

    return parquet_path


def read_label(
    gold_root: Path,
    dataset: str,
    asof_time: datetime,
    instrument_keys: list[str] | None = None,
    version: str | None = None,
    fail_on_missing_ts_available: bool = True,
) -> pd.DataFrame:
    """Read labels with point-in-time correctness (PRD §29.4).

    Only returns labels where ts_available <= asof_time, ensuring
    the forward_window has fully elapsed.

    Args:
        gold_root: Root path to gold layer
        dataset: Dataset name
        asof_time: Point-in-time cutoff
        instrument_keys: Filter to specific instruments
        version: Specific version (None for latest)
        fail_on_missing_ts_available: Raise when required ts_available column is absent

    Returns:
        DataFrame with labels available at asof_time
    """
    dataset_path = gold_root / f"dataset={dataset}" / LABEL_TYPE_DIR

    if not dataset_path.exists():
        logger.warning("Label dataset not found", dataset=dataset)
        return pd.DataFrame()

    if version:
        data_path = dataset_path / f"version={version}" / DATA_PARQUET
    else:
        versions = [d for d in dataset_path.iterdir() if d.is_dir() and d.name.startswith("version=")]
        if not versions:
            logger.warning("No versions found for label dataset", dataset=dataset)
            return pd.DataFrame()
        latest = sorted(
            versions,
            key=lambda d: _version_sort_key(d.name.replace("version=", "", 1)),
            reverse=True,
        )[0]
        data_path = latest / DATA_PARQUET

    if not data_path.exists():
        logger.warning("Label data file not found", path=str(data_path))
        return pd.DataFrame()

    # A read failure propagates. An absent dataset, an absent version and an
    # absent data file are each answered as "no labels" above, so anything
    # reaching here has bytes on disk that could not be parsed — a count that
    # is unknown, not zero. Returning empty for that fed silently incomplete
    # labels into training and evaluation with nothing to distinguish it from
    # a day that genuinely produced none.
    reader = HeberReader()
    df = reader.read_parquet_dataset(
        path=data_path,
        asof_time=asof_time,
    )

    if "ts_available" not in df.columns and not df.empty:
        message = (
            f"Label dataset {dataset} is missing required ts_available column. "
            "Cannot enforce point-in-time correctness."
        )
        if fail_on_missing_ts_available:
            raise ValueError(message)
        logger.warning("label_missing_ts_available", dataset=dataset, path=str(data_path))

    if instrument_keys and "instrument_key" in df.columns:
        df = df[df["instrument_key"].isin(instrument_keys)]

    logger.info(
        "Read label dataset",
        dataset=dataset,
        asof_time=asof_time.isoformat(),
        rows=len(df),
    )

    return df
