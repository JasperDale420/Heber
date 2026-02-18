from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


def _filter_time_range(
    df: pd.DataFrame,
    time_range: tuple[datetime | str, datetime | str],
    time_col: str = "ts_event",
) -> pd.DataFrame:
    """Apply time range filter to a DataFrame."""
    start, end = time_range
    if isinstance(start, str):
        start = pd.Timestamp(start)
    if isinstance(end, str):
        end = pd.Timestamp(end)
    return df[(df[time_col] >= start) & (df[time_col] <= end)]


def _filter_asof(
    df: pd.DataFrame,
    asof_time: datetime | str,
    available_col: str = "ts_available",
) -> pd.DataFrame:
    """Apply point-in-time correctness filter."""
    if isinstance(asof_time, str):
        asof_time = pd.Timestamp(asof_time)
    return df[df[available_col] <= asof_time]


def read_parquet_dataset(
    path: Path,
    columns: list[str] | None = None,
    filters: list[tuple] | None = None,
    time_range: tuple[datetime | str, datetime | str] | None = None,
    asof_time: datetime | str | None = None,
) -> pd.DataFrame:
    """Read parquet dataset with standard filtering.

    Args:
        path: Path to dataset directory or file
        columns: List of columns to read
        filters: PyArrow filter expression
        time_range: tuple of (start, end) for time filtering.
                    Auto-detects 'ts_event' or 'ts_label'.
        asof_time: Point-in-time cutoff. Filters rows where
                   ts_available <= asof_time.

    Returns:
        pd.DataFrame
    """
    if not path.exists():
        return pd.DataFrame()

    table = pq.read_table(
        path,
        columns=columns,
        filters=filters if filters else None,
    )
    df = table.to_pandas()

    if time_range and not df.empty:
        # Auto-detect time column for gold (ts_label) vs silver (ts_event)
        time_col = "ts_event"
        if "ts_event" not in df.columns and "ts_label" in df.columns:
            time_col = "ts_label"

        df = _filter_time_range(df, time_range, time_col)

    if asof_time and not df.empty and "ts_available" in df.columns:
        df = _filter_asof(df, asof_time)

    return df
