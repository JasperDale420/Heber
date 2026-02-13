"""Shared timestamp utilities for feature templates."""

from __future__ import annotations

import numpy as np
import pandas as pd


def rolling_max_timestamp(
    series: pd.Series,
    window: int,
) -> pd.Series:
    """Integer-window rolling max of a tz-aware timestamp column.

    Converts timestamps to int64 nanoseconds, takes a rolling max,
    and converts back.  Used to derive ``ts_available`` as the latest
    source timestamp within a lookback window.

    Args:
        series: Timestamp column (tz-aware or parseable).
        window: Rolling window size in rows.

    Returns:
        UTC-aware Timestamp series with rolling max applied.
    """
    source = pd.to_datetime(series, utc=True, errors="coerce")
    naive = source.dt.tz_convert("UTC").dt.tz_localize(None)
    ints = naive.astype("int64")
    rolled = pd.Series(ints, index=series.index).rolling(window=window, min_periods=1).max()
    return pd.to_datetime(rolled, utc=True)


def rolling_max_timestamp_time(
    series: pd.Series,
    index: pd.DatetimeIndex,
    window_hours: int,
) -> np.ndarray:
    """Time-based rolling max of a tz-aware timestamp column.

    Same logic as :func:`rolling_max_timestamp` but uses a time-based
    window (e.g. ``"24h"``) instead of a fixed row count.

    Args:
        series: Timestamp column (tz-aware or parseable).
        index: DatetimeIndex to align the rolling window on.
        window_hours: Rolling window size in hours.

    Returns:
        UTC-aware numpy datetime64 array with rolling max applied.
    """
    source = pd.to_datetime(series, utc=True, errors="coerce")
    naive = source.dt.tz_convert("UTC").dt.tz_localize(None)
    ints = naive.astype("int64")
    s = pd.Series(ints.to_numpy(), index=index)
    rolled = s.rolling(f"{window_hours}h", min_periods=1).max()
    return pd.to_datetime(rolled.values, utc=True)
