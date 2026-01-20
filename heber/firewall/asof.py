"""As-Of Query and Join utilities per PRD §10.3-10.4.

CRITICAL: All reads for research/backtest/ML must use ts_available <= T
"""

from datetime import datetime
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


def read_asof(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    filters: dict[str, Any] | None = None,
    time_col: str = "ts_event",
    available_col: str = "ts_available",
) -> pl.LazyFrame:
    """Read data as-of a specific time (PRD §10.3).
    
    This is the fundamental anti-leakage primitive. All training/backtest
    reads MUST use this function to ensure point-in-time correctness.
    
    Args:
        df: Source data (LazyFrame or DataFrame)
        asof_time: The point-in-time cutoff - only rows available at this time are returned
        filters: Optional additional filters (column: value dict)
        time_col: Column to use for time range filtering (default: ts_event)
        available_col: Column containing availability timestamp (default: ts_available)
        
    Returns:
        Filtered LazyFrame with ts_available <= asof_time
        
    Example:
        >>> quotes = read_asof(quotes_df, asof_time=datetime(2026, 1, 15, 10, 30))
        # Only returns quotes that were known at 10:30 on Jan 15
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    # Apply the critical anti-leakage filter
    result = df.filter(pl.col(available_col) <= asof_time)
    
    # Apply any additional filters
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    logger.debug(
        "read_asof",
        asof_time=asof_time.isoformat(),
        filters=filters,
    )
    
    return result


def asof_join(
    left: pl.LazyFrame | pl.DataFrame,
    right: pl.LazyFrame | pl.DataFrame,
    left_on: str,
    right_on: str,
    by: str | list[str],
    left_time_col: str = "ts_event",
    right_time_col: str = "ts_event",
    right_available_col: str = "ts_available",
    tolerance: str | None = None,
    suffix: str = "_right",
) -> pl.LazyFrame:
    """As-of join with anti-leakage protection (PRD §10.4).
    
    Joins left to the most recent prior row from right such that:
    - ts_event_right <= left_time
    - ts_available_right <= left_time
    
    This ensures we never join data that wasn't available at the time.
    
    Args:
        left: Left dataframe (driving table)
        right: Right dataframe (lookup table)
        left_on: Left time column for the join
        right_on: Right time column for the join
        by: Column(s) to join on (e.g., instrument_key)
        left_time_col: Time column in left for filtering
        right_time_col: Time column in right for join
        right_available_col: Availability column in right
        tolerance: Optional max time difference (e.g., "1h", "30m")
        suffix: Suffix for right columns in result
        
    Returns:
        Joined LazyFrame with anti-leakage guarantee
        
    Example:
        >>> # Join trades with most recent quote at time of trade
        >>> result = asof_join(
        ...     trades, quotes,
        ...     left_on="ts_event", right_on="ts_event",
        ...     by="instrument_key"
        ... )
    """
    if isinstance(left, pl.DataFrame):
        left = left.lazy()
    if isinstance(right, pl.DataFrame):
        right = right.lazy()
    
    # Ensure by is a list
    by_cols = [by] if isinstance(by, str) else list(by)
    
    # Filter right to only include rows where data was available
    # This is the anti-leakage protection - we can't see data
    # before it was available
    right_filtered = right.with_columns([
        # Create a "join-safe time" that is the max of event time and available time
        # This ensures we never join data that wasn't available yet
        pl.when(pl.col(right_available_col) > pl.col(right_time_col))
        .then(pl.col(right_available_col))
        .otherwise(pl.col(right_time_col))
        .alias("_asof_safe_time")
    ])
    
    # Perform the as-of join using the safe time
    result = left.join_asof(
        right_filtered,
        left_on=left_on,
        right_on="_asof_safe_time",
        by=by_cols,
        tolerance=tolerance,
        suffix=suffix,
        strategy="backward",  # Use most recent prior row
    )
    
    # Drop the helper column
    result = result.drop("_asof_safe_time" + suffix)
    
    logger.debug(
        "asof_join",
        left_on=left_on,
        right_on=right_on,
        by=by_cols,
    )
    
    return result


def read_asof_range(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    start_time: datetime,
    end_time: datetime,
    filters: dict[str, Any] | None = None,
    time_col: str = "ts_event",
    available_col: str = "ts_available",
) -> pl.LazyFrame:
    """Read data in a time range, as-of a specific time (PRD §10.3).
    
    This combines time range filtering with as-of cutoff.
    
    Args:
        df: Source data
        asof_time: Point-in-time cutoff for availability
        start_time: Start of the time range (on time_col)
        end_time: End of the time range (on time_col)
        filters: Optional additional filters
        time_col: Column for time range filtering
        available_col: Column for availability filtering
        
    Returns:
        Filtered LazyFrame
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    result = df.filter(
        (pl.col(time_col) >= start_time) &
        (pl.col(time_col) <= end_time) &
        (pl.col(available_col) <= asof_time)
    )
    
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    return result
