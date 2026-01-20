"""Slowly Changing Dimension (SCD) utilities for reference tables per PRD §10.6.

Reference tables with validity windows require special query logic.
"""

from datetime import datetime
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)


def read_reference_asof(
    df: pl.LazyFrame | pl.DataFrame,
    asof_time: datetime,
    key_col: str | list[str],
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    filters: dict[str, Any] | None = None,
) -> pl.LazyFrame:
    """Read reference table as-of a specific time with validity windows (PRD §10.6).
    
    For slowly changing dimensions, selects rows where:
        valid_from <= T AND (valid_to IS NULL OR valid_to > T)
    
    This ensures we get the version of the reference data that was
    valid at the specified time.
    
    Args:
        df: Source reference table
        asof_time: Point-in-time to query
        key_col: Primary key column(s) for the reference table
        valid_from_col: Column containing validity start
        valid_to_col: Column containing validity end (nullable)
        filters: Optional additional filters
        
    Returns:
        LazyFrame with valid reference rows at asof_time
        
    Example:
        >>> # Get option contracts that were valid on Jan 15, 2026
        >>> contracts = read_reference_asof(
        ...     option_contracts_df,
        ...     asof_time=datetime(2026, 1, 15, 10, 0),
        ...     key_col="occ_symbol"
        ... )
    """
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    
    # Apply validity window filter per PRD §10.6
    result = df.filter(
        (pl.col(valid_from_col) <= asof_time) &
        (
            pl.col(valid_to_col).is_null() |
            (pl.col(valid_to_col) > asof_time)
        )
    )
    
    # Apply additional filters
    if filters:
        for col, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = result.filter(pl.col(col).is_in(value))
            else:
                result = result.filter(pl.col(col) == value)
    
    logger.debug(
        "read_reference_asof",
        asof_time=asof_time.isoformat(),
        key_col=key_col,
    )
    
    return result


def join_with_reference_asof(
    left: pl.LazyFrame | pl.DataFrame,
    reference: pl.LazyFrame | pl.DataFrame,
    left_key: str | list[str],
    ref_key: str | list[str],
    left_time_col: str = "ts_event",
    ref_valid_from: str = "valid_from",
    ref_valid_to: str = "valid_to",
    suffix: str = "_ref",
) -> pl.LazyFrame:
    """Join fact table with reference table using validity windows (PRD §10.6).
    
    For each row in left, joins to the reference row that was valid at
    that row's time. This handles slowly changing dimensions correctly.
    
    Args:
        left: Fact table (e.g., trades)
        reference: Reference table with validity windows (e.g., option_contracts)
        left_key: Key column(s) in left table
        ref_key: Key column(s) in reference table
        left_time_col: Time column in left for validity check
        ref_valid_from: Validity start column in reference
        ref_valid_to: Validity end column in reference
        suffix: Suffix for reference columns
        
    Returns:
        Joined LazyFrame with point-in-time correct reference data
        
    Example:
        >>> # Join trades with option contract details valid at trade time
        >>> result = join_with_reference_asof(
        ...     trades,
        ...     option_contracts,
        ...     left_key="instrument_key",
        ...     ref_key="instrument_key"
        ... )
    """
    if isinstance(left, pl.DataFrame):
        left = left.lazy()
    if isinstance(reference, pl.DataFrame):
        reference = reference.lazy()
    
    # Ensure keys are lists
    left_keys = [left_key] if isinstance(left_key, str) else list(left_key)
    ref_keys = [ref_key] if isinstance(ref_key, str) else list(ref_key)
    
    # Join and filter by validity
    result = left.join(
        reference,
        left_on=left_keys,
        right_on=ref_keys,
        how="left",
        suffix=suffix,
    ).filter(
        # Keep only rows where reference was valid at left's time
        (pl.col(ref_valid_from + suffix) <= pl.col(left_time_col)) &
        (
            pl.col(ref_valid_to + suffix).is_null() |
            (pl.col(ref_valid_to + suffix) > pl.col(left_time_col))
        )
    )
    
    logger.debug(
        "join_with_reference_asof",
        left_key=left_keys,
        ref_key=ref_keys,
    )
    
    return result
