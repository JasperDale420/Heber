"""Duration parsing utilities for Gold layer modules.

Provides a unified parse_duration function that handles all time unit formats
used across labels, splits, and pipeline modules.
"""

from __future__ import annotations

import re
from datetime import timedelta


def parse_duration(duration_str: str) -> timedelta:
    """Parse human-readable duration string into timedelta.

    Supported units:
        M = months (approximated as 30 days)
        w = weeks
        d = days
        h = hours
        m = minutes
        s = seconds

    Note: 'M' (uppercase) = months, 'm' (lowercase) = minutes.

    Args:
        duration_str: Duration string with unit suffix, e.g. '5d', '1h', '30m', '3M'

    Returns:
        timedelta object

    Raises:
        ValueError: If format is invalid
    """
    match = re.match(r"(\d+)([Mwdhms])", duration_str)
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = int(match.group(1))
    unit = match.group(2)

    unit_map = {
        "M": timedelta(days=value * 30),
        "w": timedelta(weeks=value),
        "d": timedelta(days=value),
        "h": timedelta(hours=value),
        "m": timedelta(minutes=value),
        "s": timedelta(seconds=value),
    }
    return unit_map[unit]
