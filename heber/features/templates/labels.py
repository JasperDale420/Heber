"""Label Feature Templates (PRD §32.6).

Common label (target variable) computations.
Remember: ts_available = ts_label + forward_window
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_return_labels(
    bars_df: pd.DataFrame,
    horizons: list[int] | None = None,
) -> pd.DataFrame:
    """Compute forward-looking return labels.

    Args:
        bars_df: Bar data with [instrument_key, bar_start_ts, close]
        horizons: List of forward horizons in days (default: [1, 5, 10, 20])

    Returns:
        DataFrame with forward return labels
    """
    if horizons is None:
        horizons = [1, 5, 10, 20]

    def calc_labels(df: pd.DataFrame) -> pd.DataFrame:
        close = df["close"]
        result = {
            "instrument_key": df.name,
            "ts_label": df["bar_start_ts"],
            "ts_event": df["bar_start_ts"],
        }

        for h in horizons:
            # Forward return (what we're predicting)
            result[f"return_{h}d"] = close.shift(-h) / close - 1
            # ts_available = ts_label + horizon (label only observable after horizon passes)
            result[f"ts_available_{h}d"] = df["bar_start_ts"] + pd.Timedelta(days=h)

        result["ts_available"] = df["bar_start_ts"] + pd.Timedelta(days=max(horizons))

        return pd.DataFrame(result)

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_labels, include_groups=False)


def compute_classification_labels(
    bars_df: pd.DataFrame,
    horizon: int = 5,
    threshold: float = 0.02,
) -> pd.DataFrame:
    """Compute classification labels (up/down/neutral).

    Args:
        bars_df: Bar data with [instrument_key, bar_start_ts, close]
        horizon: Forward horizon in days
        threshold: Return threshold for up/down classification

    Returns:
        DataFrame with classification labels
    """

    def calc_labels(df: pd.DataFrame) -> pd.DataFrame:
        close = df["close"]
        forward_return = close.shift(-horizon) / close - 1

        # Classify: 1 = up, 0 = neutral, -1 = down
        classification = np.where(forward_return > threshold, 1, np.where(forward_return < -threshold, -1, 0))

        return pd.DataFrame(
            {
                "instrument_key": df.name,
                "ts_label": df["bar_start_ts"],
                "ts_event": df["bar_start_ts"],
                "ts_available": df["bar_start_ts"] + pd.Timedelta(days=horizon),
                f"return_{horizon}d": forward_return,
                f"direction_{horizon}d": classification,
                f"is_up_{horizon}d": (forward_return > threshold).astype(int),
                f"is_down_{horizon}d": (forward_return < -threshold).astype(int),
            }
        )

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_labels, include_groups=False)
