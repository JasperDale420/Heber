"""Options Flow Feature Templates (PRD §32.3).

Options flow intelligence features from Unusual Whales data.
Dependencies: Silver flow_alerts, darkpool datasets
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from heber.features.templates._utils import rolling_max_timestamp_time


def compute_flow_features(
    flow_df: pd.DataFrame,
    _bars_df: pd.DataFrame | None = None,  # Reserved for price normalization
    lookback_hours: int = 24,
) -> pd.DataFrame:
    """Compute flow-based features aggregated per underlying per timestamp.

    Args:
        flow_df: Options flow data with columns [underlying, ts_event, premium, put_call, alert_type, ...]
        _bars_df: Reserved for underlying bars normalization (not yet implemented)
        lookback_hours: Lookback window in hours

    Returns:
        DataFrame with flow features
    """
    result_frames = []

    for underlying, group in flow_df.groupby("underlying"):
        df = group.copy()
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
        df = df.dropna(subset=["ts_event"]).sort_values("ts_event")

        if df.empty:
            continue

        source = df["ts_available"] if "ts_available" in df.columns else df["ts_event"]
        ts_event = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
        ts_available = rolling_max_timestamp_time(source, ts_event, lookback_hours)
        df = df.set_index("ts_event")

        # Premium aggregates
        call_mask = df["put_call"] == "C"
        put_mask = df["put_call"] == "P"
        sweep_mask = df["alert_type"] == "SWEEP"

        total_premium = df["premium"].rolling(f"{lookback_hours}h").sum()
        call_premium = df["premium"].where(call_mask, 0).rolling(f"{lookback_hours}h").sum()
        put_premium = df["premium"].where(put_mask, 0).rolling(f"{lookback_hours}h").sum()

        result = pd.DataFrame(
            {
                "instrument_key": f"equity:{underlying}",
                "ts_event": df.index,
                "ts_available": ts_available.to_numpy(),
                # Premium aggregates
                "total_premium_24h": total_premium,
                "call_premium_24h": call_premium,
                "put_premium_24h": put_premium,
                # Call/Put ratio
                "call_put_premium_ratio": call_premium / put_premium.replace(0, np.nan),
                # Net premium (call - put)
                "net_premium_24h": call_premium - put_premium,
                # Sweep activity
                "sweep_count_24h": sweep_mask.astype(int).rolling(f"{lookback_hours}h").sum(),
            }
        )

        result_frames.append(result)

    if not result_frames:
        return pd.DataFrame()

    return pd.concat(result_frames, ignore_index=True)
