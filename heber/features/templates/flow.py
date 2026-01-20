"""Options Flow Feature Templates (PRD §32.3).

Options flow intelligence features from Unusual Whales data.
Dependencies: Silver flow_alerts, darkpool_trades datasets
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def compute_flow_features(
    flow_df: pd.DataFrame,
    bars_df: pd.DataFrame | None = None,
    lookback_hours: int = 24,
) -> pd.DataFrame:
    """Compute flow-based features aggregated per underlying per timestamp.
    
    Args:
        flow_df: Options flow data with columns [underlying, ts_event, premium, put_call, alert_type, ...]
        bars_df: Optional underlying bars for normalization
        lookback_hours: Lookback window in hours
        
    Returns:
        DataFrame with flow features
    """
    result_frames = []
    
    for underlying, group in flow_df.groupby("underlying"):
        df = group.sort_values("ts_event").copy()
        
        # Premium aggregates
        call_mask = df["put_call"] == "C"
        put_mask = df["put_call"] == "P"
        sweep_mask = df["alert_type"] == "SWEEP"
        
        total_premium = df["premium"].rolling(f"{lookback_hours}h", on="ts_event").sum()
        call_premium = df.loc[call_mask, "premium"].reindex(df.index).fillna(0).rolling(f"{lookback_hours}h", on="ts_event").sum()
        put_premium = df.loc[put_mask, "premium"].reindex(df.index).fillna(0).rolling(f"{lookback_hours}h", on="ts_event").sum()
        
        result = pd.DataFrame({
            "instrument_key": f"equity:{underlying}",
            "ts_event": df["ts_event"],
            "ts_available": pd.Timestamp.now(tz="UTC"),
            
            # Premium aggregates
            "total_premium_24h": total_premium,
            "call_premium_24h": call_premium,
            "put_premium_24h": put_premium,
            
            # Call/Put ratio
            "call_put_premium_ratio": call_premium / put_premium.replace(0, np.nan),
            
            # Net premium (call - put)
            "net_premium_24h": call_premium - put_premium,
            
            # Sweep activity
            "sweep_count_24h": sweep_mask.astype(int).rolling(f"{lookback_hours}h", on="ts_event").sum(),
        })
        
        result_frames.append(result)
    
    if not result_frames:
        return pd.DataFrame()
    
    return pd.concat(result_frames, ignore_index=True)
