"""Microstructure Feature Templates (PRD §32.4).

Market microstructure features from quotes and trades.
Dependencies: Silver quotes, trades datasets
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_microstructure_features(
    quotes_df: pd.DataFrame,
    trades_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute market microstructure features.

    Useful for execution quality and short-term alpha.

    Args:
        quotes_df: Quote data with [instrument_key, ts_event, bid_px, ask_px, bid_sz, ask_sz]
        trades_df: Optional trade data for additional metrics

    Returns:
        DataFrame with microstructure features
    """
    df = quotes_df.copy()
    df["mid_px"] = (df["bid_px"] + df["ask_px"]) / 2

    def calc_features(group: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "instrument_key": group["instrument_key"],
                "ts_event": group["ts_event"],
                "ts_available": pd.Timestamp.now(tz="UTC"),
                # Spread metrics
                "bid_ask_spread": group["ask_px"] - group["bid_px"],
                "spread_bps": (group["ask_px"] - group["bid_px"]) / group["mid_px"] * 10000,
                # Mid price
                "mid_px": group["mid_px"],
                # Depth metrics
                "bid_depth": group["bid_sz"],
                "ask_depth": group["ask_sz"],
                "depth_imbalance": (group["bid_sz"] - group["ask_sz"])
                / (group["bid_sz"] + group["ask_sz"]).replace(0, np.nan),
                # Total depth
                "total_depth": group["bid_sz"] + group["ask_sz"],
            }
        )

    return df.groupby("instrument_key", group_keys=False).apply(calc_features)
