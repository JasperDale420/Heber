"""Microstructure Feature Templates (PRD §32.4).

Market microstructure features from quotes and trades.
Dependencies: Silver quotes, trades datasets
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _derive_ts_available(group: pd.DataFrame) -> pd.Series:
    source = group["ts_available"] if "ts_available" in group.columns else group["ts_event"]
    return pd.to_datetime(source, utc=True, errors="coerce")


def compute_microstructure_features(
    quotes_df: pd.DataFrame,
    _trades_df: pd.DataFrame | None = None,  # Reserved for trade-based metrics
) -> pd.DataFrame:
    """Compute market microstructure features.

    Useful for execution quality and short-term alpha.

    Args:
        quotes_df: Quote data with [instrument_key, ts_event, bid_px, ask_px, bid_sz, ask_sz]
        _trades_df: Reserved for trade-based metrics (not yet implemented)

    Returns:
        DataFrame with microstructure features
    """
    df = quotes_df.copy()
    df["mid_px"] = (df["bid_px"] + df["ask_px"]) / 2

    def calc_features(group: pd.DataFrame) -> pd.DataFrame:
        ts_available = _derive_ts_available(group)
        return pd.DataFrame(
            {
                "instrument_key": group["instrument_key"],
                "ts_event": group["ts_event"],
                "ts_available": ts_available,
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
