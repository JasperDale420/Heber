"""Volatility Feature Templates (PRD §32.2).

Volatility features for risk management and position sizing.
Dependencies: Silver bars dataset
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from heber.features.templates._utils import rolling_max_timestamp


def compute_parkinson_vol(high: pd.Series, low: pd.Series, window: int) -> pd.Series:
    """Compute Parkinson volatility (uses high/low range).

    Args:
        high: High price series
        low: Low price series
        window: Rolling window size

    Returns:
        Annualized Parkinson volatility
    """
    log_hl = np.log(high / low)
    return np.sqrt((log_hl**2).rolling(window).mean() / (4 * np.log(2))) * np.sqrt(252)


def compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Compute Average True Range (ATR).

    Args:
        high: High price series
        low: Low price series
        close: Close price series
        period: ATR period

    Returns:
        ATR values
    """
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def compute_volatility_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """Compute volatility features for each instrument.

    Args:
        bars_df: DataFrame with OHLCV bar data

    Returns:
        DataFrame with volatility features
    """

    def calc_features(df: pd.DataFrame) -> pd.DataFrame:
        close = df["close"]
        high = df["high"]
        low = df["low"]
        returns = close.pct_change()
        source = df["ts_available"] if "ts_available" in df.columns else df["bar_start_ts"]
        ts_available = rolling_max_timestamp(source, window=60)

        return pd.DataFrame(
            {
                "instrument_key": df.name,
                "ts_event": df["bar_start_ts"],
                "ts_available": ts_available,
                # Realized volatility (annualized)
                "vol_5d": returns.rolling(5).std() * np.sqrt(252),
                "vol_20d": returns.rolling(20).std() * np.sqrt(252),
                "vol_60d": returns.rolling(60).std() * np.sqrt(252),
                # Volatility ratio (short/long)
                "vol_ratio_5_20": returns.rolling(5).std() / returns.rolling(20).std(),
                "vol_ratio_20_60": returns.rolling(20).std() / returns.rolling(60).std(),
                # Parkinson volatility (uses high/low)
                "parkinson_vol_20d": compute_parkinson_vol(high, low, 20),
                # Average True Range (ATR)
                "atr_14": compute_atr(high, low, close, 14),
                "atr_20": compute_atr(high, low, close, 20),
                # Bollinger Band width (volatility proxy)
                "bb_width_20": (
                    (close.rolling(20).mean() + 2 * close.rolling(20).std())
                    - (close.rolling(20).mean() - 2 * close.rolling(20).std())
                )
                / close.rolling(20).mean(),
                # Z-score of price
                "price_zscore_20d": (close - close.rolling(20).mean()) / close.rolling(20).std(),
                "price_zscore_60d": (close - close.rolling(60).mean()) / close.rolling(60).std(),
            }
        )

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_features, include_groups=False)
