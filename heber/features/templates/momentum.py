"""Momentum Feature Templates (PRD §32.1).

Technical momentum features for equity/crypto price action.
Dependencies: Silver bars dataset
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _derive_ts_available(df: pd.DataFrame, time_col: str, max_window: int) -> pd.Series:
    source = df["ts_available"] if "ts_available" in df.columns else df[time_col]
    source = pd.to_datetime(source, utc=True, errors="coerce")
    source_naive = source.dt.tz_convert("UTC").dt.tz_localize(None)
    source_int = source_naive.astype("int64")
    rolled = pd.Series(source_int, index=df.index).rolling(window=max_window, min_periods=1).max()
    return pd.to_datetime(rolled, utc=True)


def compute_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """Compute Relative Strength Index (RSI).

    Args:
        prices: Price series
        period: RSI lookback period

    Returns:
        RSI values (0-100)
    """
    delta = prices.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_momentum_features(bars_df: pd.DataFrame) -> pd.DataFrame:
    """Compute momentum features for each instrument.

    Input: Silver bars with columns [instrument_key, bar_start_ts, open, high, low, close, volume]
    Output: Gold features with ts_available derived from source availability

    Args:
        bars_df: DataFrame with OHLCV bar data

    Returns:
        DataFrame with momentum features
    """

    def calc_features(df: pd.DataFrame) -> pd.DataFrame:
        close = df["close"]
        ts_available = _derive_ts_available(df, "bar_start_ts", max_window=60)
        return pd.DataFrame(
            {
                "instrument_key": df.name,
                "ts_event": df["bar_start_ts"],
                "ts_available": ts_available,
                # Price momentum (returns over lookback)
                "momentum_1d": close.pct_change(1),
                "momentum_5d": close / close.shift(5) - 1,
                "momentum_10d": close / close.shift(10) - 1,
                "momentum_20d": close / close.shift(20) - 1,
                "momentum_60d": close / close.shift(60) - 1,
                # Rate of change
                "roc_5d": (close - close.shift(5)) / close.shift(5) * 100,
                "roc_20d": (close - close.shift(20)) / close.shift(20) * 100,
                # RSI (Relative Strength Index)
                "rsi_14": compute_rsi(close, 14),
                "rsi_28": compute_rsi(close, 28),
                # MACD
                "macd": close.ewm(span=12).mean() - close.ewm(span=26).mean(),
                "macd_signal": (close.ewm(span=12).mean() - close.ewm(span=26).mean()).ewm(span=9).mean(),
            }
        )

    return bars_df.groupby("instrument_key", group_keys=False).apply(calc_features, include_groups=False)
