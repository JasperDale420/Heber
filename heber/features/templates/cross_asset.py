"""Cross-Asset Feature Templates (PRD §32.5).

Cross-asset and relative value features.
Dependencies: Silver bars for multiple instruments
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def compute_relative_features(
    bars_df: pd.DataFrame,
    benchmark_key: str = "equity:SPY",
) -> pd.DataFrame:
    """Compute features relative to a benchmark (e.g., SPY).
    
    Args:
        bars_df: Bar data for all instruments
        benchmark_key: Benchmark instrument key
        
    Returns:
        DataFrame with relative features
    """
    # Get benchmark data
    benchmark = bars_df[bars_df["instrument_key"] == benchmark_key][
        ["bar_start_ts", "close"]
    ].rename(columns={"close": "benchmark_close"})
    
    # Merge with all instruments
    merged = bars_df.merge(benchmark, on="bar_start_ts", how="left")
    
    def calc_features(df: pd.DataFrame) -> pd.DataFrame:
        returns = df["close"].pct_change()
        bench_returns = df["benchmark_close"].pct_change()
        
        return pd.DataFrame({
            "instrument_key": df["instrument_key"],
            "ts_event": df["bar_start_ts"],
            "ts_available": pd.Timestamp.now(tz="UTC"),
            
            # Relative strength
            "rel_strength_20d": (
                (df["close"] / df["close"].shift(20)) /
                (df["benchmark_close"] / df["benchmark_close"].shift(20))
            ),
            
            # Beta (rolling)
            "beta_60d": (
                returns.rolling(60).cov(bench_returns) /
                bench_returns.rolling(60).var().replace(0, np.nan)
            ),
            
            # Alpha (excess return vs benchmark)
            "alpha_20d": returns.rolling(20).mean() - bench_returns.rolling(20).mean(),
            
            # Correlation to benchmark
            "corr_spy_20d": returns.rolling(20).corr(bench_returns),
            "corr_spy_60d": returns.rolling(60).corr(bench_returns),
            
            # Idiosyncratic volatility
            "idio_vol_20d": (returns - bench_returns).rolling(20).std() * np.sqrt(252),
        })
    
    # Filter out benchmark from features
    non_benchmark = merged[merged["instrument_key"] != benchmark_key]
    
    if non_benchmark.empty:
        return pd.DataFrame()
    
    return non_benchmark.groupby("instrument_key", group_keys=False).apply(calc_features)
