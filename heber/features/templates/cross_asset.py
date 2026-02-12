"""Cross-Asset Feature Templates (PRD §32.5).

Cross-asset and relative value features.
Dependencies: Silver bars for multiple instruments
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _rolling_max_datetime(series: pd.Series, window: int) -> pd.Series:
    source = pd.to_datetime(series, utc=True, errors="coerce")
    source_naive = source.dt.tz_convert("UTC").dt.tz_localize(None)
    source_int = source_naive.astype("int64")
    rolled = pd.Series(source_int, index=series.index).rolling(window=window, min_periods=1).max()
    return pd.to_datetime(rolled, utc=True)


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
    benchmark_cols = ["bar_start_ts", "close"]
    if "ts_available" in bars_df.columns:
        benchmark_cols.append("ts_available")

    benchmark = bars_df[bars_df["instrument_key"] == benchmark_key][benchmark_cols].rename(
        columns={"close": "benchmark_close", "ts_available": "benchmark_ts_available"}
    )

    # Merge with all instruments
    merged = bars_df.merge(benchmark, on="bar_start_ts", how="left")

    if "ts_available" in merged.columns:
        base_available = pd.to_datetime(merged["ts_available"], utc=True, errors="coerce")
    else:
        base_available = pd.to_datetime(merged["bar_start_ts"], utc=True, errors="coerce")

    if "benchmark_ts_available" in merged.columns:
        bench_available = pd.to_datetime(merged["benchmark_ts_available"], utc=True, errors="coerce")
        base_available = pd.concat([base_available, bench_available], axis=1).max(axis=1)

    merged["_ts_available_source"] = base_available

    def calc_features(df: pd.DataFrame) -> pd.DataFrame:
        key = df.name
        df = df.sort_values("bar_start_ts")
        returns = df["close"].pct_change()
        bench_returns = df["benchmark_close"].pct_change()
        ts_available = _rolling_max_datetime(df["_ts_available_source"], window=60)

        return pd.DataFrame(
            {
                "instrument_key": key,
                "ts_event": df["bar_start_ts"],
                "ts_available": ts_available,
                # Relative strength
                "rel_strength_20d": (
                    (df["close"] / df["close"].shift(20)) / (df["benchmark_close"] / df["benchmark_close"].shift(20))
                ),
                # Beta (rolling)
                "beta_60d": (
                    returns.rolling(60).cov(bench_returns) / bench_returns.rolling(60).var().replace(0, np.nan)
                ),
                # Alpha (excess return vs benchmark)
                "alpha_20d": returns.rolling(20).mean() - bench_returns.rolling(20).mean(),
                # Correlation to benchmark
                "corr_spy_20d": returns.rolling(20).corr(bench_returns),
                "corr_spy_60d": returns.rolling(60).corr(bench_returns),
                # Idiosyncratic volatility
                "idio_vol_20d": (returns - bench_returns).rolling(20).std() * np.sqrt(252),
            }
        )

    # Filter out benchmark from features
    non_benchmark = merged[merged["instrument_key"] != benchmark_key]

    if non_benchmark.empty:
        return pd.DataFrame()

    return non_benchmark.groupby("instrument_key", group_keys=False).apply(calc_features, include_groups=False)
