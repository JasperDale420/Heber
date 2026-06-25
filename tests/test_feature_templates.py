"""Tests for Feature Templates (PRD §32)."""

import math

import numpy as np
import pandas as pd


class TestMomentumFeatures:
    """Test momentum feature computation."""

    def test_compute_rsi(self):
        from heber.features.templates.momentum import compute_rsi

        prices = pd.Series([100, 102, 101, 103, 105, 104, 106, 108, 107, 109] * 5)
        rsi = compute_rsi(prices, period=14)

        assert len(rsi) == len(prices)
        # RSI should be between 0 and 100 (excluding NaN warmup)
        valid_rsi = rsi.dropna()
        assert (valid_rsi >= 0).all() and (valid_rsi <= 100).all()

    def test_compute_momentum_features(self):
        from heber.features.templates.momentum import compute_momentum_features

        rng = np.random.default_rng(42)
        ts_available = pd.date_range("2024-01-02", periods=100, freq="D", tz="UTC")
        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 100,
                "bar_start_ts": pd.date_range("2024-01-01", periods=100, freq="D", tz="UTC"),
                "ts_available": ts_available,
                "open": rng.standard_normal(100).cumsum() + 100,
                "high": rng.standard_normal(100).cumsum() + 101,
                "low": rng.standard_normal(100).cumsum() + 99,
                "close": rng.standard_normal(100).cumsum() + 100,
                "volume": rng.integers(1000, 10000, 100),
            }
        )

        features = compute_momentum_features(bars)

        assert "momentum_5d" in features.columns
        assert "rsi_14" in features.columns
        assert "macd" in features.columns
        assert len(features) == len(bars)
        assert features["ts_available"].reset_index(drop=True).equals(pd.Series(ts_available).reset_index(drop=True))

    def test_momentum_ts_available_rolls_forward(self):
        from heber.features.templates.momentum import compute_momentum_features

        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 3,
                "bar_start_ts": pd.date_range("2024-01-01", periods=3, freq="D", tz="UTC"),
                "ts_available": [
                    pd.Timestamp("2024-01-03", tz="UTC"),
                    pd.Timestamp("2024-01-02", tz="UTC"),
                    pd.Timestamp("2024-01-04", tz="UTC"),
                ],
                "open": [1, 2, 3],
                "high": [1, 2, 3],
                "low": [1, 2, 3],
                "close": [1, 2, 3],
                "volume": [100, 200, 300],
            }
        )

        features = compute_momentum_features(bars)
        ts_available = features["ts_available"].reset_index(drop=True)

        assert ts_available.iloc[1] == pd.Timestamp("2024-01-03", tz="UTC")


class TestVolatilityFeatures:
    """Test volatility feature computation."""

    def test_compute_atr(self):
        from heber.features.templates.volatility import compute_atr

        high = pd.Series([102, 104, 103, 105, 106] * 10)
        low = pd.Series([99, 100, 101, 102, 103] * 10)
        close = pd.Series([101, 102, 102, 104, 105] * 10)

        atr = compute_atr(high, low, close, period=14)

        assert len(atr) == len(high)
        valid_atr = atr.dropna()
        assert (valid_atr >= 0).all()

    def test_compute_volatility_features(self):
        from heber.features.templates.volatility import compute_volatility_features

        rng = np.random.default_rng(42)
        ts_available = pd.date_range("2024-01-02", periods=100, freq="D", tz="UTC")
        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 100,
                "bar_start_ts": pd.date_range("2024-01-01", periods=100, freq="D", tz="UTC"),
                "ts_available": ts_available,
                "open": rng.standard_normal(100).cumsum() + 100,
                "high": rng.standard_normal(100).cumsum() + 101,
                "low": rng.standard_normal(100).cumsum() + 99,
                "close": rng.standard_normal(100).cumsum() + 100,
                "volume": rng.integers(1000, 10000, 100),
            }
        )

        features = compute_volatility_features(bars)

        assert "vol_20d" in features.columns
        assert "atr_14" in features.columns
        assert "bb_width_20" in features.columns
        assert features["ts_available"].reset_index(drop=True).equals(pd.Series(ts_available).reset_index(drop=True))


class TestCrossAssetFeatures:
    """Test cross-asset feature computation."""

    def test_compute_relative_features(self):
        from heber.features.templates.cross_asset import compute_relative_features

        rng = np.random.default_rng(42)
        dates = pd.date_range("2024-01-01", periods=100, freq="D", tz="UTC")
        aapl_available = pd.date_range("2024-01-02", periods=100, freq="D", tz="UTC")
        spy_available = pd.date_range("2024-01-03", periods=100, freq="D", tz="UTC")

        bars = pd.concat(
            [
                pd.DataFrame(
                    {
                        "instrument_key": ["equity:AAPL"] * 100,
                        "bar_start_ts": dates,
                        "ts_available": aapl_available,
                        "close": rng.standard_normal(100).cumsum() + 150,
                    }
                ),
                pd.DataFrame(
                    {
                        "instrument_key": ["equity:SPY"] * 100,
                        "bar_start_ts": dates,
                        "ts_available": spy_available,
                        "close": rng.standard_normal(100).cumsum() + 500,
                    }
                ),
            ]
        )

        features = compute_relative_features(bars, benchmark_key="equity:SPY")

        # Should only have AAPL features (not benchmark)
        assert (features["instrument_key"] == "equity:AAPL").all()
        assert "beta_60d" in features.columns
        assert "alpha_20d" in features.columns
        assert features["ts_available"].iloc[0] == spy_available[0]


class TestFlowFeatures:
    """Test flow feature computation."""

    def test_flow_ts_available_rolls_forward(self):
        from heber.features.templates.flow import compute_flow_features

        base = pd.Timestamp("2024-01-01 09:30", tz="UTC")
        df = pd.DataFrame(
            {
                "underlying": ["AAPL"] * 3,
                "ts_event": [base, base + pd.Timedelta(hours=1), base + pd.Timedelta(hours=2)],
                "ts_available": [
                    base + pd.Timedelta(hours=2),
                    base + pd.Timedelta(hours=1),
                    base + pd.Timedelta(hours=3),
                ],
                "premium": [100.0, 110.0, 120.0],
                "put_call": ["C", "C", "P"],
                "alert_type": ["SWEEP", "BLOCK", "SWEEP"],
            }
        )

        features = compute_flow_features(df, lookback_hours=24)
        ts_available = features["ts_available"].reset_index(drop=True)

        assert ts_available.iloc[1] == base + pd.Timedelta(hours=2)

    def test_flow_rolling_window_is_time_based(self):
        from heber.features.templates.flow import compute_flow_features

        base = pd.Timestamp("2024-01-01 09:30", tz="UTC")
        df = pd.DataFrame(
            {
                "underlying": ["AAPL", "AAPL"],
                "ts_event": [base, base + pd.Timedelta(hours=25)],
                "premium": [100.0, 40.0],
                "put_call": ["C", "C"],
                "alert_type": ["SWEEP", "SWEEP"],
            }
        )

        features = compute_flow_features(df, lookback_hours=24)

        # Second point should exclude the first event (>24h apart).
        second = features.sort_values("ts_event").iloc[-1]
        assert math.isclose(second["total_premium_24h"], 40.0, rel_tol=1e-9)

    def test_flow_normalizes_string_timestamps_to_utc(self):
        from heber.features.templates.flow import compute_flow_features

        df = pd.DataFrame(
            {
                "underlying": ["AAPL", "AAPL"],
                # 09:30 ET and 15:00 UTC should normalize to the same UTC instant.
                "ts_event": ["2024-01-01T09:30:00-05:00", "2024-01-01T15:00:00Z"],
                "premium": [100.0, 50.0],
                "put_call": ["C", "P"],
                "alert_type": ["SWEEP", "BLOCK"],
            }
        )

        features = compute_flow_features(df, lookback_hours=24)

        assert str(features["ts_event"].dtype).startswith("datetime64[ns, UTC]")


class TestMicrostructureFeatures:
    """Test microstructure feature computation."""

    def test_microstructure_ts_available_uses_source(self):
        from heber.features.templates.microstructure import compute_microstructure_features

        quotes = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 3,
                "ts_event": pd.date_range("2024-01-01", periods=3, freq="min", tz="UTC"),
                "ts_available": pd.date_range("2024-01-01 00:01", periods=3, freq="min", tz="UTC"),
                "bid_px": [100.0, 100.5, 101.0],
                "ask_px": [100.2, 100.7, 101.2],
                "bid_sz": [10, 12, 14],
                "ask_sz": [11, 13, 15],
            }
        )

        features = compute_microstructure_features(quotes)
        assert features["ts_available"].reset_index(drop=True).equals(quotes["ts_available"].reset_index(drop=True))


class TestLabelFeatures:
    """Test label computation."""

    def test_compute_return_labels(self):
        from heber.features.templates.labels import compute_return_labels

        # Business days so the horizon shift spans weekends — exercises the
        # trading-day-vs-calendar-day availability semantics.
        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 30,
                "bar_start_ts": pd.bdate_range("2024-01-01", periods=30),
                "close": np.linspace(100, 115, 30),
            }
        )

        labels = compute_return_labels(bars, horizons=[1, 5]).reset_index(drop=True)

        assert "return_1d" in labels.columns
        assert "return_5d" in labels.columns
        assert "ts_available" in labels.columns
        # ts_available must be derived from the resolving bar (trading days),
        # never leak before it. Friday 2024-01-05 resolves 1d at Mon 2024-01-08.
        fri = labels.loc[labels["ts_label"] == pd.Timestamp("2024-01-05"), "ts_available_1d"].iloc[0]
        assert fri >= pd.Timestamp("2024-01-09")  # after Monday's resolving close
        assert fri != pd.Timestamp("2024-01-06")  # the old calendar-day leak (Saturday)

    def test_compute_classification_labels(self):
        from heber.features.templates.labels import compute_classification_labels

        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 30,
                "bar_start_ts": pd.bdate_range("2024-01-01", periods=30),
                "close": np.linspace(100, 115, 30),
            }
        )

        labels = compute_classification_labels(bars, horizon=5, threshold=0.02).reset_index(drop=True)

        assert "direction_5d" in labels.columns
        assert "is_up_5d" in labels.columns
        assert labels["direction_5d"].isin([-1, 0, 1]).all()
        # ts_available derived from the 5th trading bar ahead, +1 day; never the
        # calendar-day value, and strictly after ts_event where defined.
        defined = labels["ts_available"].notna()
        assert (labels.loc[defined, "ts_available"] > labels.loc[defined, "ts_event"]).all()


def run_all_template_tests() -> dict[str, bool]:
    """Run all feature template tests."""
    results = {}

    test_classes = [
        TestMomentumFeatures,
        TestVolatilityFeatures,
        TestCrossAssetFeatures,
        TestLabelFeatures,
    ]

    for test_class in test_classes:
        instance = test_class()
        for method_name in dir(instance):
            if method_name.startswith("test_"):
                try:
                    getattr(instance, method_name)()
                    results[f"{test_class.__name__}.{method_name}"] = True
                except Exception as e:
                    results[f"{test_class.__name__}.{method_name}"] = False
                    print(f"FAILED: {test_class.__name__}.{method_name}: {e}")

    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"\nFeature Template Tests: {passed}/{total} passed")

    return results


if __name__ == "__main__":
    run_all_template_tests()
