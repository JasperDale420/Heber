"""Tests for Feature Templates (PRD §32)."""

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
        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 100,
                "bar_start_ts": pd.date_range("2024-01-01", periods=100, freq="D"),
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
        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 100,
                "bar_start_ts": pd.date_range("2024-01-01", periods=100, freq="D"),
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


class TestCrossAssetFeatures:
    """Test cross-asset feature computation."""

    def test_compute_relative_features(self):
        from heber.features.templates.cross_asset import compute_relative_features

        rng = np.random.default_rng(42)
        dates = pd.date_range("2024-01-01", periods=100, freq="D")

        bars = pd.concat(
            [
                pd.DataFrame(
                    {
                        "instrument_key": ["equity:AAPL"] * 100,
                        "bar_start_ts": dates,
                        "close": rng.standard_normal(100).cumsum() + 150,
                    }
                ),
                pd.DataFrame(
                    {
                        "instrument_key": ["equity:SPY"] * 100,
                        "bar_start_ts": dates,
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


class TestLabelFeatures:
    """Test label computation."""

    def test_compute_return_labels(self):
        from heber.features.templates.labels import compute_return_labels

        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 30,
                "bar_start_ts": pd.date_range("2024-01-01", periods=30, freq="D"),
                "close": np.linspace(100, 115, 30),
            }
        )

        labels = compute_return_labels(bars, horizons=[1, 5])

        assert "return_1d" in labels.columns
        assert "return_5d" in labels.columns
        assert "ts_available" in labels.columns

    def test_compute_classification_labels(self):
        from heber.features.templates.labels import compute_classification_labels

        bars = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"] * 30,
                "bar_start_ts": pd.date_range("2024-01-01", periods=30, freq="D"),
                "close": np.linspace(100, 115, 30),
            }
        )

        labels = compute_classification_labels(bars, horizon=5, threshold=0.02)

        assert "direction_5d" in labels.columns
        assert "is_up_5d" in labels.columns
        assert labels["direction_5d"].isin([-1, 0, 1]).all()


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
