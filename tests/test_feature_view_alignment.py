"""Regression tests for Feast feature-view schema/path alignment."""

from __future__ import annotations

import importlib
import sys
import types

import pytest

try:
    import feast  # noqa: F401
except ImportError:
    feast_module = types.ModuleType("feast")

    class Entity:
        def __init__(self, name: str, description: str | None = None, join_keys: list[str] | None = None):
            self.name = name
            self.description = description
            self.join_keys = join_keys or []

    class Field:
        def __init__(self, name: str, dtype):
            self.name = name
            self.dtype = dtype

    class FileSource:
        def __init__(
            self,
            name: str,
            path: str,
            timestamp_field: str,
            created_timestamp_column: str | None = None,
        ):
            self.name = name
            self.path = path
            self.timestamp_field = timestamp_field
            self.created_timestamp_column = created_timestamp_column

    class FeatureView:
        def __init__(
            self,
            name: str,
            entities: list[Entity],
            ttl,
            schema: list[Field],
            source: FileSource,
            online: bool,
            tags: dict | None = None,
        ):
            self.name = name
            self.entities = entities
            self.ttl = ttl
            self.schema = schema
            self.source = source
            self.online = online
            self.tags = tags or {}

    feast_types = types.ModuleType("feast.types")

    class Float32:
        pass

    class Int64:
        pass

    class String:
        pass

    feast_types.Float32 = Float32
    feast_types.Int64 = Int64
    feast_types.String = String

    feast_module.Entity = Entity
    feast_module.Field = Field
    feast_module.FileSource = FileSource
    feast_module.FeatureView = FeatureView

    sys.modules["feast"] = feast_module
    sys.modules["feast.types"] = feast_types


FEATURE_VIEW_CASES = [
    (
        "features.feature_views.flow",
        "flow_features",
        [
            "total_premium_24h",
            "call_premium_24h",
            "put_premium_24h",
            "call_put_premium_ratio",
            "net_premium_24h",
            "sweep_count_24h",
        ],
    ),
    (
        "features.feature_views.microstructure",
        "microstructure_features",
        [
            "bid_ask_spread",
            "spread_bps",
            "mid_px",
            "bid_depth",
            "ask_depth",
            "depth_imbalance",
            "total_depth",
        ],
    ),
    (
        "features.feature_views.momentum",
        "momentum_features",
        [
            "momentum_1d",
            "momentum_5d",
            "momentum_10d",
            "momentum_20d",
            "momentum_60d",
            "roc_5d",
            "roc_20d",
            "rsi_14",
            "rsi_28",
            "macd",
            "macd_signal",
        ],
    ),
    (
        "features.feature_views.volatility",
        "volatility_features",
        [
            "vol_5d",
            "vol_20d",
            "vol_60d",
            "vol_ratio_5_20",
            "vol_ratio_20_60",
            "parkinson_vol_20d",
            "atr_14",
            "atr_20",
            "bb_width_20",
            "price_zscore_20d",
            "price_zscore_60d",
        ],
    ),
    (
        "features.feature_views.labels",
        "returns_1d",
        [
            "return_1d",
            "direction_1d",
            "is_up_1d",
            "is_down_1d",
        ],
    ),
    (
        "features.feature_views.labels",
        "returns_5d",
        [
            "return_5d",
            "direction_5d",
            "is_up_5d",
            "is_down_5d",
        ],
    ),
    (
        "features.feature_views.alert_labels",
        "alert_barrier_labels",
        [
            "underlying",
            "put_call",
            "dte",
            "horizon",
            "spot_at_alert",
            "atr_at_alert",
            "tp_threshold",
            "sl_threshold",
            "hit_tp_first",
            "mfe",
            "mae",
            "mfe_adj",
            "mae_adj",
            "bars_to_hit",
            "contract_hit_tp_first",
            "contract_mfe",
            "contract_mae",
            "contract_mfe_adj",
            "contract_mae_adj",
            "contract_bars_to_hit",
            "beta_neutral_return",
            "vix_at_alert",
            "vix_regime",
        ],
    ),
]


SOURCE_CASES = [
    ("features.feature_views.flow", "flow_source", "flow_features"),
    ("features.feature_views.microstructure", "microstructure_source", "microstructure_features"),
    ("features.feature_views.momentum", "momentum_source", "momentum_features"),
    ("features.feature_views.volatility", "volatility_source", "volatility_features"),
    ("features.feature_views.labels", "returns_1d_source", "labels_returns_1d"),
    ("features.feature_views.labels", "returns_5d_source", "labels_returns_5d"),
    ("features.feature_views.alert_labels", "alert_barrier_labels_source", "labels_alert_barriers"),
    ("features.feature_views.alert_labels", "alert_intraday_labels_source", "labels_alert_intraday"),
    ("features.feature_views.alert_labels", "alert_swing_labels_source", "labels_alert_swing"),
]


@pytest.mark.parametrize(("module_name", "view_name", "expected_fields"), FEATURE_VIEW_CASES)
def test_feature_view_schema_matches_expected(module_name: str, view_name: str, expected_fields: list[str]) -> None:
    module = importlib.import_module(module_name)
    feature_view = getattr(module, view_name)
    field_names = [field.name for field in feature_view.schema]
    assert field_names == expected_fields


@pytest.mark.parametrize(("module_name", "source_name", "dataset_name"), SOURCE_CASES)
def test_feature_view_source_path_matches_gold_layout(
    module_name: str,
    source_name: str,
    dataset_name: str,
) -> None:
    module = importlib.import_module(module_name)
    source = getattr(module, source_name)
    path = source.path

    assert f"dataset={dataset_name}" in path
    assert "project=" in path
    assert "version=" in path
    assert "dt=" in path
    assert path.endswith("*.parquet")
