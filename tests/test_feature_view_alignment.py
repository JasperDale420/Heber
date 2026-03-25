"""Regression tests for Feast feature-view schema/path alignment."""

from __future__ import annotations

import importlib
import sys
import types

import pytest


def _build_feast_stub():
    """Build a lightweight feast stub with real classes for schema/path assertions."""
    feast_module = types.ModuleType("feast")
    # Mark as package so ``from feast.types import ...`` works reliably.
    feast_module.__path__ = []  # type: ignore[attr-defined]

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

    class Float64:
        pass

    class Int64:
        pass

    class String:
        pass

    feast_types.Float32 = Float32
    feast_types.Float64 = Float64
    feast_types.Int64 = Int64
    feast_types.String = String

    feast_module.Entity = Entity
    feast_module.Field = Field
    feast_module.FileSource = FileSource
    feast_module.FeatureView = FeatureView
    feast_module.types = feast_types

    return feast_module, feast_types


@pytest.fixture(autouse=True)
def _install_feast_stub():
    """Install fresh feast stub before each test to prevent cross-test pollution.

    The module-level ``try: import feast`` trick fails when another test file
    (e.g. test_feast_materialization_behavior) injects a MagicMock into
    sys.modules["feast"]. We unconditionally install the real stub here so
    that feature-view modules always resolve against concrete classes.
    """
    # Save original state
    saved_feast = sys.modules.get("feast")
    saved_feast_types = sys.modules.get("feast.types")
    saved_fv = {k: v for k, v in sys.modules.items() if k.startswith("features.feature_views")}

    feast_module, feast_types = _build_feast_stub()
    sys.modules["feast"] = feast_module
    sys.modules["feast.types"] = feast_types
    # Evict cached feature-view modules so they re-evaluate against stub
    for key in [k for k in sys.modules if k.startswith("features.feature_views")]:
        del sys.modules[key]
    yield
    # Restore original state so we don't pollute other test files
    if saved_feast is not None:
        sys.modules["feast"] = saved_feast
    else:
        sys.modules.pop("feast", None)
    if saved_feast_types is not None:
        sys.modules["feast.types"] = saved_feast_types
    else:
        sys.modules.pop("feast.types", None)
    for key in [k for k in sys.modules if k.startswith("features.feature_views")]:
        del sys.modules[key]
    sys.modules.update(saved_fv)


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
            "watch_id",
            "instrument_key",
            "occ_symbol",
            "underlying",
            "put_call",
            "horizon",
            "outcome",
            "outcome_reason",
            "hit_tp_first",
            "contract_hit_tp_first",
            "mfe",
            "mae",
            "bars_to_hit",
            "time_to_mfe_seconds",
            "time_to_mae_seconds",
            "mfe_mae_ratio",
            "excursion_velocity",
            "capture_efficiency",
            "contract_mfe",
            "contract_mae",
            "contract_mfe_adj",
            "contract_mae_adj",
            "contract_bars_to_hit",
            "trading_minutes_to_hit",
            "outcome_return",
            "entry_price",
            "spot_at_alert",
            "window_duration_hours",
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
    # Evict cached module to force re-evaluation with the test feast mock
    sys.modules.pop(module_name, None)
    module = importlib.import_module(module_name)
    source = getattr(module, source_name)
    path = source.path

    assert f"dataset={dataset_name}" in path
    assert "project=" in path
    assert "version=" in path
    assert "dt=" in path
    assert path.endswith("*.parquet")


def test_feast_stub_behaves_like_package_for_types_import() -> None:
    import feast.types as feast_types  # type: ignore[import-not-found]

    assert hasattr(feast_types, "Float32")
    assert hasattr(feast_types, "Float64")
    assert hasattr(feast_types, "Int64")
    assert hasattr(feast_types, "String")
