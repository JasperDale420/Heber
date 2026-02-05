"""Volatility feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files for volatility features
volatility_source = FileSource(
    name="volatility_source",
    path=gold_dataset_glob("volatility_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: volatility indicators
volatility_features = FeatureView(
    name="volatility_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="vol_5d", dtype=Float32),
        Field(name="vol_20d", dtype=Float32),
        Field(name="vol_60d", dtype=Float32),
        Field(name="vol_ratio_5_20", dtype=Float32),
        Field(name="vol_ratio_20_60", dtype=Float32),
        Field(name="parkinson_vol_20d", dtype=Float32),
        Field(name="atr_14", dtype=Float32),
        Field(name="atr_20", dtype=Float32),
        Field(name="bb_width_20", dtype=Float32),
        Field(name="price_zscore_20d", dtype=Float32),
        Field(name="price_zscore_60d", dtype=Float32),
    ],
    source=volatility_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "volatility",
    },
)
