"""Momentum feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
momentum_source = FileSource(
    name="momentum_source",
    path=gold_dataset_glob("momentum_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: momentum indicators
momentum_features = FeatureView(
    name="momentum_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="momentum_1d", dtype=Float32),
        Field(name="momentum_5d", dtype=Float32),
        Field(name="momentum_10d", dtype=Float32),
        Field(name="momentum_20d", dtype=Float32),
        Field(name="momentum_60d", dtype=Float32),
        Field(name="roc_5d", dtype=Float32),
        Field(name="roc_20d", dtype=Float32),
        Field(name="rsi_14", dtype=Float32),
        Field(name="rsi_28", dtype=Float32),
        Field(name="macd", dtype=Float32),
        Field(name="macd_signal", dtype=Float32),
    ],
    source=momentum_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "technical",
    },
)
