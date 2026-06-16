"""Straddle momentum feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
straddle_momentum_source = FileSource(
    name="straddle_momentum_source",
    path=gold_dataset_glob("straddle_momentum_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: straddle momentum (implied-vs-realized vol dynamics)
straddle_momentum_features = FeatureView(
    name="straddle_momentum_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="straddle_return_1m", dtype=Float32),
        Field(name="straddle_return_3m", dtype=Float32),
    ],
    source=straddle_momentum_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "volatility",
    },
)
