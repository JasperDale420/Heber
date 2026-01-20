"""Volatility feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity

# Source: Gold Parquet files for volatility features
volatility_source = FileSource(
    name="volatility_source",
    path="/data/gold/dataset=volatility_features/",
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: volatility indicators
volatility_features = FeatureView(
    name="volatility_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="volatility_5d", dtype=Float32),
        Field(name="volatility_10d", dtype=Float32),
        Field(name="volatility_20d", dtype=Float32),
        Field(name="volatility_60d", dtype=Float32),
        Field(name="atr_14", dtype=Float32),
        Field(name="atr_20", dtype=Float32),
        Field(name="bollinger_upper", dtype=Float32),
        Field(name="bollinger_lower", dtype=Float32),
        Field(name="bollinger_width", dtype=Float32),
    ],
    source=volatility_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "volatility",
    },
)
