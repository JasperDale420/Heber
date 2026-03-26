"""Market regime feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import market
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
market_regime_source = FileSource(
    name="market_regime_source",
    path=gold_dataset_glob("market_regime_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: market regime indicators
market_regime_features = FeatureView(
    name="market_regime_features",
    entities=[market],
    ttl=timedelta(days=90),
    schema=[
        Field(name="dispersion", dtype=Float64),
        Field(name="vol_of_vol", dtype=Float64),
        Field(name="breadth_proxy", dtype=Float64),
        Field(name="yield_curve_slope", dtype=Float64),
    ],
    source=market_regime_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "regime",
    },
)
