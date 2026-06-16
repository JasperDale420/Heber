"""GEX regime feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import market
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
gex_regime_source = FileSource(
    name="gex_regime_source",
    path=gold_dataset_glob("gex_regime_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: GEX regime indicators
gex_regime_features = FeatureView(
    name="gex_regime_features",
    entities=[market],
    ttl=timedelta(days=90),
    schema=[
        Field(name="net_gex", dtype=Float64),
        Field(name="gex_regime", dtype=Float64),
        Field(name="gex_flip_distance", dtype=Float64),
    ],
    source=gex_regime_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "regime",
    },
)
