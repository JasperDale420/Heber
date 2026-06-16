"""Flow normalization feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
flow_normalization_source = FileSource(
    name="flow_normalization_source",
    path=gold_dataset_glob("flow_normalization_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: per-ticker daily flow normalization metrics
flow_normalization_features = FeatureView(
    name="flow_normalization_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="adv_premium_20d", dtype=Float64),
        Field(name="adv_volume_20d", dtype=Float64),
        Field(name="adv_oi_20d", dtype=Float64),
    ],
    source=flow_normalization_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "flow",
    },
)
