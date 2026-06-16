"""OI momentum feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
oi_momentum_source = FileSource(
    name="oi_momentum_source",
    path=gold_dataset_glob("oi_momentum_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: per-ticker daily OI momentum metrics
oi_momentum_features = FeatureView(
    name="oi_momentum_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="oi_buildup_ratio", dtype=Float64),
        Field(name="new_position_signal", dtype=Float64),
        Field(name="oi_change_momentum_5d", dtype=Float64),
    ],
    source=oi_momentum_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "flow",
    },
)
