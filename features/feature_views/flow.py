"""Options flow feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files for flow features
flow_source = FileSource(
    name="flow_source",
    path=gold_dataset_glob("flow_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: options flow indicators
flow_features = FeatureView(
    name="flow_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="total_premium_24h", dtype=Float32),
        Field(name="call_premium_24h", dtype=Float32),
        Field(name="put_premium_24h", dtype=Float32),
        Field(name="call_put_premium_ratio", dtype=Float32),
        Field(name="net_premium_24h", dtype=Float32),
        Field(name="sweep_count_24h", dtype=Float32),
    ],
    source=flow_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "flow",
        "data_source": "unusual_whales",
    },
)
