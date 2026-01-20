"""Options flow feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64

from features.entities import equity

# Source: Gold Parquet files for flow features
flow_source = FileSource(
    name="flow_source",
    path="/data/gold/dataset=flow_features/",
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: options flow indicators
flow_features = FeatureView(
    name="flow_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="net_call_premium", dtype=Float32),
        Field(name="net_put_premium", dtype=Float32),
        Field(name="put_call_ratio", dtype=Float32),
        Field(name="unusual_activity_score", dtype=Float32),
        Field(name="whale_net_premium", dtype=Float32),
        Field(name="smart_money_divergence", dtype=Float32),
        Field(name="total_volume", dtype=Int64),
        Field(name="open_interest_change", dtype=Int64),
    ],
    source=flow_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "flow",
        "data_source": "unusual_whales",
    },
)
