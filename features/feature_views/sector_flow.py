"""Sector flow alignment feature views for Feast.

Per-alert sector context signals -- captures whether an alert's direction agrees
with the broader sector flow at the time of the alert.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import alert
from features.feature_views._paths import gold_dataset_glob

sector_flow_source = FileSource(
    name="sector_flow_source",
    path=gold_dataset_glob("sector_flow_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

sector_flow_features = FeatureView(
    name="sector_flow_features",
    entities=[alert],
    ttl=timedelta(days=90),
    schema=[
        Field(name="sector_flow_alignment", dtype=Float64),
        Field(name="sector_call_put_ratio", dtype=Float64),
    ],
    source=sector_flow_source,
    online=True,
    tags={"owner": "quant_team", "category": "sector_flow"},
)
