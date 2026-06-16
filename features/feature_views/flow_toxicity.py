"""Flow toxicity (VPIN) feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
flow_toxicity_source = FileSource(
    name="flow_toxicity_source",
    path=gold_dataset_glob("flow_toxicity_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: per-ticker daily flow toxicity metrics
flow_toxicity_features = FeatureView(
    name="flow_toxicity_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="flow_toxicity_1d", dtype=Float64),
        Field(name="toxicity_acceleration", dtype=Float64),
    ],
    source=flow_toxicity_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "flow",
    },
)
