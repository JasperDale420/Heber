"""Flow context feature views for Feast.

Per-alert clustering and directional agreement signals — captures whether an alert
is part of a cluster of similar activity on the same ticker.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64

from features.entities import alert
from features.feature_views._paths import gold_dataset_glob

flow_context_source = FileSource(
    name="flow_context_source",
    path=gold_dataset_glob("flow_context_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

flow_context_features = FeatureView(
    name="flow_context_features",
    entities=[alert],
    ttl=timedelta(days=90),
    schema=[
        Field(name="same_ticker_alerts_1h", dtype=Int64),
        Field(name="directional_agreement_4h", dtype=Float64),
        Field(name="repeat_ticker_days_5d", dtype=Int64),
    ],
    source=flow_context_source,
    online=True,
    tags={"owner": "quant_team", "category": "flow_context"},
)
