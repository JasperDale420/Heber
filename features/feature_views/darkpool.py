"""Darkpool confirmation feature views for Feast.

Per-ticker daily darkpool activity metrics -- cross-market confirmation of
institutional activity by comparing darkpool prints with options flow.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

darkpool_source = FileSource(
    name="darkpool_source",
    path=gold_dataset_glob("darkpool_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

darkpool_features = FeatureView(
    name="darkpool_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="darkpool_notional_1d", dtype=Float64),
        Field(name="darkpool_premium_ratio", dtype=Float64),
        Field(name="darkpool_activity_zscore", dtype=Float64),
    ],
    source=darkpool_source,
    online=True,
    tags={"owner": "quant_team", "category": "darkpool"},
)
