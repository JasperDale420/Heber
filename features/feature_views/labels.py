"""Label feature views for Feast (PRD §31.11)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files for return labels
returns_5d_source = FileSource(
    name="returns_5d_source",
    path=gold_dataset_glob("labels_returns_5d"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: 5-day forward return labels
returns_5d = FeatureView(
    name="labels_returns_5d",
    entities=[equity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="return_5d", dtype=Float32),
        Field(name="direction_5d", dtype=Int64),
        Field(name="is_up_5d", dtype=Int64),
        Field(name="is_down_5d", dtype=Int64),
    ],
    source=returns_5d_source,
    online=False,
    tags={
        "dataset_type": "label",
        "forward_window": "5d",
        "label_horizon": "close_to_close",
        "owner": "quant_team",
    },
)

# Source: Gold Parquet files for 1-day return labels
returns_1d_source = FileSource(
    name="returns_1d_source",
    path=gold_dataset_glob("labels_returns_1d"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: 1-day forward return labels
returns_1d = FeatureView(
    name="labels_returns_1d",
    entities=[equity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="return_1d", dtype=Float32),
        Field(name="direction_1d", dtype=Int64),
        Field(name="is_up_1d", dtype=Int64),
        Field(name="is_down_1d", dtype=Int64),
    ],
    source=returns_1d_source,
    online=False,
    tags={
        "dataset_type": "label",
        "forward_window": "1d",
        "label_horizon": "close_to_close",
        "owner": "quant_team",
    },
)
