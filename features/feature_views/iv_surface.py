"""IV surface feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files for IV surface features
iv_surface_source = FileSource(
    name="iv_surface_source",
    path=gold_dataset_glob("iv_surface_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: implied volatility surface indicators
iv_surface_features = FeatureView(
    name="iv_surface_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="put_call_iv_skew", dtype=Float64),
        Field(name="term_structure_slope", dtype=Float64),
        Field(name="iv_change_1d", dtype=Float64),
    ],
    source=iv_surface_source,
    online=True,
    tags={"owner": "quant_team", "category": "volatility"},
)
