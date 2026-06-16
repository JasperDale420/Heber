"""Trend-scanning label feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int32

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
trend_scan_source = FileSource(
    name="trend_scan_source",
    path=gold_dataset_glob("trend_scan_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: trend-scanning labels (Lopez de Prado AFML Ch 3.5)
trend_scan_features = FeatureView(
    name="trend_scan_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="trend_scan_horizon", dtype=Int32),
        Field(name="trend_scan_t_value", dtype=Float32),
    ],
    source=trend_scan_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "labels",
        "reference": "AFML_ch3.5",
    },
)
