"""Microstructure feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files for microstructure features
microstructure_source = FileSource(
    name="microstructure_source",
    path=gold_dataset_glob("microstructure_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: market microstructure indicators
microstructure_features = FeatureView(
    name="microstructure_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="bid_ask_spread", dtype=Float32),
        Field(name="spread_bps", dtype=Float32),
        Field(name="mid_px", dtype=Float32),
        Field(name="bid_depth", dtype=Int64),
        Field(name="ask_depth", dtype=Int64),
        Field(name="depth_imbalance", dtype=Float32),
        Field(name="total_depth", dtype=Int64),
    ],
    source=microstructure_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "microstructure",
    },
)
