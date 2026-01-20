"""Microstructure feature views for Feast (PRD §31)."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64

from features.entities import equity

# Source: Gold Parquet files for microstructure features
microstructure_source = FileSource(
    name="microstructure_source",
    path="/data/gold/dataset=microstructure_features/",
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
        Field(name="bid_ask_spread_pct", dtype=Float32),
        Field(name="quoted_depth_bid", dtype=Int64),
        Field(name="quoted_depth_ask", dtype=Int64),
        Field(name="trade_imbalance", dtype=Float32),
        Field(name="vwap", dtype=Float32),
        Field(name="twap", dtype=Float32),
        Field(name="kyle_lambda", dtype=Float32),
        Field(name="amihud_illiquidity", dtype=Float32),
    ],
    source=microstructure_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "microstructure",
    },
)
