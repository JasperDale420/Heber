"""Market tide context feature views for Feast."""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64

from features.entities import market
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
market_tide_context_source = FileSource(
    name="market_tide_context_source",
    path=gold_dataset_glob("market_tide_context_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",  # Point-in-time gate
)

# Feature View: market tide context indicators
market_tide_context_features = FeatureView(
    name="market_tide_context_features",
    entities=[market],
    ttl=timedelta(days=90),
    schema=[
        Field(name="market_sentiment_score", dtype=Float64),
        Field(name="market_premium_momentum", dtype=Float64),
    ],
    source=market_tide_context_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "sentiment",
    },
)
