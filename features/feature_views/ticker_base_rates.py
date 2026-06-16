"""Ticker base rates feature view for Feast.

Per-ticker daily base rate statistics computed from historical alert labels.
Captures which tickers are historically predictable, creating a feedback loop
for prediction.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
ticker_base_rates_source = FileSource(
    name="ticker_base_rates_source",
    path=gold_dataset_glob("ticker_base_rates"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: per-ticker base rate statistics
ticker_base_rates_features = FeatureView(
    name="ticker_base_rates",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="ticker_win_rate_90d", dtype=Float64),
        Field(name="ticker_alert_frequency", dtype=Int64),
        Field(name="ticker_flow_predictability", dtype=Float64),
    ],
    source=ticker_base_rates_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "base_rates",
        "data_source": "labels_alert_barriers",
        "window": "90d_rolling",
    },
)
