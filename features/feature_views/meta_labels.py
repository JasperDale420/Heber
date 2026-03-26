"""Meta-label feature views for Feast (PRD §31.13).

Features captured at alert arrival time for training meta-models that predict
which flow alerts will hit their target price before stop loss.

Schema matches the actual Gold Parquet output from the watch feature extractor
(meta_label_features dataset, 48 columns including Gold contract fields).
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

from features.entities import alert
from features.feature_views._paths import gold_dataset_glob

# =============================================================================
# Meta-Label Features (captured at alert time for meta-model training)
# =============================================================================

meta_label_features_source = FileSource(
    name="meta_label_features_source",
    path=gold_dataset_glob("meta_label_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

meta_label_features = FeatureView(
    name="meta_label_features",
    entities=[alert],
    ttl=timedelta(days=365),
    schema=[
        # Gold contract fields
        Field(name="instrument_key", dtype=String),
        # Alert identifiers / context
        Field(name="alert_time", dtype=String),  # ISO timestamp kept as string for partition col
        Field(name="symbol", dtype=String),
        # Contract info
        Field(name="occ_symbol", dtype=String),
        Field(name="underlying", dtype=String),
        Field(name="strike", dtype=Float64),
        Field(name="expiry", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="days_to_expiry", dtype=Int64),
        # Alert characteristics
        Field(name="premium", dtype=Float64),
        Field(name="volume", dtype=Float64),
        Field(name="open_interest", dtype=Float64),
        Field(name="volume_oi_ratio", dtype=Float64),
        Field(name="alert_type", dtype=String),
        Field(name="side", dtype=String),
        Field(name="aggressor", dtype=String),
        # Prices at alert time
        Field(name="spot_price", dtype=Float64),
        Field(name="contract_price", dtype=Float64),
        # Moneyness
        Field(name="moneyness", dtype=Float64),
        Field(name="log_moneyness", dtype=Float64),
        # Greeks
        Field(name="delta", dtype=Float64),
        Field(name="gamma", dtype=Float64),
        Field(name="theta", dtype=Float64),
        Field(name="vega", dtype=Float64),
        Field(name="iv", dtype=Float64),
        # Market context (underlying returns / vol)
        Field(name="underlying_30d_return", dtype=Float64),
        Field(name="underlying_5d_return", dtype=Float64),
        Field(name="underlying_1d_return", dtype=Float64),
        Field(name="realized_vol_20d", dtype=Float64),
        Field(name="iv_rank", dtype=Float64),
        # GEX / Market Structure
        Field(name="gex", dtype=Float64),
        Field(name="vex", dtype=Float64),
        # Max Pain
        Field(name="max_pain_strike", dtype=Float64),
        Field(name="max_pain_distance_pct", dtype=Float64),
        # Market Tide
        Field(name="market_tide_net_premium", dtype=Float64),
        Field(name="market_tide_direction", dtype=String),
        # Timing features
        Field(name="hour_of_day", dtype=Int64),
        Field(name="minute_of_hour", dtype=Int64),
        Field(name="day_of_week", dtype=Int64),
        Field(name="minutes_since_open", dtype=Int64),
        Field(name="minutes_to_close", dtype=Int64),
        # Sentiment / type flags
        Field(name="is_bullish", dtype=Int64),
        Field(name="is_bearish", dtype=Int64),
        Field(name="is_sweep", dtype=Int64),
        Field(name="is_block", dtype=Int64),
        Field(name="is_unusual", dtype=Int64),
    ],
    source=meta_label_features_source,
    online=False,  # Features are offline-only for training
    tags={
        "dataset_type": "feature",
        "feature_method": "alert_snapshot",
        "owner": "quant_team",
        "data_source": "unusual_whales",
        "use_case": "meta_labeling",
    },
)
