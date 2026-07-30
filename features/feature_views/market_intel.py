"""Market intelligence feature views for Feast.

Covers Gold datasets produced by the MarketIntelPipeline:
  - darkpool_features
  - greek_exposure_features
  - options_sentiment_features
  - ftd_features
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# ---------------------------------------------------------------------------
# Dark Pool Features
# ---------------------------------------------------------------------------

darkpool_source = FileSource(
    name="darkpool_source",
    path=gold_dataset_glob("darkpool_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

darkpool_features = FeatureView(
    name="darkpool_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="total_volume", dtype=Float32),
        Field(name="total_notional", dtype=Float32),
        Field(name="avg_price", dtype=Float32),
        Field(name="trade_count", dtype=Int64),
        Field(name="avg_trade_size", dtype=Float32),
        Field(name="pct_above_nbbo", dtype=Float32),
        Field(name="pct_below_nbbo", dtype=Float32),
    ],
    source=darkpool_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "darkpool",
        "data_source": "unusual_whales",
    },
)

# ---------------------------------------------------------------------------
# Greek Exposure Features
# ---------------------------------------------------------------------------

greek_exposure_source = FileSource(
    name="greek_exposure_source",
    path=gold_dataset_glob("greek_exposure_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

greek_exposure_features = FeatureView(
    name="greek_exposure_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="net_gamma_exposure", dtype=Float32),
        Field(name="net_delta_exposure", dtype=Float32),
        Field(name="put_call_gamma_ratio", dtype=Float32),
        Field(name="max_gamma_strike", dtype=Float32),
        Field(name="weighted_avg_dte", dtype=Float32),
    ],
    source=greek_exposure_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "greeks",
        "data_source": "unusual_whales",
    },
)

# ---------------------------------------------------------------------------
# Options Sentiment Features
# ---------------------------------------------------------------------------

options_sentiment_source = FileSource(
    name="options_sentiment_source",
    path=gold_dataset_glob("options_sentiment_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

options_sentiment_features = FeatureView(
    name="options_sentiment_features",
    entities=[equity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="iv_rank", dtype=Float32),
        Field(name="current_iv", dtype=Float32),
        Field(name="iv_percentile", dtype=Float32),
        Field(name="market_call_put_ratio", dtype=Float32),
        Field(name="market_net_volume", dtype=Float32),
        Field(name="market_sentiment", dtype=Float32),
        Field(name="sector_sentiment", dtype=Float32),
        Field(name="sector_call_put_ratio", dtype=Float32),
        Field(name="sector_net_volume", dtype=Float32),
        Field(name="sector", dtype=String),
    ],
    source=options_sentiment_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "sentiment",
        "data_source": "unusual_whales",
    },
)

# ---------------------------------------------------------------------------
# FTD Features
# ---------------------------------------------------------------------------

ftd_source = FileSource(
    name="ftd_source",
    path=gold_dataset_glob("ftd_features"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

ftd_features = FeatureView(
    name="ftd_features",
    entities=[equity],
    ttl=timedelta(days=90),
    schema=[
        Field(name="ftd_quantity", dtype=Int64),
        Field(name="ftd_value", dtype=Float32),
        Field(name="ftd_trade_count", dtype=Int64),
        Field(name="ftd_avg_price", dtype=Float32),
    ],
    source=ftd_source,
    online=True,
    tags={
        "owner": "quant_team",
        "category": "ftd",
        "data_source": "sec",
    },
)
