"""Excursion analytics feature view for Feast.

Cross-system excursion profiles aggregated from all trading system ledgers.
One row per closed trade with MFE/MAE metrics, capture efficiency, and
holding duration for cross-system analysis and meta-strategy optimization.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

from features.entities import equity
from features.feature_views._paths import gold_dataset_glob

# Source: Gold Parquet files
excursion_analytics_source = FileSource(
    name="excursion_analytics_source",
    path=gold_dataset_glob("excursion_analytics"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

# Feature View: per-trade excursion analytics across all systems
excursion_analytics_features = FeatureView(
    name="excursion_analytics",
    entities=[equity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="system", dtype=String),
        Field(name="strategy", dtype=String),
        Field(name="symbol", dtype=String),
        Field(name="side", dtype=String),
        Field(name="mfe_r", dtype=Float64),
        Field(name="mae_r", dtype=Float64),
        Field(name="time_to_mfe_seconds", dtype=Float64),
        Field(name="time_to_mae_seconds", dtype=Float64),
        Field(name="mfe_mae_ratio", dtype=Float64),
        Field(name="capture_efficiency", dtype=Float64),
        Field(name="excursion_velocity", dtype=Float64),
        Field(name="pnl_r", dtype=Float64),
        Field(name="regime_entry", dtype=String),
        Field(name="holding_seconds", dtype=Int64),
    ],
    source=excursion_analytics_source,
    online=False,
    tags={
        "owner": "quant_team",
        "category": "excursion",
        "data_source": "trading_system_ledgers",
        "systems": "3Roses,Kairos,Cerberus,Orion,whalehunter,trading-bot,options-bot",
    },
)
