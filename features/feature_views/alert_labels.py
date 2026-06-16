"""Alert barrier label feature views for Feast (PRD §31.12).

Barrier-based labels for flow alerts: "Should I take this alert?"
Uses ATR-scaled TP/SL barriers with DTE-aware horizons.

Schema reconciled against actual Gold Parquet output from the watch service
(labels_alert_barriers dataset, 27 columns as of 2026-03-10).
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64, String

from features.entities import alert
from features.feature_views._paths import gold_dataset_glob

# =============================================================================
# All Horizons Combined
# =============================================================================

alert_barrier_labels_source = FileSource(
    name="alert_barrier_labels_source",
    path=gold_dataset_glob("labels_alert_barriers"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

alert_barrier_labels = FeatureView(
    name="labels_alert_barriers",
    entities=[alert],
    ttl=timedelta(days=365),
    schema=[
        # Identifiers
        Field(name="watch_id", dtype=String),
        Field(name="instrument_key", dtype=String),
        # Contract info
        Field(name="occ_symbol", dtype=String),
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        # Timing / horizon
        Field(name="horizon", dtype=String),  # intraday, swing, leap
        # Outcome
        Field(name="outcome", dtype=String),  # hit_tp, hit_sl, expired
        Field(name="outcome_reason", dtype=String),
        # THE LABEL: 1 = take it, 0 = skip
        Field(name="hit_tp_first", dtype=Int64),
        Field(name="contract_hit_tp_first", dtype=Int64),
        # Path statistics (double precision in parquet)
        Field(name="mfe", dtype=Float64),
        Field(name="mae", dtype=Float64),
        Field(name="bars_to_hit", dtype=Int64),
        # Temporal excursion metrics
        Field(name="time_to_mfe_seconds", dtype=Float64),
        Field(name="time_to_mae_seconds", dtype=Float64),
        Field(name="mfe_mae_ratio", dtype=Float64),
        Field(name="excursion_velocity", dtype=Float64),
        Field(name="capture_efficiency", dtype=Float64),
        # Option contract outcomes
        Field(name="contract_mfe", dtype=Float64),
        Field(name="contract_mae", dtype=Float64),
        Field(name="contract_mfe_adj", dtype=Float64),
        Field(name="contract_mae_adj", dtype=Float64),
        Field(name="contract_bars_to_hit", dtype=Int64),
        # Trading time metrics
        Field(name="trading_minutes_to_hit", dtype=Int64),
        Field(name="outcome_return", dtype=Float64),
        # Context at alert time
        Field(name="entry_price", dtype=Float64),
        Field(name="spot_at_alert", dtype=Float64),
        Field(name="window_duration_hours", dtype=Float64),
    ],
    source=alert_barrier_labels_source,
    online=False,  # Labels are offline-only for training
    tags={
        "dataset_type": "label",
        "label_method": "barrier_tp_sl",
        "horizons": "intraday,swing,leap",
        "owner": "quant_team",
        "data_source": "unusual_whales",
    },
)


# =============================================================================
# Intraday Only (for fast iteration on 0DTE strategies)
# NOTE: Planned but not yet populated. No Gold data exists for this dataset.
# =============================================================================

alert_intraday_labels_source = FileSource(
    name="alert_intraday_labels_source",
    path=gold_dataset_glob("labels_alert_intraday"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

alert_intraday_labels = FeatureView(
    name="labels_alert_intraday",
    entities=[alert],
    ttl=timedelta(days=90),
    schema=[
        Field(name="watch_id", dtype=String),
        Field(name="instrument_key", dtype=String),
        Field(name="occ_symbol", dtype=String),
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="horizon", dtype=String),
        Field(name="outcome", dtype=String),
        Field(name="outcome_reason", dtype=String),
        Field(name="hit_tp_first", dtype=Int64),
        Field(name="contract_hit_tp_first", dtype=Int64),
        Field(name="mfe", dtype=Float64),
        Field(name="mae", dtype=Float64),
        Field(name="bars_to_hit", dtype=Int64),
        # Temporal excursion metrics
        Field(name="time_to_mfe_seconds", dtype=Float64),
        Field(name="time_to_mae_seconds", dtype=Float64),
        Field(name="mfe_mae_ratio", dtype=Float64),
        Field(name="excursion_velocity", dtype=Float64),
        Field(name="capture_efficiency", dtype=Float64),
        Field(name="contract_mfe", dtype=Float64),
        Field(name="contract_mae", dtype=Float64),
        Field(name="contract_mfe_adj", dtype=Float64),
        Field(name="contract_mae_adj", dtype=Float64),
        Field(name="contract_bars_to_hit", dtype=Int64),
        Field(name="trading_minutes_to_hit", dtype=Int64),
        Field(name="outcome_return", dtype=Float64),
        Field(name="entry_price", dtype=Float64),
        Field(name="spot_at_alert", dtype=Float64),
        Field(name="window_duration_hours", dtype=Float64),
    ],
    source=alert_intraday_labels_source,
    online=False,
    tags={
        "dataset_type": "label",
        "label_method": "barrier_tp_sl",
        "horizons": "intraday",
        "dte_range": "0-2",
        "owner": "quant_team",
        "data_source": "unusual_whales",
        "use_case": "0dte_scalping",
        "status": "planned",
    },
)


# =============================================================================
# Swing Only (weekly/bi-weekly options)
# NOTE: Planned but not yet populated. No Gold data exists for this dataset.
# =============================================================================

alert_swing_labels_source = FileSource(
    name="alert_swing_labels_source",
    path=gold_dataset_glob("labels_alert_swing"),
    timestamp_field="ts_event",
    created_timestamp_column="ts_available",
)

alert_swing_labels = FeatureView(
    name="labels_alert_swing",
    entities=[alert],
    ttl=timedelta(days=180),
    schema=[
        Field(name="watch_id", dtype=String),
        Field(name="instrument_key", dtype=String),
        Field(name="occ_symbol", dtype=String),
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="horizon", dtype=String),
        Field(name="outcome", dtype=String),
        Field(name="outcome_reason", dtype=String),
        Field(name="hit_tp_first", dtype=Int64),
        Field(name="contract_hit_tp_first", dtype=Int64),
        Field(name="mfe", dtype=Float64),
        Field(name="mae", dtype=Float64),
        Field(name="bars_to_hit", dtype=Int64),
        # Temporal excursion metrics
        Field(name="time_to_mfe_seconds", dtype=Float64),
        Field(name="time_to_mae_seconds", dtype=Float64),
        Field(name="mfe_mae_ratio", dtype=Float64),
        Field(name="excursion_velocity", dtype=Float64),
        Field(name="capture_efficiency", dtype=Float64),
        Field(name="contract_mfe", dtype=Float64),
        Field(name="contract_mae", dtype=Float64),
        Field(name="contract_mfe_adj", dtype=Float64),
        Field(name="contract_mae_adj", dtype=Float64),
        Field(name="contract_bars_to_hit", dtype=Int64),
        Field(name="trading_minutes_to_hit", dtype=Int64),
        Field(name="outcome_return", dtype=Float64),
        Field(name="entry_price", dtype=Float64),
        Field(name="spot_at_alert", dtype=Float64),
        Field(name="window_duration_hours", dtype=Float64),
    ],
    source=alert_swing_labels_source,
    online=False,
    tags={
        "dataset_type": "label",
        "label_method": "barrier_tp_sl",
        "horizons": "swing",
        "dte_range": "3-21",
        "owner": "quant_team",
        "data_source": "unusual_whales",
        "use_case": "weekly_options",
        "status": "planned",
    },
)
