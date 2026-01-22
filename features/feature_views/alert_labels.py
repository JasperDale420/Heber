"""Alert barrier label feature views for Feast (PRD §31.12).

Barrier-based labels for flow alerts: "Should I take this alert?"
Uses ATR-scaled TP/SL barriers with DTE-aware horizons.
"""

from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

from features.entities import alert

# =============================================================================
# All Horizons Combined
# =============================================================================

alert_barrier_labels_source = FileSource(
    name="alert_barrier_labels_source",
    path="/data/gold/dataset=labels_alert_barriers/type=label/",
    timestamp_field="ts_alert",
    created_timestamp_column="ts_available",
)

alert_barrier_labels = FeatureView(
    name="labels_alert_barriers",
    entities=[alert],
    ttl=timedelta(days=365),
    schema=[
        # Alert context
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="dte", dtype=Int64),
        Field(name="horizon", dtype=String),  # intraday, swing, leap
        # Prices at alert
        Field(name="spot_at_alert", dtype=Float32),
        Field(name="atr_at_alert", dtype=Float32),
        # Barrier thresholds used
        Field(name="tp_threshold", dtype=Float32),
        Field(name="sl_threshold", dtype=Float32),
        # THE LABEL: 1 = take it, 0 = skip
        Field(name="hit_tp_first", dtype=Int64),
        # Raw MFE/MAE
        Field(name="mfe", dtype=Float32),
        Field(name="mae", dtype=Float32),
        # Slippage-adjusted MFE/MAE
        Field(name="mfe_adj", dtype=Float32),
        Field(name="mae_adj", dtype=Float32),
        Field(name="bars_to_hit", dtype=Float32),
        # Beta-neutral (SPY-relative) return
        Field(name="beta_neutral_return", dtype=Float32),
        # Regime context
        Field(name="vix_at_alert", dtype=Float32),
        Field(name="vix_regime", dtype=String),  # low, normal, high
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
# =============================================================================

alert_intraday_labels_source = FileSource(
    name="alert_intraday_labels_source",
    path="/data/gold/dataset=labels_alert_intraday/type=label/",
    timestamp_field="ts_alert",
    created_timestamp_column="ts_available",
)

alert_intraday_labels = FeatureView(
    name="labels_alert_intraday",
    entities=[alert],
    ttl=timedelta(days=90),
    schema=[
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="dte", dtype=Int64),
        Field(name="spot_at_alert", dtype=Float32),
        Field(name="atr_at_alert", dtype=Float32),
        Field(name="tp_threshold", dtype=Float32),
        Field(name="sl_threshold", dtype=Float32),
        Field(name="hit_tp_first", dtype=Int64),
        Field(name="mfe", dtype=Float32),
        Field(name="mae", dtype=Float32),
        Field(name="bars_to_hit", dtype=Float32),
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
    },
)


# =============================================================================
# Swing Only (weekly/bi-weekly options)
# =============================================================================

alert_swing_labels_source = FileSource(
    name="alert_swing_labels_source",
    path="/data/gold/dataset=labels_alert_swing/type=label/",
    timestamp_field="ts_alert",
    created_timestamp_column="ts_available",
)

alert_swing_labels = FeatureView(
    name="labels_alert_swing",
    entities=[alert],
    ttl=timedelta(days=180),
    schema=[
        Field(name="underlying", dtype=String),
        Field(name="put_call", dtype=String),
        Field(name="dte", dtype=Int64),
        Field(name="spot_at_alert", dtype=Float32),
        Field(name="atr_at_alert", dtype=Float32),
        Field(name="tp_threshold", dtype=Float32),
        Field(name="sl_threshold", dtype=Float32),
        Field(name="hit_tp_first", dtype=Int64),
        Field(name="mfe", dtype=Float32),
        Field(name="mae", dtype=Float32),
        Field(name="bars_to_hit", dtype=Float32),
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
    },
)
