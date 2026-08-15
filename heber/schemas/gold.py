"""Canonical Gold Arrow types shared across writers and repair tooling.

Gold partitions were historically written with whatever type pandas inferred
from the rows in hand. That drifts: a partition where every value of a column
happened to be null infers Arrow ``null``, a column of ``datetime.date`` infers
``date32`` while the same column of ``YYYYMMDD`` strings infers ``string``, and
``pyarrow`` cannot unify those across partitions. The dataset then fails to read
as a whole — ``MetaLabelDatasetBuilder`` caught the error and returned an empty
DataFrame, so training silently saw nothing.

These are declared *types per column*, not a closed schema: a column listed here
is coerced to its declared type on write, and a column not listed is passed
through untouched. Adding a feature therefore cannot silently drop it — the new
column simply keeps inferred typing until it is declared here.
"""

from __future__ import annotations

import pyarrow as pa

# Types for the meta_label_features Gold dataset (project=watch).
META_LABEL_FEATURES_TYPES: dict[str, pa.DataType] = {
    # Identity and instrument
    "alert_id": pa.string(),
    "instrument_key": pa.string(),
    "occ_symbol": pa.string(),
    "symbol": pa.string(),
    "underlying": pa.string(),
    "put_call": pa.string(),
    "side": pa.string(),
    "aggressor": pa.string(),
    "alert_type": pa.string(),
    "quality_flags": pa.list_(pa.string()),
    # Timestamps. alert_time is nanosecond to match ts_event/ts_available;
    # 16 legacy partitions wrote it as microsecond.
    "alert_time": pa.timestamp("ns", tz="UTC"),
    "ts_event": pa.timestamp("ns", tz="UTC"),
    "ts_available": pa.timestamp("ns", tz="UTC"),
    # Contract terms. expiry is a calendar date; writing it as an integer
    # YYYYMMDD made pyarrow read the value as a raw day count (year 57442).
    "expiry": pa.date32(),
    "strike": pa.float64(),
    "days_to_expiry": pa.int64(),
    # Trade sizing
    "premium": pa.float64(),
    "volume": pa.float64(),
    "open_interest": pa.float64(),
    "volume_oi_ratio": pa.float64(),
    "contract_price": pa.float64(),
    "spot_price": pa.float64(),
    "moneyness": pa.float64(),
    "log_moneyness": pa.float64(),
    # Greeks and implied vol
    "delta": pa.float64(),
    "gamma": pa.float64(),
    "theta": pa.float64(),
    "vega": pa.float64(),
    "iv": pa.float64(),
    "iv_rank": pa.float64(),
    # Market context
    "gex": pa.float64(),
    "vex": pa.float64(),
    "max_pain_strike": pa.float64(),
    "max_pain_distance_pct": pa.float64(),
    "market_tide_net_premium": pa.float64(),
    "market_tide_direction": pa.string(),
    "realized_vol_20d": pa.float64(),
    "underlying_1d_return": pa.float64(),
    "underlying_5d_return": pa.float64(),
    "underlying_30d_return": pa.float64(),
    # Alert classification flags
    "is_sweep": pa.int64(),
    "is_block": pa.int64(),
    "is_bullish": pa.int64(),
    "is_bearish": pa.int64(),
    "is_unusual": pa.int64(),
    # Calendar position
    "hour_of_day": pa.int64(),
    "minute_of_hour": pa.int64(),
    "day_of_week": pa.int64(),
    "minutes_since_open": pa.int64(),
    "minutes_to_close": pa.int64(),
}
