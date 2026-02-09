from __future__ import annotations

import importlib
import warnings
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

import heber.watch.models as watch_models
from heber.watch.models import WatchOutcome, WatchStatus


def test_watch_models_do_not_emit_class_config_deprecation_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        importlib.reload(watch_models)

    class_config_warnings = [
        warning
        for warning in caught
        if warning.category.__name__ == "PydanticDeprecatedSince20"
        and "class-based `config` is deprecated" in str(warning.message)
    ]
    assert not class_config_warnings


def test_watch_outcome_rejects_unknown_horizon_value() -> None:
    now = datetime(2026, 2, 8, 15, 0, tzinfo=UTC)

    with pytest.raises(ValidationError):
        WatchOutcome(
            watch_id="w1",
            alert_id="a1",
            occ_symbol="AAPL260220C00100000",
            underlying="AAPL",
            put_call="C",
            horizon="not_a_real_horizon",
            status=WatchStatus.HIT_TP,
            outcome_time=now,
            outcome_return=0.25,
            bars_to_hit=3,
            mfe=0.3,
            mae=-0.1,
            hit_tp_first=1,
            entry_price=2.5,
            spot_at_alert=150.0,
            alert_time=now,
            window_duration_hours=4.0,
            trading_minutes_to_hit=45,
        )
