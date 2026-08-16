from datetime import UTC, datetime

import pandas as pd

from heber.ml.datasets import MetaLabelDatasetBuilder
from heber.watch.checker import outcome_to_label_row
from heber.watch.models import WatchOutcome, WatchStatus


def _sample_outcome(status: WatchStatus) -> WatchOutcome:
    now = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    return WatchOutcome(
        watch_id="w1",
        alert_id="a1",
        occ_symbol="AAPL250117C00150000",
        underlying="AAPL",
        put_call="C",
        horizon="intraday",
        status=status,
        outcome_time=now,
        outcome_return=0.25,
        bars_to_hit=3,
        mfe=0.3,
        mae=-0.1,
        mfe_adj=0.28,
        mae_adj=-0.12,
        hit_tp_first=1 if status == WatchStatus.HIT_TP else 0,
        entry_price=2.5,
        spot_at_alert=150.0,
        alert_time=now,
        window_duration_hours=4.0,
        trading_minutes_to_hit=45,
    )


def test_outcome_to_label_row_columns():
    outcome = _sample_outcome(WatchStatus.HIT_TP)
    row = outcome_to_label_row(outcome)

    assert row["outcome"] == "hit_tp"
    assert row["hit_tp_first"] == 1
    assert row["mfe"] == outcome.mfe
    assert row["mae"] == outcome.mae
    assert row["bars_to_hit"] == outcome.bars_to_hit


def test_meta_label_builder_normalizes_legacy_columns():
    builder = MetaLabelDatasetBuilder()

    features = pd.DataFrame(
        {
            "alert_id": ["a1"],
            "feature_x": [1.0],
        }
    )

    legacy_outcomes = pd.DataFrame(
        {
            "alert_id": ["a1"],
            "outcome_reason": ["hit_tp"],
            "contract_hit_tp_first": [1],
            "contract_mfe": [0.3],
            "contract_mae": [-0.1],
            "contract_bars_to_hit": [3],
            "outcome_return": [0.25],
            "trading_minutes_to_hit": [45],
            # Written on every label row since the first version of the writer;
            # the builder refuses outcomes it cannot check against their horizon.
            "horizon": ["swing"],
            "window_duration_hours": [448.0],
        }
    )

    joined = builder._join_features_outcomes(features, legacy_outcomes)
    labeled = builder._add_meta_label(joined)

    assert labeled.shape[0] == 1
    assert labeled["hit_tp_first"].iloc[0] == 1
    assert labeled["meta_label"].iloc[0] == 1
