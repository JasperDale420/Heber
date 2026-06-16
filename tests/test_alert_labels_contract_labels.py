"""Regression tests for alert label contract label caching."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from heber.features.pipelines.alert_labels import AlertLabelsPipeline


def _sample_times() -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(datetime(2026, 1, 1, tzinfo=UTC))
    alert_time = start + timedelta(days=1)
    later = alert_time + timedelta(days=1)
    return start, alert_time, later


def test_add_contract_labels_uses_cached_occ_symbol_lookup(monkeypatch) -> None:
    pipeline = AlertLabelsPipeline()
    start, ts_alert, later = _sample_times()

    labels = pd.DataFrame(
        {
            "alert_id": ["alert-1", "alert-2"],
            "ts_alert": [ts_alert, ts_alert],
        }
    )
    flow_alerts = pd.DataFrame(
        {
            "event_id": ["alert-1", "alert-2"],
            "occ_symbol": ["O:ABC123", "O:XYZ789"],
        }
    )
    option_bars = pd.DataFrame(
        {
            "symbol": ["O:ABC123", "O:ABC123", "O:XYZ789", "O:XYZ789"],
            "timestamp": [start, later, start, later],
            "close": [1.0, 1.2, 2.0, 2.3],
        }
    )

    async def _fake_fetch_option_bars(occ_symbols, bar_start, bar_end):  # noqa: ANN001, ARG001
        return option_bars

    monkeypatch.setattr(pipeline, "_fetch_option_bars", _fake_fetch_option_bars)

    result = pipeline._add_contract_labels(labels, flow_alerts, start, later)

    assert len(result) == 2
    assert "contract_hit_tp_first" in result.columns
