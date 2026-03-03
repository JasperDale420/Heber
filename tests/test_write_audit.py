"""Tests for write-time null-field audit logging."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import polars as pl
import pyarrow as pa
from heber.quality.write_audit import (
    EXPECTED_NON_NULL,
    audit_null_fields,
    write_null_fields_total,
)


class TestAuditNullFieldsPandas:
    """Test audit with pandas DataFrames."""

    def test_no_nulls_no_warnings(self, caplog):
        """Clean data produces no log output."""
        df = pd.DataFrame(
            {
                "open": [1.0, 2.0],
                "high": [1.5, 2.5],
                "low": [0.5, 1.5],
                "close": [1.2, 2.2],
                "volume": [100, 200],
                "ts_event": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="bars")
        assert result == {}

    def test_nulls_logged_per_column(self, caplog):
        """Null columns emit structured warnings."""
        df = pd.DataFrame(
            {
                "open": [1.0, None],
                "high": [1.5, 2.5],
                "low": [None, None],
                "close": [1.2, 2.2],
                "volume": [100, 200],
                "ts_event": pd.to_datetime(["2025-01-01", "2025-01-02"]),
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="bars")
        assert "open" in result
        assert result["open"] == 1
        assert "low" in result
        assert result["low"] == 2
        assert "high" not in result

    def test_unknown_dataset_audits_all_columns(self):
        """Datasets not in EXPECTED_NON_NULL get all columns checked."""
        df = pd.DataFrame(
            {
                "foo": [1, None],
                "bar": [None, None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="unknown_feed")
        assert "foo" in result
        assert result["foo"] == 1
        assert "bar" in result
        assert result["bar"] == 2

    def test_greek_exposure_treats_strike_expiry_dte_as_optional(self):
        """greek_exposure should not warn on null option-specific fields."""
        df = pd.DataFrame(
            {
                "event_id": ["e1"],
                "provider": ["uw"],
                "feed": ["greek_exposure"],
                "instrument_type": ["equity"],
                "instrument_key": ["equity:SPY"],
                "symbol": ["SPY"],
                "ts_event": pd.to_datetime(["2026-03-03T02:13:58Z"]),
                "ts_ingest": pd.to_datetime(["2026-03-03T02:13:59Z"]),
                "ts_available": pd.to_datetime(["2026-03-03T02:13:58Z"]),
                "source": ["api"],
                "schema_version": ["v1"],
                "quality_flags": [[]],
                "call_gamma": [1.0],
                "put_gamma": [2.0],
                "call_delta": [3.0],
                "put_delta": [4.0],
                "call_vanna": [5.0],
                "put_vanna": [6.0],
                "call_charm": [7.0],
                "put_charm": [8.0],
                "strike": [None],
                "expiry": [None],
                "dte": [None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="greek_exposure")
        assert result == {}

    def test_none_data_returns_empty(self):
        """None data gracefully returns empty dict."""
        result = audit_null_fields(None, layer="silver", dataset="bars")
        assert result == {}

    def test_context_passed_to_logger(self, caplog):
        """Context dict is included in log messages."""
        df = pd.DataFrame({"open": [None], "ts_event": [None]})
        with patch("heber.quality.write_audit.logger") as mock_logger:
            audit_null_fields(
                df,
                layer="silver",
                dataset="bars",
                context={"partition": "feed=bars/dt=2025-01-01"},
            )
            # Check that warning was called with context
            calls = mock_logger.warning.call_args_list
            assert len(calls) >= 1
            for call in calls:
                assert call.kwargs["partition"] == "feed=bars/dt=2025-01-01"


class TestAuditNullFieldsPolars:
    """Test audit with Polars DataFrames."""

    def test_no_nulls_no_warnings(self):
        """Clean Polars data produces no warnings."""
        df = pl.DataFrame(
            {
                "price": [1.0, 2.0],
                "size": [100, 200],
                "ts_event": ["2025-01-01", "2025-01-02"],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="trades")
        assert result == {}

    def test_nulls_detected(self):
        """Null values detected in Polars DataFrame."""
        df = pl.DataFrame(
            {
                "price": [1.0, None],
                "size": [100, 200],
                "ts_event": ["2025-01-01", None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="trades")
        assert "price" in result
        assert result["price"] == 1
        assert "ts_event" in result
        assert result["ts_event"] == 1
        assert "size" not in result


class TestAuditNullFieldsArrow:
    """Test audit with PyArrow Tables."""

    def test_no_nulls_no_warnings(self):
        """Clean Arrow table produces no warnings."""
        table = pa.table(
            {
                "bid_px": [1.0, 2.0],
                "ask_px": [1.5, 2.5],
                "bid_sz": [100, 200],
                "ask_sz": [150, 250],
                "ts_event": ["2025-01-01", "2025-01-02"],
            }
        )
        result = audit_null_fields(table, layer="silver", dataset="quotes")
        assert result == {}

    def test_nulls_detected(self):
        """Null values detected in Arrow Table."""
        table = pa.table(
            {
                "bid_px": pa.array([1.0, None]),
                "ask_px": pa.array([1.5, 2.5]),
                "bid_sz": pa.array([100, None]),
                "ask_sz": pa.array([150, 250]),
                "ts_event": pa.array(["2025-01-01", "2025-01-02"]),
            }
        )
        result = audit_null_fields(table, layer="silver", dataset="quotes")
        assert "bid_px" in result
        assert result["bid_px"] == 1
        assert "bid_sz" in result
        assert result["bid_sz"] == 1
        assert "ask_px" not in result


class TestPrometheusMetrics:
    """Test Prometheus counter integration."""

    def test_counter_incremented(self):
        """Prometheus counter increments for each null column."""
        df = pd.DataFrame(
            {
                "open": [None, None, 1.0],
                "high": [1.0, 2.0, 3.0],
                "low": [None, 1.0, 2.0],
                "close": [1.0, 2.0, 3.0],
                "volume": [100, 200, 300],
                "ts_event": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            }
        )

        before_open = write_null_fields_total.labels(layer="silver", dataset="bars", column="open")._value.get()
        before_low = write_null_fields_total.labels(layer="silver", dataset="bars", column="low")._value.get()

        audit_null_fields(df, layer="silver", dataset="bars")

        after_open = write_null_fields_total.labels(layer="silver", dataset="bars", column="open")._value.get()
        after_low = write_null_fields_total.labels(layer="silver", dataset="bars", column="low")._value.get()

        assert after_open - before_open == 2  # 2 null values in 'open'
        assert after_low - before_low == 1  # 1 null value in 'low'


class TestExpectedNonNull:
    """Test EXPECTED_NON_NULL configuration."""

    def test_silver_datasets_have_contracts(self):
        """Silver datasets have expected non-null columns defined."""
        for dataset in ("bars", "quotes", "trades", "flow_alerts", "greek_exposure"):
            assert ("silver", dataset) in EXPECTED_NON_NULL

    def test_gold_datasets_have_contracts(self):
        """Gold datasets have expected non-null columns defined."""
        assert ("gold", "meta_label_features") in EXPECTED_NON_NULL
        assert ("gold", "labels_alert_barriers") in EXPECTED_NON_NULL

    def test_feature_enrichment_columns_tracked(self):
        """Enrichment columns are tracked for meta_label_features."""
        cols = EXPECTED_NON_NULL[("gold", "meta_label_features")]
        enrichment_cols = ["delta", "gamma", "theta", "vega", "iv", "iv_rank", "gex", "vex"]
        for col in enrichment_cols:
            assert col in cols, f"Enrichment column {col} not tracked"
