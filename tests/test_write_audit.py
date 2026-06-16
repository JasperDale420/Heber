"""Tests for write-time null-field audit logging."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
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


class TestCanonicalRowPerSnapshotDatasets:
    """Datasets that legitimately leave most Arrow columns null because the
    canonical record is a single snapshot row whose payload lives in a
    serialized blob (chain_json). The audit must not warn on those.
    """

    def test_option_chain_snapshot_canonical_row_does_not_warn_on_per_contract_nulls(self):
        """The canonical option_chain_snapshot row has chain_json + a few
        aggregate columns; per-contract fields (occ_symbol, strike, bid, ask)
        are legitimately null and must not trigger audit warnings.
        """
        df = pd.DataFrame(
            {
                "snapshot_ts": pd.to_datetime(["2026-04-28T14:30:00Z"]),
                "underlying": ["AAPL"],
                "chain_json": ['{"data": {"contracts": []}}'],
                # Per-contract Arrow columns — legitimately null on canonical row.
                "occ_symbol": [None],
                "strike": [None],
                "bid": [None],
                "ask": [None],
                "last": [None],
                "volume": [None],
                "put_call": [None],
                "underlying_price": [None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="option_chain_snapshot")
        assert result == {}, f"audit raised false-positive warnings: {result}"

    def test_option_chain_snapshot_warns_when_canonical_field_is_null(self):
        """If chain_json itself is null, that IS a real problem and should warn."""
        df = pd.DataFrame(
            {
                "snapshot_ts": pd.to_datetime(["2026-04-28T14:30:00Z"]),
                "underlying": ["AAPL"],
                "chain_json": [None],
                "occ_symbol": [None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="option_chain_snapshot")
        assert "chain_json" in result

    def test_oi_change_canonical_aggregate_row_does_not_warn_on_optional_nulls(self):
        df = pd.DataFrame(
            {
                "oi_date": ["2026-04-28"],
                "call_oi": [1000],
                "put_oi": [800],
                "call_oi_change": [50],
                "put_oi_change": [-20],
                # Optional UW detail fields — legitimately null on aggregates.
                "last_ask": [None],
                "last_bid": [None],
                "avg_price": [None],
                "option_symbol": [None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="oi_change")
        assert result == {}

    def test_darkpool_does_not_warn_on_missing_nbbo_context(self):
        df = pd.DataFrame(
            {
                "underlying": ["TSLA"],
                "price": [250.5],
                "size": [100],
                "ts_event": pd.to_datetime(["2026-04-28T14:30:00Z"]),
                # NBBO context legitimately absent for off-hours / low-quality prints.
                "nbbo_bid_size": [None],
                "nbbo_ask_size": [None],
                "sale_cond_codes": [None],
                "trade_code": [None],
            }
        )
        result = audit_null_fields(df, layer="silver", dataset="darkpool")
        assert result == {}
