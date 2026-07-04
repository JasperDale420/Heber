"""Deep behavioral tests for Bronze->Silver row coercion (heber.writer.normalizer).

Exercises every coercion branch in ``_coerce_value`` and the public surface:
``envelope_to_silver_row``, ``enforce_required_non_null_fields``,
``missing_required_non_null_fields``, plus the aggregate-payload helpers
``extract_item_event_timestamp`` and ``explode_aggregate_payload``.

All timestamps are fixed; no wall-clock assertions.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from heber.models.envelope import EventEnvelope
from heber.writer.ingest_contracts import UnmappedFeedError
from heber.writer.normalizer import (
    MissingRequiredFieldsError,
    _coerce_bool,
    _coerce_date,
    _coerce_float,
    _coerce_int,
    _coerce_list,
    _coerce_string,
    _coerce_timestamp,
    _coerce_value,
    enforce_required_non_null_fields,
    envelope_to_silver_row,
    explode_aggregate_payload,
    extract_item_event_timestamp,
    missing_required_non_null_fields,
)

pytestmark = pytest.mark.unit

# Fixed reference times — never wall-clock.
TS_EVENT = datetime(2026, 1, 2, 15, 0, 0, tzinfo=UTC)
TS_INGEST = datetime(2026, 1, 2, 15, 0, 1, tzinfo=UTC)


def _bars_envelope(payload: dict, **overrides) -> EventEnvelope:
    base = dict(
        event_id="evt-1",
        provider="alpaca",
        feed="daily_bars",
        source="rest",
        instrument_type="equity",
        instrument_key="equity:AAPL",
        symbol="AAPL",
        ts_event=TS_EVENT,
        ts_ingest=TS_INGEST,
        payload=payload,
    )
    base.update(overrides)
    return EventEnvelope(**base)


# ---------------------------------------------------------------------------
# _coerce_float
# ---------------------------------------------------------------------------
class TestCoerceFloat:
    def test_numeric_string(self):
        assert _coerce_float("1.5") == 1.5

    def test_plain_number(self):
        assert _coerce_float(3) == 3.0

    def test_empty_string_is_none(self):
        assert _coerce_float("") is None

    def test_non_numeric_string_raises(self):
        # _coerce_float itself raises; _coerce_value swallows to None.
        with pytest.raises(ValueError):
            _coerce_float("abc")
        assert _coerce_value("abc", pa.float64()) is None


# ---------------------------------------------------------------------------
# _coerce_int
# ---------------------------------------------------------------------------
class TestCoerceInt:
    def test_integer_string(self):
        assert _coerce_int("42") == 42

    def test_float_string_truncates(self):
        # int(float("3.99")) == 3 (truncation toward zero, not rounding)
        assert _coerce_int("3.99") == 3

    def test_negative_float_string_truncates_toward_zero(self):
        assert _coerce_int("-3.99") == -3

    def test_empty_string_is_none(self):
        assert _coerce_int("") is None

    def test_overflow_inf_returns_none_via_coerce_value(self):
        # int(float("inf")) raises OverflowError -> swallowed to None.
        assert _coerce_value(float("inf"), pa.int64()) is None

    def test_large_epoch_string_preserved(self):
        assert _coerce_int("1500000000") == 1500000000


# ---------------------------------------------------------------------------
# _coerce_date
# ---------------------------------------------------------------------------
class TestCoerceDate:
    def test_from_datetime(self):
        assert _coerce_date(datetime(2026, 1, 2, 3, 4, tzinfo=UTC)) == date(2026, 1, 2)

    def test_from_date(self):
        d = date(2026, 1, 2)
        assert _coerce_date(d) == d

    def test_from_iso_string(self):
        assert _coerce_date("2026-01-02") == date(2026, 1, 2)

    def test_from_z_suffixed_string(self):
        assert _coerce_date("2026-01-02T03:04:05Z") == date(2026, 1, 2)

    def test_unparseable_type_returns_none(self):
        assert _coerce_date(123) is None

    def test_bad_string_swallowed_to_none_via_coerce_value(self):
        assert _coerce_value("not-a-date", pa.date32()) is None


# ---------------------------------------------------------------------------
# _coerce_timestamp
# ---------------------------------------------------------------------------
class TestCoerceTimestamp:
    def test_datetime_passthrough(self):
        assert _coerce_timestamp(TS_EVENT) == TS_EVENT

    def test_epoch_seconds(self):
        assert _coerce_timestamp(1_700_000_000) == datetime.fromtimestamp(1_700_000_000, tz=UTC)

    def test_epoch_milliseconds_scaled(self):
        # > 10_000_000_000 treated as ms and divided by 1000.
        assert _coerce_timestamp(1_700_000_000_000) == datetime.fromtimestamp(1_700_000_000, tz=UTC)

    def test_float_epoch_string(self):
        result = _coerce_timestamp("1710000000.5")
        assert result == datetime.fromtimestamp(1710000000.5, tz=UTC)
        assert result.tzinfo is not None

    def test_small_digit_string_treated_as_epoch_seconds(self):
        # "100".isdigit() True -> int path -> 100 seconds after epoch.
        assert _coerce_timestamp("100") == datetime.fromtimestamp(100, tz=UTC)

    def test_aware_iso_string_normalized_to_utc(self):
        result = _coerce_timestamp("2026-01-02T03:04:05+00:00")
        assert result.tzinfo is not None
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_z_suffixed_iso_string(self):
        result = _coerce_timestamp("2026-01-02T03:04:05Z")
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_unparseable_type_returns_none(self):
        assert _coerce_timestamp(["not", "a", "ts"]) is None

    def test_naive_iso_string_should_be_utc_aware(self):
        result = _coerce_timestamp("2026-01-02T03:04:05")
        assert result is not None
        assert result.tzinfo is not None, "naive ISO timestamp must be made UTC-aware"


# ---------------------------------------------------------------------------
# _coerce_bool
# ---------------------------------------------------------------------------
class TestCoerceBool:
    def test_passthrough_bool(self):
        assert _coerce_bool(True) is True
        assert _coerce_bool(False) is False

    def test_numeric_truthiness(self):
        assert _coerce_bool(2) is True
        assert _coerce_bool(0) is False
        assert _coerce_bool(0.0) is False

    @pytest.mark.parametrize("truthy", ["1", "true", "T", "yes", "Y"])
    def test_truthy_strings(self, truthy):
        assert _coerce_bool(truthy) is True

    @pytest.mark.parametrize("falsy", ["0", "false", "F", "no", "N"])
    def test_falsy_strings(self, falsy):
        assert _coerce_bool(falsy) is False

    def test_unrecognized_string_returns_none(self):
        assert _coerce_bool("maybe") is None


# ---------------------------------------------------------------------------
# _coerce_list
# ---------------------------------------------------------------------------
class TestCoerceList:
    def test_list_passthrough(self):
        assert _coerce_list([1, 2]) == [1, 2]

    def test_tuple_to_list(self):
        assert _coerce_list((1, 2)) == [1, 2]

    def test_set_to_list(self):
        assert sorted(_coerce_list({1, 2})) == [1, 2]

    def test_json_array_string(self):
        assert _coerce_list("[1, 2, 3]") == [1, 2, 3]

    def test_empty_string_is_empty_list(self):
        assert _coerce_list("") == []

    def test_non_json_string_wrapped(self):
        assert _coerce_list("hello") == ["hello"]

    def test_json_non_array_string_wrapped(self):
        # Valid JSON but not a list -> falls through to single-element wrap.
        assert _coerce_list('{"a": 1}') == ['{"a": 1}']

    def test_scalar_wrapped(self):
        assert _coerce_list(5) == [5]


# ---------------------------------------------------------------------------
# _coerce_string
# ---------------------------------------------------------------------------
class TestCoerceString:
    def test_dict_serialized_to_json(self):
        assert _coerce_string({"a": 1}) == '{"a": 1}'

    def test_list_serialized_to_json(self):
        assert _coerce_string([1, 2]) == "[1, 2]"

    def test_scalar_stringified(self):
        assert _coerce_string(5) == "5"

    def test_dict_with_nonserializable_uses_default_str(self):
        # default=str keeps json.dumps from blowing up on exotic values.
        assert _coerce_string({"d": date(2026, 1, 2)}) == '{"d": "2026-01-02"}'


# ---------------------------------------------------------------------------
# _coerce_value dispatch + None short-circuit
# ---------------------------------------------------------------------------
class TestCoerceValueDispatch:
    def test_none_short_circuits(self):
        assert _coerce_value(None, pa.float64()) is None

    def test_unhandled_type_returns_value_unchanged(self):
        # binary type hits no branch -> returned as-is.
        sentinel = object()
        assert _coerce_value(sentinel, pa.binary()) is sentinel

    def test_float_dispatch(self):
        assert _coerce_value("1.5", pa.float64()) == 1.5

    def test_int_dispatch(self):
        assert _coerce_value("7", pa.int64()) == 7

    def test_bool_dispatch(self):
        assert _coerce_value("yes", pa.bool_()) is True

    def test_list_dispatch(self):
        assert _coerce_value("[1]", pa.list_(pa.int64())) == [1]

    def test_string_dispatch(self):
        assert _coerce_value({"a": 1}, pa.string()) == '{"a": 1}'


# ---------------------------------------------------------------------------
# envelope_to_silver_row
# ---------------------------------------------------------------------------
class TestEnvelopeToSilverRow:
    def test_full_row_with_string_numerics(self):
        env = _bars_envelope(
            {
                "o": "1.5",
                "h": "2",
                "l": "1",
                "c": "1.8",
                "v": "1000",
                "n": "42",
                "vw": "1.6",
                "timeframe": "1Day",
            }
        )
        row = envelope_to_silver_row(env)
        # Feed alias daily_bars -> bars.
        assert row["feed"] == "bars"
        assert row["open"] == 1.5 and isinstance(row["open"], float)
        assert row["trade_count"] == 42 and isinstance(row["trade_count"], int)
        assert row["volume"] == 1000.0
        assert row["timeframe"] == "1Day"

    def test_envelope_metadata_columns_preserved(self):
        env = _bars_envelope({"o": 1, "h": 2, "l": 1, "c": 2, "v": 10})
        row = envelope_to_silver_row(env)
        assert row["event_id"] == "evt-1"
        assert row["provider"] == "alpaca"
        assert row["instrument_key"] == "equity:AAPL"
        assert row["ts_event"] == TS_EVENT
        assert row["ts_ingest"] == TS_INGEST

    def test_missing_payload_field_coerces_to_none(self):
        env = _bars_envelope({"o": 1, "h": 2, "l": 1, "c": 2})  # no volume
        row = envelope_to_silver_row(env)
        assert row["volume"] is None

    def test_ts_available_defaults_to_ts_ingest(self):
        env = _bars_envelope({"o": 1, "h": 2, "l": 1, "c": 2, "v": 5})
        row = envelope_to_silver_row(env)
        assert row["ts_available"] == TS_INGEST

    def test_ts_available_never_before_ts_event(self):
        # ts_ingest earlier than ts_event -> availability bumped up to ts_event.
        early_ingest = datetime(2026, 1, 2, 14, 0, 0, tzinfo=UTC)
        env = _bars_envelope(
            {"o": 1, "h": 2, "l": 1, "c": 2, "v": 5},
            ts_ingest=early_ingest,
        )
        row = envelope_to_silver_row(env)
        assert row["ts_available"] == TS_EVENT
        assert row["ts_available"] >= row["ts_event"]

    def test_explicit_ts_available_respected_when_after_event(self):
        explicit = datetime(2026, 1, 2, 16, 0, 0, tzinfo=UTC)
        env = _bars_envelope(
            {"o": 1, "h": 2, "l": 1, "c": 2, "v": 5},
            ts_available=explicit,
        )
        row = envelope_to_silver_row(env)
        assert row["ts_available"] == explicit

    def test_source_key_priority_short_alpaca_keys(self):
        # Short Alpaca key "o" maps to "open"; explicit "open" not present.
        env = _bars_envelope({"o": 7.0, "h": 8, "l": 6, "c": 7, "v": 1})
        row = envelope_to_silver_row(env)
        assert row["open"] == 7.0

    def test_unmapped_feed_raises(self):
        env = _bars_envelope({"o": 1}, feed="totally_unknown_feed")
        with pytest.raises(UnmappedFeedError):
            envelope_to_silver_row(env)


# ---------------------------------------------------------------------------
# missing_required_non_null_fields / enforce_required_non_null_fields
# ---------------------------------------------------------------------------
class TestRequiredFields:
    def test_no_missing_returns_empty(self):
        row = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}
        assert missing_required_non_null_fields("bars", row) == []

    def test_none_and_blank_are_missing_sorted(self):
        row = {"open": 1.0, "high": None, "low": "", "close": 2.0}
        # high (None), low (blank), volume (absent) -> sorted.
        assert missing_required_non_null_fields("bars", row) == ["high", "low", "volume"]

    def test_whitespace_only_string_is_missing(self):
        row = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": "   "}
        assert "volume" in missing_required_non_null_fields("bars", row)

    def test_zero_value_is_not_missing(self):
        row = {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0}
        assert missing_required_non_null_fields("bars", row) == []

    def test_unknown_feed_has_no_requirements(self):
        assert missing_required_non_null_fields("no_such_feed", {}) == []

    def test_enforce_passes_when_complete(self):
        row = {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100}
        enforce_required_non_null_fields("bars", row)  # no raise

    def test_enforce_raises_with_details(self):
        row = {"open": 1.0, "high": None}
        with pytest.raises(MissingRequiredFieldsError) as exc:
            enforce_required_non_null_fields("bars", row, event_id="abc-123")
        err = exc.value
        assert err.feed == "bars"
        assert err.event_id == "abc-123"
        assert err.missing_fields == ["close", "high", "low", "volume"]
        assert "missing_required_fields:bars:" in str(err)
        assert "event_id=abc-123" in str(err)

    def test_enforce_error_message_omits_event_id_when_absent(self):
        row = {"open": 1.0}
        with pytest.raises(MissingRequiredFieldsError) as exc:
            enforce_required_non_null_fields("bars", row)
        assert "event_id=" not in str(exc.value)


# ---------------------------------------------------------------------------
# extract_item_event_timestamp
# ---------------------------------------------------------------------------
class TestExtractItemEventTimestamp:
    def test_iso_string_timestamp_key(self):
        result = extract_item_event_timestamp({"timestamp": "2026-01-02T03:04:05Z"})
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_naive_iso_string_made_utc(self):
        # This helper (unlike _coerce_timestamp) correctly stamps UTC on naive input.
        result = extract_item_event_timestamp({"t": "2026-01-02T03:04:05"})
        assert result.tzinfo is not None
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_epoch_ms_key(self):
        result = extract_item_event_timestamp({"ts_event": 1_700_000_000_000})
        assert result == datetime.fromtimestamp(1_700_000_000, tz=UTC)

    def test_naive_datetime_made_utc(self):
        naive = datetime(2026, 1, 2, 3, 4, 5)
        result = extract_item_event_timestamp({"timestamp": naive})
        assert result.tzinfo is not None
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_aware_datetime_converted_to_utc(self):
        from datetime import timedelta, timezone

        plus5 = timezone(timedelta(hours=5))
        aware = datetime(2026, 1, 2, 8, 0, 0, tzinfo=plus5)
        result = extract_item_event_timestamp({"timestamp": aware})
        assert result == datetime(2026, 1, 2, 3, 0, 0, tzinfo=UTC)

    def test_key_priority_timestamp_over_t(self):
        result = extract_item_event_timestamp({"timestamp": "2026-01-02T03:04:05Z", "t": 1_700_000_000})
        assert result == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_missing_keys_returns_none(self):
        assert extract_item_event_timestamp({"foo": "bar"}) is None

    def test_unparseable_string_returns_none(self):
        assert extract_item_event_timestamp({"timestamp": "not-a-time"}) is None

    def test_non_supported_raw_type_returns_none(self):
        assert extract_item_event_timestamp({"timestamp": ["x"]}) is None


# ---------------------------------------------------------------------------
# explode_aggregate_payload
# ---------------------------------------------------------------------------
class TestExplodeAggregatePayload:
    def test_non_aggregate_feed_passthrough(self):
        env = _bars_envelope({"bid_px": 1}, feed="quotes")
        assert explode_aggregate_payload(env) == [env]

    def test_bars_list_exploded_per_item(self):
        env = _bars_envelope(
            {
                "symbol": "AAPL",
                "timeframe": "1Min",
                "bars": [
                    {"o": 1, "t": "2026-01-02T15:00:00Z"},
                    {"o": 2, "t": "2026-01-02T15:01:00Z"},
                ],
            },
            feed="bars",
        )
        items = explode_aggregate_payload(env)
        assert len(items) == 2
        assert items[0].event_id == "evt-1:0"
        assert items[1].event_id == "evt-1:1"
        # Context keys folded into each item payload.
        assert items[0].payload["timeframe"] == "1Min"
        # Per-item ts_event parsed from the item's own timestamp.
        assert items[1].ts_event == datetime(2026, 1, 2, 15, 1, 0, tzinfo=UTC)

    def test_empty_list_yields_nothing(self):
        env = _bars_envelope({"bars": []}, feed="bars")
        assert explode_aggregate_payload(env) == []

    def test_missing_list_key_passthrough(self):
        env = _bars_envelope({"symbol": "AAPL"}, feed="bars")
        assert explode_aggregate_payload(env) == [env]

    def test_non_dict_items_skipped(self):
        env = _bars_envelope(
            {"bars": [{"o": 1, "t": "2026-01-02T15:00:00Z"}, "garbage", 42]},
            feed="bars",
        )
        items = explode_aggregate_payload(env)
        assert len(items) == 1
        assert items[0].event_id == "evt-1:0"

    def test_feed_override_routes_explosion(self):
        # Envelope feed is daily_bars; override makes it explode as bars.
        env = _bars_envelope(
            {"bars": [{"o": 1, "t": "2026-01-02T15:00:00Z"}]},
            feed="daily_bars",
        )
        assert explode_aggregate_payload(env) == [env]  # no override -> passthrough
        items = explode_aggregate_payload(env, feed_override="bars")
        assert len(items) == 1

    def test_item_without_timestamp_falls_back_to_envelope_ts_event(self):
        env = _bars_envelope({"trades": [{"p": 1, "s": 5}]}, feed="trades")
        items = explode_aggregate_payload(env)
        assert len(items) == 1
        assert items[0].ts_event == TS_EVENT

    def test_symbol_backfilled_from_envelope(self):
        env = _bars_envelope({"trades": [{"p": 1, "s": 5}]}, feed="trades")
        items = explode_aggregate_payload(env)
        assert items[0].payload["symbol"] == "AAPL"
