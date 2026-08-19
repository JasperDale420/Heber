"""Semantic invariants on Silver data that nothing else in Heber asserts.

`heber.health_monitor` already covers row counts (partition, liveness),
freshness (liveness), per-column null rates (statistical), schema drift
(schema) and volume trends (volume). It does not check whether the values
themselves make sense — a negative price, a bar whose high is below its low,
a crossed quote, a repeated event_id, or an instrument_type outside the
contract are all corrupt data that every one of those checks would call
healthy. These are the checks that catch that.
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from heber.quality.silver_invariants import (
    FEED_INVARIANTS,
    check_frame,
    duplicate_row_count,
)
from heber.schemas.silver import SILVER_SCHEMAS


def _bars(**overrides) -> pd.DataFrame:  # noqa: ANN003
    now = datetime.now(UTC)
    frame = pd.DataFrame(
        {
            "event_id": ["a", "b"],
            "instrument_key": ["equity:AAPL", "equity:MSFT"],
            "instrument_type": ["equity", "equity"],
            "ts_event": [now - timedelta(minutes=5)] * 2,
            "ts_available": [now - timedelta(minutes=4)] * 2,
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "volume": [100, 200],
        }
    )
    for k, v in overrides.items():
        frame[k] = v
    return frame


def _violations(frame: pd.DataFrame, feed: str = "bars") -> dict[str, int]:
    return {v.name: v.rows for v in check_frame(frame, FEED_INVARIANTS[feed]).violations}


def _not_run(frame: pd.DataFrame, feed: str = "bars") -> tuple[str, ...]:
    return check_frame(frame, FEED_INVARIANTS[feed]).not_run


@pytest.mark.unit
def test_clean_bars_report_no_violations() -> None:
    assert _violations(_bars()) == {}


@pytest.mark.unit
def test_negative_price_is_caught() -> None:
    assert _violations(_bars(close=[-1.0, 20.5])).get("close_non_negative") == 1


@pytest.mark.unit
def test_negative_volume_is_caught() -> None:
    assert _violations(_bars(volume=[-5, 200])).get("volume_non_negative") == 1


@pytest.mark.unit
def test_high_below_low_is_caught() -> None:
    assert _violations(_bars(high=[8.0, 21.0])).get("high_ge_low") == 1


@pytest.mark.unit
def test_high_must_be_the_highest() -> None:
    """high below close is corrupt even when high >= low still holds."""
    v = _violations(_bars(high=[10.2, 21.0]))
    assert v.get("high_is_highest") == 1
    assert "high_ge_low" not in v


@pytest.mark.unit
def test_low_must_be_the_lowest() -> None:
    assert _violations(_bars(low=[10.4, 19.0])).get("low_is_lowest") == 1


@pytest.mark.unit
def test_duplicate_row_count_counts_surplus_rows() -> None:
    """Two rows sharing one id is one violation, not two."""
    assert duplicate_row_count(Counter({"a": 1, "b": 1})) == 0
    assert duplicate_row_count(Counter({"a": 2})) == 1
    assert duplicate_row_count(Counter({"a": 3, "b": 2})) == 3


@pytest.mark.unit
def test_unknown_instrument_type_is_caught() -> None:
    assert _violations(_bars(instrument_type=["equity", "banana"])).get("instrument_type_allowed") == 1


@pytest.mark.unit
def test_ts_available_before_ts_event_is_caught() -> None:
    """The zero-leakage invariant, on Silver.

    health_monitor's ml_readiness audits this on Gold only, so Silver has no
    other guard.
    """
    now = datetime.now(UTC)
    frame = _bars()
    frame["ts_available"] = [now - timedelta(hours=1), now]
    assert _violations(frame).get("ts_available_ge_ts_event") == 1


@pytest.mark.unit
def test_future_ts_available_is_caught() -> None:
    now = datetime.now(UTC)
    frame = _bars()
    frame["ts_available"] = [now + timedelta(hours=2), now]
    assert _violations(frame).get("ts_available_not_future") == 1


@pytest.mark.unit
def test_missing_columns_are_reported_as_not_run_not_as_passing() -> None:
    """A check that cannot run has verified nothing.

    Reporting it as a pass is exactly how the retired Soda checks hid: they
    named bid_price/ask_price for a feed whose columns are bid_px/ask_px, so
    every quote check matched nothing and looked green.
    """
    frame = _bars().drop(columns=["volume", "high", "low"])
    v = _violations(frame)
    assert "volume_non_negative" not in v
    assert "high_ge_low" not in v
    not_run = _not_run(frame)
    assert "volume_non_negative" in not_run
    assert "high_ge_low" in not_run
    assert "high_is_highest" in not_run


@pytest.mark.unit
def test_nulls_do_not_trigger_value_violations() -> None:
    """Null coverage belongs to health_monitor's statistical check; a null is
    not a negative number and must not be double-reported here.
    """
    frame = _bars()
    frame["close"] = [None, 20.5]
    assert "close_non_negative" not in _violations(frame)


@pytest.mark.unit
def test_empty_frame_reports_nothing() -> None:
    assert _violations(_bars().iloc[0:0]) == {}


@pytest.mark.unit
def test_quotes_crossed_spread_is_caught() -> None:
    now = datetime.now(UTC)
    frame = pd.DataFrame(
        {
            "event_id": ["a", "b"],
            "instrument_key": ["equity:AAPL"] * 2,
            "ts_event": [now - timedelta(minutes=5)] * 2,
            "ts_available": [now - timedelta(minutes=4)] * 2,
            "bid_px": [10.0, 20.0],
            "ask_px": [9.5, 20.5],
        }
    )
    assert _violations(frame, "quotes").get("ask_ge_bid") == 1


@pytest.mark.unit
def test_every_configured_feed_has_a_spec() -> None:
    """The four feeds the retired Soda checks covered must all still be covered."""
    assert set(FEED_INVARIANTS) == {"bars", "quotes", "trades", "flow_alerts"}


@pytest.mark.unit
@pytest.mark.parametrize("feed", sorted(FEED_INVARIANTS))
def test_every_invariant_names_real_silver_columns(feed: str) -> None:
    """Guard against the failure that made the retired Soda checks useless.

    Its quote checks referenced bid_price/ask_price against a schema whose
    columns are bid_px/ask_px, so they silently matched nothing. A rename now
    breaks this test instead.
    """
    schema_columns = set(SILVER_SCHEMAS[feed].names)
    for invariant in FEED_INVARIANTS[feed].invariants:
        unknown = set(invariant.columns) - schema_columns
        assert not unknown, f"{feed}.{invariant.name} references non-schema columns {unknown}"


@pytest.mark.unit
def test_required_identity_columns_are_checked() -> None:
    """health_monitor's statistical check profiles numeric columns only, so a
    null event_id or instrument_key has no other guard anywhere.
    """
    frame = _bars()
    frame.loc[0, "event_id"] = None
    assert _violations(frame).get("event_id_required") == 1

    frame = _bars()
    frame.loc[0, "instrument_key"] = "   "
    assert _violations(frame).get("instrument_key_required") == 1


@pytest.mark.unit
def test_unparsable_values_are_violations_not_nulls() -> None:
    """Junk in a numeric column must not be coerced away into a null and then
    excluded — that would let the worst rows through untouched.
    """
    frame = _bars()
    frame["close"] = ["not-a-number", 20.5]
    assert _violations(frame).get("close_non_negative") == 1


@pytest.mark.unit
def test_predicate_errors_are_reported_as_not_run() -> None:
    """An invariant that raises has verified nothing and must not read as a pass."""
    from heber.quality.silver_invariants import FeedInvariants, Invariant

    def boom(_df: pd.DataFrame) -> pd.Series:
        raise ValueError("column is unusable")

    spec = FeedInvariants(
        feed="bars",
        invariants=(Invariant(name="explodes", columns=("close",), predicate=boom),),
    )
    outcome = check_frame(_bars(), spec)
    assert outcome.violations == ()
    assert outcome.not_run == ("explodes",)


@pytest.mark.unit
def test_unreadable_partition_makes_the_scan_non_passing(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """A partition that cannot be read must not be quietly omitted.

    If one instrument_type partition is corrupt and the rest are clean, an
    "ok" here would claim a full scan while having skipped a slice of the
    day — the same shape of blind spot the retired Soda check had.
    """
    from datetime import date as date_type

    from heber.ops.daily_health import _check_silver_invariants

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    good = silver / "feed=bars" / f"instrument_type=equity/dt={dt.isoformat()}"
    bad = silver / "feed=bars" / f"instrument_type=crypto/dt={dt.isoformat()}"
    good.mkdir(parents=True)
    bad.mkdir(parents=True)
    _bars().to_parquet(good / "part-0.parquet")
    (bad / "part-0.parquet").write_bytes(b"this is not a parquet file")

    class _Settings:
        silver_path = silver

    monkeypatch.setattr("heber.ops.daily_health.pd.read_parquet", pd.read_parquet)
    result = _check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]

    assert result["status"] == "fail", "an unreadable partition must not report a clean scan"
    assert result["observed"]["read_failures"], "the unreadable partition must be named"
    assert result["observed"]["scan_complete"] is False


@pytest.mark.unit
def test_duplicate_event_ids_are_found_across_instrument_types(tmp_path) -> None:  # noqa: ANN001
    """event_id is a global dedup key, so a duplicate split across two
    instrument_type partitions must still be caught.
    """
    from datetime import date as date_type

    from heber.ops.daily_health import _check_silver_invariants

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    for instrument_type in ("equity", "crypto"):
        partition = silver / "feed=bars" / f"instrument_type={instrument_type}/dt={dt.isoformat()}"
        partition.mkdir(parents=True)
        frame = _bars().iloc[[0]].copy()
        frame["event_id"] = ["shared-id"]
        frame.to_parquet(partition / "part-0.parquet")

    class _Settings:
        silver_path = silver

    result = _check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert result["observed"]["violations"]["bars"]["event_id_unique"] == 1


@pytest.mark.unit
def test_all_partitions_unreadable_is_a_failure_not_a_warning(tmp_path) -> None:  # noqa: ANN001
    """Total read loss must not read as the benign "no data today" case."""
    from datetime import date as date_type

    from heber.ops.daily_health import _check_silver_invariants

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    partition = silver / "feed=bars" / f"instrument_type=equity/dt={dt.isoformat()}"
    partition.mkdir(parents=True)
    (partition / "part-0.parquet").write_bytes(b"not parquet")

    class _Settings:
        silver_path = silver

    result = _check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert result["status"] == "fail"
    assert result["observed"]["read_failures"]


@pytest.mark.unit
def test_no_partitions_at_all_is_a_warning() -> None:
    """Absence of data is a different problem from corrupt data."""
    from datetime import date as date_type
    from pathlib import Path

    from heber.ops.daily_health import _check_silver_invariants

    class _Settings:
        silver_path = Path("/nonexistent-silver-root")

    result = _check_silver_invariants(date_type(2026, 3, 9), _Settings())  # type: ignore[arg-type]
    assert result["status"] == "warn"
    assert result["observed"]["read_failures"] == []


@pytest.mark.unit
def test_blank_identity_is_caught_in_string_extension_dtypes() -> None:
    """A whitespace event_id in a pandas string column is neither NA nor object.

    Missing this would let a blank id evade both required-field validation and
    duplicate detection.
    """
    for dtype in ("string", "object"):
        frame = _bars()
        frame["event_id"] = pd.Series(["   ", "b"], dtype=dtype)
        assert _violations(frame).get("event_id_required") == 1, f"dtype={dtype}"


@pytest.mark.unit
def test_unparsable_timestamp_counts_once_and_is_not_called_future() -> None:
    """One junk timestamp is one violating row, reported as unparsable.

    Previously it tripped every relation touching the column, so a single bad
    row inflated the count and claimed a future-timestamp violation even
    though parsing had failed before future-ness could be judged.
    """
    frame = _bars()
    frame["ts_available"] = ["definitely-not-a-time", frame["ts_available"].iloc[1]]
    v = _violations(frame)
    assert v.get("ts_available_parseable") == 1
    assert "ts_available_not_future" not in v
    assert "ts_available_ge_ts_event" not in v


@pytest.mark.unit
def test_completeness_contracts_hold_the_retired_one_percent_thresholds() -> None:
    """bars OHLC were held to <1% missing by the retired checks.

    health_monitor profiles null rates against a single 5% default, so
    relying on it alone would have quietly relaxed these columns.
    """
    from heber.quality.silver_invariants import ColumnFill, completeness_violations

    spec = FEED_INVARIANTS["bars"]
    assert dict(spec.completeness)["close"] == 0.01

    # 2% missing: inside health_monitor's 5% default, outside this contract.
    tally = {"close": ColumnFill(nulls=2, total=100)}
    assert completeness_violations(spec, tally) == {"close_completeness": 2}

    # The retired contract was strict ("< 1%"), so exactly 1% is a breach.
    assert completeness_violations(spec, {"close": ColumnFill(nulls=1, total=100)}) == {"close_completeness": 1}
    # A fully-populated column is never a breach, whatever the threshold.
    assert completeness_violations(spec, {"close": ColumnFill(nulls=0, total=100)}) == {}


@pytest.mark.unit
@pytest.mark.parametrize("column", ["high", "low"])
def test_unparsable_ohlc_bounds_are_caught(column: str) -> None:
    """Junk in high/low must not slip through.

    The comparison invariants require both operands to parse, so without a
    standalone numeric check on these two columns a string high would satisfy
    every rule by being absent from all of them.
    """
    frame = _bars()
    frame[column] = ["not-a-number", frame[column].iloc[1]]
    assert _violations(frame).get(f"{column}_non_negative") == 1


@pytest.mark.unit
def test_duplicate_tally_degrades_instead_of_exhausting_memory(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Past the cap the check reports not-run, rather than reporting a clean
    pass off a partial tally or dying and taking the whole report with it.
    """
    from datetime import date as date_type

    from heber.ops import daily_health as dh

    monkeypatch.setattr(dh, "_MAX_TRACKED_EVENT_IDS", 1)

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    partition = silver / "feed=bars" / f"instrument_type=equity/dt={dt.isoformat()}"
    partition.mkdir(parents=True)
    _bars().to_parquet(partition / "part-0.parquet")

    class _Settings:
        silver_path = silver

    result = dh._check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert "event_id_unique" in result["observed"]["not_run"]["bars"]
    assert result["observed"]["scan_complete"] is False
    assert result["status"] in {"warn", "fail"}


@pytest.mark.unit
def test_applestuff_and_partial_writes_do_not_fail_a_partition(tmp_path) -> None:  # noqa: ANN001
    """AppleDouble sidecars and .tmp partial writes are ordinary noise here.

    Letting pyarrow auto-discover a partition directory turns both into an
    unreadable partition and therefore a failed critical check — a false
    incident on the supported volume layout.
    """
    from datetime import date as date_type

    from heber.ops.daily_health import _check_silver_invariants

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    partition = silver / "feed=bars" / f"instrument_type=equity/dt={dt.isoformat()}"
    partition.mkdir(parents=True)
    _bars().to_parquet(partition / "part-0.parquet")
    (partition / "._part-0.parquet").write_bytes(b"apple-double sidecar")
    (partition / "part-1.parquet.tmp").write_bytes(b"partial write")

    class _Settings:
        silver_path = silver

    result = _check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert result["observed"]["read_failures"] == []
    assert result["status"] == "ok"


@pytest.mark.unit
def test_near_cap_tally_still_reports_duplicates_of_known_ids(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """A frame of ids already being tracked must not abandon the tally.

    Counting a frame's distinct ids and adding that to the running total
    double-counts everything already seen, so a partition of pure repeats
    could trip the cap without the distinct set growing at all — and take the
    duplicates it was meant to report down with it.
    """
    from datetime import date as date_type

    from heber.ops import daily_health as dh

    monkeypatch.setattr(dh, "_MAX_TRACKED_EVENT_IDS", 1)

    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    for instrument_type in ("equity", "crypto"):
        partition = silver / "feed=bars" / f"instrument_type={instrument_type}/dt={dt.isoformat()}"
        partition.mkdir(parents=True)
        frame = _bars().iloc[[0]].copy()
        frame["event_id"] = ["only-id"]
        frame.to_parquet(partition / "part-0.parquet")

    class _Settings:
        silver_path = silver

    result = dh._check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert "bars" not in result["observed"].get("not_run", {}), "tally abandoned despite no new ids"
    assert result["observed"]["violations"]["bars"]["event_id_unique"] == 1


@pytest.mark.unit
def test_crossed_quotes_warn_above_tolerance_and_are_silent_below() -> None:
    """The retired contract allowed crossed quotes during volatility and only
    *warned* above 1%. Below that they are not reported; above it they are a
    warning, not a critical failure of the whole daily report.
    """
    from heber.quality.silver_invariants import split_by_tolerance

    spec = FEED_INVARIANTS["quotes"]
    assert split_by_tolerance(spec, {"ask_ge_bid": 1}, 1000) == ({}, {})
    assert split_by_tolerance(spec, {"ask_ge_bid": 11}, 1000) == ({}, {"ask_ge_bid": 11})
    # Untolerated invariants are failures regardless of rate.
    assert split_by_tolerance(spec, {"bid_px_non_negative": 1}, 1000) == (
        {"bid_px_non_negative": 1},
        {},
    )


@pytest.mark.unit
def test_crossed_quotes_above_tolerance_warn_rather_than_fail(tmp_path) -> None:  # noqa: ANN001
    """End-to-end: a tolerated invariant breaching its rate must not turn the
    whole critical daily check red.
    """
    from datetime import date as date_type

    from heber.ops.daily_health import _check_silver_invariants

    now = datetime.now(UTC)
    rows = 100
    frame = pd.DataFrame(
        {
            "event_id": [f"id-{i}" for i in range(rows)],
            "instrument_key": ["equity:AAPL"] * rows,
            "ts_event": [now - timedelta(minutes=5)] * rows,
            "ts_available": [now - timedelta(minutes=4)] * rows,
            "bid_px": [10.0] * rows,
            # 5 crossed rows in 100 = 5%, above the 1% tolerance.
            "ask_px": [9.0] * 5 + [10.5] * (rows - 5),
        }
    )
    dt = date_type(2026, 3, 9)
    silver = tmp_path / "silver"
    partition = silver / "feed=quotes" / f"instrument_type=equity/dt={dt.isoformat()}"
    partition.mkdir(parents=True)
    frame.to_parquet(partition / "part-0.parquet")

    class _Settings:
        silver_path = silver

    result = _check_silver_invariants(dt, _Settings())  # type: ignore[arg-type]
    assert result["status"] == "warn", "a tolerated breach must not fail the daily report"
    assert result["observed"]["warnings"]["quotes"]["ask_ge_bid"] == 5
    assert result["observed"]["violations"] == {}
