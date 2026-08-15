"""Deep behavioral edge-case tests for HeberReader (W2).

Complements ``test_heber_reader.py`` with boundary-condition coverage:

* ``asof_join`` — gaps, out-of-order rows, exact-boundary equality on
  ``ts_available == left_time``, tolerance boundary, late availability
  (``ts_available > ts_event``), suffix collisions, empty right table,
  missing-column validation.
* ``read_asof`` / ``read_silver`` — boundary equality on ``ts_event`` range
  ends, ``prune_by_dt`` partition-boundary correctness, ``batch_size`` path
  equivalence vs unbatched, column-projection essentials, mixed
  ``string`` / ``large_string`` / ``dictionary<string>`` fragment unification.

All timestamps are fixed (base 2026-01-15T10:00:00Z). Silver layouts are
built in ``tmp_path`` with hive partitions ``feed=X/instrument_type=Y/dt=Z``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import heber.reader.core as core_module
from heber.reader.core import HeberReader, HeberReadError, _collect_parquet_files

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ts(offset_hours: int = 0) -> datetime:
    """Fixed UTC timestamp helper (base = 2026-01-15T10:00:00Z)."""
    return datetime(2026, 1, 15, 10, 0, 0, tzinfo=UTC) + timedelta(hours=offset_hours)


def _write_partition(
    base: Path,
    feed: str,
    instrument_type: str,
    rows: list[dict],
    *,
    file_name: str = "data.parquet",
    schema: pa.Schema | None = None,
) -> Path:
    """Write one Silver hive partition (feed/instrument_type/dt) from ``rows``.

    The ``dt`` partition value is derived from the first row's ``ts_event``,
    matching the writer contract (``dt = ts_event.strftime("%Y-%m-%d")``).
    Returns the partition directory.
    """
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, schema=schema) if schema is not None else pa.Table.from_pandas(df)
    dt_value = pd.to_datetime(rows[0]["ts_event"]).strftime("%Y-%m-%d")
    partition_dir = base / "silver" / f"feed={feed}" / f"instrument_type={instrument_type}" / f"dt={dt_value}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(partition_dir / file_name))
    return partition_dir


def _multi_day_rows() -> list[dict]:
    """Rows spanning three calendar days (Jan 15, 16, 17 2026)."""
    return [
        {"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 150.0},
        {"ts_event": _ts(24), "ts_available": _ts(25), "instrument_key": "equity:AAPL", "close": 151.0},
        {"ts_event": _ts(48), "ts_available": _ts(49), "instrument_key": "equity:AAPL", "close": 152.0},
    ]


# ---------------------------------------------------------------------------
# asof_join — boundary equality, gaps, ordering, late availability
# ---------------------------------------------------------------------------


class TestAsofJoinBoundaries:
    def _reader(self) -> HeberReader:
        return HeberReader(Path("/tmp/heber-asof-unused"))

    def test_exact_boundary_ts_available_equals_left_time(self) -> None:
        """merge_asof is inclusive on the backward boundary: a right row whose
        availability lands exactly on ``left_time`` must still match."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(5)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2)]),
                "ts_available": pd.to_datetime([_ts(5)]),  # == left_time
                "feature": [42.0],
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 1
        assert result["feature"].iloc[0] == pytest.approx(42.0)

    def test_late_availability_excludes_future_visible_row(self) -> None:
        """A right row whose ts_event is prior but ts_available is AFTER
        left_time must NOT join — _safe_time = max(ts_event, ts_available)
        enforces the anti-leakage firewall."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(3)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(1)]),  # event is before left
                "ts_available": pd.to_datetime([_ts(6)]),  # but not available until later
                "feature": [7.0],
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 1
        assert pd.isna(result["feature"].iloc[0])

    def test_late_availability_uses_safe_time_not_ts_event(self) -> None:
        """When ts_available > ts_event, the join window is keyed off
        ts_available. A left row at _ts(7) sees the row available at _ts(6)."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(7)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(1)]),
                "ts_available": pd.to_datetime([_ts(6)]),
                "feature": [7.0],
            }
        )
        result = reader.asof_join(left, right)
        assert result["feature"].iloc[0] == pytest.approx(7.0)

    def test_gap_no_prior_right_row_yields_nan(self) -> None:
        """A left row earlier than every right row gets no match (gap)."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(0)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(5)]),
                "ts_available": pd.to_datetime([_ts(5)]),
                "feature": [99.0],
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 1
        assert pd.isna(result["feature"].iloc[0])

    def test_picks_most_recent_prior_row(self) -> None:
        """Among multiple eligible prior rows, the latest is chosen."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(10)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2), _ts(5), _ts(8)]),
                "ts_available": pd.to_datetime([_ts(2), _ts(5), _ts(8)]),
                "feature": [1.0, 2.0, 3.0],
            }
        )
        result = reader.asof_join(left, right)
        assert result["feature"].iloc[0] == pytest.approx(3.0)

    def test_out_of_order_right_rows_are_sorted_internally(self) -> None:
        """asof_join sorts both frames; unsorted right input must still produce
        the correct most-recent-prior match."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(9)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(8), _ts(2), _ts(5)]),  # shuffled
                "ts_available": pd.to_datetime([_ts(8), _ts(2), _ts(5)]),
                "feature": [30.0, 10.0, 20.0],
            }
        )
        result = reader.asof_join(left, right)
        assert result["feature"].iloc[0] == pytest.approx(30.0)

    def test_out_of_order_left_rows_preserve_per_row_correctness(self) -> None:
        """Unsorted left rows each get the correct prior match after the
        internal sort."""
        reader = self._reader()
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(9), _ts(4)]),  # descending
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(3), _ts(7)]),
                "ts_available": pd.to_datetime([_ts(3), _ts(7)]),
                "feature": [3.0, 7.0],
            }
        )
        result = reader.asof_join(left, right).sort_values("ts_event").reset_index(drop=True)
        # left @4 sees only the row available @3; left @9 sees the @7 row.
        assert result["feature"].iloc[0] == pytest.approx(3.0)
        assert result["feature"].iloc[1] == pytest.approx(7.0)

    def test_tolerance_exact_boundary_matches(self) -> None:
        """A gap exactly equal to the tolerance must still join (inclusive)."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(1)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                "ts_available": pd.to_datetime([_ts(0)]),
                "feature": [5.0],
            }
        )
        # exactly 1h gap, tolerance 1h → inclusive match
        result = reader.asof_join(left, right, tolerance="1h")
        assert result["feature"].iloc[0] == pytest.approx(5.0)

    def test_tolerance_just_over_boundary_excludes(self) -> None:
        """A gap one second beyond tolerance must not join."""
        reader = self._reader()
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0) + timedelta(hours=1, seconds=1)]),
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                "ts_available": pd.to_datetime([_ts(0)]),
                "feature": [5.0],
            }
        )
        result = reader.asof_join(left, right, tolerance="1h")
        assert pd.isna(result["feature"].iloc[0])

    def test_empty_right_table_keeps_left_rows_with_nan(self) -> None:
        """An empty right frame yields the left rows unmatched (no exception)."""
        reader = self._reader()
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL", "equity:TSLA"],
                "ts_event": pd.to_datetime([_ts(2), _ts(3)]),
            }
        )
        # Empty right table with tz-aware timestamp columns, mirroring what
        # read_silver returns for a no-match scan (UTC-aware throughout).
        right = pd.DataFrame(
            {
                "instrument_key": pd.Series([], dtype="object"),
                "ts_event": pd.Series([], dtype="datetime64[ns, UTC]"),
                "ts_available": pd.Series([], dtype="datetime64[ns, UTC]"),
                "feature": pd.Series([], dtype="float64"),
            }
        )
        result = reader.asof_join(left, right)
        assert len(result) == 2
        assert result["feature"].isna().all()

    def test_suffix_collision_applies_suffix_to_right_column(self) -> None:
        """When left and right share a non-key column, the right one gets the
        suffix and the left value is preserved unsuffixed."""
        reader = self._reader()
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(5)]),
                "close": [100.0],  # collides with right
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2)]),
                "ts_available": pd.to_datetime([_ts(2)]),
                "close": [200.0],  # collides with left
            }
        )
        result = reader.asof_join(left, right, suffix="_right")
        assert result["close"].iloc[0] == pytest.approx(100.0)  # left preserved
        assert "close_right" in result.columns
        assert result["close_right"].iloc[0] == pytest.approx(200.0)

    def test_custom_suffix_string(self) -> None:
        reader = self._reader()
        left = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(5)]),
                "px": [1.0],
            }
        )
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2)]),
                "ts_available": pd.to_datetime([_ts(2)]),
                "px": [2.0],
            }
        )
        result = reader.asof_join(left, right, suffix="_lookup")
        assert "px_lookup" in result.columns

    def test_missing_left_time_column_raises(self) -> None:
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"]})  # no ts_event
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                "ts_available": pd.to_datetime([_ts(0)]),
            }
        )
        with pytest.raises(ValueError, match="left DataFrame missing columns"):
            reader.asof_join(left, right)

    def test_missing_right_available_column_raises(self) -> None:
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(0)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(0)]),
                # ts_available missing
            }
        )
        with pytest.raises(ValueError, match="right DataFrame missing columns"):
            reader.asof_join(left, right)

    def test_does_not_mutate_input_frames(self) -> None:
        """asof_join copies right internally; neither input gains _safe_time."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "ts_event": pd.to_datetime([_ts(5)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "ts_event": pd.to_datetime([_ts(2)]),
                "ts_available": pd.to_datetime([_ts(2)]),
                "feature": [1.0],
            }
        )
        reader.asof_join(left, right)
        assert "_safe_time" not in right.columns
        assert "_safe_time" not in left.columns

    def test_custom_time_column_names(self) -> None:
        """Non-default left_time/right_time/right_available column names work."""
        reader = self._reader()
        left = pd.DataFrame({"instrument_key": ["equity:AAPL"], "obs_time": pd.to_datetime([_ts(5)])})
        right = pd.DataFrame(
            {
                "instrument_key": ["equity:AAPL"],
                "event_at": pd.to_datetime([_ts(2)]),
                "ready_at": pd.to_datetime([_ts(2)]),
                "feature": [11.0],
            }
        )
        result = reader.asof_join(
            left,
            right,
            left_time="obs_time",
            right_time="event_at",
            right_available="ready_at",
        )
        assert result["feature"].iloc[0] == pytest.approx(11.0)


# ---------------------------------------------------------------------------
# read_silver / read_asof — boundary equality, prune_by_dt, batch equivalence
# ---------------------------------------------------------------------------


class TestReadSilverBoundaries:
    def _multi_day(self, tmp_path: Path) -> HeberReader:
        _write_partition(tmp_path, "bars", "equity", [_multi_day_rows()[0]], file_name="d0.parquet")
        _write_partition(tmp_path, "bars", "equity", [_multi_day_rows()[1]], file_name="d1.parquet")
        _write_partition(tmp_path, "bars", "equity", [_multi_day_rows()[2]], file_name="d2.parquet")
        return HeberReader(tmp_path)

    def test_time_range_lower_bound_inclusive(self, tmp_path: Path) -> None:
        """ts_event >= start is inclusive: a row exactly on the start is kept."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", time_range=(_ts(0), _ts(0)))
        assert len(df) == 1
        assert pd.Timestamp(df["ts_event"].iloc[0]) == pd.Timestamp(_ts(0))

    def test_time_range_upper_bound_inclusive(self, tmp_path: Path) -> None:
        """ts_event <= end is inclusive: a row exactly on the end is kept."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", time_range=(_ts(48), _ts(48)))
        assert len(df) == 1
        assert pd.Timestamp(df["ts_event"].iloc[0]) == pd.Timestamp(_ts(48))

    def test_time_range_excludes_one_second_outside(self, tmp_path: Path) -> None:
        """A window ending one second before a row excludes it."""
        reader = self._multi_day(tmp_path)
        end = _ts(24) - timedelta(seconds=1)
        df = reader.read_silver("bars", time_range=(_ts(0), end))
        assert len(df) == 1  # only the _ts(0) row

    def test_asof_boundary_inclusive_on_ts_available(self, tmp_path: Path) -> None:
        """ts_available <= asof is inclusive at the exact boundary."""
        reader = self._multi_day(tmp_path)
        # row 0 available at _ts(1); asof exactly _ts(1) must include it
        df = reader.read_silver("bars", asof_time=_ts(1))
        assert len(df) == 1

    def test_prune_by_dt_matches_unpruned_result(self, tmp_path: Path) -> None:
        """prune_by_dt narrows partition dirs but yields identical rows to the
        unpruned ts_event scan."""
        reader = self._multi_day(tmp_path)
        rng = (_ts(0), _ts(24))
        unpruned = reader.read_silver("bars", time_range=rng)
        pruned = reader.read_silver("bars", time_range=rng, prune_by_dt=True)
        assert len(pruned) == len(unpruned) == 2
        assert set(pruned["ts_event"]) == set(unpruned["ts_event"])

    def test_prune_by_dt_partition_boundary_keeps_edge_day(self, tmp_path: Path) -> None:
        """A time_range whose end falls on the start of the last day must keep
        that day's matching rows (dt-string prune is inclusive on the date)."""
        reader = self._multi_day(tmp_path)
        # end at midnight-ish of Jan 17 (the _ts(48) row's exact event time)
        df = reader.read_silver("bars", time_range=(_ts(24), _ts(48)), prune_by_dt=True)
        assert len(df) == 2
        assert set(df["instrument_key"]) == {"equity:AAPL"}

    def test_prune_by_dt_single_day_window(self, tmp_path: Path) -> None:
        """A window entirely inside one day returns only that day's row."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", time_range=(_ts(24), _ts(24)), prune_by_dt=True)
        assert len(df) == 1
        assert pd.Timestamp(df["ts_event"].iloc[0]) == pd.Timestamp(_ts(24))

    def test_prune_by_dt_without_time_range_is_noop(self, tmp_path: Path) -> None:
        """prune_by_dt with no time_range reads everything (predicate skipped)."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", prune_by_dt=True)
        assert len(df) == 3

    def test_batch_size_equivalence_full_read(self, tmp_path: Path) -> None:
        """Batched read returns the same rows/columns as the unbatched path."""
        reader = self._multi_day(tmp_path)
        unbatched = reader.read_silver("bars").sort_values("ts_event").reset_index(drop=True)
        batched = reader.read_silver("bars", batch_size=1).sort_values("ts_event").reset_index(drop=True)
        assert len(batched) == len(unbatched) == 3
        assert set(batched.columns) == set(unbatched.columns)
        assert list(batched["close"]) == list(unbatched["close"])

    def test_batch_size_equivalence_with_filters(self, tmp_path: Path) -> None:
        """Batched + filtered read equals the unbatched + filtered read."""
        reader = self._multi_day(tmp_path)
        rng = (_ts(0), _ts(24))
        unbatched = reader.read_silver("bars", time_range=rng).sort_values("ts_event").reset_index(drop=True)
        batched = (
            reader.read_silver("bars", time_range=rng, batch_size=1).sort_values("ts_event").reset_index(drop=True)
        )
        assert list(batched["ts_event"]) == list(unbatched["ts_event"])

    def test_batch_size_empty_result_has_projected_schema(self, tmp_path: Path) -> None:
        """An empty batched result still returns a frame (no rows), not a crash."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", instrument_keys=["equity:NONE"], batch_size=2)
        assert df.empty

    def test_batch_size_progress_logging_threshold(self, tmp_path: Path) -> None:
        """A batched read large enough to cross the batch_size*10 logging
        threshold returns every row (exercises the progress-log branch)."""
        rows = [
            {
                "ts_event": _ts(i),
                "ts_available": _ts(i + 1),
                "instrument_key": "equity:AAPL",
                "close": float(i),
            }
            for i in range(12)
        ]
        _write_partition(tmp_path, "bars", "equity", rows)
        reader = HeberReader(tmp_path)
        df = reader.read_silver("bars", batch_size=1)
        assert len(df) == 12

    def test_projection_keeps_essential_columns(self, tmp_path: Path) -> None:
        """Requesting only 'close' still yields ts_event/ts_available/
        instrument_key — the essential set is force-included."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", columns=["close"])
        for col in ("close", "ts_event", "ts_available", "instrument_key"):
            assert col in df.columns

    def test_projection_unknown_column_silently_dropped(self, tmp_path: Path) -> None:
        """A projection naming a column absent from the schema is dropped, not
        raised — only schema-present columns survive intersection."""
        reader = self._multi_day(tmp_path)
        df = reader.read_silver("bars", columns=["close", "does_not_exist"])
        assert "does_not_exist" not in df.columns
        assert "close" in df.columns

    def test_instrument_type_partition_narrows_scan(self, tmp_path: Path) -> None:
        """instrument_type selects the hive partition subdir; an absent type
        returns empty rather than mixing types."""
        _write_partition(
            tmp_path,
            "bars",
            "equity",
            [{"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 1.0}],
        )
        reader = HeberReader(tmp_path)
        present = reader.read_silver("bars", instrument_type="equity")
        absent = reader.read_silver("bars", instrument_type="option")
        assert len(present) == 1
        assert absent.empty

    def test_read_asof_equivalent_to_read_silver_asof(self, tmp_path: Path) -> None:
        """read_asof is a thin delegate; result must equal read_silver(asof=)."""
        reader = self._multi_day(tmp_path)
        via_asof = reader.read_asof("bars", asof_time=_ts(25)).sort_values("ts_event").reset_index(drop=True)
        via_silver = reader.read_silver("bars", asof_time=_ts(25)).sort_values("ts_event").reset_index(drop=True)
        assert list(via_asof["ts_event"]) == list(via_silver["ts_event"])
        assert len(via_asof) == 2  # rows available by _ts(25): _ts(1) and _ts(25)


# ---------------------------------------------------------------------------
# Mixed string-encoding fragment unification
# ---------------------------------------------------------------------------


class TestStringEncodingUnification:
    """Two fragments under the same feed with different string encodings must
    merge cleanly: plain ``string`` vs ``large_string`` vs
    ``dictionary<string>``. Exercises the manual-unification fallback in
    ``_open_dataset_safe`` and the post-read ``_coerce_dict_columns_to_string``.
    """

    def _row(self, key: str, hours: int) -> dict:
        return {
            "ts_event": _ts(hours),
            "ts_available": _ts(hours + 1),
            "instrument_key": key,
            "label": f"tag-{hours}",
        }

    def test_large_string_and_plain_string_fragments_unify(self, tmp_path: Path) -> None:
        plain_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("label", pa.string()),
            ]
        )
        large_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.large_string()),
                ("label", pa.large_string()),
            ]
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:AAPL", 0)], file_name="plain.parquet", schema=plain_schema
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:AAPL", 24)], file_name="large.parquet", schema=large_schema
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("tags")
        assert len(df) == 2
        # Coerced to plain python strings on read.
        assert set(df["label"]) == {"tag-0", "tag-24"}
        assert all(isinstance(v, str) for v in df["label"])

    def test_dictionary_and_plain_string_fragments_unify(self, tmp_path: Path) -> None:
        plain_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("label", pa.string()),
            ]
        )
        dict_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("label", pa.dictionary(pa.int32(), pa.string())),
            ]
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:AAPL", 0)], file_name="plain.parquet", schema=plain_schema
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:AAPL", 24)], file_name="dict.parquet", schema=dict_schema
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("tags")
        assert len(df) == 2
        assert set(df["label"]) == {"tag-0", "tag-24"}
        # Result must be plain strings, not pandas Categorical.
        assert df["label"].dtype == object

    def test_filter_pushdown_survives_encoding_unification(self, tmp_path: Path) -> None:
        """An instrument_key filter must still prune correctly when fragments
        carry mismatched string encodings."""
        plain_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("label", pa.string()),
            ]
        )
        large_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.large_string()),
                ("label", pa.large_string()),
            ]
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:AAPL", 0)], file_name="plain.parquet", schema=plain_schema
        )
        _write_partition(
            tmp_path, "tags", "equity", [self._row("equity:TSLA", 24)], file_name="large.parquet", schema=large_schema
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("tags", instrument_keys=["equity:TSLA"])
        assert len(df) == 1
        assert df["instrument_key"].iloc[0] == "equity:TSLA"


# ---------------------------------------------------------------------------
# Manual schema-unification fallback (_open_dataset_safe second path)
# ---------------------------------------------------------------------------


class TestManualSchemaUnification:
    """When two fragments disagree on a column's type (e.g. int64 vs float64),
    the reader falls back to reading each fragment's physical schema and
    widening to a common type. These tests force that conflict and assert the
    merged read.
    """

    def _row(self, key: str, hours: int, n: object) -> dict:
        return {
            "ts_event": _ts(hours),
            "ts_available": _ts(hours + 1),
            "instrument_key": key,
            "n": n,
        }

    def test_int64_and_float64_fragments_widen_to_float(self, tmp_path: Path) -> None:
        int_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.int64()),
            ]
        )
        float_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.float64()),
            ]
        )
        # Both fragments share dt=2026-01-15 so pyarrow must cross-merge their
        # schemas — int64 vs float64 fails the first attempt and forces the
        # manual-unification fallback to widen to float64.
        _write_partition(
            tmp_path, "nums", "equity", [self._row("equity:AAPL", 0, 5)], file_name="int.parquet", schema=int_schema
        )
        _write_partition(
            tmp_path,
            "nums",
            "equity",
            [self._row("equity:AAPL", 1, 2.5)],
            file_name="float.parquet",
            schema=float_schema,
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("nums")
        assert len(df) == 2
        # Both values readable; int fragment widened to float losslessly.
        assert set(df["n"]) == {5.0, 2.5}

    def test_filter_pushdown_survives_numeric_unification(self, tmp_path: Path) -> None:
        """instrument_key pruning still works through the manual-unification
        fallback (the unified schema must retain the partition column)."""
        int_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.int64()),
            ]
        )
        float_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.float64()),
            ]
        )
        # Same dt partition forces the cross-fragment merge / fallback.
        _write_partition(
            tmp_path, "nums", "equity", [self._row("equity:AAPL", 0, 5)], file_name="int.parquet", schema=int_schema
        )
        _write_partition(
            tmp_path,
            "nums",
            "equity",
            [self._row("equity:TSLA", 1, 2.5)],
            file_name="float.parquet",
            schema=float_schema,
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("nums", instrument_keys=["equity:TSLA"])
        assert len(df) == 1
        assert df["instrument_key"].iloc[0] == "equity:TSLA"
        assert df["n"].iloc[0] == pytest.approx(2.5)

    def test_partition_column_preserved_through_unification(self, tmp_path: Path) -> None:
        """Counterpart to the xfailed single-partition case: when the int/float
        conflict sits alongside fragments in a SECOND dt partition, pyarrow's
        dataset-level schema inference samples across all fragments and widens
        ``n`` to float64 on its own, so the read succeeds and the hive ``dt``
        partition column is preserved. Documents that the bug is specific to the
        single-partition two-fragment layout."""
        int_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.int64()),
            ]
        )
        float_schema = pa.schema(
            [
                ("ts_event", pa.timestamp("us", tz="UTC")),
                ("ts_available", pa.timestamp("us", tz="UTC")),
                ("instrument_key", pa.string()),
                ("n", pa.float64()),
            ]
        )
        # dt=2026-01-15 holds the conflicting int/float fragments (forces the
        # fallback); dt=2026-01-16 holds one more float fragment. The unified
        # re-open must still expose the hive ``dt`` partition column for both.
        _write_partition(
            tmp_path, "nums", "equity", [self._row("equity:AAPL", 0, 5)], file_name="int.parquet", schema=int_schema
        )
        _write_partition(
            tmp_path,
            "nums",
            "equity",
            [self._row("equity:AAPL", 1, 2.5)],
            file_name="float.parquet",
            schema=float_schema,
        )
        _write_partition(
            tmp_path,
            "nums",
            "equity",
            [self._row("equity:AAPL", 24, 7.0)],
            file_name="float.parquet",
            schema=float_schema,
        )
        reader = HeberReader(tmp_path)
        df = reader.read_silver("nums")
        assert "dt" in df.columns
        assert set(df["dt"]) == {"2026-01-15", "2026-01-16"}
        assert len(df) == 3


def _pin_file_order(monkeypatch: pytest.MonkeyPatch, first: str) -> None:
    """Force ``_collect_parquet_files`` to enumerate ``first`` ahead of the rest.

    pyarrow infers a dataset's schema from a *subset* of fragments (one, by
    default), so which physical file lands first in the list decides the
    inferred type of a conflicting column. On a real volume that order comes
    from ``rglob`` — i.e. from filesystem enumeration order, which is arbitrary
    and differs between machines. Pinning it here makes both orderings
    reproducible instead of leaving the assertion at the mercy of the FS.
    """
    real = core_module._collect_parquet_files

    def ordered(*args: object, **kwargs: object) -> list[str]:
        files = real(*args, **kwargs)
        return sorted(files, key=lambda f: (Path(f).name != first, Path(f).name))

    monkeypatch.setattr(core_module, "_collect_parquet_files", ordered)


class TestNullTypedColumnUnification:
    """A column that was entirely null when one fragment was written lands on
    disk as Arrow ``null`` type, while another fragment types it ``string``.

    pyarrow infers the dataset schema lazily from a subset of fragments, so the
    conflict does not surface at ``ds.dataset()`` — it surfaces at scan time as
    ``ArrowNotImplementedError: Unsupported cast from string to null``. That is
    not an ``ArrowInvalid``, so it escapes the read-path handler entirely and
    propagates to the caller, taking the whole feed's read down with it.

    Each case runs with the null fragment enumerated first (the broken
    direction — inferred type is ``null``, so the concrete fragment cannot be
    cast into it) and last (the benign direction), because production hits
    whichever order the filesystem happens to yield.
    """

    def _row(self, key: str, hours: int, value: object, column: str) -> dict:
        return {
            "ts_event": _ts(hours),
            "ts_available": _ts(hours + 1),
            "instrument_key": key,
            column: value,
        }

    def _schemas(self, column: str, concrete: pa.DataType, null_type: pa.DataType) -> tuple[pa.Schema, pa.Schema]:
        base = [
            ("ts_event", pa.timestamp("us", tz="UTC")),
            ("ts_available", pa.timestamp("us", tz="UTC")),
            ("instrument_key", pa.string()),
        ]
        return pa.schema([*base, (column, null_type)]), pa.schema([*base, (column, concrete)])

    def _build(
        self,
        tmp_path: Path,
        column: str,
        concrete: pa.DataType,
        value: object,
        key: str = "equity:AAPL",
        null_type: pa.DataType | None = None,
        null_value: object = None,
    ) -> None:
        null_schema, concrete_schema = self._schemas(column, concrete, null_type or pa.null())
        # Both fragments share one dt partition so pyarrow must reconcile them.
        _write_partition(
            tmp_path,
            "bars",
            "equity",
            [self._row("equity:AAPL", 0, null_value, column)],
            file_name="a_null.parquet",
            schema=null_schema,
        )
        _write_partition(
            tmp_path,
            "bars",
            "equity",
            [self._row(key, 1, value, column)],
            file_name="b_concrete.parquet",
            schema=concrete_schema,
        )

    @pytest.mark.parametrize("first", ["a_null.parquet", "b_concrete.parquet"])
    def test_null_and_string_fragments_unify(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: str) -> None:
        self._build(tmp_path, "note", pa.string(), "hello")
        _pin_file_order(monkeypatch, first)
        df = HeberReader(tmp_path).read_silver("bars")
        assert len(df) == 2
        assert set(df["note"].dropna()) == {"hello"}

    @pytest.mark.parametrize("first", ["a_null.parquet", "b_concrete.parquet"])
    def test_null_and_float_fragments_unify(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: str) -> None:
        """null vs a non-string concrete type must widen to that type too."""
        self._build(tmp_path, "vwap", pa.float64(), 12.5)
        _pin_file_order(monkeypatch, first)
        df = HeberReader(tmp_path).read_silver("bars")
        assert len(df) == 2
        assert df["vwap"].dropna().tolist() == [pytest.approx(12.5)]

    @pytest.mark.parametrize("first", ["a_null.parquet", "b_concrete.parquet"])
    def test_filter_pushdown_survives_null_unification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: str
    ) -> None:
        self._build(tmp_path, "note", pa.string(), "hello", key="equity:TSLA")
        _pin_file_order(monkeypatch, first)
        df = HeberReader(tmp_path).read_silver("bars", instrument_keys=["equity:TSLA"])
        assert len(df) == 1
        assert df["instrument_key"].iloc[0] == "equity:TSLA"

    @pytest.mark.parametrize("first", ["a_null.parquet", "b_concrete.parquet"])
    def test_list_of_null_and_list_of_string_fragments_unify(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: str
    ) -> None:
        """An empty list column serializes as ``list<element: null>``.

        Silver's ``quality_flags`` is a ``list<string>``; a fragment where every
        row had an empty list lands on disk as ``list<element: null>``. That is
        not a null *type* at the top level, so it needs the same resolution one
        level down — it raises the identical "Unsupported cast from string to
        null" as the scalar case.
        """
        self._build(
            tmp_path,
            "quality_flags",
            pa.list_(pa.string()),
            ["ok"],
            null_type=pa.list_(pa.null()),
            null_value=[],
        )
        _pin_file_order(monkeypatch, first)
        df = HeberReader(tmp_path).read_silver("bars")
        assert len(df) == 2
        flags = [list(v) for v in df["quality_flags"] if v is not None and len(v) > 0]
        assert flags == [["ok"]]

    def test_unresolvable_conflict_surfaces_instead_of_reading_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When unification cannot rescue the read, the failure must surface as
        a ``HeberReadError`` — not be flattened into an empty DataFrame.

        An empty result is indistinguishable from "this feed had no data
        today", which is exactly how the bars audit stayed silently broken.
        """
        self._build(tmp_path, "note", pa.string(), "hello")
        _pin_file_order(monkeypatch, "a_null.parquet")
        monkeypatch.setattr(core_module, "_unified_dataset", lambda *_a, **_k: None)

        with pytest.raises(HeberReadError, match="unreadable"):
            HeberReader(tmp_path).read_silver("bars")

    @pytest.mark.parametrize("first", ["a_null.parquet", "b_concrete.parquet"])
    def test_asof_leakage_gate_survives_null_unification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, first: str
    ) -> None:
        """The ``ts_available`` anti-leakage predicate must still be enforced
        when the read is served from the unified-schema retry.

        Recovering from a schema conflict must never widen what a
        point-in-time read can see: the concrete fragment here is available at
        ``_ts(2)``, so an asof of ``_ts(1)`` must exclude it.
        """
        self._build(tmp_path, "note", pa.string(), "hello")
        _pin_file_order(monkeypatch, first)
        reader = HeberReader(tmp_path)

        # Fragment ts_available values are _ts(1) (null frag) and _ts(2).
        visible = reader.read_silver("bars", asof_time=_ts(1))
        assert len(visible) == 1
        assert visible["note"].isna().all(), "future-available row leaked into an asof read"

        # Past the later fragment's availability, both rows are visible.
        assert len(reader.read_silver("bars", asof_time=_ts(2))) == 2


# ---------------------------------------------------------------------------
# File-collection edge cases (sidecars / tmp files) on the read path
# ---------------------------------------------------------------------------


class TestFileCollectionEdges:
    def test_appledouble_sidecar_file_skipped(self, tmp_path: Path) -> None:
        """A ``._<name>.parquet`` AppleDouble sidecar alongside a real file must
        be skipped, not break the open."""
        part = _write_partition(
            tmp_path,
            "bars",
            "equity",
            [{"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 1.0}],
        )
        # Drop a junk AppleDouble sidecar next to the real parquet.
        (part / "._data.parquet").write_bytes(b"not a parquet file")
        reader = HeberReader(tmp_path)
        df = reader.read_silver("bars")
        assert len(df) == 1

    def test_tmp_partial_write_skipped(self, tmp_path: Path) -> None:
        """A ``*.parquet.tmp`` partial write must not be picked up by the scan."""
        part = _write_partition(
            tmp_path,
            "bars",
            "equity",
            [{"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 1.0}],
        )
        (part / "part-0001.parquet.tmp").write_bytes(b"\x00\x01")
        reader = HeberReader(tmp_path)
        df = reader.read_silver("bars")
        assert len(df) == 1

    def test_sidecar_partition_directory_skipped(self, tmp_path: Path) -> None:
        """Files inside a ``._dt=...`` sidecar directory are excluded."""
        _write_partition(
            tmp_path,
            "bars",
            "equity",
            [{"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 1.0}],
        )
        feed_dir = tmp_path / "silver" / "feed=bars" / "instrument_type=equity"
        junk_dir = feed_dir / "._dt=2026-01-15"
        junk_dir.mkdir(parents=True, exist_ok=True)
        (junk_dir / "data.parquet").write_bytes(b"junk")
        reader = HeberReader(tmp_path)
        df = reader.read_silver("bars")
        assert len(df) == 1


class TestCollectParquetFilesHelper:
    """Direct unit coverage of ``_collect_parquet_files`` — the branch that
    accepts a single file path (used by ``read_label``) plus the
    skip-by-name guards on that direct-file branch."""

    def _make_file(self, tmp_path: Path, name: str) -> Path:
        df = pd.DataFrame(
            {"ts_event": [_ts(0)], "ts_available": [_ts(1)], "instrument_key": ["equity:AAPL"], "v": [1.0]}
        )
        path = tmp_path / name
        pq.write_table(pa.Table.from_pandas(df), str(path))
        return path

    def test_direct_file_returned(self, tmp_path: Path) -> None:
        path = self._make_file(tmp_path, "data.parquet")
        assert _collect_parquet_files(path) == [str(path)]

    def test_direct_sidecar_file_skipped(self, tmp_path: Path) -> None:
        path = self._make_file(tmp_path, "._data.parquet")
        assert _collect_parquet_files(path) == []

    def test_direct_tmp_file_skipped(self, tmp_path: Path) -> None:
        # Bare bytes are fine — the skip happens on the name before any read.
        path = tmp_path / "part-0001.parquet.tmp"
        path.write_bytes(b"\x00")
        assert _collect_parquet_files(path) == []

    def test_missing_root_returns_empty(self, tmp_path: Path) -> None:
        assert _collect_parquet_files(tmp_path / "does-not-exist") == []


class TestReadGoldEdges:
    """Gold-read branches not exercised by the existing suite: ts_label time
    column, time_range pushdown, and specific-version-not-on-disk."""

    def _write_gold(self, base: Path, version: str, rows: list[dict]) -> None:
        df = pd.DataFrame(rows)
        dt_value = pd.to_datetime(rows[0]["ts_label"]).strftime("%Y-%m-%d")
        part = base / "gold" / "dataset=labels" / "project=kairos" / f"version={version}" / f"dt={dt_value}"
        part.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df), str(part / "data.parquet"))

    def test_time_range_uses_ts_label_when_no_ts_event(self, tmp_path: Path) -> None:
        """When the Gold dataset lacks ts_event, time_range applies to
        ts_label (the fallback time column)."""
        rows = [
            {"ts_label": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "label": 1},
            {"ts_label": _ts(2), "ts_available": _ts(3), "instrument_key": "equity:AAPL", "label": 0},
        ]
        self._write_gold(tmp_path, "v1", rows)
        reader = HeberReader(tmp_path)
        df = reader.read_gold("labels", project="kairos", version="v1", time_range=(_ts(0), _ts(0)))
        assert len(df) == 1
        assert pd.Timestamp(df["ts_label"].iloc[0]) == pd.Timestamp(_ts(0))

    def test_specific_version_missing_returns_empty(self, tmp_path: Path) -> None:
        rows = [{"ts_label": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "label": 1}]
        self._write_gold(tmp_path, "v1", rows)
        reader = HeberReader(tmp_path)
        # version=v9 was never written → no files → empty frame, no exception.
        df = reader.read_gold("labels", project="kairos", version="v9")
        assert df.empty


class TestReadParquetDatasetEdges:
    """Branches of read_parquet_dataset: direct-file read, asof pushdown, and
    the no-files-after-filtering case."""

    def test_direct_file_path_read(self, tmp_path: Path) -> None:
        df_in = pd.DataFrame(
            {
                "ts_event": [_ts(0), _ts(1)],
                "ts_available": [_ts(1), _ts(2)],
                "instrument_key": ["equity:AAPL", "equity:AAPL"],
                "v": [1.0, 2.0],
            }
        )
        path = tmp_path / "single.parquet"
        pq.write_table(pa.Table.from_pandas(df_in), str(path))
        reader = HeberReader(tmp_path)
        out = reader.read_parquet_dataset(path)
        assert len(out) == 2

    def test_asof_pushdown_on_arbitrary_path(self, tmp_path: Path) -> None:
        part = _write_partition(
            tmp_path,
            "bars",
            "equity",
            [
                {"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL", "close": 1.0},
                {"ts_event": _ts(2), "ts_available": _ts(5), "instrument_key": "equity:AAPL", "close": 2.0},
            ],
        )
        feed_dir = part.parent
        reader = HeberReader(tmp_path)
        out = reader.read_parquet_dataset(feed_dir, asof_time=_ts(1))
        assert len(out) == 1  # only the row available by _ts(1)

    def test_only_sidecars_present_returns_empty(self, tmp_path: Path) -> None:
        """A directory containing only AppleDouble sidecar parquet files yields
        no readable files → empty frame (dataset_obj is None branch)."""
        d = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-15"
        d.mkdir(parents=True, exist_ok=True)
        (d / "._junk.parquet").write_bytes(b"junk")
        reader = HeberReader(tmp_path)
        out = reader.read_parquet_dataset(d)
        assert out.empty

    def test_corrupt_parquet_file_raises(self, tmp_path: Path) -> None:
        """A file named ``*.parquet`` holding non-parquet bytes is collected (the
        name passes the filter) but fails at open, and that must raise.

        ``read_parquet_dataset`` gets the same contract as the Silver and Gold
        reads: absence answers empty, unreadable bytes do not. It used to raise
        only when the scan failed, so a file that failed one step earlier — at
        open — still came back as an empty frame, and `read_label` reported zero
        labels for a dataset nobody could read.
        """
        d = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-15"
        d.mkdir(parents=True, exist_ok=True)
        (d / "corrupt.parquet").write_bytes(b"this is not parquet data at all")
        reader = HeberReader(tmp_path)
        with pytest.raises(HeberReadError):
            reader.read_parquet_dataset(d)


class TestSilverReadErrorBranches:
    """read_silver's defensive empty-return branches."""

    def test_corrupt_silver_file_raises(self, tmp_path: Path) -> None:
        """A corrupt parquet under a Silver feed must fail the read, not empty it.

        This asserted the opposite until a zero-byte fragment under
        `feed=darkpool` silently emptied that entire feed and a Gold
        regeneration reported `no_data` and exited 0. An unreadable partition
        and an unwritten one are different answers.
        """
        d = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-15"
        d.mkdir(parents=True, exist_ok=True)
        (d / "corrupt.parquet").write_bytes(b"definitely not parquet")
        reader = HeberReader(tmp_path)
        with pytest.raises(HeberReadError):
            reader.read_silver("bars")

    def test_feed_dir_with_only_sidecars_returns_empty(self, tmp_path: Path) -> None:
        """When every candidate file is filtered out, _open_dataset_safe returns
        None and read_silver short-circuits to an empty frame."""
        d = tmp_path / "silver" / "feed=bars" / "instrument_type=equity" / "dt=2026-01-15"
        d.mkdir(parents=True, exist_ok=True)
        (d / "._only.parquet").write_bytes(b"junk")
        reader = HeberReader(tmp_path)
        df = reader.read_silver("bars")
        assert df.empty


class TestGoldReadErrorBranches:
    def test_corrupt_gold_file_raises(self, tmp_path: Path) -> None:
        """Gold gets the same contract as Silver: unreadable is not empty.

        A dataset whose files cannot be opened must not answer "no data" —
        that made a corrupt fragment indistinguishable from a version nobody
        had written, which is how a whole feed went missing unnoticed.
        """
        part = tmp_path / "gold" / "dataset=feat" / "project=kairos" / "version=v1" / "dt=2026-01-15"
        part.mkdir(parents=True, exist_ok=True)
        (part / "corrupt.parquet").write_bytes(b"not parquet")
        reader = HeberReader(tmp_path)
        with pytest.raises(HeberReadError):
            reader.read_gold("feat", project="kairos", version="v1")


class TestTimeframeFilter:
    """`timeframe` is pushed into the scan, and fails closed when unsupported."""

    def _row(self, hours: int, tf: str) -> dict:
        return {
            "ts_event": _ts(hours),
            "ts_available": _ts(hours + 1),
            "instrument_key": "equity:AAPL",
            "timeframe": tf,
            "close": 100.0 + hours,
        }

    def test_filters_to_the_requested_interval(self, tmp_path: Path) -> None:
        _write_partition(
            tmp_path,
            "bars",
            "equity",
            [self._row(0, "1Day"), self._row(1, "1Min"), self._row(2, "5Min")],
        )
        reader = HeberReader(tmp_path)

        assert len(reader.read_silver("bars", instrument_type="equity")) == 3
        daily = reader.read_silver("bars", instrument_type="equity", timeframe="1Day")
        assert len(daily) == 1
        assert set(daily["timeframe"]) == {"1Day"}

    def test_raises_when_the_feed_has_no_timeframe_column(self, tmp_path: Path) -> None:
        """Failing open would return every interval to a caller asking for one."""
        _write_partition(
            tmp_path,
            "tags",
            "equity",
            [{"ts_event": _ts(0), "ts_available": _ts(1), "instrument_key": "equity:AAPL"}],
        )
        reader = HeberReader(tmp_path)

        with pytest.raises(ValueError, match="timeframe"):
            reader.read_silver("tags", instrument_type="equity", timeframe="1Day")
