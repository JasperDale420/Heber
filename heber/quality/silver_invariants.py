"""Semantic invariants on Silver data.

`heber.health_monitor` already answers "is data arriving, on time, with the
expected shape". Partition and liveness cover row counts and freshness, schema
covers drift, volume covers row-count trends, and statistical covers null
rates — but only for *numeric* columns, so it says nothing about a missing
``event_id`` or an unparsable timestamp. None of them look at whether the
values themselves make sense either: a negative price, a bar whose high sits
below its low, a crossed quote, a repeated ``event_id``, or an
``instrument_type`` outside the contract would pass every one of those checks
while being plainly corrupt.

These are the checks for that, and they are the residue of the retired Soda
integration — the part of it not already covered elsewhere.

Two design rules, both learned from how that integration failed:

* A check that cannot run is reported, never skipped silently. Column names
  drift, and an invariant quietly matching nothing looks identical to an
  invariant passing. The retired Soda checks named ``bid_price``/``ask_price``
  for a feed whose columns are ``bid_px``/``ask_px``, and nothing ever said so.
* A value that is present but unparsable is a violation, not a null. Coercing
  junk to NaN and then excluding NaN would let the worst rows through.

Column names here are asserted against ``SILVER_SCHEMAS`` by the test suite,
so a schema rename breaks a test rather than silently disabling a check.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)

# Per PRD section 6.2 / models.envelope. A value outside this set means the
# producer emitted something the writer's own key validation should have
# rejected, so it is worth surfacing rather than tolerating.
INSTRUMENT_TYPES = ("equity", "option", "index", "etf", "crypto", "forex")

# There is deliberately no allow-list for flow_alerts.alert_type. The retired
# Soda checks pinned it to ("SWEEP", "BLOCK", "UNUSUAL", "SPLIT",
# "GOLDEN_SWEEP"), which matches nothing the provider actually sends — a day's
# live data holds RepeatedHits, RepeatedHitsAscendingFill, FloorTradeLargeCap
# and others, so every row would have been flagged. It is a vendor-controlled
# vocabulary that grows without notice, so pinning it produces false alarms
# rather than findings. instrument_type is different: Heber defines that set
# itself and the writer already rejects anything outside it.


@dataclass(frozen=True)
class Violation:
    """Rows failing one invariant."""

    name: str
    rows: int
    columns: tuple[str, ...]


@dataclass(frozen=True)
class Invariant:
    """One rule, plus the columns it needs to be able to run at all.

    ``predicate`` returns a boolean Series that is True for *violating* rows.
    It is only ever called when every column in ``columns`` is present, so
    predicates never have to guard for missing columns themselves.
    """

    name: str
    columns: tuple[str, ...]
    predicate: Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class FeedInvariants:
    """The invariant set for one Silver feed.

    ``completeness`` maps a column to the highest null fraction tolerated for
    it. These are per-column contracts carried over from the retired Soda
    checks; health_monitor's statistical check also profiles null rates but
    against a single global default (5%), so relying on it alone would have
    silently relaxed the columns that were held to 1%.
    """

    feed: str
    invariants: tuple[Invariant, ...] = field(default_factory=tuple)
    completeness: tuple[tuple[str, float], ...] = field(default_factory=tuple)
    # Invariants that tolerate a small rate before counting as a finding, as
    # a fraction of the feed's rows. The retired quotes contract allowed
    # crossed quotes during volatility and only warned above 1%; treating a
    # single transient crossed quote as a critical daily failure would be a
    # stricter policy than was ever agreed.
    tolerances: tuple[tuple[str, float], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class FrameOutcome:
    """What running a feed's invariants over one frame produced.

    ``not_run`` is as important as ``violations``: an invariant that could not
    execute has verified nothing, and reporting that as a pass is how a broken
    check hides.
    """

    violations: tuple[Violation, ...] = ()
    not_run: tuple[str, ...] = ()
    rows: int = 0


def _numeric(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Coerce to numeric, and separately flag values that were present but
    could not be parsed — those are corruption, not absence.
    """
    values = pd.to_numeric(series, errors="coerce")
    unparsable = series.notna() & values.isna()
    return values, unparsable


def _timestamps(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    values = pd.to_datetime(series, utc=True, errors="coerce")
    unparsable = series.notna() & values.isna()
    return values, unparsable


def _non_negative(column: str) -> Invariant:
    """Flag negative values, and values that are present but not numeric.

    Nulls are not violations — null coverage for numeric columns is
    health_monitor's statistical check, and double-reporting would make both
    reports harder to trust. Required-field nullness is covered separately by
    ``_required`` for the identity columns where it actually matters.
    """

    def predicate(df: pd.DataFrame) -> pd.Series:
        values, unparsable = _numeric(df[column])
        return unparsable | (values.notna() & (values < 0))

    return Invariant(name=f"{column}_non_negative", columns=(column,), predicate=predicate)


def _required(column: str) -> Invariant:
    """Flag rows missing an identity/availability column.

    health_monitor's statistical check profiles numeric columns only, so
    ``event_id`` and ``instrument_key`` (strings) and the timestamps have no
    null coverage anywhere else. A row without them breaks joins and
    point-in-time reads while looking perfectly healthy to every other check.
    """

    def predicate(df: pd.DataFrame) -> pd.Series:
        values = df[column]
        missing = values.isna()
        # Blank detection has to cover pandas' string extension dtypes, not
        # just object: a whitespace-only value in a string[python] or
        # string[pyarrow] column is neither NA nor object, so an
        # object-only check would let it through — and a blank event_id
        # would then evade duplicate detection too.
        if pd.api.types.is_string_dtype(values) or values.dtype == object:
            blank = values.fillna("").astype(str).str.strip().eq("")
            return missing | blank
        return missing

    return Invariant(name=f"{column}_required", columns=(column,), predicate=predicate)


def _allowed_values(column: str, allowed: tuple[str, ...]) -> Invariant:
    def predicate(df: pd.DataFrame) -> pd.Series:
        values = df[column]
        return values.notna() & ~values.isin(allowed)

    return Invariant(name=f"{column}_allowed", columns=(column,), predicate=predicate)


def _compare(name: str, left: str, right: str) -> Invariant:
    """Flag rows where ``left`` is strictly less than ``right``."""

    def predicate(df: pd.DataFrame) -> pd.Series:
        # Unparsable values are reported by the column's own non_negative
        # invariant; counting them again here would inflate the row count.
        lhs, _ = _numeric(df[left])
        rhs, _ = _numeric(df[right])
        return lhs.notna() & rhs.notna() & (lhs < rhs)

    return Invariant(name=name, columns=(left, right), predicate=predicate)


def _high_is_highest() -> Invariant:
    def predicate(df: pd.DataFrame) -> pd.Series:
        high, _ = _numeric(df["high"])
        openp, _ = _numeric(df["open"])
        close, _ = _numeric(df["close"])
        return high.notna() & ((openp.notna() & (high < openp)) | (close.notna() & (high < close)))

    return Invariant(name="high_is_highest", columns=("high", "open", "close"), predicate=predicate)


def _low_is_lowest() -> Invariant:
    def predicate(df: pd.DataFrame) -> pd.Series:
        low, _ = _numeric(df["low"])
        openp, _ = _numeric(df["open"])
        close, _ = _numeric(df["close"])
        return low.notna() & ((openp.notna() & (low > openp)) | (close.notna() & (low > close)))

    return Invariant(name="low_is_lowest", columns=("low", "open", "close"), predicate=predicate)


def _ts_available_ge_ts_event() -> Invariant:
    """The zero-leakage invariant, on Silver.

    health_monitor's ml_readiness audits this on Gold datasets only, so
    without this Silver has no guard at all — and Silver is what every Gold
    build reads from.
    """

    def predicate(df: pd.DataFrame) -> pd.Series:
        # Parseability is reported by its own invariant, so a single junk
        # timestamp is one violating row there rather than a violation of
        # every relation that happens to touch the column.
        available, _ = _timestamps(df["ts_available"])
        event, _ = _timestamps(df["ts_event"])
        return available.notna() & event.notna() & (available < event)

    return Invariant(
        name="ts_available_ge_ts_event",
        columns=("ts_available", "ts_event"),
        predicate=predicate,
    )


def _ts_available_not_future() -> Invariant:
    """A row cannot become queryable later than now — a future ``ts_available``
    means a clock or a producer is wrong, and point-in-time reads would hide
    the row until that timestamp passes.
    """

    def predicate(df: pd.DataFrame) -> pd.Series:
        available, _ = _timestamps(df["ts_available"])
        return available.notna() & (available > datetime.now(UTC))

    return Invariant(name="ts_available_not_future", columns=("ts_available",), predicate=predicate)


def _parseable_timestamp(column: str) -> Invariant:
    """A present-but-unparsable timestamp is corruption in its own right.

    It gets its own invariant so one bad row is counted once, and is not
    reported as (say) a future timestamp when parsing failed before
    future-ness could be judged at all.
    """

    def predicate(df: pd.DataFrame) -> pd.Series:
        _, unparsable = _timestamps(df[column])
        return unparsable

    return Invariant(name=f"{column}_parseable", columns=(column,), predicate=predicate)


# event_id uniqueness is deliberately NOT here: it is a set-wide property, not
# a row predicate, and a feed's rows span several instrument_type partitions.
# Checking it per frame would miss the same id appearing in two of them.
# ``duplicate_row_count`` handles it at feed level instead.
_SHARED = (
    _parseable_timestamp("ts_event"),
    _parseable_timestamp("ts_available"),
    _required("event_id"),
    _required("instrument_key"),
    _required("ts_event"),
    _required("ts_available"),
    _ts_available_ge_ts_event(),
    _ts_available_not_future(),
)


FEED_INVARIANTS: dict[str, FeedInvariants] = {
    "bars": FeedInvariants(
        feed="bars",
        invariants=(
            *_SHARED,
            _non_negative("open"),
            _non_negative("high"),
            _non_negative("low"),
            _non_negative("close"),
            _non_negative("volume"),
            _compare("high_ge_low", "high", "low"),
            _high_is_highest(),
            _low_is_lowest(),
            _allowed_values("instrument_type", INSTRUMENT_TYPES),
        ),
        completeness=(
            ("open", 0.01),
            ("high", 0.01),
            ("low", 0.01),
            ("close", 0.01),
            ("volume", 0.05),
        ),
    ),
    "quotes": FeedInvariants(
        feed="quotes",
        invariants=(
            *_SHARED,
            # Canonical Silver names — bid_price/ask_price do not exist.
            _non_negative("bid_px"),
            _non_negative("ask_px"),
            _non_negative("bid_sz"),
            _non_negative("ask_sz"),
            _compare("ask_ge_bid", "ask_px", "bid_px"),
        ),
        tolerances=(("ask_ge_bid", 0.01),),
        completeness=(
            ("bid_px", 0.0),
            ("ask_px", 0.0),
            ("bid_sz", 0.05),
            ("ask_sz", 0.05),
        ),
    ),
    "trades": FeedInvariants(
        feed="trades",
        invariants=(
            *_SHARED,
            _non_negative("price"),
            _non_negative("size"),
        ),
        completeness=(
            ("price", 0.01),
            ("size", 0.05),
        ),
    ),
    "flow_alerts": FeedInvariants(
        feed="flow_alerts",
        invariants=(
            *_SHARED,
            _non_negative("premium"),
            _non_negative("volume"),
        ),
        completeness=(
            ("premium", 0.05),
            ("volume", 0.05),
        ),
    ),
}


@dataclass(frozen=True)
class ColumnFill:
    """Accumulated null tally for one column across a feed's partitions."""

    nulls: int = 0
    total: int = 0


def accumulate_fill(df: pd.DataFrame, spec: FeedInvariants, tally: dict[str, ColumnFill]) -> None:
    """Add one frame's null counts to a feed-level tally.

    Fill rate is a property of the feed's whole day, not of one partition —
    a column 100% null in a small partition and complete everywhere else is
    not a 100% failure.
    """
    for column, _threshold in spec.completeness:
        if column not in df.columns:
            continue
        current = tally.get(column, ColumnFill())
        tally[column] = ColumnFill(
            nulls=current.nulls + int(df[column].isna().sum()),
            total=current.total + len(df),
        )


def completeness_violations(spec: FeedInvariants, tally: dict[str, ColumnFill]) -> dict[str, int]:
    """Columns whose null fraction exceeds their contract, as null row counts."""
    breached: dict[str, int] = {}
    for column, threshold in spec.completeness:
        fill = tally.get(column)
        if fill is None or fill.total == 0:
            continue
        # The retired contracts were strict — "missing_percent(close) < 1%" —
        # so exactly at the threshold is a breach, not a pass.
        if (fill.nulls / fill.total) >= threshold and fill.nulls > 0:
            breached[f"{column}_completeness"] = fill.nulls
    return breached


def split_by_tolerance(
    spec: FeedInvariants, violations: dict[str, int], rows: int
) -> tuple[dict[str, int], dict[str, int]]:
    """Split violations into failures and warnings.

    An invariant with a tolerance was a *warning* in the retired contract —
    crossed quotes, for instance, were expected during volatility and only
    warned above 1% of rows. Breaching it is worth reporting but is not the
    same as a negative price, so it must not fail the daily report. Below its
    tolerance it is not reported at all.

    Rates are judged over the feed's whole day rather than per partition, so
    a burst concentrated in one hour is measured against the day's volume the
    way the retired percentage checks were.
    """
    tolerated = dict(spec.tolerances)
    failures: dict[str, int] = {}
    warnings: dict[str, int] = {}
    for name, count in violations.items():
        limit = tolerated.get(name)
        if limit is None:
            failures[name] = count
            continue
        if rows > 0 and (count / rows) <= limit:
            continue
        warnings[name] = count
    return failures, warnings


def duplicate_row_count(counts: Counter[str]) -> int:
    """Surplus rows sharing an id — two rows with one id is one violation.

    Takes accumulated counts rather than a frame so a feed's whole day can be
    tallied across every instrument_type partition without holding them all
    in memory at once.
    """
    return sum(count - 1 for count in counts.values() if count > 1)


def check_frame(df: pd.DataFrame, spec: FeedInvariants) -> FrameOutcome:
    """Run one feed's invariants over a frame.

    An invariant whose columns are absent is returned in ``not_run`` rather
    than skipped, so a schema rename surfaces as "this was never checked"
    instead of as a pass.
    """
    if df.empty:
        return FrameOutcome(rows=0)

    violations: list[Violation] = []
    not_run: list[str] = []
    for invariant in spec.invariants:
        if not all(column in df.columns for column in invariant.columns):
            not_run.append(invariant.name)
            continue
        try:
            failing = invariant.predicate(df)
        except Exception:
            # A predicate that blows up has verified nothing; record it as
            # not-run so it cannot be mistaken for a pass, and keep going so
            # one bad column does not take the feed's other checks down.
            logger.warning(
                "silver_invariant_errored",
                feed=spec.feed,
                invariant=invariant.name,
                exc_info=True,
            )
            not_run.append(invariant.name)
            continue
        count = int(failing.sum())
        if count:
            violations.append(Violation(name=invariant.name, rows=count, columns=invariant.columns))
    return FrameOutcome(violations=tuple(violations), not_run=tuple(not_run), rows=len(df))
