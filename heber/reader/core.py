"""HeberReader — canonical thin filesystem reader for the Heber data lakehouse.

Replaces the old HeberClient (HTTP-based) and all hand-rolled pyarrow readers
in downstream systems.  All predicates are pushed into the pyarrow.dataset scan
so Parquet row-group pruning eliminates unnecessary I/O before data reaches memory.

Usage::

    from heber.reader import HeberReader
    from pathlib import Path

    reader = HeberReader(Path("/Volumes/heber/data"))

    # Point-in-time correct Silver read — ts_available pushed into scan
    bars = reader.read_asof(
        "bars",
        asof_time="2026-01-15T09:30:00Z",
        time_range=("2026-01-01", "2026-01-15"),
        instrument_keys=["equity:AAPL", "equity:TSLA"],
    )

    # Write Gold features (validates anti-leakage invariants)
    reader.write_gold("momentum_features", df, project="kairos", version="v1")

    # Read Gold features
    feats = reader.read_gold("momentum_features", project="kairos")
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import structlog

from heber.config import settings
from heber.utils.versioning import version_sort_key as _version_sort_key

logger = structlog.get_logger(__name__)

_VERSION_PREFIX = "version="


class SchemaContractError(ValueError):
    """A producer contract violation the reader refuses to paper over.

    Distinct from ``pyarrow.lib.ArrowInvalid`` (which is also a ``ValueError``)
    so that a genuine corrupt-file error can still be swallowed into an empty
    read while a contract breach propagates to the caller.
    """


def _coerce_dict_columns_to_string(table: pa.Table) -> pa.Table:
    """Cast string-family columns to plain ``pa.string()``.

    When Silver files are written by different code paths (real-time writer vs
    compactor) their Parquet encoding for string columns may differ — one
    path writes ``string``, another writes ``large_string``, a third writes
    ``dictionary<string, int32>``. ``pyarrow.dataset`` refuses to merge
    these schemas across fragments.

    Applying this coercion after ``to_table()`` ensures downstream consumers
    always see plain string columns regardless of on-disk encoding.
    """
    needs_cast = False
    for field in table.schema:
        if pa.types.is_dictionary(field.type) or pa.types.is_large_string(field.type):
            needs_cast = True
            break
    if not needs_cast:
        return table

    arrays: list[pa.ChunkedArray] = []
    fields: list[pa.Field] = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_dictionary(field.type) and (
            pa.types.is_string(field.type.value_type) or pa.types.is_large_string(field.type.value_type)
        ):
            col = col.cast(pa.string())
            field = field.with_type(pa.string())
        elif pa.types.is_large_string(field.type):
            col = col.cast(pa.string())
            field = field.with_type(pa.string())
        arrays.append(col)
        fields.append(field)
    return pa.table(arrays, schema=pa.schema(fields))


def _collect_parquet_files(root: str | Path) -> list[str]:
    """Walk a Parquet dataset root and return only readable parquet files.

    Skips three classes of files that would otherwise cause pyarrow to
    raise on dataset open:

    1. ``._<name>.parquet`` AppleDouble sidecar files copied by macOS Finder /
       APFS xattr handling. ``stat()`` returns ``EPERM`` even though the
       primary file is fine.
    2. Files inside ``._<name>`` sidecar directories (e.g.
       ``._dt=2026-03-11/...``) — same root cause but at the partition-dir
       level.
    3. ``*.tmp`` files (typically ``part-*.parquet.tmp``) — partial writes
       from interrupted writers. Even when the rest of the partition is
       healthy, pyarrow's auto-discovery includes them and errors with
       ``Parquet file size is X bytes, smaller than the minimum file
       footer (8 bytes)``.
    4. Anything under a ``_quarantine`` directory. Writers divert rows that
       failed validation there (e.g. Gold feature rows whose enrichment
       returned no Greeks) specifically to keep them out of downstream
       reads; including them would defeat the quarantine.

    Returning an explicit file list to ``ds.dataset(...)`` short-circuits
    pyarrow's recursive directory walk and lets hive partitioning still
    be inferred from the path components.
    """
    base = Path(root)
    if not base.exists():
        return []
    if base.is_file():
        # Callers like read_label() pass the data file directly; rglob on a
        # file path yields nothing, so handle it explicitly. Apply the same
        # name-based filters used for the directory walk below (skip AppleDouble
        # sidecars and partial `.tmp` writes) and require a real .parquet file.
        if (
            base.suffix == ".parquet"
            and not base.name.startswith("._")
            and not base.name.endswith(".tmp")
            and not any(part.startswith("._") for part in base.parts)
            and "_quarantine" not in base.parts
        ):
            return [str(base)]
        return []
    files: list[str] = []
    for f in base.rglob("*.parquet"):
        # Filter by name BEFORE any stat() call. On macOS bind mounts inside
        # Docker, `._<name>.parquet` AppleDouble sidecars raise EPERM from
        # stat(), so calling f.is_file() first crashes the walk before we
        # get a chance to skip them by name. Ordering matters here.
        if f.name.startswith("._"):
            continue
        if any(part.startswith("._") for part in f.parts):
            continue
        if "_quarantine" in f.parts:
            continue
        try:
            if not f.is_file():
                continue
        except OSError:
            # Defensive: any other unstattable path (rare) is also skipped.
            continue
        files.append(str(f))
    return files


def _normalize_string_type(dt: pa.DataType) -> pa.DataType:
    """Collapse string-family Arrow types into plain ``pa.string()``.

    Different Parquet writers in this lakehouse emit string columns under
    several Arrow encodings:

    * ``string`` — the historical/default encoding from the real-time writer.
    * ``large_string`` — emitted by the compactor for files that may exceed
      the 2 GiB string-buffer limit (newer parquet builds).
    * ``dictionary<string|large_string, int*>`` — emitted by some writers
      for low-cardinality columns.

    ``pyarrow.dataset`` refuses to merge fragments whose schemas differ on
    these — even though the on-the-wire payload is identical UTF-8 bytes —
    raising ``ArrowTypeError: Unable to merge: ... incompatible types:
    large_string vs string``. Folding everything down to ``pa.string()`` at
    schema-unification time gives the dataset scanner a single target type
    it can cast every fragment into, and is the same lossless coercion we
    already apply post-read in ``_coerce_dict_columns_to_string``.
    """
    if pa.types.is_dictionary(dt) and (pa.types.is_string(dt.value_type) or pa.types.is_large_string(dt.value_type)):
        return pa.string()
    if pa.types.is_large_string(dt):
        return pa.string()
    return dt


_TIMESTAMP_UNIT_ORDER = ("s", "ms", "us", "ns")

# Columns every scan predicate is built from. Their absence is what makes a
# read silently return unfiltered rows, so they are what schema unification is
# for; a dataset whose inferred schema already has them needs no unification.
_PREDICATE_COLUMNS = ("ts_event", "ts_available", "instrument_key")

# Substrings Arrow uses when fragments disagree on a column's type, as opposed
# to a file being unreadable. Only the former is worth a second attempt with
# every fragment's schema merged.
_SCHEMA_CONFLICT_MARKERS = ("unable to merge", "would lose data", "casting from", "unsupported cast")


def _is_schema_conflict(exc: Exception) -> bool:
    """True when an ArrowInvalid describes a cross-fragment type disagreement."""
    message = str(exc).lower()
    return any(marker in message for marker in _SCHEMA_CONFLICT_MARKERS)


def _finer_timestamp(left: pa.DataType, right: pa.DataType, column: str = "") -> pa.DataType:
    """Return whichever timestamp type can hold both without losing precision.

    Different writers emit ``alert_time``/``ts_event`` at different resolutions.
    Casting the finer one down raises ``ArrowInvalid`` the moment a value has
    precision the coarser unit cannot express, and the caller turns that into
    an empty read — so always widen rather than truncate. A timezone on either
    side is kept; the lakehouse contract is that timestamps are UTC-aware.
    """
    if left.tz != right.tz:
        # The lakehouse contract is timezone-aware UTC everywhere, so this is a
        # producer bug. Arrow reads a naive value as a UTC instant; if it was
        # ever local time that silently moves the row across a cutoff. On the
        # columns cutoffs are built from, refuse rather than pick a side.
        if column in _PREDICATE_COLUMNS:
            raise SchemaContractError(
                f"fragments disagree on the timezone of {column} ({left} vs {right}); "
                "refusing to guess a point-in-time boundary"
            )
        logger.warning(
            "heber_reader_timestamp_timezone_mismatch",
            column=column,
            left=str(left),
            right=str(right),
            hint="fragments disagree on timezone; values are being read as UTC instants",
        )
    unit = max(left.unit, right.unit, key=_TIMESTAMP_UNIT_ORDER.index)
    return pa.timestamp(unit, tz=left.tz or right.tz)


def _needs_schema_union(file_list: list[str], required: tuple[tuple[str, ...], ...] = ()) -> bool:
    """Decide whether a dataset is worth unifying, for two footer reads.

    Unification reads one footer per fragment — measured at 3s over 120
    fragments and 197s over 9,415 on the lakehouse volume — so it has to earn
    its cost. It earns it only when a predicate column is missing from the
    schema ``pyarrow`` would infer (the first fragment's) *and* the fragments
    disagree, which is the signature of columns added part-way through a
    dataset's life. Paths sort by partition date, so the oldest and newest
    fragment are the two most likely to differ.

    A dataset that never had one of these columns, or one where a column
    appeared and later disappeared, keeps the plain inference path; the
    exception-triggered unification below still covers hard type conflicts.
    """
    if len(file_list) < 2:
        return False
    ordered = sorted(file_list)
    try:
        first = pq.read_schema(ordered[0])
        last = pq.read_schema(ordered[-1])
    except Exception:
        # Cannot tell — assume it evolved and take the accurate path.
        logger.debug("heber_reader_schema_probe_failed", path=ordered[0], exc_info=True)
        return True
    # A column the caller is actually filtering on is not negotiable: if the
    # inferred schema lacks it the predicate would be dropped and the read would
    # quietly return unfiltered rows, so merge regardless of what the endpoints
    # look like. A middle partition can hold the column while the oldest and
    # newest agree, which no endpoint comparison can see.
    # Each entry is a set of alternatives — a time predicate is satisfied by
    # ts_event *or* ts_label, so only demand the union when none is present.
    if any(not any(c in first.names for c in group) for group in required):
        return True
    if all(c in first.names for c in _PREDICATE_COLUMNS):
        return False
    return not first.equals(last, check_metadata=False)


def _open_dataset_safe(
    path: str,
    partitioning: ds.Partitioning | None = None,
    unify_schema: bool | str = False,
    required_columns: tuple[tuple[str, ...], ...] = (),
) -> ds.Dataset | None:
    """Open a Parquet dataset, recovering from string/dictionary schema conflicts.

    Pre-filters the file list to skip macOS sidecars (``._*``) and
    partial-write ``*.tmp`` files; these are picked up by
    ``ds.dataset()``'s automatic directory walk and break the open with
    ``EPERM`` or ``ArrowInvalid: Parquet file size is X bytes``.

    On the first attempt we let ``pyarrow.dataset`` infer the unified schema.
    If that fails because fragments disagree on string-family encodings —
    ``ArrowInvalid`` for dictionary-vs-string, ``ArrowTypeError`` for
    ``large_string`` vs ``string`` — we manually read the schemas of
    individual files, fold all string-family types down to plain
    ``pa.string()``, and re-open the dataset with the explicit schema.

    ``unify_schema`` is ``False`` (infer), ``"auto"`` (unify only when
    ``_needs_schema_union`` says it would recover something) or ``True``
    (always). Unifying skips the inference attempt entirely. The
    inferred schema is the *first fragment's*, so a dataset whose columns grew
    over time exposes only the columns its oldest partition had — every later
    column is silently dropped, and any predicate on one (``ts_available``,
    ``ts_event``, ``instrument_key``) is silently skipped rather than failing.
    Unifying costs one footer read per fragment, so it is worth paying where
    schemas evolve and correctness of the predicate matters.
    """
    file_list = _collect_parquet_files(path)
    if not file_list:
        return None

    # A hive partitioning factory records what it discovered the first time it
    # is used, and then demands that any explicit schema declare all of it — so
    # it gets used exactly once per call. That is why the decision to unify is
    # made from fragment footers rather than from a trial dataset.
    unify = unify_schema is True or (unify_schema == "auto" and _needs_schema_union(file_list, required_columns))
    if unify:
        logger.info("heber_reader_unifying_evolved_schema", path=path, fragments=len(file_list))
    else:
        try:
            return ds.dataset(file_list, format="parquet", partitioning=partitioning)
        except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError, pa.lib.ArrowTypeError):
            logger.info("heber_reader_schema_conflict_detected", path=path)

    # Manual schema unification from individual fragment metadata.
    try:
        # Open without partitioning first — purely to enumerate fragments
        # and read their physical schemas. Partition columns are not part
        # of fragment.physical_schema, so this read is partition-agnostic
        # and avoids the cross-fragment merge that just failed above.
        raw = ds.dataset(file_list, format="parquet")
        schemas: list[pa.Schema] = []
        for fragment in raw.get_fragments():
            try:
                schemas.append(fragment.physical_schema)
            except Exception:
                logger.debug("fragment_schema_read_failed", path=str(path), exc_info=True)
                continue
        if not schemas:
            return None

        # Build the data-column unified schema. Fold all string-family
        # encodings (dictionary, large_string) down to plain pa.string()
        # so the scanner has a single cast target — pyarrow can cast
        # string<->large_string and decode dictionaries on read, but it
        # refuses to *merge* across fragments with these mismatches.
        # Numeric mismatches (e.g. ``trade_count`` int64 vs double from
        # different writers) widen to the more general type — float64
        # losslessly holds int values up to 2^53 and avoids the
        # ``ArrowInvalid: Float value X was truncated converting to int64``
        # that fires when a float-typed fragment is read into an int-typed
        # schema.
        field_map: dict[str, pa.DataType] = {}
        field_order: list[str] = []
        for schema in schemas:
            for field in schema:
                incoming = _normalize_string_type(field.type)
                if field.name not in field_map:
                    field_order.append(field.name)
                    field_map[field.name] = incoming
                else:
                    existing = field_map[field.name]
                    if existing.equals(incoming):
                        continue
                    if pa.types.is_string(existing) or pa.types.is_string(incoming):
                        field_map[field.name] = pa.string()
                    elif pa.types.is_timestamp(existing) and pa.types.is_timestamp(incoming):
                        field_map[field.name] = _finer_timestamp(existing, incoming, field.name)
                    elif pa.types.is_floating(existing) or pa.types.is_floating(incoming):
                        # int64 vs float64 → widen to float64
                        field_map[field.name] = pa.float64()

        # Append hive partition columns to the unified schema. Without
        # them, ``ds.dataset(file_list, schema=unified, partitioning=hive)``
        # raises ``ArrowInvalid: No match for FieldRef.Name(dt) in ...``
        # because the partition factory expects the schema to declare each
        # discovered partition column. Given an explicit file list the factory
        # reads ``key=value`` from *every* segment of the absolute path, not
        # just those below the common prefix — a Gold read rooted at
        # ``version=v1`` still yields ``dataset``, ``project`` and ``version``.
        # Declaring fewer than it discovers drops those columns from the read
        # and makes any predicate on one raise ``No match for FieldRef.Name``.
        # Each is typed ``pa.string()`` to match pyarrow's default hive
        # behaviour.
        partition_columns: list[str] = []
        seen_partition: set[str] = set()
        for fp in file_list:
            # Directory segments only — hive keys come from directories, and a
            # file happening to be named ``x=y.parquet`` is not a partition.
            for segment in Path(fp).parent.parts:
                eq = segment.find("=")
                if eq <= 0:
                    continue
                name = segment[:eq]
                if name in seen_partition or name in field_map:
                    continue
                seen_partition.add(name)
                partition_columns.append(name)

        unified_fields = [pa.field(n, field_map[n]) for n in field_order]
        unified_fields.extend(pa.field(n, pa.string()) for n in partition_columns)
        unified = pa.schema(unified_fields)

        return ds.dataset(file_list, format="parquet", partitioning=partitioning, schema=unified)
    except SchemaContractError:
        # A contract violation the union deliberately refuses to paper over
        # (e.g. disagreeing timezones on a point-in-time column). Surface it.
        raise
    except Exception:
        logger.warning("heber_reader_manual_schema_unification_failed", path=path, exc_info=True)
        return None


def _to_utc(ts: Any) -> pa.Scalar:
    """Coerce a timestamp-like value to a UTC-aware PyArrow scalar."""
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return pa.scalar(t.to_pydatetime(), type=pa.timestamp("us", tz="UTC"))


def _build_scan_filter(exprs: list[ds.Expression]) -> ds.Expression | None:
    """Combine a list of PyArrow expressions into a single AND filter."""
    result: ds.Expression | None = None
    for expr in exprs:
        result = expr if result is None else result & expr
    return result


def _detect_time_col(schema_names: set[str]) -> str | None:
    """Return the canonical time column present in the schema, if any."""
    for candidate in ("ts_event", "ts_label"):
        if candidate in schema_names:
            return candidate
    return None


_OP_MAP: dict[str, str] = {
    "==": "__eq__",
    "=": "__eq__",
    "!=": "__ne__",
    "<": "__lt__",
    "<=": "__le__",
    ">": "__gt__",
    ">=": "__ge__",
    "in": "isin",
}


def _tuple_filters_to_exprs(
    filters: list[tuple[str, str, Any]],
    schema_names: set[str],
) -> list[ds.Expression]:
    """Convert old-style (col, op, value) tuple filters to PyArrow expressions."""
    exprs: list[ds.Expression] = []
    for col, op, val in filters:
        if col not in schema_names:
            continue
        field = ds.field(col)
        method = _OP_MAP.get(op)
        if method == "isin":
            exprs.append(field.isin(val))
        elif method:
            exprs.append(getattr(field, method)(pa.scalar(val)))
    return exprs


def _discover_version_dirs(base_path: Path) -> list[Path]:
    """Return version= directories under *base_path*, sorted newest-first by semver."""
    dirs = [d for d in base_path.glob("**/version=*") if d.is_dir()]
    return sorted(
        dirs,
        key=lambda d: _version_sort_key(d.name.removeprefix(_VERSION_PREFIX)),
        reverse=True,
    )


def _scan_batched(
    dataset_obj: ds.Dataset,
    scan_filter: ds.Expression | None,
    projection: list[str] | None,
    batch_size: int,
) -> pa.Table:
    """Read a dataset in batches via a Scanner to cap peak memory.

    Instead of materializing the full filtered result with ``to_table()``,
    this creates a ``Scanner`` with ``batch_size`` and iterates over record
    batches.  The batches are collected and concatenated into a single
    ``pa.Table`` at the end.

    This is functionally equivalent to ``dataset.to_table()`` but the Scanner
    streams data from disk in fixed-size batches, keeping the working set
    bounded even for very large scans (e.g. 100 days of equity bars).
    """
    scanner = dataset_obj.scanner(
        filter=scan_filter,
        columns=projection,
        batch_size=batch_size,
    )
    batches: list[pa.RecordBatch] = []
    total_rows = 0
    for batch in scanner.to_batches():
        batches.append(batch)
        total_rows += batch.num_rows
        if total_rows % (batch_size * 10) == 0:
            logger.debug("heber_reader_batched_progress", rows_so_far=total_rows)

    if not batches:
        schema = scanner.projected_schema
        empty_arrays = {name: pa.array([], type=schema.field(name).type) for name in schema.names}
        return pa.table(empty_arrays)

    return pa.Table.from_batches(batches, schema=batches[0].schema)


class HeberReader:
    """Thin filesystem reader for the Heber Bronze/Silver/Gold lakehouse.

    All I/O is direct pyarrow.dataset reads against the mounted Heber volume —
    no HTTP, no lakeFS, no Catalog API required.  Time predicates and
    ``ts_available`` (point-in-time) filters are pushed into the dataset scan
    before ``to_table()`` so row-group pruning removes data that would never
    survive the filter anyway.

    Parameters
    ----------
    data_root:
        Root directory for Heber data.  Defaults to ``settings.data_root``
        (``/Volumes/heber/data``).  Silver lives at ``{data_root}/silver/``,
        Gold at ``{data_root}/gold/``.
    """

    def __init__(self, data_root: Path | None = None) -> None:
        self._root = Path(data_root or settings.data_root)
        self._gold_root = Path(data_root) / "gold" if data_root is not None else Path(settings.gold_path)
        logger.info("heber_reader_init", data_root=str(self._root), gold_root=str(self._gold_root))

    # ------------------------------------------------------------------
    # Context-manager support (no resources to close, but keeps API clean)
    # ------------------------------------------------------------------

    def close(self) -> None:
        """No-op — nothing to release."""

    def __enter__(self) -> HeberReader:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Silver reads
    # ------------------------------------------------------------------

    def read_silver(
        self,
        dataset: str,
        time_range: tuple[str | datetime, str | datetime] | None = None,
        instrument_keys: list[str] | None = None,
        instrument_type: str | None = None,
        asof_time: str | datetime | None = None,
        columns: list[str] | None = None,
        batch_size: int | None = None,
        prune_by_dt: bool = False,
    ) -> pd.DataFrame:
        """Read Silver layer data with optional point-in-time correctness.

        Parameters
        ----------
        dataset:
            Silver feed name (e.g. ``"bars"``, ``"flow_alerts"``).
        time_range:
            ``(start_iso, end_iso)`` pushed as a ``ts_event`` scan filter.
        instrument_keys:
            Pushed as an ``instrument_key`` scan filter.
        instrument_type:
            If provided, narrows the scan path to the
            ``instrument_type={value}`` hive partition directory.
        asof_time:
            When set, adds ``ts_available <= asof_time`` to the scan filter
            before ``to_table()`` — no post-filter on a full DataFrame.
        columns:
            Column projection.  ``ts_event``, ``ts_available``, and
            ``instrument_key`` are always included when they exist in the
            schema.
        batch_size:
            When set, reads data in batches of this many rows using PyArrow's
            ``Scanner`` to cap peak memory.  Batches are concatenated into a
            single table at the end.  Default ``None`` reads all matching rows
            at once (original behaviour).
        prune_by_dt:
            When ``True`` and ``time_range`` is set, also push a predicate on the
            ``dt`` hive partition (``dt`` between the start/end *dates*) so PyArrow
            skips non-matching partition directories instead of opening every
            file to evaluate the ``ts_event`` filter.  Safe because Silver writes
            ``dt = ts_event.strftime("%Y-%m-%d")`` (see ``heber/writer/utils.py``),
            so the partition date always equals the event date; the ``ts_event``
            predicate still trims precisely within boundary partitions.  Off by
            default to preserve existing behaviour for callers that don't opt in.

        Returns
        -------
        pd.DataFrame
            Empty DataFrame when no data matches or the path does not exist.
        """
        base_path = self._root / "silver" / f"feed={dataset}"
        if instrument_type:
            base_path = base_path / f"instrument_type={instrument_type}"

        if not base_path.exists():
            logger.warning("heber_reader_path_missing", path=str(base_path))
            return pd.DataFrame()

        try:
            dataset_obj = _open_dataset_safe(
                str(base_path),
                partitioning=ds.partitioning(flavor="hive"),
            )
        except OSError:
            logger.warning("heber_reader_open_failed", path=str(base_path), exc_info=True)
            return pd.DataFrame()

        if dataset_obj is None:
            logger.warning("heber_reader_open_failed", path=str(base_path))
            return pd.DataFrame()

        schema_names = set(dataset_obj.schema.names)

        # Column projection — always keep essential columns
        projection: list[str] | None = None
        if columns:
            essential = {"ts_event", "ts_available", "instrument_key"}
            projection = sorted((set(columns) | (essential & schema_names)) & schema_names)

        # Build scan filter — predicates pushed before to_table()
        exprs: list[ds.Expression] = []

        if time_range and "ts_event" in schema_names:
            exprs.append(ds.field("ts_event") >= _to_utc(time_range[0]))
            exprs.append(ds.field("ts_event") <= _to_utc(time_range[1]))

        if prune_by_dt and time_range and "dt" in schema_names:
            # Hive dt partition is an ISO date string (dt=YYYY-MM-DD); ISO dates
            # sort lexically, so string comparison prunes partition directories.
            exprs.append(ds.field("dt") >= _to_utc(time_range[0]).as_py().date().isoformat())
            exprs.append(ds.field("dt") <= _to_utc(time_range[1]).as_py().date().isoformat())

        if asof_time and "ts_available" in schema_names:
            exprs.append(ds.field("ts_available") <= _to_utc(asof_time))

        if instrument_keys and "instrument_key" in schema_names:
            exprs.append(ds.field("instrument_key").isin(instrument_keys))

        scan_filter = _build_scan_filter(exprs)

        try:
            if batch_size is not None:
                table = _scan_batched(dataset_obj, scan_filter, projection, batch_size)
            else:
                table = dataset_obj.to_table(filter=scan_filter, columns=projection)
            table = _coerce_dict_columns_to_string(table)
            df = table.to_pandas()
        except (pa.lib.ArrowInvalid, OSError):
            logger.warning("heber_reader_read_failed", path=str(base_path), exc_info=True)
            return pd.DataFrame()

        logger.info(
            "heber_reader_silver_read",
            dataset=dataset,
            rows=len(df),
            instrument_type=instrument_type or "all",
            asof=str(asof_time) if asof_time else None,
        )
        return df

    def read_asof(
        self,
        dataset: str,
        asof_time: str | datetime,
        instrument_keys: list[str] | None = None,
        instrument_type: str | None = None,
        time_range: tuple[str | datetime, str | datetime] | None = None,
        columns: list[str] | None = None,
        batch_size: int | None = None,
    ) -> pd.DataFrame:
        """Read Silver data with point-in-time correctness.

        Equivalent to ``read_silver(..., asof_time=asof_time, ...)``.  The
        ``ts_available <= asof_time`` predicate is pushed into the pyarrow
        dataset scan — not applied as a post-filter after reading all rows.

        Parameters
        ----------
        dataset:
            Silver feed name.
        asof_time:
            Point-in-time cutoff.  Only rows where ``ts_available <= asof_time``
            are returned, enforcing the zero-leakage firewall.
        instrument_keys:
            Optional instrument_key filter.
        instrument_type:
            Optional hive partition filter.
        time_range:
            ``(start_iso, end_iso)`` applied to ``ts_event``.
        columns:
            Optional column projection.
        batch_size:
            When set, reads data in batches to cap peak memory.
        """
        return self.read_silver(
            dataset=dataset,
            time_range=time_range,
            instrument_keys=instrument_keys,
            instrument_type=instrument_type,
            asof_time=asof_time,
            columns=columns,
            batch_size=batch_size,
        )

    # ------------------------------------------------------------------
    # Point-in-time join (preserved from old SDK)
    # ------------------------------------------------------------------

    def asof_join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on_keys: list[str] | None = None,
        left_time: str = "ts_event",
        right_time: str = "ts_event",
        right_available: str = "ts_available",
        tolerance: str | None = None,
        suffix: str = "_right",
    ) -> pd.DataFrame:
        """Point-in-time correct as-of join.

        Joins ``left`` to the most recent prior row from ``right`` where
        ``ts_event_right <= left_time`` **and** ``ts_available_right <= left_time``.

        Parameters
        ----------
        left:
            Driving table.
        right:
            Lookup table.
        on_keys:
            Join key columns (default ``["instrument_key"]``).
        left_time:
            Time column in ``left`` used for the merge.
        right_time:
            Time column in ``right``.
        right_available:
            Availability column in ``right`` (enforces anti-leakage).
        tolerance:
            Max time gap (e.g. ``"1h"``, ``"30m"``).  ``None`` means unbounded.
        suffix:
            Column suffix applied to ``right`` columns on collision.
        """
        if on_keys is None:
            on_keys = ["instrument_key"]

        missing_left = {left_time} - set(left.columns)
        if missing_left:
            raise ValueError(f"asof_join: left DataFrame missing columns: {missing_left}")
        missing_right = {right_time, right_available} - set(right.columns)
        if missing_right:
            raise ValueError(f"asof_join: right DataFrame missing columns: {missing_right}")

        right = right.copy()
        right["_safe_time"] = right[[right_time, right_available]].max(axis=1)

        left = left.sort_values(left_time)
        right = right.sort_values("_safe_time")

        result = pd.merge_asof(
            left,
            right,
            left_on=left_time,
            right_on="_safe_time",
            by=on_keys,
            tolerance=pd.Timedelta(tolerance) if tolerance else None,
            direction="backward",
            suffixes=("", suffix),
        )

        if "_safe_time" in result.columns:
            result = result.drop(columns=["_safe_time"])

        logger.debug(
            "heber_reader_asof_join",
            left_rows=len(left),
            right_rows=len(right),
            result_rows=len(result),
        )
        return result

    # ------------------------------------------------------------------
    # Gold reads
    # ------------------------------------------------------------------

    def read_gold(
        self,
        dataset: str,
        project: str | None = None,
        version: str | None = None,
        time_range: tuple[str | datetime, str | datetime] | None = None,
        instrument_keys: list[str] | None = None,
        asof_time: str | datetime | None = None,
    ) -> pd.DataFrame:
        """Read Gold layer features/labels.

        Path layout: ``gold/dataset={dataset}/project={project}/version={version}/dt={date}/``.
        When ``version`` is ``None`` the lexicographically latest ``version=*`` directory
        is used.

        Parameters
        ----------
        dataset:
            Gold dataset name (e.g. ``"momentum_features"``).
        project:
            Project namespace filter (e.g. ``"kairos"``).  When ``None`` all
            projects under the dataset are scanned.
        version:
            Specific version to read.  ``None`` picks the latest.
        time_range:
            ``(start_iso, end_iso)`` applied to ``ts_event`` (or ``ts_label``
            when ``ts_event`` is absent).
        instrument_keys:
            Pushed as an ``instrument_key`` scan filter.
        asof_time:
            When set, adds ``ts_available <= asof_time`` to the scan.
        """
        gold_path = self._gold_root / f"dataset={dataset}"
        if project:
            gold_path = gold_path / f"project={project}"

        if not gold_path.exists():
            logger.warning("heber_reader_gold_not_found", path=str(gold_path))
            return pd.DataFrame()

        scan_result = self._resolve_gold_scan_path(gold_path, version)
        if scan_result is None:
            return pd.DataFrame()
        scan_path, resolved_version = scan_result

        # Columns this call's predicates need. A missing one is not a cosmetic
        # loss — the predicate would be dropped and the caller would get rows it
        # filtered out — so it forces the schema merge.
        required: list[tuple[str, ...]] = []
        if time_range:
            required.append(("ts_event", "ts_label"))
        if asof_time:
            required.append(("ts_available",))
        if instrument_keys:
            required.append(("instrument_key",))

        try:
            dataset_obj = _open_dataset_safe(
                str(scan_path),
                partitioning=ds.partitioning(flavor="hive"),
                # Gold schemas evolve as feature pipelines add columns; "auto"
                # pays the per-fragment footer read only where that happened.
                unify_schema="auto",
                required_columns=tuple(required),
            )
        except OSError:
            logger.warning("heber_reader_gold_open_failed", path=str(scan_path), exc_info=True)
            return pd.DataFrame()

        if dataset_obj is None:
            logger.warning("heber_reader_gold_open_failed", path=str(scan_path))
            return pd.DataFrame()

        schema_names = set(dataset_obj.schema.names)

        time_col = _detect_time_col(schema_names)

        exprs: list[ds.Expression] = []

        if time_range and time_col:
            exprs.append(ds.field(time_col) >= _to_utc(time_range[0]))
            exprs.append(ds.field(time_col) <= _to_utc(time_range[1]))

        if asof_time:
            # Fail closed. Dropping the predicate because the column is absent
            # hands the caller rows from after its cutoff while it believes the
            # point-in-time filter applied — the exact leakage this guards.
            if "ts_available" not in schema_names:
                raise ValueError(
                    f"asof_time requested but dataset={dataset} has no ts_available column; "
                    "refusing to return unfiltered rows"
                )
            exprs.append(ds.field("ts_available") <= _to_utc(asof_time))

        if instrument_keys and "instrument_key" in schema_names:
            exprs.append(ds.field("instrument_key").isin(instrument_keys))

        if resolved_version and "version" in schema_names:
            exprs.append(ds.field("version") == pa.scalar(resolved_version))

        scan_filter = _build_scan_filter(exprs)

        try:
            table = dataset_obj.to_table(filter=scan_filter)
            table = _coerce_dict_columns_to_string(table)
            df = table.to_pandas()
        except OSError:
            # Storage is unavailable — not a schema problem. Chasing it with a
            # full schema traversal would cost minutes and change nothing.
            logger.warning("heber_reader_gold_read_failed", path=str(scan_path), exc_info=True)
            return pd.DataFrame()
        except pa.lib.ArrowInvalid as exc:
            if not _is_schema_conflict(exc):
                # Malformed Parquet, not a schema disagreement — merging every
                # fragment's schema cannot fix it and costs minutes at scale.
                logger.warning("heber_reader_gold_read_failed", path=str(scan_path), exc_info=True)
                return pd.DataFrame()
            # A type conflict between fragments (a timestamp written at a finer
            # resolution than the schema, say) only surfaces when the scan runs,
            # not when the dataset opens — so the cheap gate above cannot see it.
            # Merging every fragment's schema resolves it; returning an empty
            # frame here would report a whole dataset as no data.
            logger.info("heber_reader_gold_scan_conflict_retrying_unified", path=str(scan_path), exc_info=True)
            unified_obj = _open_dataset_safe(
                str(scan_path),
                partitioning=ds.partitioning(flavor="hive"),
                unify_schema=True,
            )
            if unified_obj is None:
                logger.warning("heber_reader_gold_read_failed", path=str(scan_path))
                return pd.DataFrame()
            try:
                table = unified_obj.to_table(filter=scan_filter)
                table = _coerce_dict_columns_to_string(table)
                df = table.to_pandas()
            except (pa.lib.ArrowInvalid, OSError):
                logger.warning("heber_reader_gold_read_failed", path=str(scan_path), exc_info=True)
                return pd.DataFrame()

        logger.info(
            "heber_reader_gold_read",
            dataset=dataset,
            project=project or "all",
            version=version or "latest",
            rows=len(df),
        )
        return df

    def _resolve_gold_scan_path(
        self,
        gold_path: Path,
        version: str | None,
    ) -> tuple[Path, str] | None:
        """Determine the filesystem scan path for a Gold read.

        Returns ``(scan_path, resolved_version)`` or ``None`` when no
        version directories exist.
        """
        if version:
            return gold_path / f"{_VERSION_PREFIX}{version}", version
        version_dirs = _discover_version_dirs(gold_path)
        if not version_dirs:
            logger.warning("heber_reader_gold_no_versions", path=str(gold_path))
            return None
        latest_dir = version_dirs[0]
        resolved = latest_dir.name.removeprefix(_VERSION_PREFIX)
        # Scan from parent so hive partitioning exposes the version= column.
        return latest_dir.parent, resolved

    # ------------------------------------------------------------------
    # Gold writes
    # ------------------------------------------------------------------

    def write_gold(
        self,
        dataset: str,
        df: pd.DataFrame,
        project: str,
        version: str,
        metadata: dict[str, Any] | None = None,
    ) -> Path | None:
        """Write a DataFrame to the Gold layer.

        Enforces the zero-leakage invariant (``ts_available >= ts_event``)
        and partitions output by date.

        Path: ``gold/dataset={dataset}/project={project}/version={version}/dt={date}/part-{ts}.parquet``

        Parameters
        ----------
        dataset:
            Gold dataset name.
        df:
            DataFrame to persist.  Must contain ``instrument_key``, ``ts_event``,
            and ``ts_available``.
        project:
            Project namespace (e.g. ``"kairos"``).
        version:
            Version tag (e.g. ``"v1"``).
        metadata:
            Optional key-value metadata stored in the parquet schema metadata.

        Returns
        -------
        Path
            Path to the first written parquet file (for compatibility with callers
            that expect a single path back).
        """
        if df.empty:
            logger.warning("heber_reader_gold_write_empty", dataset=dataset)
            return None

        required_cols = {"instrument_key", "ts_event", "ts_available"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        ts_avail = pd.to_datetime(df["ts_available"], utc=True)
        ts_evt = pd.to_datetime(df["ts_event"], utc=True)
        if (ts_avail < ts_evt).any():
            raise ValueError("ts_available cannot be before ts_event (look-ahead detected)")

        df = df.copy()
        df["dt"] = pd.to_datetime(df["ts_event"], utc=True).dt.date

        output_paths: list[Path] = []

        for dt, group in df.groupby("dt"):
            partition_dir = (
                self._gold_root
                / f"dataset={dataset}"
                / f"project={project}"
                / f"{_VERSION_PREFIX}{version}"
                / f"dt={dt}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            ts_str = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            out_path = partition_dir / f"part-{ts_str}-{uuid.uuid4().hex[:8]}.parquet"

            write_df = group.drop(columns=["dt"]).reset_index(drop=True)
            table = pa.Table.from_pandas(write_df, preserve_index=False)

            if metadata:
                existing = table.schema.metadata or {}
                encoded = {k.encode(): json.dumps(v).encode() for k, v in metadata.items()}
                table = table.replace_schema_metadata({**existing, **encoded})

            pq.write_table(table, str(out_path), compression="snappy", use_dictionary=False)
            output_paths.append(out_path)

        logger.info(
            "heber_reader_gold_written",
            dataset=dataset,
            project=project,
            version=version,
            rows=len(df),
            files=len(output_paths),
        )
        return output_paths[0]

    # ------------------------------------------------------------------
    # Arbitrary-path read (for callers with configured paths)
    # ------------------------------------------------------------------

    def read_parquet_dataset(
        self,
        path: Path,
        columns: list[str] | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        time_range: tuple[str | datetime, str | datetime] | None = None,
        asof_time: str | datetime | None = None,
    ) -> pd.DataFrame:
        """Read a parquet dataset at an arbitrary path with optional filtering.

        Accepts the same ``filters`` tuple format as PyArrow:
        ``[("column", "op", value), ...]``.  Use this when the caller already
        knows the full partition path (e.g. ``DatasetConfig.features_path``).
        """
        if not path.exists():
            return pd.DataFrame()

        try:
            dataset_obj = _open_dataset_safe(
                str(path),
                partitioning=ds.partitioning(flavor="hive"),
            )
        except OSError:
            logger.warning("heber_reader_arbitrary_open_failed", path=str(path), exc_info=True)
            return pd.DataFrame()

        if dataset_obj is None:
            logger.warning("heber_reader_arbitrary_open_failed", path=str(path))
            return pd.DataFrame()

        schema_names = set(dataset_obj.schema.names)

        exprs: list[ds.Expression] = []

        if filters:
            exprs.extend(_tuple_filters_to_exprs(filters, schema_names))

        if time_range:
            time_col = _detect_time_col(schema_names)
            if time_col:
                exprs.append(ds.field(time_col) >= _to_utc(time_range[0]))
                exprs.append(ds.field(time_col) <= _to_utc(time_range[1]))

        if asof_time and "ts_available" in schema_names:
            exprs.append(ds.field("ts_available") <= _to_utc(asof_time))

        scan_filter = _build_scan_filter(exprs)

        try:
            table = dataset_obj.to_table(filter=scan_filter, columns=columns)
            table = _coerce_dict_columns_to_string(table)
            return table.to_pandas()
        except (pa.lib.ArrowInvalid, OSError):
            logger.warning("heber_reader_arbitrary_read_failed", path=str(path), exc_info=True)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Gold version discovery
    # ------------------------------------------------------------------

    def list_gold_versions(self, dataset: str, project: str | None = None) -> list[str]:
        """List available versions for a Gold dataset (filesystem, no lakeFS).

        Parameters
        ----------
        dataset:
            Gold dataset name.
        project:
            Optional project namespace filter.

        Returns
        -------
        list[str]
            Version strings sorted newest-first (e.g. ``["v3", "v2", "v1"]``).
        """
        base = self._gold_root / f"dataset={dataset}"
        if project:
            base = base / f"project={project}"

        if not base.exists():
            return []

        return [d.name.removeprefix(_VERSION_PREFIX) for d in _discover_version_dirs(base)]
