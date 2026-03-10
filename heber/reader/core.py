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

logger = structlog.get_logger(__name__)


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
        logger.info("heber_reader_init", data_root=str(self._root))

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
            dataset_obj = ds.dataset(
                str(base_path),
                format="parquet",
                partitioning=ds.partitioning(flavor="hive"),
            )
        except Exception:
            logger.warning("heber_reader_open_failed", path=str(base_path), exc_info=True)
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

        if asof_time and "ts_available" in schema_names:
            exprs.append(ds.field("ts_available") <= _to_utc(asof_time))

        if instrument_keys and "instrument_key" in schema_names:
            exprs.append(ds.field("instrument_key").isin(instrument_keys))

        scan_filter = _build_scan_filter(exprs)

        try:
            table = dataset_obj.to_table(filter=scan_filter, columns=projection)
            df = table.to_pandas()
        except Exception:
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
        """
        return self.read_silver(
            dataset=dataset,
            time_range=time_range,
            instrument_keys=instrument_keys,
            instrument_type=instrument_type,
            asof_time=asof_time,
            columns=columns,
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

        # Drop the helper column (may appear with suffix if name collides)
        safe_col = "_safe_time" + suffix if "_safe_time" + suffix in result.columns else "_safe_time"
        if safe_col in result.columns:
            result = result.drop(columns=[safe_col])

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
        gold_path = self._root / "gold" / f"dataset={dataset}"
        if project:
            gold_path = gold_path / f"project={project}"

        if not gold_path.exists():
            logger.warning("heber_reader_gold_not_found", path=str(gold_path))
            return pd.DataFrame()

        scan_result = self._resolve_gold_scan_path(gold_path, version)
        if scan_result is None:
            return pd.DataFrame()
        scan_path, resolved_version = scan_result

        try:
            dataset_obj = ds.dataset(
                str(scan_path),
                format="parquet",
                partitioning=ds.partitioning(flavor="hive"),
            )
        except Exception:
            logger.warning("heber_reader_gold_open_failed", path=str(scan_path), exc_info=True)
            return pd.DataFrame()

        schema_names = set(dataset_obj.schema.names)

        time_col = _detect_time_col(schema_names)

        exprs: list[ds.Expression] = []

        if time_range and time_col:
            exprs.append(ds.field(time_col) >= _to_utc(time_range[0]))
            exprs.append(ds.field(time_col) <= _to_utc(time_range[1]))

        if asof_time and "ts_available" in schema_names:
            exprs.append(ds.field("ts_available") <= _to_utc(asof_time))

        if instrument_keys and "instrument_key" in schema_names:
            exprs.append(ds.field("instrument_key").isin(instrument_keys))

        if resolved_version and "version" in schema_names:
            exprs.append(ds.field("version") == pa.scalar(resolved_version))

        scan_filter = _build_scan_filter(exprs)

        try:
            table = dataset_obj.to_table(filter=scan_filter)
            df = table.to_pandas()
        except Exception:
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
            return gold_path / f"version={version}", version
        version_dirs = sorted(
            [d for d in gold_path.glob("**/version=*") if d.is_dir()],
        )
        if not version_dirs:
            logger.warning("heber_reader_gold_no_versions", path=str(gold_path))
            return None
        latest_dir = version_dirs[-1]
        resolved = latest_dir.name.replace("version=", "")
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

        if (df["ts_available"] < df["ts_event"]).any():
            raise ValueError("ts_available cannot be before ts_event (look-ahead detected)")

        df = df.copy()
        df["dt"] = pd.to_datetime(df["ts_event"]).dt.date

        output_paths: list[Path] = []

        for dt, group in df.groupby("dt"):
            partition_dir = (
                self._root / "gold" / f"dataset={dataset}" / f"project={project}" / f"version={version}" / f"dt={dt}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            ts_str = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            out_path = partition_dir / f"part-{ts_str}-{uuid.uuid4().hex[:8]}.parquet"

            write_df = group.drop(columns=["dt"])
            table = pa.Table.from_pandas(write_df)

            if metadata:
                existing = table.schema.metadata or {}
                encoded = {k.encode(): json.dumps(v).encode() for k, v in metadata.items()}
                table = table.replace_schema_metadata({**existing, **encoded})

            pq.write_table(table, str(out_path), compression="snappy")
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
            dataset_obj = ds.dataset(
                str(path),
                format="parquet",
                partitioning=ds.partitioning(flavor="hive"),
            )
        except Exception:
            logger.warning("heber_reader_arbitrary_open_failed", path=str(path), exc_info=True)
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
            return table.to_pandas()
        except Exception:
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
        base = self._root / "gold" / f"dataset={dataset}"
        if project:
            base = base / f"project={project}"

        if not base.exists():
            return []

        version_tags = sorted(
            {d.name.replace("version=", "") for d in base.glob("**/version=*") if d.is_dir()},
            reverse=True,
        )
        return version_tags
