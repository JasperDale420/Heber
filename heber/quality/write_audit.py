"""Write-time null-field audit logging.

Inspects DataFrames at write time for unexpected null values and emits
structured warning logs plus Prometheus counters. Does not block writes.
"""

from __future__ import annotations

from typing import Any

import structlog
from prometheus_client import Counter

logger = structlog.get_logger(__name__)

write_null_fields_total = Counter(
    "heber_write_null_fields_total",
    "Rows with null values detected at write time",
    ["layer", "dataset", "column"],
)

# Columns expected to be non-null per (layer, dataset).
# Datasets not listed here still get audited — all columns are checked.
EXPECTED_NON_NULL: dict[tuple[str, str], list[str]] = {
    # Silver datasets
    ("silver", "bars"): [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ts_event",
    ],
    ("silver", "quotes"): [
        "bid_px",
        "ask_px",
        "bid_sz",
        "ask_sz",
        "ts_event",
    ],
    ("silver", "trades"): [
        "price",
        "size",
        "ts_event",
    ],
    ("silver", "flow_alerts"): [
        "underlying",
        "put_call",
        "premium",
        "volume",
        "ts_event",
    ],
    # strike/expiry/dte can be null for non-option aggregation rows
    ("silver", "greek_exposure"): [
        "symbol",
        "ts_event",
        "call_gamma",
        "put_gamma",
        "call_delta",
        "put_delta",
        "call_vanna",
        "put_vanna",
        "call_charm",
        "put_charm",
    ],
    ("silver", "historic_option_volume"): [
        "hov_date",
        "volume",
        "ts_event",
    ],
    # option_chain_snapshot stores one row per underlying snapshot with the
    # full chain serialized in `chain_json`. The Arrow schema also carries
    # per-contract columns (occ_symbol, strike, bid, ask, etc.) for legacy
    # ChainSnapshotRecord compatibility; those are LEGITIMATELY null in the
    # canonical dataset. Restrict the audit to the actually-required fields
    # per REQUIRED_FIELDS_BY_FEED["option_chain_snapshot"] in ingest_contracts.
    ("silver", "option_chain_snapshot"): [
        "snapshot_ts",
        "underlying",
        "chain_json",
    ],
    # oi_change canonical record: oi_date + call/put OI and OI-change. Other
    # UW detail fields (last_ask/last_bid/avg_price/prev_*) are optional and
    # often absent for aggregate rows.
    ("silver", "oi_change"): [
        "oi_date",
        "call_oi",
        "put_oi",
        "call_oi_change",
        "put_oi_change",
    ],
    # darkpool: NBBO context (nbbo_bid_size, nbbo_ask_size, sale_cond_codes,
    # trade_code) is sometimes absent from the upstream feed for off-hours
    # or low-quality prints. Required is symbol/price/size/ts.
    ("silver", "darkpool"): [
        "underlying",
        "price",
        "size",
        "ts_event",
    ],
    # Gold feature datasets
    ("gold", "meta_label_features"): [
        "alert_id",
        "alert_time",
        "symbol",
        "underlying",
        "strike",
        "put_call",
        "premium",
        "volume",
        "delta",
        "gamma",
        "theta",
        "vega",
        "iv",
        "iv_rank",
        "gex",
        "vex",
        "max_pain_strike",
        "max_pain_distance_pct",
        "market_tide_net_premium",
        "market_tide_direction",
    ],
    # Gold label datasets
    ("gold", "labels_alert_barriers"): [
        "alert_id",
        "ts_event",
        "label",
    ],
}


def audit_null_fields(
    data: Any,
    layer: str,
    dataset: str,
    *,
    context: dict[str, str] | None = None,
) -> dict[str, int]:
    """Inspect a DataFrame for null values and log warnings.

    Supports pandas DataFrames and PyArrow Tables.
    Does not raise — this is observe-and-continue.

    Args:
        data: DataFrame or Arrow Table to audit.
        layer: Data layer (bronze, silver, gold).
        dataset: Dataset name.
        context: Additional context for log messages (partition, path, etc).

    Returns:
        Dict mapping column name to null count for columns with nulls.
    """
    if data is None:
        return {}

    columns_to_check = EXPECTED_NON_NULL.get((layer, dataset))
    null_report: dict[str, int] = {}

    try:
        null_report = _compute_null_counts(data, columns_to_check)
    except Exception:
        logger.debug(
            "Write audit skipped: failed to inspect data",
            layer=layer,
            dataset=dataset,
            exc_info=True,
        )
        return {}

    if not null_report:
        return {}

    total_rows = _row_count(data)
    log_context = {"layer": layer, "dataset": dataset, "total_rows": total_rows}
    if context:
        log_context.update(context)

    for col, null_count in null_report.items():
        null_pct = round(null_count / total_rows * 100, 1) if total_rows > 0 else 0
        logger.warning(
            "Null values detected at write time",
            column=col,
            null_count=null_count,
            null_pct=null_pct,
            **log_context,
        )
        write_null_fields_total.labels(
            layer=layer,
            dataset=dataset,
            column=col,
        ).inc(null_count)

    return null_report


def _compute_null_counts(
    data: Any,
    columns: list[str] | None,
) -> dict[str, int]:
    """Compute null counts for columns in the data.

    Works with pandas and PyArrow.
    """
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        return _pandas_null_counts(data, columns)

    try:
        import pyarrow as pa

        if isinstance(data, pa.Table):
            return _arrow_null_counts(data, columns)
    except ImportError:
        pass

    return {}


def _pandas_null_counts(
    df: Any,
    columns: list[str] | None,
) -> dict[str, int]:
    """Compute null counts for a pandas DataFrame."""
    cols = columns if columns else list(df.columns)
    existing = [c for c in cols if c in df.columns]
    report: dict[str, int] = {}
    for col in existing:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            report[col] = null_count
    return report


def _arrow_null_counts(
    table: Any,
    columns: list[str] | None,
) -> dict[str, int]:
    """Compute null counts for a PyArrow Table."""
    all_names = table.column_names
    cols = columns if columns else all_names
    existing = [c for c in cols if c in all_names]
    report: dict[str, int] = {}
    for col in existing:
        column = table.column(col)
        null_count = column.null_count
        if null_count > 0:
            report[col] = null_count
    return report


def _row_count(data: Any) -> int:
    """Get row count from any supported data type."""
    if hasattr(data, "__len__"):
        return len(data)
    if hasattr(data, "num_rows"):
        return int(data.num_rows)
    return 0
