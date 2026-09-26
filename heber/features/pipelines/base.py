"""Shared helpers for Gold feature pipelines.

Every feature pipeline reads Silver, computes per-ticker (or market-level)
features, and writes Gold.  The boilerplate around that — stamping
``ts_available`` for zero-leakage writes and deriving ``instrument_key`` from a
symbol or a fixed market-level key — was previously copy-pasted byte-for-byte
across ~12 modules.  This module is the single home for those helpers.

Pipelines whose ``ts_available`` is *not* simply "now" (e.g. forward-looking
label pipelines that gate availability on a forward window) define their own
``_ensure_ts_available`` and do not use :func:`ensure_ts_available` here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd


def ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``ts_available`` column for zero-leakage Gold writes.

    Stamps the current time when the column is absent.  Used by daily
    feature pipelines where a row becomes queryable as soon as it is
    written (no forward window to wait on).
    """
    if "ts_available" not in df.columns:
        df["ts_available"] = datetime.now(UTC)
    return df


def ensure_instrument_key(df: pd.DataFrame, symbol_col: str = "symbol") -> pd.DataFrame:
    """Add ``instrument_key`` from a symbol column if missing.

    Bare symbols are prefixed with ``equity:``; values that already carry a
    ``type:`` prefix are passed through unchanged.
    """
    if "instrument_key" not in df.columns and symbol_col in df.columns:
        df["instrument_key"] = df[symbol_col].apply(lambda s: s if ":" in str(s) else f"equity:{s}")
    return df


def ensure_market_instrument_key(df: pd.DataFrame, instrument_key: str) -> pd.DataFrame:
    """Set ``instrument_key`` to a fixed market-level key.

    Market-level pipelines (GEX regime, market tide, market regime) emit one
    row per day for the whole market, keyed by a constant such as
    ``market:gex_regime``.
    """
    df["instrument_key"] = instrument_key
    return df


def to_daily_close(bars: pd.DataFrame) -> pd.DataFrame:
    """Reduce bars to one daily close per instrument_key.

    Prefers pre-aggregated ``1Day`` bars where available, falling back to the
    last intraday close of the day otherwise.
    """
    if bars.empty:
        return bars

    df = bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["date"] = df["ts_event"].dt.date

    # Prefer pre-aggregated daily bars
    if "timeframe" in df.columns:
        daily_mask = df["timeframe"] == "1Day"
        if daily_mask.any():
            daily = df[daily_mask].copy()
            daily_keys = set(zip(daily["instrument_key"], daily["date"], strict=False))
            intraday = df[~daily_mask].copy()
            if not intraday.empty:
                intraday["_key"] = list(zip(intraday["instrument_key"], intraday["date"], strict=False))
                intraday = intraday[~intraday["_key"].isin(daily_keys)].drop(columns=["_key"])
            df = pd.concat([daily, intraday], ignore_index=True)

    # Take last close per (instrument_key, date)
    df = df.sort_values(["instrument_key", "ts_event"])
    daily = df.groupby(["instrument_key", "date"]).agg(close=("close", "last")).reset_index()
    daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)

    return daily[["instrument_key", "ts_event", "close"]]


def merge_features(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Outer-merge all feature DataFrames on ts_event."""
    merged: pd.DataFrame | None = None

    for _name, df in frames.items():
        if df.empty:
            continue

        df = df.copy()
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

        if merged is None:
            merged = df
        else:
            merged = merged.merge(df, on="ts_event", how="outer")

    return merged if merged is not None else pd.DataFrame()


def gold_dataset_result(
    *,
    status: str,
    rows: int,
    path: str | None = None,
    error: str | None = None,
    components: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the poller-facing result for one Gold dataset."""
    result: dict[str, Any] = {
        "status": status,
        "rows": int(rows),
        "path": path,
    }
    if error is not None:
        result["error"] = error
    if components is not None:
        result["components"] = components
    return result
