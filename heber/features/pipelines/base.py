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
