"""Equity feature pipelines — compute Gold datasets from Silver bars/quotes/flow_alerts.

Produces:
  - flow_features        (daily flow aggregates per symbol)
  - momentum_features    (RSI, MACD, momentum per symbol)
  - volatility_features  (historical vol, ATR, Bollinger per symbol)
  - microstructure_features (bid/ask spread, depth from quotes)
  - labels_returns_1d    (1-day forward returns per symbol)
  - labels_returns_5d    (5-day forward returns per symbol)

Usage:
    uv run python -m heber.features.pipelines.equity_features --start 2026-01-01 --end 2026-03-10
    uv run python -m heber.features.pipelines.equity_features --start 2026-01-01 --end 2026-03-10 --dry-run
    uv run python -m heber.features.pipelines.equity_features \
        --start 2026-01-01 --end 2026-03-10 --datasets flow momentum
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog

from heber.reader import HeberReader

logger = structlog.get_logger(__name__)

LOOKBACK_DAYS = 90  # Extra history for computing rolling indicators
FORWARD_DAYS = 10  # Extra future data for computing forward return labels


def _resample_bars_to_daily(bars: pd.DataFrame) -> pd.DataFrame:
    """Resample intraday bars to daily OHLCV.

    Uses pre-aggregated '1Day' bars where available.  For dates that only have
    intraday bars, resamples them to daily.  The two sets are concatenated so the
    output always contains one row per (instrument_key, date).
    """
    if bars.empty:
        return bars

    bars = bars.copy()
    bars["ts_event"] = pd.to_datetime(bars["ts_event"], utc=True)
    bars["date"] = bars["ts_event"].dt.date

    for col in ("open", "high", "low", "close", "volume"):
        if col in bars.columns:
            bars[col] = pd.to_numeric(bars[col], errors="coerce")

    parts: list[pd.DataFrame] = []

    # Use pre-aggregated daily bars where available
    if "timeframe" in bars.columns:
        daily_mask = bars["timeframe"] == "1Day"
        if daily_mask.any():
            daily_pre = bars[daily_mask].copy()
            logger.info("Found pre-aggregated daily bars", rows=len(daily_pre))
            parts.append(daily_pre)
            # Get dates covered by daily bars, per instrument
            daily_keys = set(zip(daily_pre["instrument_key"], daily_pre["date"], strict=False))
            # Filter out those (instrument, date) combos from the intraday bars
            intraday = bars[~daily_mask].copy()
            if not intraday.empty:
                intraday["_key"] = list(zip(intraday["instrument_key"], intraday["date"], strict=False))
                intraday = intraday[~intraday["_key"].isin(daily_keys)].drop(columns=["_key"])
        else:
            intraday = bars.copy()
    else:
        intraday = bars.copy()

    # Resample remaining intraday bars → daily
    if not intraday.empty:
        logger.info("Resampling intraday bars to daily", intraday_rows=len(intraday))

        agg_spec: dict[str, tuple[str, str]] = {
            "open": ("open", "first"),
            "high": ("high", "max"),
            "low": ("low", "min"),
            "close": ("close", "last"),
        }
        if "volume" in intraday.columns:
            agg_spec["volume"] = ("volume", "sum")
        if "vwap" in intraday.columns:
            agg_spec["vwap"] = ("vwap", "last")
        if "trade_count" in intraday.columns:
            agg_spec["trade_count"] = ("trade_count", "sum")

        resampled = (
            intraday.sort_values(["instrument_key", "ts_event"])
            .groupby(["instrument_key", "date"])
            .agg(**agg_spec)
            .reset_index()
        )

        # Carry symbol from instrument_key
        if "symbol" not in resampled.columns:
            resampled["symbol"] = resampled["instrument_key"].str.replace(r"^equity:", "", regex=True)

        logger.info("Resampled intraday to daily", resampled_rows=len(resampled))
        parts.append(resampled)

    if not parts:
        return pd.DataFrame()

    daily = pd.concat(parts, ignore_index=True)

    # Normalize ts_event to midnight UTC for consistent date filtering
    daily["ts_event"] = pd.to_datetime(daily["date"], utc=True)

    # Ensure symbol column
    if "symbol" not in daily.columns:
        daily["symbol"] = daily["instrument_key"].str.replace(r"^equity:", "", regex=True)

    logger.info("Daily bars ready", total_rows=len(daily), symbols=daily["instrument_key"].nunique())
    return daily


def _ensure_ts_available(df: pd.DataFrame) -> pd.DataFrame:
    """Add ts_available column for zero-leakage Gold writes."""
    if "ts_available" not in df.columns:
        df["ts_available"] = datetime.now(UTC)
    return df


def _ensure_instrument_key(df: pd.DataFrame, symbol_col: str = "symbol") -> pd.DataFrame:
    """Add instrument_key if missing."""
    if "instrument_key" not in df.columns and symbol_col in df.columns:
        df["instrument_key"] = df[symbol_col].apply(lambda s: s if ":" in str(s) else f"equity:{s}")
    return df


# ---------------------------------------------------------------------------
# Flow Features
# ---------------------------------------------------------------------------


def compute_flow_features(
    flow_alerts: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate daily flow statistics per underlying symbol.

    Input: Silver flow_alerts with columns: underlying, ts_event, premium, put_call, is_sweep
    Output: One row per (symbol, date) with aggregated flow metrics.
    """
    if flow_alerts.empty:
        return pd.DataFrame()

    df = flow_alerts.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date
    df["premium"] = pd.to_numeric(df.get("premium", 0), errors="coerce").fillna(0)

    groups = df.groupby(["underlying", "date"])

    result = groups.agg(
        total_premium_24h=("premium", "sum"),
        sweep_count_24h=("is_sweep", lambda x: x.sum() if "is_sweep" in df.columns else 0),
    ).reset_index()

    # Call/put split
    call_prem = df[df["put_call"] == "C"].groupby(["underlying", "date"])["premium"].sum().reset_index()
    call_prem.columns = ["underlying", "date", "call_premium_24h"]

    put_prem = df[df["put_call"] == "P"].groupby(["underlying", "date"])["premium"].sum().reset_index()
    put_prem.columns = ["underlying", "date", "put_premium_24h"]

    result = result.merge(call_prem, on=["underlying", "date"], how="left")
    result = result.merge(put_prem, on=["underlying", "date"], how="left")
    result["call_premium_24h"] = result["call_premium_24h"].fillna(0)
    result["put_premium_24h"] = result["put_premium_24h"].fillna(0)

    result["call_put_premium_ratio"] = np.where(
        result["put_premium_24h"] > 0,
        result["call_premium_24h"] / result["put_premium_24h"],
        np.nan,
    )
    result["net_premium_24h"] = result["call_premium_24h"] - result["put_premium_24h"]

    # --- net_bull_premium_lr ---
    # Ask-side premium = aggressive buys; bid-side premium = aggressive sells.
    # net_bull_prem = ask_side - bid_side per (underlying, date), then log-rescale
    # and cross-sectional z-score at each date.
    if "total_ask_side_prem" in df.columns and "total_bid_side_prem" in df.columns:
        df["_ask_prem"] = pd.to_numeric(df["total_ask_side_prem"], errors="coerce").fillna(0)
        df["_bid_prem"] = pd.to_numeric(df["total_bid_side_prem"], errors="coerce").fillna(0)
        bull_agg = (
            df.groupby(["underlying", "date"])
            .agg(
                _total_ask=("_ask_prem", "sum"),
                _total_bid=("_bid_prem", "sum"),
            )
            .reset_index()
        )
        bull_agg["_net_bull"] = bull_agg["_total_ask"] - bull_agg["_total_bid"]
        # Log-rescale with sign preservation
        bull_agg["_signed_log"] = np.sign(bull_agg["_net_bull"]) * np.log1p(np.abs(bull_agg["_net_bull"]))
        # Cross-sectional z-score per date
        date_stats = bull_agg.groupby("date")["_signed_log"].agg(["mean", "std"]).reset_index()
        date_stats.columns = ["date", "_cs_mean", "_cs_std"]
        bull_agg = bull_agg.merge(date_stats, on="date", how="left")
        bull_agg["net_bull_premium_lr"] = np.where(
            bull_agg["_cs_std"] > 0,
            (bull_agg["_signed_log"] - bull_agg["_cs_mean"]) / bull_agg["_cs_std"],
            0.0,
        )
        result = result.merge(
            bull_agg[["underlying", "date", "net_bull_premium_lr"]],
            on=["underlying", "date"],
            how="left",
        )
    else:
        result["net_bull_premium_lr"] = np.nan

    # --- sweep_volume_share ---
    # Fraction of total contracts executed via sweeps, winsorized [1st, 99th] cross-sectionally.
    if "is_sweep" in df.columns and "total_size" in df.columns:
        total_size_numeric = pd.to_numeric(df["total_size"], errors="coerce").fillna(0)
        df["_sweep_contracts"] = np.where(df["is_sweep"].fillna(False), total_size_numeric, 0)
        df["_total_contracts"] = pd.to_numeric(df["total_size"], errors="coerce").fillna(0)
        sweep_agg = (
            df.groupby(["underlying", "date"])
            .agg(
                _sweep_sum=("_sweep_contracts", "sum"),
                _total_sum=("_total_contracts", "sum"),
            )
            .reset_index()
        )
        sweep_agg["sweep_volume_share"] = np.where(
            sweep_agg["_total_sum"] > 0,
            sweep_agg["_sweep_sum"] / sweep_agg["_total_sum"],
            np.nan,
        )

        # Winsorize cross-sectionally per date at [1st, 99th] percentiles
        def _winsorize_group(g: pd.Series) -> pd.Series:
            lo, hi = g.quantile(0.01), g.quantile(0.99)
            return g.clip(lower=lo, upper=hi)

        sweep_agg["sweep_volume_share"] = sweep_agg.groupby("date")["sweep_volume_share"].transform(_winsorize_group)
        result = result.merge(
            sweep_agg[["underlying", "date", "sweep_volume_share"]],
            on=["underlying", "date"],
            how="left",
        )
    else:
        result["sweep_volume_share"] = np.nan

    # Rename for Gold schema
    result = result.rename(columns={"underlying": "symbol", "date": "ts_event"})
    result["ts_event"] = pd.to_datetime(result["ts_event"], utc=True)

    return _ensure_instrument_key(_ensure_ts_available(result))


# ---------------------------------------------------------------------------
# Momentum Features
# ---------------------------------------------------------------------------


def compute_momentum_features(bars: pd.DataFrame) -> pd.DataFrame:
    """Compute momentum indicators from daily bars.

    Input: Silver bars with columns: instrument_key, ts_event, close
    Output: One row per (symbol, date) with momentum metrics.
    """
    if bars.empty:
        return pd.DataFrame()

    df = bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df = df.sort_values(["instrument_key", "ts_event"])

    results = []
    for symbol, group in df.groupby("instrument_key"):
        g = group.set_index("ts_event")["close"].astype(float)

        # Momentum (price change over N days)
        mom_1d = g.pct_change(1, fill_method=None)
        mom_5d = g.pct_change(5, fill_method=None)
        mom_10d = g.pct_change(10, fill_method=None)
        mom_20d = g.pct_change(20, fill_method=None)
        mom_60d = g.pct_change(60, fill_method=None)

        # Rate of change
        roc_5d = mom_5d * 100
        roc_20d = mom_20d * 100

        # RSI
        rsi_14 = _compute_rsi(g, 14)
        rsi_28 = _compute_rsi(g, 28)

        # MACD
        ema_12 = g.ewm(span=12, adjust=False).mean()
        ema_26 = g.ewm(span=26, adjust=False).mean()
        macd = ema_12 - ema_26
        macd_signal = macd.ewm(span=9, adjust=False).mean()

        feat = pd.DataFrame(
            {
                "instrument_key": symbol,
                "ts_event": g.index,
                "momentum_1d": mom_1d.values,
                "momentum_5d": mom_5d.values,
                "momentum_10d": mom_10d.values,
                "momentum_20d": mom_20d.values,
                "momentum_60d": mom_60d.values,
                "roc_5d": roc_5d.values,
                "roc_20d": roc_20d.values,
                "rsi_14": rsi_14.values,
                "rsi_28": rsi_28.values,
                "macd": macd.values,
                "macd_signal": macd_signal.values,
            }
        )
        results.append(feat)

    if not results:
        return pd.DataFrame()

    out = pd.concat(results, ignore_index=True)
    out["symbol"] = out["instrument_key"].str.replace(r"^equity:", "", regex=True)
    return _ensure_ts_available(out)


def _compute_rsi(prices: pd.Series, period: int) -> pd.Series:
    """Compute RSI using exponential moving average."""
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ---------------------------------------------------------------------------
# Volatility Features
# ---------------------------------------------------------------------------


def compute_volatility_features(bars: pd.DataFrame) -> pd.DataFrame:
    """Compute volatility indicators from daily bars.

    Input: Silver bars with columns: instrument_key, ts_event, open, high, low, close
    Output: One row per (symbol, date) with volatility metrics.
    """
    if bars.empty:
        return pd.DataFrame()

    df = bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df = df.sort_values(["instrument_key", "ts_event"])

    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    results = []
    for symbol, group in df.groupby("instrument_key"):
        g = group.set_index("ts_event")
        close = g["close"]
        high = g["high"]
        low = g["low"]
        log_ret = np.log(close / close.shift(1))

        # Historical volatility (annualized)
        vol_5d = log_ret.rolling(5).std() * np.sqrt(252)
        vol_20d = log_ret.rolling(20).std() * np.sqrt(252)
        vol_60d = log_ret.rolling(60).std() * np.sqrt(252)
        vol_ratio_5_20 = vol_5d / vol_20d.replace(0, np.nan)
        vol_ratio_20_60 = vol_20d / vol_60d.replace(0, np.nan)

        # Parkinson volatility
        log_hl = np.log(high / low.replace(0, np.nan))
        parkinson_vol_20d = np.sqrt((1 / (4 * np.log(2))) * (log_hl**2).rolling(20).mean()) * np.sqrt(252)

        # ATR
        tr = pd.concat(
            [
                high - low,
                (high - close.shift(1)).abs(),
                (low - close.shift(1)).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr_14 = tr.rolling(14).mean()
        atr_20 = tr.rolling(20).mean()

        # Bollinger bandwidth
        sma_20 = close.rolling(20).mean()
        std_20 = close.rolling(20).std()
        bb_width_20 = (2 * std_20) / sma_20.replace(0, np.nan)

        # Z-scores
        price_zscore_20d = (close - sma_20) / std_20.replace(0, np.nan)
        sma_60 = close.rolling(60).mean()
        std_60 = close.rolling(60).std()
        price_zscore_60d = (close - sma_60) / std_60.replace(0, np.nan)

        feat = pd.DataFrame(
            {
                "instrument_key": symbol,
                "ts_event": g.index,
                "vol_5d": vol_5d.values,
                "vol_20d": vol_20d.values,
                "vol_60d": vol_60d.values,
                "vol_ratio_5_20": vol_ratio_5_20.values,
                "vol_ratio_20_60": vol_ratio_20_60.values,
                "parkinson_vol_20d": parkinson_vol_20d.values,
                "atr_14": atr_14.values,
                "atr_20": atr_20.values,
                "bb_width_20": bb_width_20.values,
                "price_zscore_20d": price_zscore_20d.values,
                "price_zscore_60d": price_zscore_60d.values,
            }
        )
        results.append(feat)

    if not results:
        return pd.DataFrame()

    out = pd.concat(results, ignore_index=True)
    out["symbol"] = out["instrument_key"].str.replace(r"^equity:", "", regex=True)
    return _ensure_ts_available(out)


# ---------------------------------------------------------------------------
# Microstructure Features
# ---------------------------------------------------------------------------


def compute_microstructure_features(quotes: pd.DataFrame) -> pd.DataFrame:
    """Compute microstructure features from quote snapshots.

    Input: Silver quotes with columns: instrument_key, ts_event, bid_px, ask_px, bid_sz, ask_sz
    Output: One row per (symbol, date) with microstructure metrics (daily averages).
    """
    if quotes.empty:
        return pd.DataFrame()

    df = quotes.copy()

    # Normalize Silver column names (bid_px -> bid_price, ask_px -> ask_price, etc.)
    col_renames = {}
    if "bid_px" in df.columns and "bid_price" not in df.columns:
        col_renames["bid_px"] = "bid_price"
    if "ask_px" in df.columns and "ask_price" not in df.columns:
        col_renames["ask_px"] = "ask_price"
    if "bid_sz" in df.columns and "bid_size" not in df.columns:
        col_renames["bid_sz"] = "bid_size"
    if "ask_sz" in df.columns and "ask_size" not in df.columns:
        col_renames["ask_sz"] = "ask_size"
    if col_renames:
        df = df.rename(columns=col_renames)

    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["date"] = df["ts_event"].dt.date

    for col in ("bid_price", "ask_price", "bid_size", "ask_size"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["bid_ask_spread"] = df["ask_price"] - df["bid_price"]
    df["mid_px"] = (df["ask_price"] + df["bid_price"]) / 2
    df["spread_bps"] = np.where(
        df["mid_px"] > 0,
        (df["bid_ask_spread"] / df["mid_px"]) * 10000,
        np.nan,
    )

    if "bid_size" in df.columns and "ask_size" in df.columns:
        df["total_depth"] = df["bid_size"] + df["ask_size"]
        df["depth_imbalance"] = np.where(
            df["total_depth"] > 0,
            (df["bid_size"] - df["ask_size"]) / df["total_depth"],
            0,
        )
    else:
        df["total_depth"] = np.nan
        df["depth_imbalance"] = np.nan

    has_bid_size = "bid_size" in df.columns
    has_ask_size = "ask_size" in df.columns

    agg = (
        df.groupby(["instrument_key", "date"])
        .agg(
            bid_ask_spread=("bid_ask_spread", "mean"),
            spread_bps=("spread_bps", "mean"),
            mid_px=("mid_px", "last"),
            bid_depth=("bid_size", "mean") if has_bid_size else ("bid_price", "count"),
            ask_depth=("ask_size", "mean") if has_ask_size else ("ask_price", "count"),
            depth_imbalance=("depth_imbalance", "mean"),
            total_depth=("total_depth", "mean"),
        )
        .reset_index()
    )

    agg = agg.rename(columns={"date": "ts_event"})
    agg["ts_event"] = pd.to_datetime(agg["ts_event"], utc=True)
    agg["symbol"] = agg["instrument_key"].str.replace(r"^equity:", "", regex=True)

    return _ensure_instrument_key(_ensure_ts_available(agg), symbol_col="instrument_key")


# ---------------------------------------------------------------------------
# Return Labels (1d, 5d)
# ---------------------------------------------------------------------------


def compute_return_labels(bars: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Compute forward return labels from daily bars.

    Args:
        bars: Silver bars with columns: instrument_key, ts_event, close
        horizon: Number of trading days forward (1 or 5)

    Output: One row per (symbol, date) with return and direction labels.
    """
    if bars.empty:
        return pd.DataFrame()

    df = bars.copy()
    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.sort_values(["instrument_key", "ts_event"])

    results = []
    for symbol, group in df.groupby("instrument_key"):
        g = group.set_index("ts_event")["close"]
        fwd_return = g.shift(-horizon) / g - 1

        feat = pd.DataFrame(
            {
                "instrument_key": symbol,
                "ts_event": g.index,
                f"return_{horizon}d": fwd_return.values,
            }
        )
        feat[f"direction_{horizon}d"] = np.sign(feat[f"return_{horizon}d"]).astype("Int64")
        feat[f"is_up_{horizon}d"] = (feat[f"return_{horizon}d"] > 0).astype("Int64")
        feat[f"is_down_{horizon}d"] = (feat[f"return_{horizon}d"] < 0).astype("Int64")

        # Drop rows where forward return is NaN (last N days have no future data)
        feat = feat.dropna(subset=[f"return_{horizon}d"])
        results.append(feat)

    if not results:
        return pd.DataFrame()

    out = pd.concat(results, ignore_index=True)
    out["symbol"] = out["instrument_key"].str.replace(r"^equity:", "", regex=True)
    out["ts_available"] = pd.to_datetime(out["ts_event"], utc=True) + timedelta(days=horizon)
    return out


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

ALL_DATASETS = ("flow", "momentum", "volatility", "microstructure", "returns_1d", "returns_5d")

DATASET_TO_GOLD_NAME = {
    "flow": "flow_features",
    "momentum": "momentum_features",
    "volatility": "volatility_features",
    "microstructure": "microstructure_features",
    "returns_1d": "labels_returns_1d",
    "returns_5d": "labels_returns_5d",
}


class EquityFeaturePipeline:
    """Pipeline to compute equity-level Gold features from Silver data."""

    def __init__(
        self,
        reader: HeberReader | None = None,
        project: str = "watch",
        version: str = "v1",
    ):
        self.reader = reader or HeberReader()
        self.project = project
        self.version = version

    def run(
        self,
        start_date: str | datetime,
        end_date: str | datetime,
        datasets: tuple[str, ...] = ALL_DATASETS,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the pipeline for specified datasets.

        Args:
            start_date: Start of date range
            end_date: End of date range
            datasets: Which datasets to compute
            dry_run: If True, compute but don't write

        Returns:
            Dict with pipeline stats per dataset.
        """
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        # When end_date is midnight (date-only input), extend to end of day
        # so intraday ts_event values are included in the range.
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date = end_date.replace(hour=23, minute=59, second=59)

        stats: dict[str, Any] = {}

        # Load Silver data once and reuse
        bars = None
        quotes = None
        flow_alerts = None

        bar_datasets = {"momentum", "volatility", "returns_1d", "returns_5d"}
        needs_forward = {"returns_1d", "returns_5d"} & set(datasets)
        if bar_datasets & set(datasets):
            bar_start = start_date - timedelta(days=LOOKBACK_DAYS)
            # Load extra future bars for forward return label computation
            bar_end = end_date + timedelta(days=FORWARD_DAYS) if needs_forward else end_date
            logger.info("Loading Silver bars", start=bar_start.isoformat(), end=bar_end.isoformat())
            bars = self.reader.read_silver("bars", instrument_type="equity", time_range=(bar_start, bar_end))
            logger.info("Loaded bars", rows=len(bars))
            bars = _resample_bars_to_daily(bars)

        if "microstructure" in datasets:
            logger.info("Loading Silver quotes")
            quotes = self.reader.read_silver("quotes", instrument_type="equity", time_range=(start_date, end_date))
            logger.info("Loaded quotes", rows=len(quotes))

        if "flow" in datasets:
            logger.info("Loading Silver flow_alerts")
            flow_alerts = self.reader.read_silver(
                "flow_alerts", instrument_type="option", time_range=(start_date, end_date)
            )
            logger.info("Loaded flow_alerts", rows=len(flow_alerts))

        # Compute each dataset
        for ds_name in datasets:
            gold_name = DATASET_TO_GOLD_NAME[ds_name]
            logger.info("Computing dataset", dataset=gold_name)

            try:
                if ds_name == "flow":
                    df = compute_flow_features(flow_alerts)
                elif ds_name == "momentum":
                    df = compute_momentum_features(bars)
                elif ds_name == "volatility":
                    df = compute_volatility_features(bars)
                elif ds_name == "microstructure":
                    df = compute_microstructure_features(quotes)
                elif ds_name == "returns_1d":
                    df = compute_return_labels(bars, horizon=1)
                elif ds_name == "returns_5d":
                    df = compute_return_labels(bars, horizon=5)
                else:
                    logger.warning("Unknown dataset", dataset=ds_name)
                    continue

                if df.empty:
                    logger.warning("No data computed", dataset=gold_name)
                    stats[gold_name] = {"status": "no_data", "rows": 0}
                    continue

                # Filter to requested date range only (exclude lookback)
                if "ts_event" in df.columns:
                    df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)
                    df = df[df["ts_event"] >= pd.to_datetime(start_date, utc=True)]
                    df = df[df["ts_event"] <= pd.to_datetime(end_date, utc=True)]

                if not dry_run:
                    output_path = self.reader.write_gold(
                        dataset=gold_name,
                        df=df,
                        project=self.project,
                        version=self.version,
                    )
                    logger.info("Wrote to Gold", dataset=gold_name, rows=len(df), path=str(output_path))
                else:
                    logger.info("Dry run — would write", dataset=gold_name, rows=len(df))
                    output_path = None

                stats[gold_name] = {
                    "status": "success",
                    "rows": len(df),
                    "path": str(output_path) if output_path else None,
                }

            except Exception as exc:
                logger.error("Failed to compute dataset", dataset=gold_name, error=str(exc), exc_info=True)
                stats[gold_name] = {"status": "error", "error": str(exc)}

        return stats


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute equity Gold features from Silver data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--datasets",
        nargs="*",
        choices=list(ALL_DATASETS),
        default=list(ALL_DATASETS),
        help="Which datasets to compute (default: all)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--project", default="watch", help="Gold project name")
    parser.add_argument("--version", default="v1", help="Gold version")

    args = parser.parse_args()

    pipeline = EquityFeaturePipeline(project=args.project, version=args.version)
    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        datasets=tuple(args.datasets),
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("EQUITY FEATURES PIPELINE RESULTS")
    print("=" * 60)
    for dataset, info in stats.items():
        status = info.get("status", "unknown")
        rows = info.get("rows", 0)
        print(f"  {dataset}: {status} ({rows} rows)")
    print()


if __name__ == "__main__":
    main()
