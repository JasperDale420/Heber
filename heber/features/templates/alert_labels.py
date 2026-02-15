"""Alert Barrier Labels (PRD §32.7).

Compute barrier-based labels for flow alerts: "Should I take this alert?"

Key concepts:
- **Barrier labeling**: Did price hit TP before SL?
- **ATR-scaled thresholds**: Volatility-normalized, works across regimes
- **DTE-aware horizons**: Intraday (0-2 DTE), Swing (3-21 DTE), LEAP (22+ DTE)
- **MFE/MAE tracking**: Max favorable/adverse excursion for position sizing
- **Beta-neutral returns**: SPY-relative for market-neutral analysis
- **Regime context**: VIX level at alert time
- **Slippage model**: Execution cost assumptions

This follows the "referee not fan fiction" approach:
- Labels come from market data outcomes, not hand-labeling
- Rules-based simulation with TP/SL barriers
- Volatility-normalized for regime robustness
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum

import numpy as np
import pandas as pd

# =============================================================================
# Constants
# =============================================================================

# Default slippage assumptions (as decimal)
DEFAULT_SLIPPAGE_PCT = 0.001  # 0.1% round-trip slippage
DEFAULT_COMMISSION_PER_CONTRACT = 0.65  # Per contract fee

# SPY as market proxy for beta-neutral calculation
MARKET_PROXY = "equity:SPY"

# VIX thresholds for regime classification
VIX_LOW = 15.0
VIX_HIGH = 25.0


class AlertHorizon(str, Enum):
    """DTE-based horizon classification."""

    INTRADAY = "intraday"  # 0-2 DTE: 30min - 2h window
    SWING = "swing"  # 3-21 DTE: 1-5 day window
    LEAP = "leap"  # 22+ DTE: 5-30 day window


class VixRegime(str, Enum):
    """VIX-based market regime."""

    LOW = "low"  # VIX < 15: calm markets
    NORMAL = "normal"  # VIX 15-25: typical volatility
    HIGH = "high"  # VIX > 25: elevated fear


@dataclass
class BarrierConfig:
    """Configuration for TP/SL barriers per horizon."""

    horizon: AlertHorizon
    tp_atr_mult: float  # TP = +tp_atr_mult × ATR
    sl_atr_mult: float  # SL = -sl_atr_mult × ATR
    max_window_bars: int  # Maximum bars to look forward
    atr_period: int  # ATR lookback period

    @classmethod
    def intraday(cls) -> BarrierConfig:
        """Intraday config: tight barriers, 2h max window (assuming 5-min bars)."""
        return cls(
            horizon=AlertHorizon.INTRADAY,
            tp_atr_mult=0.75,
            sl_atr_mult=0.50,
            max_window_bars=24,  # 2h @ 5-min bars
            atr_period=14,
        )

    @classmethod
    def swing(cls) -> BarrierConfig:
        """Swing config: wider barriers, 5-day max window (daily bars)."""
        return cls(
            horizon=AlertHorizon.SWING,
            tp_atr_mult=1.5,
            sl_atr_mult=1.0,
            max_window_bars=5,  # 5 days
            atr_period=14,
        )

    @classmethod
    def leap(cls) -> BarrierConfig:
        """LEAP config: widest barriers, 30-day max window (daily bars)."""
        return cls(
            horizon=AlertHorizon.LEAP,
            tp_atr_mult=2.5,
            sl_atr_mult=1.5,
            max_window_bars=30,  # 30 days
            atr_period=20,
        )


@dataclass
class ContractBarrierConfig:
    """Configuration for option contract TP/SL barriers.

    Unlike underlying barriers (ATR-scaled), contract barriers are
    typically fixed percentages since option returns are already
    volatility-adjusted.
    """

    tp_pct: float = 0.25  # Take profit at +25%
    sl_pct: float = 0.15  # Stop loss at -15%
    max_window_bars: int = 5  # Max bars to track

    @classmethod
    def aggressive(cls) -> ContractBarrierConfig:
        """Aggressive: quick scalp targets."""
        return cls(tp_pct=0.15, sl_pct=0.10, max_window_bars=3)

    @classmethod
    def moderate(cls) -> ContractBarrierConfig:
        """Moderate: balanced risk/reward."""
        return cls(tp_pct=0.25, sl_pct=0.15, max_window_bars=5)

    @classmethod
    def conservative(cls) -> ContractBarrierConfig:
        """Conservative: wider targets for swing trades."""
        return cls(tp_pct=0.50, sl_pct=0.25, max_window_bars=10)


@dataclass
class SlippageModel:
    """Model for execution costs and slippage."""

    spread_pct: float = 0.0005  # Half spread (entry cost)
    slippage_pct: float = 0.0005  # Market impact
    commission_per_contract: float = 0.65
    # Option-specific: wider spreads
    option_spread_pct: float = 0.02  # Options often have 2-5% spreads

    @property
    def total_entry_cost_pct(self) -> float:
        """Total cost to enter position as % of notional."""
        return self.spread_pct + self.slippage_pct

    @property
    def total_roundtrip_cost_pct(self) -> float:
        """Total cost to enter and exit as % of notional."""
        return 2 * self.total_entry_cost_pct

    @property
    def option_roundtrip_cost_pct(self) -> float:
        """Total cost for option roundtrip."""
        return 2 * self.option_spread_pct

    def adjust_mfe(self, mfe: float) -> float:
        """Adjust MFE for execution costs."""
        return mfe - self.total_roundtrip_cost_pct

    def adjust_mae(self, mae: float) -> float:
        """Adjust MAE for execution costs (makes it worse)."""
        return mae - self.total_entry_cost_pct

    def adjust_option_mfe(self, mfe: float) -> float:
        """Adjust option MFE for wider spreads."""
        return mfe - self.option_roundtrip_cost_pct

    def adjust_option_mae(self, mae: float) -> float:
        """Adjust option MAE for entry spread."""
        return mae - self.option_spread_pct


def classify_horizon(dte: int) -> AlertHorizon:
    """Classify alert horizon based on days-to-expiry."""
    if dte <= 2:
        return AlertHorizon.INTRADAY
    elif dte <= 21:
        return AlertHorizon.SWING
    else:
        return AlertHorizon.LEAP


def classify_vix_regime(vix: float | None) -> str:
    """Classify market regime based on VIX level."""
    if vix is None or not np.isfinite(vix):
        return VixRegime.NORMAL.value
    elif vix < VIX_LOW:
        return VixRegime.LOW.value
    elif vix > VIX_HIGH:
        return VixRegime.HIGH.value
    else:
        return VixRegime.NORMAL.value


def compute_atr(
    bars_df: pd.DataFrame,
    period: int = 14,
) -> pd.DataFrame:
    """Compute Average True Range for volatility scaling.

    Uses the canonical ATR implementation from volatility module,
    applying it per-instrument via groupby.
    """
    from heber.features.templates.volatility import compute_atr as _core_atr

    df = bars_df.copy()
    df = df.sort_values(["instrument_key", "bar_start_ts"]).reset_index(drop=True)

    # Apply ATR per instrument group using canonical implementation
    df["atr"] = (
        df.groupby("instrument_key", group_keys=False)
        .apply(
            lambda g: _core_atr(g["high"], g["low"], g["close"], period).fillna(method="bfill"),
            include_groups=False,
        )
        .reset_index(level=0, drop=True)
    )

    return df


def _get_price_at_time(
    bars_df: pd.DataFrame,
    instrument: str,
    target_time: pd.Timestamp,
) -> float | None:
    """Get closing price for instrument at or before target time."""
    instrument_bars = bars_df[bars_df["instrument_key"] == instrument]
    before_target = instrument_bars[instrument_bars["bar_start_ts"] <= target_time]
    if before_target.empty:
        return None
    return float(before_target.iloc[-1]["close"])


def _compute_beta_neutral_return(
    underlying_return: float,
    spy_return: float | None,
    beta: float = 1.0,
) -> float | None:
    """Compute beta-neutral (market-adjusted) return.

    beta_neutral_return = underlying_return - beta * spy_return

    Args:
        underlying_return: Raw return of the underlying
        spy_return: Return of SPY over same period
        beta: Stock's beta to SPY (default 1.0 for simplicity)

    Returns:
        Beta-neutral return, or None if SPY data missing
    """
    if spy_return is None or not np.isfinite(spy_return) or not np.isfinite(underlying_return) or not np.isfinite(beta):
        return None
    return underlying_return - beta * spy_return


def _compute_barrier_outcome(
    price_path: np.ndarray,
    entry_price: float,
    is_call: bool,
    tp_threshold: float,
    sl_threshold: float,
    slippage_model: SlippageModel | None = None,
) -> dict:
    """Compute barrier-based outcome for a price path."""
    if len(price_path) == 0:
        return {
            "hit_tp_first": np.nan,
            "mfe": np.nan,
            "mae": np.nan,
            "mfe_adj": np.nan,
            "mae_adj": np.nan,
            "bars_to_hit": np.nan,
        }

    returns = (price_path - entry_price) / entry_price

    # For calls: favorable = up, adverse = down
    # For puts: flip the sign
    if is_call:
        favorable = returns
        adverse = returns
    else:
        favorable = -returns
        adverse = -returns

    # MFE/MAE (raw)
    mfe = float(np.nanmax(favorable))
    mae = float(np.nanmin(adverse))

    # Adjusted for slippage
    if slippage_model:
        mfe_adj = slippage_model.adjust_mfe(mfe)
        mae_adj = slippage_model.adjust_mae(mae)
    else:
        mfe_adj = mfe
        mae_adj = mae

    # Barrier hits (use adjusted thresholds for realistic simulation)
    adjusted_tp = tp_threshold
    adjusted_sl = sl_threshold
    if slippage_model:
        # Need MORE movement to hit TP after costs
        adjusted_tp = tp_threshold + slippage_model.total_roundtrip_cost_pct

    tp_hit_idx = np.nonzero(favorable >= adjusted_tp)[0]
    sl_hit_idx = np.nonzero(adverse <= -adjusted_sl)[0]

    tp_first_bar = tp_hit_idx[0] if len(tp_hit_idx) > 0 else np.inf
    sl_first_bar = sl_hit_idx[0] if len(sl_hit_idx) > 0 else np.inf

    if tp_first_bar < sl_first_bar:
        hit_tp_first = 1
        bars_to_hit = int(tp_first_bar) + 1
    elif sl_first_bar < tp_first_bar:
        hit_tp_first = 0
        bars_to_hit = int(sl_first_bar) + 1
    else:
        hit_tp_first = 0
        bars_to_hit = np.nan

    return {
        "hit_tp_first": hit_tp_first,
        "mfe": mfe,
        "mae": mae,
        "mfe_adj": mfe_adj,
        "mae_adj": mae_adj,
        "bars_to_hit": bars_to_hit,
    }


def _compute_contract_barrier_outcome(
    option_price_path: np.ndarray,
    entry_price: float,
    config: ContractBarrierConfig,
    slippage_model: SlippageModel | None = None,
) -> dict:
    """Compute barrier-based outcome for an option contract price path.

    Unlike underlying barriers which use ATR scaling, contract barriers
    use fixed percentage thresholds (e.g., +25% TP, -15% SL) since options
    already embed volatility.

    Args:
        option_price_path: Array of option mid prices after entry
        entry_price: Option price at alert time
        config: ContractBarrierConfig with TP/SL percentages
        slippage_model: Optional execution cost model

    Returns:
        Dict with contract-based labels
    """
    if len(option_price_path) == 0 or entry_price <= 0:
        return {
            "contract_hit_tp_first": np.nan,
            "contract_mfe": np.nan,
            "contract_mae": np.nan,
            "contract_mfe_adj": np.nan,
            "contract_mae_adj": np.nan,
            "contract_bars_to_hit": np.nan,
        }

    # Compute returns (options are always long, so no call/put flip needed)
    returns = (option_price_path - entry_price) / entry_price

    # MFE/MAE
    mfe = float(np.nanmax(returns))
    mae = float(np.nanmin(returns))

    # Adjust for option spreads
    if slippage_model:
        mfe_adj = slippage_model.adjust_option_mfe(mfe)
        mae_adj = slippage_model.adjust_option_mae(mae)
    else:
        mfe_adj = mfe
        mae_adj = mae

    # Barrier thresholds (adjust TP for roundtrip costs)
    tp_threshold = config.tp_pct
    sl_threshold = config.sl_pct

    if slippage_model:
        tp_threshold = config.tp_pct + slippage_model.option_roundtrip_cost_pct

    # Find barrier hits
    tp_hit_idx = np.nonzero(returns >= tp_threshold)[0]
    sl_hit_idx = np.nonzero(returns <= -sl_threshold)[0]

    tp_first_bar = tp_hit_idx[0] if len(tp_hit_idx) > 0 else np.inf
    sl_first_bar = sl_hit_idx[0] if len(sl_hit_idx) > 0 else np.inf

    if tp_first_bar < sl_first_bar:
        hit_tp_first = 1
        bars_to_hit = int(tp_first_bar) + 1
    elif sl_first_bar < tp_first_bar:
        hit_tp_first = 0
        bars_to_hit = int(sl_first_bar) + 1
    else:
        hit_tp_first = 0
        bars_to_hit = np.nan

    return {
        "contract_hit_tp_first": hit_tp_first,
        "contract_mfe": mfe,
        "contract_mae": mae,
        "contract_mfe_adj": mfe_adj,
        "contract_mae_adj": mae_adj,
        "contract_bars_to_hit": bars_to_hit,
    }


def _process_single_alert(
    alert: pd.Series,
    bars: pd.DataFrame,
    spy_bars: pd.DataFrame | None,
    vix_data: pd.DataFrame | None,
    config: BarrierConfig,
    slippage_model: SlippageModel | None,
) -> dict:
    """Process a single alert and compute all labels."""
    alert_id = alert["event_id"]
    underlying = alert["underlying"]
    ts_alert = alert["ts_event"]
    put_call = alert["put_call"]
    expiry = alert["expiry"]

    # Calculate DTE
    alert_date = ts_alert.date() if hasattr(ts_alert, "date") else ts_alert
    dte = (expiry - alert_date).days if isinstance(expiry, date) else 5
    horizon = classify_horizon(dte)

    # Get underlying bars
    underlying_bars = bars[bars["instrument_key"] == underlying]
    if underlying_bars.empty:
        return _empty_result(alert_id, underlying, put_call, horizon, dte)

    # Get ATR and spot at alert time
    entry_data = _get_entry_data(underlying_bars, ts_alert, alert)
    if entry_data is None:
        return _empty_result(alert_id, underlying, put_call, horizon, dte)

    atr_at_alert, spot_at_alert = entry_data

    # Compute thresholds
    tp_threshold = config.tp_atr_mult * atr_at_alert / spot_at_alert
    sl_threshold = config.sl_atr_mult * atr_at_alert / spot_at_alert

    # Get forward price path
    future_bars = underlying_bars[underlying_bars["bar_start_ts"] > ts_alert].head(config.max_window_bars)
    if future_bars.empty:
        return _empty_result(alert_id, underlying, put_call, horizon, dte)

    price_path = future_bars["close"].values
    is_call = put_call.upper() == "C"

    # Compute barrier outcome
    outcome = _compute_barrier_outcome(price_path, spot_at_alert, is_call, tp_threshold, sl_threshold, slippage_model)

    # Get VIX at alert time
    vix_at_alert = _get_vix_at_alert(vix_data, ts_alert)

    # Compute beta-neutral return
    raw_return = outcome["mfe"] if outcome["hit_tp_first"] == 1 else outcome["mae"]
    beta_neutral_return = _compute_spy_relative_return(spy_bars, ts_alert, config, raw_return)

    return {
        "alert_id": alert_id,
        "underlying": underlying,
        "put_call": put_call,
        "ts_alert": ts_alert,
        "ts_event": ts_alert,
        "dte": dte,
        "horizon": horizon.value,
        "spot_at_alert": spot_at_alert,
        "atr_at_alert": atr_at_alert,
        "tp_threshold": tp_threshold,
        "sl_threshold": sl_threshold,
        "hit_tp_first": outcome["hit_tp_first"],
        "mfe": outcome["mfe"],
        "mae": outcome["mae"],
        "mfe_adj": outcome["mfe_adj"],
        "mae_adj": outcome["mae_adj"],
        "bars_to_hit": outcome["bars_to_hit"],
        "beta_neutral_return": beta_neutral_return,
        "vix_at_alert": vix_at_alert,
        "vix_regime": classify_vix_regime(vix_at_alert),
        "ts_available": ts_alert + _window_delta_for_config(config),
    }


def _get_entry_data(
    underlying_bars: pd.DataFrame,
    ts_alert: pd.Timestamp,
    alert: pd.Series,
) -> tuple[float, float] | None:
    """Get ATR and spot price at alert time."""
    bars_at_alert = underlying_bars[underlying_bars["bar_start_ts"] <= ts_alert]
    if bars_at_alert.empty:
        return None
    atr_at_alert = float(bars_at_alert.iloc[-1]["atr"])
    if not np.isfinite(atr_at_alert):
        return None

    bar_close_at_alert = float(bars_at_alert.iloc[-1]["close"])
    alert_spot = alert.get("spot_px")
    try:
        alert_spot_value = float(alert_spot) if alert_spot is not None else None
    except (TypeError, ValueError):
        alert_spot_value = None

    if alert_spot_value is None or not np.isfinite(alert_spot_value) or alert_spot_value <= 0:
        spot_at_alert = bar_close_at_alert
    else:
        spot_at_alert = alert_spot_value

    if not np.isfinite(spot_at_alert) or spot_at_alert <= 0:
        return None

    return atr_at_alert, spot_at_alert


def _get_vix_at_alert(vix_data: pd.DataFrame | None, ts_alert: pd.Timestamp) -> float | None:
    """Get VIX level at alert time."""
    if vix_data is None or len(vix_data) == 0:
        return None
    vix_before = vix_data[vix_data["bar_start_ts"] <= ts_alert]
    if vix_before.empty:
        return None
    vix_value = float(vix_before.iloc[-1]["close"])
    if not np.isfinite(vix_value):
        return None
    return vix_value


def _window_delta_for_config(config: BarrierConfig) -> timedelta:
    """Compute forward label window duration for a horizon config."""
    if config.horizon == AlertHorizon.INTRADAY:
        # Intraday config assumes 5-minute bars (see BarrierConfig.intraday()).
        return timedelta(minutes=config.max_window_bars * 5)
    return timedelta(days=config.max_window_bars)


def _compute_spy_relative_return(
    spy_bars: pd.DataFrame | None,
    ts_alert: pd.Timestamp,
    config: BarrierConfig,
    raw_return: float,
) -> float | None:
    """Compute SPY-relative beta-neutral return."""
    if spy_bars is None or len(spy_bars) == 0 or not np.isfinite(raw_return):
        return None

    spy_at_alert = _get_price_at_time(spy_bars, MARKET_PROXY, ts_alert)
    end_time = ts_alert + _window_delta_for_config(config)
    spy_at_end = _get_price_at_time(spy_bars, MARKET_PROXY, end_time)

    if (
        spy_at_alert is None
        or spy_at_end is None
        or not np.isfinite(spy_at_alert)
        or not np.isfinite(spy_at_end)
        or spy_at_alert <= 0
    ):
        return None

    spy_return = (spy_at_end - spy_at_alert) / spy_at_alert
    return _compute_beta_neutral_return(raw_return, spy_return)


def _empty_result(
    alert_id: str,
    underlying: str,
    put_call: str,
    horizon: AlertHorizon,
    dte: int,
) -> dict:
    """Create empty result row for alerts with insufficient data."""
    return {
        "alert_id": alert_id,
        "underlying": underlying,
        "put_call": put_call,
        "ts_alert": pd.NaT,
        "ts_event": pd.NaT,
        "dte": dte,
        "horizon": horizon.value,
        "spot_at_alert": np.nan,
        "atr_at_alert": np.nan,
        "tp_threshold": np.nan,
        "sl_threshold": np.nan,
        "hit_tp_first": np.nan,
        "mfe": np.nan,
        "mae": np.nan,
        "mfe_adj": np.nan,
        "mae_adj": np.nan,
        "bars_to_hit": np.nan,
        "beta_neutral_return": np.nan,
        "vix_at_alert": np.nan,
        "vix_regime": VixRegime.NORMAL.value,
        "ts_available": pd.NaT,
    }


def compute_barrier_labels(
    flow_alerts_df: pd.DataFrame,
    bars_df: pd.DataFrame,
    spy_bars_df: pd.DataFrame | None = None,
    vix_bars_df: pd.DataFrame | None = None,
    configs: dict[AlertHorizon, BarrierConfig] | None = None,
    slippage_model: SlippageModel | None = None,
) -> pd.DataFrame:
    """Compute barrier-based labels for flow alerts.

    For each alert, determines if price hit TP before SL within the
    appropriate horizon window. Uses ATR-scaled barriers for
    volatility-normalized thresholds.

    Args:
        flow_alerts_df: Flow alerts with columns:
            - event_id: Alert identifier
            - underlying: Stock ticker
            - ts_event: Alert timestamp
            - put_call: 'C' or 'P'
            - expiry: Option expiry date
            - spot_px: Underlying price at alert (optional)

        bars_df: Bar data with columns:
            - instrument_key: Instrument identifier
            - bar_start_ts: Bar timestamp
            - open, high, low, close: OHLC prices

        spy_bars_df: Optional SPY bars for beta-neutral calculation

        vix_bars_df: Optional VIX bars for regime context
            - instrument_key: Should be 'VIX' or similar
            - bar_start_ts: Timestamp
            - close: VIX level

        configs: Barrier configurations per horizon

        slippage_model: Optional execution cost model

    Returns:
        DataFrame with barrier labels per alert including:
            - hit_tp_first: THE LABEL (1=take, 0=skip)
            - mfe, mae: Raw max favorable/adverse excursion
            - mfe_adj, mae_adj: Slippage-adjusted MFE/MAE
            - beta_neutral_return: SPY-relative return
            - vix_at_alert, vix_regime: Market regime context
    """
    if configs is None:
        configs = {
            AlertHorizon.INTRADAY: BarrierConfig.intraday(),
            AlertHorizon.SWING: BarrierConfig.swing(),
            AlertHorizon.LEAP: BarrierConfig.leap(),
        }

    if slippage_model is None:
        slippage_model = SlippageModel()

    # Validate required columns
    required_alert_cols = {"event_id", "underlying", "ts_event", "put_call", "expiry"}
    missing = required_alert_cols - set(flow_alerts_df.columns)
    if missing:
        raise ValueError(f"Flow alerts missing required columns: {missing}")

    required_bar_cols = {"instrument_key", "bar_start_ts", "high", "low", "close"}
    missing = required_bar_cols - set(bars_df.columns)
    if missing:
        raise ValueError(f"Bars missing required columns: {missing}")

    # Prepare data
    alerts = flow_alerts_df.copy()
    alerts["ts_event"] = pd.to_datetime(alerts["ts_event"], utc=True)
    alerts["expiry"] = pd.to_datetime(alerts["expiry"]).dt.date

    bars = bars_df.copy()
    bars["bar_start_ts"] = pd.to_datetime(bars["bar_start_ts"], utc=True)
    bars = bars.sort_values(["instrument_key", "bar_start_ts"])

    # Compute ATR if not present
    if "atr" not in bars.columns:
        bars = compute_atr(bars, period=14)

    # Prepare SPY bars
    spy_bars = None
    if spy_bars_df is not None and len(spy_bars_df) > 0:
        spy_bars = spy_bars_df.copy()
        spy_bars["bar_start_ts"] = pd.to_datetime(spy_bars["bar_start_ts"], utc=True)
        spy_bars = spy_bars.sort_values("bar_start_ts")

    # Prepare VIX data
    vix_data = None
    if vix_bars_df is not None and len(vix_bars_df) > 0:
        vix_data = vix_bars_df.copy()
        vix_data["bar_start_ts"] = pd.to_datetime(vix_data["bar_start_ts"], utc=True)
        vix_data = vix_data.sort_values("bar_start_ts")

    # Process each alert
    results = []
    for _, alert in alerts.iterrows():
        expiry = alert["expiry"]
        alert_date = alert["ts_event"].date()
        dte = (expiry - alert_date).days if isinstance(expiry, date) else 5
        horizon = classify_horizon(dte)
        config = configs.get(horizon, BarrierConfig.swing())

        result = _process_single_alert(alert, bars, spy_bars, vix_data, config, slippage_model)
        results.append(result)

    return pd.DataFrame(results)


def compute_multi_horizon_labels(
    flow_alerts_df: pd.DataFrame,
    intraday_bars_df: pd.DataFrame,
    daily_bars_df: pd.DataFrame,
    spy_daily_bars_df: pd.DataFrame | None = None,
    vix_daily_bars_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute labels for ALL horizons, using appropriate bar granularity.

    Intraday alerts use 5-min bars for fine-grained labeling.
    Swing/LEAP alerts use daily bars.
    """
    alerts = flow_alerts_df.copy()
    alerts["expiry"] = pd.to_datetime(alerts["expiry"]).dt.date
    alerts["ts_event"] = pd.to_datetime(alerts["ts_event"], utc=True)

    # Classify each alert
    def get_dte(row):
        alert_date = row["ts_event"].date()
        if isinstance(row["expiry"], date):
            return (row["expiry"] - alert_date).days
        return 5

    alerts["dte"] = alerts.apply(get_dte, axis=1)
    alerts["horizon"] = alerts["dte"].apply(classify_horizon)

    # Split by horizon
    intraday_alerts = alerts[alerts["horizon"] == AlertHorizon.INTRADAY]
    swing_leap_alerts = alerts[alerts["horizon"].isin([AlertHorizon.SWING, AlertHorizon.LEAP])]

    results = []

    # Process intraday with 5-min bars
    if len(intraday_alerts) > 0 and len(intraday_bars_df) > 0:
        intraday_labels = compute_barrier_labels(
            intraday_alerts,
            intraday_bars_df,
            spy_bars_df=spy_daily_bars_df,  # Still use daily SPY
            vix_bars_df=vix_daily_bars_df,
            configs={AlertHorizon.INTRADAY: BarrierConfig.intraday()},
        )
        results.append(intraday_labels)

    # Process swing/LEAP with daily bars
    if len(swing_leap_alerts) > 0 and len(daily_bars_df) > 0:
        swing_leap_labels = compute_barrier_labels(
            swing_leap_alerts,
            daily_bars_df,
            spy_bars_df=spy_daily_bars_df,
            vix_bars_df=vix_daily_bars_df,
            configs={
                AlertHorizon.SWING: BarrierConfig.swing(),
                AlertHorizon.LEAP: BarrierConfig.leap(),
            },
        )
        results.append(swing_leap_labels)

    if results:
        return pd.concat(results, ignore_index=True)
    return pd.DataFrame()


# =============================================================================
# Auxiliary: Analysis Functions
# =============================================================================


def compute_win_rate_by_feature(
    labeled_df: pd.DataFrame,
    feature_col: str,
    bins: int = 10,
) -> pd.DataFrame:
    """Analyze win rate stratified by a feature."""
    df = labeled_df.dropna(subset=["hit_tp_first", feature_col])

    if df[feature_col].dtype in [np.float64, np.float32, np.int64, np.int32]:
        df = df.copy()
        df["bin"] = pd.qcut(df[feature_col], q=bins, duplicates="drop")
    else:
        df = df.copy()
        df["bin"] = df[feature_col]

    stats = (
        df.groupby("bin")
        .agg(
            count=("hit_tp_first", "count"),
            wins=("hit_tp_first", "sum"),
            win_rate=("hit_tp_first", "mean"),
        )
        .reset_index()
    )

    return stats


def compute_regime_analysis(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze win rates by VIX regime."""
    df = labeled_df.dropna(subset=["hit_tp_first", "vix_regime"])

    stats = (
        df.groupby("vix_regime")
        .agg(
            count=("hit_tp_first", "count"),
            wins=("hit_tp_first", "sum"),
            win_rate=("hit_tp_first", "mean"),
            avg_mfe=("mfe", "mean"),
            avg_mae=("mae", "mean"),
            avg_beta_neutral=("beta_neutral_return", "mean"),
        )
        .reset_index()
    )

    return stats
