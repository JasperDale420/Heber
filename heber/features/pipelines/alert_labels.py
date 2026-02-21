"""Alert Labels Pipeline - Orchestrates label computation for flow alerts.

This pipeline:
1. Reads flow alerts from Silver
2. Fetches corresponding bars (underlying symbols + SPY + UVXY)
3. Fetches option bars from Data Gateway (for contract-based labels)
4. Computes barrier-based labels (both underlying and contract)
5. Writes labels to Gold

Usage:
    python -m heber.features.pipelines.alert_labels --start 2024-01-01 --end 2024-01-31
"""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timedelta
from typing import Any

import httpx
import pandas as pd
import structlog

from heber.features.templates.alert_labels import (
    ContractBarrierConfig,
    SlippageModel,
    _compute_contract_barrier_outcome,
    compute_barrier_labels,
    compute_multi_horizon_labels,
)
from heber.sdk import HeberClient
from heber.watch.gateway import gateway_auth_headers

logger = structlog.get_logger(__name__)

# Constants
MARKET_PROXY = "equity:SPY"
VIX_PROXY = "equity:UVXY"  # Use UVXY as VIX proxy (Alpaca doesn't have VIX)
LOOKBACK_DAYS = 30  # Days of bar history needed for ATR
DATA_GATEWAY_URL = "http://localhost:8000"  # Data Gateway API


class AlertLabelsPipeline:
    """Pipeline for computing barrier-based labels on flow alerts.

    Supports two labeling modes:
    1. Underlying-only: Uses stock price path for barrier detection
    2. Contract + Underlying: Primary label from option prices, secondary from underlying
    """

    def __init__(
        self,
        client: HeberClient | None = None,
        slippage_model: SlippageModel | None = None,
        contract_config: ContractBarrierConfig | None = None,
        output_dataset: str = "labels_alert_barriers",
        project: str = "quant",
        version: str = "v1",
        gateway_url: str = DATA_GATEWAY_URL,
        gateway_api_key: str | None = None,
    ):
        """Initialize the pipeline.

        Args:
            client: HeberClient instance (created if None)
            slippage_model: Execution cost model
            contract_config: Contract barrier config (TP/SL percentages)
            output_dataset: Name for output Gold dataset
            project: Project name for Gold output
            version: Version string for Gold output
            gateway_url: Data Gateway URL for fetching option bars
            gateway_api_key: Data Gateway API key for authenticated option-bars fetches
        """
        self.client = client or HeberClient()
        self.slippage_model = slippage_model or SlippageModel()
        self.contract_config = contract_config or ContractBarrierConfig.moderate()
        self.output_dataset = output_dataset
        self.project = project
        self.version = version
        self.gateway_url = gateway_url
        self.gateway_api_key = (
            gateway_api_key
            or os.getenv("HEBER_WATCH_GATEWAY_API_KEY")
            or os.getenv("DATA_GATEWAY_API_KEY")
            or os.getenv("GATEWAY_API_KEY")
        )

    def _require_gateway_api_key(self) -> str:
        key = (self.gateway_api_key or "").strip()
        if not key:
            raise ValueError(
                "HEBER_WATCH_GATEWAY_API_KEY (or DATA_GATEWAY_API_KEY/GATEWAY_API_KEY) is required "
                "when use_contract_labels=True"
            )
        return key

    @staticmethod
    def _canonical_equity_key(symbol: Any) -> str:
        """Convert plain ticker symbols to canonical equity instrument keys."""
        value = str(symbol).strip()
        if ":" in value:
            return value
        return f"equity:{value}"

    @classmethod
    def _expand_equity_keys(cls, symbols: list[str]) -> list[str]:
        """Expand symbol filters to include both canonical and legacy raw keys."""
        expanded: list[str] = []
        for symbol in symbols:
            raw = str(symbol).strip()
            if raw.lower().startswith("equity:"):
                candidates = (raw.split(":", 1)[1], raw)
            elif ":" in raw:
                candidates = (raw,)
            else:
                candidates = (raw, cls._canonical_equity_key(raw))
            for candidate in candidates:
                if candidate not in expanded:
                    expanded.append(candidate)
        return expanded

    @classmethod
    def _normalize_bar_instrument_keys(cls, bars: pd.DataFrame) -> pd.DataFrame:
        """Normalize bar instrument keys to canonical format for joins."""
        if bars.empty or "instrument_key" not in bars.columns:
            return bars
        normalized = bars.copy()
        normalized["instrument_key"] = normalized["instrument_key"].map(
            lambda value: cls._canonical_equity_key(value) if pd.notna(value) else value
        )
        return normalized

    @classmethod
    def _normalize_alert_underlyings(cls, alerts: pd.DataFrame) -> pd.DataFrame:
        """Normalize alert underlyings to canonical format for label joins."""
        if alerts.empty or "underlying" not in alerts.columns:
            return alerts
        normalized = alerts.copy()
        normalized["underlying"] = normalized["underlying"].map(
            lambda value: cls._canonical_equity_key(value) if pd.notna(value) else value
        )
        return normalized

    def run(
        self,
        start_date: datetime | str,
        end_date: datetime | str,
        use_intraday_bars: bool = False,
        use_contract_labels: bool = True,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Run the label computation pipeline.

        Args:
            start_date: Start of alert date range
            end_date: End of alert date range
            use_intraday_bars: Use 5-min bars for intraday alerts
            use_contract_labels: Fetch option bars and compute contract labels
            dry_run: If True, compute but don't write

        Returns:
            Dict with pipeline stats
        """
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        logger.info(
            "Starting alert labels pipeline",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            use_contract_labels=use_contract_labels,
        )

        # Step 1: Load flow alerts
        logger.info("Loading flow alerts from Silver")
        flow_alerts = self._load_flow_alerts(start_date, end_date)

        if flow_alerts.empty:
            logger.warning("No flow alerts found in date range")
            return {"status": "no_data", "alerts_count": 0}

        logger.info("Loaded flow alerts", count=len(flow_alerts))

        # Step 2: Get unique underlyings
        flow_alerts = self._normalize_alert_underlyings(flow_alerts)

        underlyings = flow_alerts["underlying"].dropna().unique().tolist()
        all_symbols = list(set(underlyings + [MARKET_PROXY, VIX_PROXY]))

        logger.info("Fetching bars for symbols", count=len(all_symbols))

        # Step 3: Load bars (with lookback for ATR)
        bar_start = start_date - timedelta(days=LOOKBACK_DAYS)
        bar_end = end_date + timedelta(days=30)
        daily_bars = self._load_bars(all_symbols, bar_start, bar_end)

        if daily_bars.empty:
            logger.error("No bars found for symbols")
            return {"status": "no_bars", "alerts_count": len(flow_alerts)}

        logger.info("Loaded daily bars", count=len(daily_bars))

        # Step 4: Extract SPY and UVXY bars
        spy_bars = daily_bars[daily_bars["instrument_key"] == MARKET_PROXY].copy()
        vix_bars = daily_bars[daily_bars["instrument_key"] == VIX_PROXY].copy()

        logger.info(
            "Market context data",
            spy_bars=len(spy_bars),
            uvxy_bars=len(vix_bars),
        )

        # Step 5: Compute underlying-based labels
        logger.info("Computing underlying barrier labels")

        if use_intraday_bars:
            intraday_bars = self._load_intraday_bars(all_symbols, bar_start, bar_end)
            labels = compute_multi_horizon_labels(
                flow_alerts,
                intraday_bars,
                daily_bars,
                spy_daily_bars_df=spy_bars,
                vix_daily_bars_df=vix_bars,
            )
        else:
            labels = compute_barrier_labels(
                flow_alerts,
                daily_bars,
                spy_bars_df=spy_bars,
                vix_bars_df=vix_bars,
                slippage_model=self.slippage_model,
            )

        logger.info("Computed underlying labels", count=len(labels))

        # Step 6: Fetch option bars and compute contract labels
        if use_contract_labels:
            self._require_gateway_api_key()
            logger.info("Fetching option bars from Data Gateway")
            labels = self._add_contract_labels(labels, flow_alerts, bar_start, bar_end)

        # Step 7: Prepare for Gold write
        labels = self._prepare_for_gold(labels)

        # Step 8: Write to Gold
        if not dry_run:
            output_path = self.client.write_gold(
                dataset=self.output_dataset,
                df=labels,
                project=self.project,
                version=self.version,
            )
            logger.info("Wrote labels to Gold", path=str(output_path))
        else:
            logger.info("Dry run - skipping write")
            output_path = None

        # Compute stats
        stats = self._compute_stats(labels)
        stats["alerts_count"] = len(flow_alerts)
        stats["labels_count"] = len(labels)
        stats["status"] = "success"
        stats["output_path"] = str(output_path) if output_path else None

        logger.info("Pipeline complete", **stats)

        return stats

    def _add_contract_labels(
        self,
        labels: pd.DataFrame,
        flow_alerts: pd.DataFrame,
        bar_start: datetime,
        bar_end: datetime,
    ) -> pd.DataFrame:
        """Add contract-based labels by fetching option bars.

        This fetches option price data from the Data Gateway and computes
        contract-level TP/SL barrier labels.
        """
        if "occ_symbol" not in flow_alerts.columns:
            logger.warning("No occ_symbol in flow alerts, skipping contract labels")
            return labels

        occ_symbols = flow_alerts["occ_symbol"].dropna().unique().tolist()
        if not occ_symbols:
            logger.warning("No OCC symbols found, skipping contract labels")
            return labels

        logger.info("Fetching option bars", contracts=len(occ_symbols))

        option_bars = asyncio.run(self._fetch_option_bars(occ_symbols, bar_start, bar_end))

        if option_bars.empty:
            logger.warning("No option bars returned from gateway")
            return self._add_empty_contract_columns(labels)

        logger.info("Fetched option bars", count=len(option_bars))

        contract_labels = [
            self._compute_single_contract_label(row, flow_alerts, option_bars) for _, row in labels.iterrows()
        ]

        contract_df = pd.DataFrame(contract_labels)
        for col in contract_df.columns:
            labels[col] = contract_df[col].values

        return labels

    def _add_empty_contract_columns(self, labels: pd.DataFrame) -> pd.DataFrame:
        """Add empty contract label columns to DataFrame."""
        for col in [
            "contract_hit_tp_first",
            "contract_mfe",
            "contract_mae",
            "contract_mfe_adj",
            "contract_mae_adj",
            "contract_bars_to_hit",
        ]:
            labels[col] = pd.NA
        return labels

    def _compute_single_contract_label(
        self,
        row: pd.Series,
        flow_alerts: pd.DataFrame,
        option_bars: pd.DataFrame,
    ) -> dict:
        """Compute contract barrier label for a single alert."""
        alert_id = row["alert_id"]
        occ_symbol = self._get_occ_symbol_for_alert(alert_id, flow_alerts)

        if occ_symbol is None or pd.isna(occ_symbol):
            return self._empty_contract_result()

        contract_bars = option_bars[option_bars["symbol"] == occ_symbol]
        if contract_bars.empty:
            return self._empty_contract_result()

        ts_alert = row["ts_alert"]
        entry_bars = contract_bars[contract_bars["timestamp"] <= ts_alert]
        if entry_bars.empty:
            return self._empty_contract_result()

        entry_price = entry_bars.iloc[-1]["close"]
        future_bars = contract_bars[contract_bars["timestamp"] > ts_alert].head(self.contract_config.max_window_bars)

        if future_bars.empty:
            return self._empty_contract_result()

        price_path = future_bars["close"].values
        return _compute_contract_barrier_outcome(price_path, entry_price, self.contract_config, self.slippage_model)

    def _get_occ_symbol_for_alert(self, alert_id: str, flow_alerts: pd.DataFrame) -> str | None:
        """Look up OCC symbol for an alert ID."""
        if alert_id not in flow_alerts["event_id"].values:
            return None
        return flow_alerts.loc[flow_alerts["event_id"] == alert_id, "occ_symbol"].iloc[0]

    async def _fetch_option_bars(
        self,
        occ_symbols: list[str],
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """Fetch option bars from the Data Gateway API."""
        gateway_key = self._require_gateway_api_key()
        auth_headers = gateway_auth_headers(gateway_key)
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Batch symbols (API may have limits)
                batch_size = 100
                all_bars = []

                for i in range(0, len(occ_symbols), batch_size):
                    batch = occ_symbols[i : i + batch_size]
                    symbols_param = ",".join(batch)

                    response = await client.get(
                        f"{self.gateway_url}/api/v1/alpaca/options/bars",
                        headers=auth_headers,
                        params={
                            "symbols": symbols_param,
                            "timeframe": "1Day",
                            "start": start.isoformat(),
                            "end": end.isoformat(),
                        },
                    )

                    if response.status_code == 200:
                        data = response.json()
                        bars = data.get("data", {}).get("bars", [])
                        all_bars.extend(bars)
                    else:
                        logger.warning(
                            "Option bars request failed",
                            status=response.status_code,
                            batch_size=len(batch),
                        )

                if not all_bars:
                    return pd.DataFrame()

                df = pd.DataFrame(all_bars)
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                return df

        except Exception as e:
            logger.error("Failed to fetch option bars", error=str(e))
            return pd.DataFrame()

    def _empty_contract_result(self) -> dict:
        """Return empty contract label result."""
        return {
            "contract_hit_tp_first": pd.NA,
            "contract_mfe": pd.NA,
            "contract_mae": pd.NA,
            "contract_mfe_adj": pd.NA,
            "contract_mae_adj": pd.NA,
            "contract_bars_to_hit": pd.NA,
        }

    def _load_flow_alerts(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Load flow alerts from Silver."""
        alerts = self.client.read_silver(
            dataset="flow_alerts",
            time_range=(start_date, end_date),
        )

        if alerts.empty:
            return alerts

        # Ensure required columns
        required = ["event_id", "underlying", "ts_event", "put_call", "expiry"]
        missing = set(required) - set(alerts.columns)
        if missing:
            column_map = {
                "event_id": "id",
                "underlying": "ticker",
            }
            for new_col, old_col in column_map.items():
                if new_col in missing and old_col in alerts.columns:
                    alerts[new_col] = alerts[old_col]

        return alerts

    def _load_bars(
        self,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Load daily bars from Silver."""
        bars = self.client.read_silver(
            dataset="bars",
            time_range=(start_date, end_date),
            instrument_keys=self._expand_equity_keys(symbols),
        )

        # Keep joins stable even when historical files used raw tickers.
        bars = self._normalize_bar_instrument_keys(bars)

        if bars.empty or "timeframe" not in bars.columns:
            return bars

        timeframe = bars["timeframe"].astype(str).str.lower().str.replace(" ", "", regex=False)
        daily = bars[timeframe.isin({"1day", "1d", "day"})]
        return daily if not daily.empty else bars

    def _load_intraday_bars(
        self,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pd.DataFrame:
        """Load intraday (5-min) bars from Silver."""
        bars = self.client.read_silver(
            dataset="bars",
            time_range=(start_date, end_date),
            instrument_keys=self._expand_equity_keys(symbols),
        )

        bars = self._normalize_bar_instrument_keys(bars)

        if bars.empty:
            logger.warning("No intraday bars found, using daily bars")
            return self._load_bars(symbols, start_date, end_date)

        if "timeframe" not in bars.columns:
            logger.warning("Bars missing timeframe column, using daily bars")
            return self._load_bars(symbols, start_date, end_date)

        timeframe = bars["timeframe"].astype(str).str.lower().str.replace(" ", "", regex=False)
        intraday = bars[timeframe.isin({"5min", "5m"})]
        if intraday.empty:
            logger.warning("No 5-minute bars found, using daily bars")
            return self._load_bars(symbols, start_date, end_date)

        return intraday

    def _prepare_for_gold(self, labels: pd.DataFrame) -> pd.DataFrame:
        """Prepare labels DataFrame for Gold write."""
        df = labels.copy()

        if "instrument_key" not in df.columns:
            df["instrument_key"] = df["alert_id"]

        if "ts_event" not in df.columns and "ts_alert" in df.columns:
            df["ts_event"] = df["ts_alert"]

        if "ts_available" not in df.columns:
            df["ts_available"] = df["ts_event"] + timedelta(days=1)

        # Drop rows with no labels at all
        label_cols = ["hit_tp_first", "contract_hit_tp_first"]
        existing_cols = [c for c in label_cols if c in df.columns]
        if existing_cols:
            df = df.dropna(subset=existing_cols, how="all")

        return df

    def _compute_stats(self, labels: pd.DataFrame) -> dict[str, Any]:
        """Compute summary statistics from labels."""
        stats = {}

        # Underlying stats
        if "hit_tp_first" in labels.columns:
            valid = labels.dropna(subset=["hit_tp_first"])
            if not valid.empty:
                stats["underlying_win_rate"] = float(valid["hit_tp_first"].mean())
                stats["avg_mfe"] = float(valid["mfe"].mean()) if "mfe" in valid else None
                stats["by_horizon"] = valid.groupby("horizon")["hit_tp_first"].mean().to_dict()

        # Contract stats
        if "contract_hit_tp_first" in labels.columns:
            valid = labels.dropna(subset=["contract_hit_tp_first"])
            if not valid.empty:
                stats["contract_win_rate"] = float(valid["contract_hit_tp_first"].mean())
                stats["contract_avg_mfe"] = float(valid["contract_mfe"].mean()) if "contract_mfe" in valid else None

        return stats


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Compute alert barrier labels")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--intraday", action="store_true", help="Use intraday bars")
    parser.add_argument("--no-contract", action="store_true", help="Skip contract labels")
    parser.add_argument("--dry-run", action="store_true", help="Don't write output")
    parser.add_argument("--output", default="labels_alert_barriers", help="Output dataset name")
    parser.add_argument("--project", default="quant", help="Project name")
    parser.add_argument("--version", default="v1", help="Version string")
    parser.add_argument("--gateway-url", default=DATA_GATEWAY_URL, help="Data Gateway URL")

    args = parser.parse_args()

    pipeline = AlertLabelsPipeline(
        output_dataset=args.output,
        project=args.project,
        version=args.version,
        gateway_url=args.gateway_url,
    )

    stats = pipeline.run(
        start_date=args.start,
        end_date=args.end,
        use_intraday_bars=args.intraday,
        use_contract_labels=not args.no_contract,
        dry_run=args.dry_run,
    )

    print(f"\nPipeline complete: {stats['status']}")
    print(f"  Alerts: {stats.get('alerts_count', 0)}")
    print(f"  Labels: {stats.get('labels_count', 0)}")
    if stats.get("underlying_win_rate") is not None:
        print(f"  Underlying win rate: {stats['underlying_win_rate']:.1%}")
    if stats.get("contract_win_rate") is not None:
        print(f"  Contract win rate: {stats['contract_win_rate']:.1%}")
    if stats.get("output_path"):
        print(f"  Output: {stats['output_path']}")


if __name__ == "__main__":
    main()
