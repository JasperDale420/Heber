"""Alert Feature Extraction for Meta-Labeling.

Captures features at alert time for training meta-models that predict
which flow alerts will hit their target price before stop loss.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import polars as pl
import structlog

from heber.config import settings
from heber.ml.datasets import persist_features_to_gold as persist_features_frame_to_gold
from heber.watch.gateway import gateway_url_candidates

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from heber.models.silver import FlowAlertRecord

logger = structlog.get_logger(__name__)


@dataclass
class AlertFeatures:
    """Features captured at alert arrival time for meta-model training.

    These features must only use information available at the moment
    the alert is received - no lookahead allowed.
    """

    # Identifiers
    alert_id: str
    alert_time: datetime
    symbol: str

    # Contract info
    occ_symbol: str | None
    underlying: str
    strike: float
    expiry: date
    put_call: str  # C or P
    days_to_expiry: int

    # Alert characteristics
    premium: float
    volume: float
    open_interest: float | None
    volume_oi_ratio: float | None
    alert_type: str  # SWEEP, BLOCK, etc
    side: str | None  # bid/ask/mid
    aggressor: str | None

    # Prices at alert time
    spot_price: float | None  # Underlying price
    contract_price: float | None  # Option mid

    # Moneyness (requires spot)
    moneyness: float | None = None  # strike / spot (>1 = OTM for calls)
    log_moneyness: float | None = None

    # Greeks (if available from alert or lookup)
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    iv: float | None = None

    # Market context (from underlying quotes/bars)
    underlying_30d_return: float | None = None
    underlying_5d_return: float | None = None
    underlying_1d_return: float | None = None
    realized_vol_20d: float | None = None
    iv_rank: float | None = None  # IV percentile over past year

    # Timing features
    hour_of_day: int = 0
    minute_of_hour: int = 0
    day_of_week: int = 0  # 0=Monday
    minutes_since_open: int = 0
    minutes_to_close: int = 0

    # Tags/sentiment (from UW)
    is_bullish: int = 0
    is_bearish: int = 0
    is_sweep: int = 0
    is_block: int = 0
    is_unusual: int = 0

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        result = {}
        for k, v in self.__dict__.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, date):
                result[k] = v.isoformat()
            else:
                result[k] = v
        return result

    @classmethod
    def from_dict(cls, data: dict) -> AlertFeatures:
        """Reconstruct from dictionary."""
        parsed_data = dict(data)
        # Convert date/time strings back
        if isinstance(parsed_data.get("alert_time"), str):
            alert_time = datetime.fromisoformat(parsed_data["alert_time"])
            if alert_time.tzinfo is None:
                alert_time = alert_time.replace(tzinfo=UTC)
            else:
                alert_time = alert_time.astimezone(UTC)
            parsed_data["alert_time"] = alert_time
        if isinstance(parsed_data.get("expiry"), str):
            parsed_data["expiry"] = date.fromisoformat(parsed_data["expiry"])
        return cls(**parsed_data)

    def to_feature_array(self, feature_names: list[str] | None = None) -> list[float]:
        """Convert to numeric array for model input.

        Args:
            feature_names: Specific features to include. If None, uses all numeric.

        Returns:
            List of float values for model input.
        """
        if feature_names is None:
            feature_names = self.numeric_feature_names()

        values = []
        for name in feature_names:
            val = getattr(self, name, None)
            # Convert None to 0 or NaN handling as needed
            if val is None:
                values.append(0.0)
            elif isinstance(val, bool):
                values.append(float(val))
            elif isinstance(val, int | float):
                values.append(float(val))
            else:
                values.append(0.0)
        return values

    @classmethod
    def numeric_feature_names(cls) -> list[str]:
        """Return list of numeric feature names for model training."""
        return [
            "days_to_expiry",
            "premium",
            "volume",
            "open_interest",
            "volume_oi_ratio",
            "spot_price",
            "contract_price",
            "moneyness",
            "log_moneyness",
            "delta",
            "gamma",
            "theta",
            "vega",
            "iv",
            "underlying_30d_return",
            "underlying_5d_return",
            "underlying_1d_return",
            "realized_vol_20d",
            "iv_rank",
            "hour_of_day",
            "minute_of_hour",
            "day_of_week",
            "minutes_since_open",
            "minutes_to_close",
            "is_bullish",
            "is_bearish",
            "is_sweep",
            "is_block",
            "is_unusual",
        ]


class AlertFeatureExtractor:
    """Extracts features from flow alerts at arrival time.

    Integrates with market data sources to enrich alerts with
    context features (returns, volatility, IV rank, etc.).
    """

    # Market hours (Eastern Time)
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MIN = 30
    MARKET_CLOSE_HOUR = 16
    MARKET_CLOSE_MIN = 0
    MARKET_TIMEZONE = ZoneInfo("America/New_York")

    def __init__(
        self,
        redis: Redis | None = None,
        gateway_url: str | None = None,
    ):
        """Initialize extractor.

        Args:
            redis: Redis client for caching/lookups.
            gateway_url: Data Gateway URL for market data enrichment.
        """
        self.redis = redis
        self.gateway_url = gateway_url

    async def extract(self, alert: FlowAlertRecord) -> AlertFeatures:
        """Extract features from a flow alert.

        Args:
            alert: The flow alert record.

        Returns:
            AlertFeatures with all available features populated.
        """
        import math

        # Parse alert time
        alert_time = alert.ts_event
        market_time = self._to_market_time(alert_time)

        # Compute days to expiry
        dte = (alert.expiry - alert_time.date()).days

        # Compute volume/OI ratio
        vol_oi = None
        if alert.open_interest and alert.open_interest > 0:
            vol_oi = alert.volume / alert.open_interest

        # Compute moneyness
        moneyness = None
        log_moneyness = None
        if alert.spot_px and alert.spot_px > 0:
            moneyness = alert.strike / alert.spot_px
            if moneyness > 0:
                log_moneyness = math.log(moneyness)

        # Extract timing features
        hour = market_time.hour
        minute = market_time.minute
        day_of_week = market_time.weekday()

        # Minutes since open (9:30 AM ET)
        market_open_minutes = self.MARKET_OPEN_HOUR * 60 + self.MARKET_OPEN_MIN
        current_minutes = hour * 60 + minute
        mins_since_open = max(0, current_minutes - market_open_minutes)

        # Minutes to close (4:00 PM ET)
        market_close_minutes = self.MARKET_CLOSE_HOUR * 60 + self.MARKET_CLOSE_MIN
        mins_to_close = max(0, market_close_minutes - current_minutes)

        # Parse tags for sentiment/type features
        tags = alert.tags or []
        tags_lower = [t.lower() for t in tags]

        is_bullish = 1 if "bullish" in tags_lower else 0
        is_bearish = 1 if "bearish" in tags_lower else 0
        is_unusual = 1 if "unusual" in tags_lower else 0

        alert_type = (alert.alert_type or "").upper()
        is_sweep = 1 if alert_type == "SWEEP" or "sweep" in tags_lower else 0
        is_block = 1 if alert_type == "BLOCK" or "block" in tags_lower else 0

        features = AlertFeatures(
            # Identifiers
            alert_id=alert.event_id,
            alert_time=alert_time,
            symbol=alert.underlying,
            # Contract
            occ_symbol=alert.occ_symbol,
            underlying=alert.underlying,
            strike=alert.strike,
            expiry=alert.expiry,
            put_call=alert.put_call,
            days_to_expiry=dte,
            # Alert chars
            premium=alert.premium,
            volume=alert.volume,
            open_interest=alert.open_interest,
            volume_oi_ratio=vol_oi,
            alert_type=alert.alert_type,
            side=alert.side,
            aggressor=alert.aggressor,
            # Prices
            spot_price=alert.spot_px,
            contract_price=alert.contract_px,
            moneyness=moneyness,
            log_moneyness=log_moneyness,
            # Timing
            hour_of_day=hour,
            minute_of_hour=minute,
            day_of_week=day_of_week,
            minutes_since_open=mins_since_open,
            minutes_to_close=mins_to_close,
            # Sentiment/type
            is_bullish=is_bullish,
            is_bearish=is_bearish,
            is_sweep=is_sweep,
            is_block=is_block,
            is_unusual=is_unusual,
        )

        # Enrich with market context (async lookups)
        features = await self._enrich_market_context(features)

        # Enrich with Greeks from Alpaca option chain
        features = await self._enrich_greeks(features)

        # Enrich with IV rank from UW
        features = await self._enrich_iv_rank(features)

        return features

    def _to_market_time(self, dt: datetime) -> datetime:
        """Normalize timestamps to market timezone for time-of-day features.

        Naive datetimes are treated as UTC to match calendar normalization.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(self.MARKET_TIMEZONE)

    @staticmethod
    def _coerce_optional_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def _enrich_iv_rank(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with IV rank from Unusual Whales.

        IV rank indicates where current IV is relative to the past year's range.
        0 = lowest IV of the year, 100 = highest IV of the year.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping IV rank enrichment")
            return features

        try:
            import httpx

            routes = gateway_url_candidates(
                self.gateway_url,
                f"/uw/options/{features.underlying}/iv-rank",
            )
            data: dict | None = None

            async with httpx.AsyncClient(timeout=10.0) as client:
                last_status: int | None = None
                for route in routes:
                    response = await client.get(route)
                    last_status = response.status_code
                    if response.status_code == 200:
                        data = response.json()
                        break
                if data is None:
                    logger.debug(
                        "Failed to fetch IV rank",
                        symbol=features.underlying,
                        status=last_status,
                        routes=routes,
                    )
                    return features

            # Extract IV rank from response
            iv_data = data.get("data", {})
            if iv_data and iv_data.get("iv_rank") is not None:
                features.iv_rank = float(iv_data["iv_rank"])
                logger.debug(
                    "Enriched IV rank",
                    symbol=features.underlying,
                    iv_rank=features.iv_rank,
                )

        except Exception as e:
            logger.warning(
                "Failed to enrich IV rank",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    async def _enrich_greeks(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with Greeks from Alpaca option chain.

        Fetches option chain for the specific strike/expiry/type and extracts
        delta, gamma, theta, vega, and IV.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping Greeks enrichment")
            return features

        try:
            import httpx

            routes = gateway_url_candidates(
                self.gateway_url,
                f"/alpaca/options/chain/{features.underlying}",
            )
            params = {
                "expiration_date": features.expiry.isoformat(),
                "strike_price_gte": features.strike - 0.01,
                "strike_price_lte": features.strike + 0.01,
                "option_type": "call" if features.put_call == "C" else "put",
            }
            data: dict | None = None

            async with httpx.AsyncClient(timeout=10.0) as client:
                last_status: int | None = None
                for route in routes:
                    response = await client.get(route, params=params)
                    last_status = response.status_code
                    if response.status_code == 200:
                        data = response.json()
                        break
                if data is None:
                    logger.debug(
                        "Failed to fetch option chain for Greeks",
                        symbol=features.underlying,
                        status=last_status,
                        routes=routes,
                    )
                    return features

            # Extract contracts from response
            contracts = data.get("data", {}).get("contracts", [])
            if not contracts:
                logger.debug("No contracts found for Greeks", symbol=features.underlying)
                return features

            # Find matching contract by strike
            contract = None
            for c in contracts:
                strike_price = self._coerce_optional_float(c.get("strike_price"))
                if strike_price is None:
                    continue
                if abs(strike_price - features.strike) < 0.01:
                    contract = c
                    break

            if not contract:
                for c in contracts:
                    if self._coerce_optional_float(c.get("strike_price")) is not None:
                        contract = c
                        break
            if not contract:
                logger.debug("No valid contracts found for Greeks", symbol=features.underlying)
                return features

            # Extract Greeks
            delta = contract.get("delta")
            gamma = contract.get("gamma")
            theta = contract.get("theta")
            vega = contract.get("vega")
            implied_vol = contract.get("implied_volatility")

            features.delta = self._coerce_optional_float(delta)
            features.gamma = self._coerce_optional_float(gamma)
            features.theta = self._coerce_optional_float(theta)
            features.vega = self._coerce_optional_float(vega)
            features.iv = self._coerce_optional_float(implied_vol)

            logger.debug(
                "Enriched Greeks",
                symbol=features.underlying,
                delta=features.delta,
                gamma=features.gamma,
                iv=features.iv,
            )

        except Exception as e:
            logger.warning(
                "Failed to enrich Greeks",
                error=str(e),
                underlying=features.underlying,
            )

        return features

    async def _enrich_market_context(self, features: AlertFeatures) -> AlertFeatures:
        """Enrich features with market context data from Data Gateway.

        Fetches recent bars for underlying from Alpaca via Data Gateway,
        then computes:
        - 1d, 5d, 30d returns
        - 20d realized volatility

        Note: IV rank requires options analytics not yet available.
        """
        if not self.gateway_url:
            logger.debug("No gateway URL, skipping market enrichment")
            return features

        try:
            import math
            from datetime import timedelta

            import httpx

            symbol = features.underlying
            end_date = features.alert_time.date()
            # Fetch 35 days to ensure we have 30 trading days
            start_date = end_date - timedelta(days=50)

            routes = gateway_url_candidates(
                self.gateway_url,
                "/alpaca/stocks/bars",
            )
            params = {
                "symbol": symbol,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "timeframe": "1Day",
                "limit": 50,
            }
            data: dict | None = None

            async with httpx.AsyncClient(timeout=10.0) as client:
                last_status: int | None = None
                for route in routes:
                    response = await client.get(route, params=params)
                    last_status = response.status_code
                    if response.status_code == 200:
                        data = response.json()
                        break
                if data is None:
                    logger.warning(
                        "Failed to fetch bars for enrichment",
                        symbol=symbol,
                        status=last_status,
                        routes=routes,
                    )
                    return features

            # Extract bars from response
            bars = data.get("data", {}).get("bars", [])
            if not bars or len(bars) < 2:
                logger.debug("Insufficient bars for enrichment", symbol=symbol, count=len(bars))
                return features

            # Sort by timestamp descending (most recent first)
            bars = sorted(bars, key=lambda b: b.get("t", ""), reverse=True)

            # Preserve day alignment (including zero closes) so return horizons
            # do not silently skip invalid days and shift to older bars.
            closes: list[float | None] = []
            for bar in bars:
                close_raw = bar.get("c")
                if close_raw is None:
                    closes.append(None)
                    continue
                try:
                    closes.append(float(close_raw))
                except (TypeError, ValueError):
                    closes.append(None)

            if len(closes) < 2:
                return features

            # Compute returns
            current_close = closes[0]
            if current_close is None or current_close <= 0:
                return features

            # 1-day return
            if len(closes) >= 2 and closes[1] is not None and closes[1] > 0:
                features.underlying_1d_return = (current_close / closes[1]) - 1.0

            # 5-day return
            if len(closes) >= 6 and closes[5] is not None and closes[5] > 0:
                features.underlying_5d_return = (current_close / closes[5]) - 1.0

            # 30-day return
            if len(closes) >= 31 and closes[30] is not None and closes[30] > 0:
                features.underlying_30d_return = (current_close / closes[30]) - 1.0

            # 20-day realized volatility (annualized)
            if len(closes) >= 21:
                daily_returns = []
                for i in range(20):
                    if closes[i] is not None and closes[i + 1] is not None and closes[i] > 0 and closes[i + 1] > 0:
                        daily_returns.append(math.log(closes[i] / closes[i + 1]))

                if daily_returns:
                    mean_return = sum(daily_returns) / len(daily_returns)
                    variance = sum((r - mean_return) ** 2 for r in daily_returns) / len(daily_returns)
                    daily_vol = math.sqrt(variance)
                    features.realized_vol_20d = daily_vol * math.sqrt(252)  # Annualize

            logger.debug(
                "Enriched market context",
                symbol=symbol,
                return_1d=features.underlying_1d_return,
                return_5d=features.underlying_5d_return,
                return_30d=features.underlying_30d_return,
                vol_20d=features.realized_vol_20d,
            )

        except Exception as e:
            logger.warning(
                "Failed to enrich market context",
                error=str(e),
                underlying=features.underlying,
            )

        return features


# Redis key pattern for feature storage
FEATURES_KEY = "heber:watch:features:{alert_id}"
DEFAULT_FEATURES_OUTPUT_PATH = settings.gold_path / "dataset=meta_label_features" / "project=watch" / "version=v1"


async def store_features(redis: Redis, features: AlertFeatures) -> None:
    """Store extracted features in Redis.

    Args:
        redis: Redis client.
        features: Extracted features.
    """
    import json

    key = FEATURES_KEY.format(alert_id=features.alert_id)
    await redis.set(key, json.dumps(features.to_dict()), ex=86400 * 7)  # 7 day TTL


async def get_features(redis: Redis, alert_id: str) -> AlertFeatures | None:
    """Retrieve stored features from Redis.

    Args:
        redis: Redis client.
        alert_id: Alert identifier.

    Returns:
        AlertFeatures if found, None otherwise.
    """
    import json

    key = FEATURES_KEY.format(alert_id=alert_id)
    data = await redis.get(key)
    if data is None:
        return None
    return AlertFeatures.from_dict(json.loads(data))


def persist_features_to_gold(features: AlertFeatures, output_path: Path | None = None) -> None:
    """Persist one feature row into Gold meta-label feature partitions."""
    row = {k: v for k, v in features.__dict__.items()}
    features_df = pl.DataFrame([row])
    persist_features_frame_to_gold(
        features_df=features_df,
        output_path=output_path or DEFAULT_FEATURES_OUTPUT_PATH,
        partition_col="alert_time",
    )
