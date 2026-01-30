"""Meta-Label Inference Service.

Scores new alerts with trained meta-model to predict success probability.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from heber.ml.trainer import MetaModelTrainer
    from heber.models.silver import FlowAlertRecord

logger = structlog.get_logger(__name__)


@dataclass
class InferenceConfig:
    """Configuration for inference service."""

    # Model path
    model_path: Path | None = None

    # Score thresholds
    high_confidence_threshold: float = 0.7
    low_confidence_threshold: float = 0.3

    # Cache settings
    cache_ttl_seconds: int = 300  # 5 min cache for scores

    # Feature extraction
    gateway_url: str = "http://localhost:8000"


class MetaLabelScorer:
    """Scores alerts with trained meta-model.

    Can be used standalone or integrated into AlertWatchConsumer
    to filter/prioritize alerts based on predicted success probability.
    """

    def __init__(
        self,
        config: InferenceConfig | None = None,
        model: MetaModelTrainer | None = None,
        redis: Redis | None = None,
    ):
        """Initialize scorer.

        Args:
            config: Inference configuration
            model: Pre-loaded model (or load from config.model_path)
            redis: Redis client for caching
        """
        self.config = config or InferenceConfig()
        self.redis = redis
        self._model = model
        self._feature_extractor = None

    async def initialize(self) -> None:
        """Load model and feature extractor."""
        if self._model is None and self.config.model_path:
            from heber.ml.trainer import MetaModelTrainer

            self._model = MetaModelTrainer.load(self.config.model_path)
            logger.info("Loaded meta-model", path=str(self.config.model_path))

        # Initialize feature extractor
        from heber.watch.features import AlertFeatureExtractor

        self._feature_extractor = AlertFeatureExtractor(
            redis=self.redis,
            gateway_url=self.config.gateway_url,
        )

        logger.info("Inference service initialized")

    async def score(self, alert: FlowAlertRecord) -> float | None:
        """Score a single alert.

        Args:
            alert: Flow alert record to score

        Returns:
            Probability of TP hit (0-1), or None if scoring fails
        """
        if self._model is None:
            logger.warning("Model not loaded, cannot score")
            return None

        try:
            # Check cache first
            if self.redis:
                cached = await self._get_cached_score(alert.event_id)
                if cached is not None:
                    return cached

            # Extract features
            features = await self._feature_extractor.extract(alert)

            # Get feature vector
            feature_array = features.to_feature_array()

            # Predict
            import numpy as np

            x = np.array([feature_array])
            score = float(self._model.predict_proba(x)[0])

            # Cache result
            if self.redis:
                await self._cache_score(alert.event_id, score)

            logger.debug(
                "Scored alert",
                alert_id=alert.event_id,
                score=score,
                symbol=alert.underlying,
            )

            return score

        except Exception as e:
            logger.error(
                "Failed to score alert",
                alert_id=alert.event_id,
                error=str(e),
                exc_info=True,
            )
            return None

    async def score_batch(
        self,
        alerts: list[FlowAlertRecord],
    ) -> dict[str, float | None]:
        """Score multiple alerts.

        Args:
            alerts: List of alerts to score

        Returns:
            Dict mapping alert_id to score (or None if failed)
        """
        results = {}
        tasks = [self.score(alert) for alert in alerts]
        scores = await asyncio.gather(*tasks, return_exceptions=True)

        for alert, score in zip(alerts, scores, strict=False):
            if isinstance(score, Exception):
                results[alert.event_id] = None
            else:
                results[alert.event_id] = score

        return results

    def classify(
        self,
        score: float,
    ) -> str:
        """Classify score into confidence bucket.

        Args:
            score: Probability score

        Returns:
            "high", "medium", or "low" confidence
        """
        if score >= self.config.high_confidence_threshold:
            return "high"
        elif score >= self.config.low_confidence_threshold:
            return "medium"
        else:
            return "low"

    async def score_and_filter(
        self,
        alerts: list[FlowAlertRecord],
        min_score: float = 0.5,
    ) -> list[tuple[FlowAlertRecord, float]]:
        """Score alerts and filter by minimum threshold.

        Args:
            alerts: Alerts to evaluate
            min_score: Minimum score to include

        Returns:
            List of (alert, score) tuples above threshold, sorted by score
        """
        scores = await self.score_batch(alerts)

        results = []
        for alert in alerts:
            score = scores.get(alert.event_id)
            if score is not None and score >= min_score:
                results.append((alert, score))

        # Sort by score descending
        results.sort(key=lambda x: x[1], reverse=True)

        logger.info(
            "Filtered alerts by score",
            total=len(alerts),
            passed=len(results),
            min_score=min_score,
        )

        return results

    async def _get_cached_score(self, alert_id: str) -> float | None:
        """Get cached score from Redis."""
        if not self.redis:
            return None

        key = f"heber:meta_score:{alert_id}"
        cached = await self.redis.get(key)

        if cached:
            return float(cached)
        return None

    async def _cache_score(self, alert_id: str, score: float) -> None:
        """Cache score in Redis."""
        if not self.redis:
            return

        key = f"heber:meta_score:{alert_id}"
        await self.redis.set(key, str(score), ex=self.config.cache_ttl_seconds)


class AlertGate:
    """Gate that uses meta-model to filter alerts before watch creation.

    Can be integrated into AlertWatchConsumer to only create watches
    for high-probability alerts.
    """

    def __init__(
        self,
        scorer: MetaLabelScorer,
        min_score: float = 0.5,
        enabled: bool = True,
    ):
        """Initialize gate.

        Args:
            scorer: MetaLabelScorer instance
            min_score: Minimum score to allow through gate
            enabled: Whether gate is active (if False, all alerts pass)
        """
        self.scorer = scorer
        self.min_score = min_score
        self.enabled = enabled

    async def should_create_watch(self, alert: FlowAlertRecord) -> bool:
        """Determine if watch should be created for this alert.

        Args:
            alert: Flow alert to evaluate

        Returns:
            True if watch should be created
        """
        if not self.enabled:
            return True

        score = await self.scorer.score(alert)

        if score is None:
            # On failure, default to allowing (fail-open)
            logger.warning(
                "Gate scoring failed, allowing alert",
                alert_id=alert.event_id,
            )
            return True

        passed = score >= self.min_score
        logger.info(
            "Gate decision",
            alert_id=alert.event_id,
            score=score,
            threshold=self.min_score,
            passed=passed,
        )

        return passed
