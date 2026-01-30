"""Alert Watch Service - Real-time tracking of flow alert outcomes.

This package provides streaming infrastructure for tracking option contract
outcomes after flow alerts fire. Instead of relying on historical data,
it polls quotes in real-time to determine if TP/SL barriers are hit.
"""

from heber.watch.checker import BarrierChecker, outcome_to_label_row
from heber.watch.consumer import AlertWatchConsumer
from heber.watch.features import AlertFeatureExtractor, AlertFeatures, get_features, store_features
from heber.watch.manager import WatchManager
from heber.watch.models import (
    POLL_CONFIG,
    AlertWatch,
    WatchHorizon,
    WatchKeys,
    WatchOutcome,
    WatchSnapshot,
    WatchStatus,
)
from heber.watch.poller import SnapshotPoller
from heber.watch.writer import LabelWriter, WatchService, run_watch_service

__all__ = [
    # Models
    "AlertWatch",
    "WatchSnapshot",
    "WatchOutcome",
    "WatchStatus",
    "WatchHorizon",
    "WatchKeys",
    "POLL_CONFIG",
    # Features (meta-labeling)
    "AlertFeatures",
    "AlertFeatureExtractor",
    "store_features",
    "get_features",
    # Services
    "WatchManager",
    "SnapshotPoller",
    "BarrierChecker",
    "AlertWatchConsumer",
    "LabelWriter",
    "WatchService",
    # Functions
    "outcome_to_label_row",
    "run_watch_service",
]
