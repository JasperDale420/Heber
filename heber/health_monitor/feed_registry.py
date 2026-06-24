"""Must-flow feed registry for the critical-feed liveness alarm.

Two cadence classes:
  - continuous: rows must keep landing during an active ET window.
  - daily: today's partition must exist by an ET deadline.

Floors are absolute (not a rolling baseline) so a slowly-degrading or
chronically-low feed is still flagged. Override floors via
``HEBER_ALERT_FLOOR_OVERRIDES`` (JSON map feed -> floor); floor 0 disables a feed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class FeedRule:
    feed: str
    kind: Literal["continuous", "daily"]
    window_start_et: str  # "HH:MM" — active-window start (continuous); ignored for daily
    window_end_et: str  # "HH:MM" — active-window end (continuous) / deadline (daily)
    lookback_minutes: int  # sliding window for continuous; ignored for daily
    floor: int  # min rows required in the window (continuous) / by deadline (daily)


DEFAULT_REGISTRY: list[FeedRule] = [
    FeedRule("flow_alerts", "continuous", "09:30", "16:00", 60, 1),
    # Darkpool prints flow from the open through after-hours (~19:00 ET), but never
    # pre-market: a 04:00 start produced ~100 false "feed appears dark" criticals per
    # pre-market hour. Start at the open; keep the after-hours tail to 20:00.
    FeedRule("darkpool", "continuous", "09:30", "20:00", 60, 1),
    FeedRule("bars", "continuous", "09:30", "16:00", 30, 1),
    FeedRule("trades", "continuous", "09:30", "16:00", 30, 1),
    FeedRule("oi_change", "daily", "", "17:30", 0, 1),
    FeedRule("greek_exposure", "daily", "", "17:30", 0, 1),
]


def resolved_registry(floor_overrides: dict[str, int]) -> list[FeedRule]:
    """Apply per-feed floor overrides. Floor 0 disables a feed (drops it)."""
    rules: list[FeedRule] = []
    for rule in DEFAULT_REGISTRY:
        if rule.feed in floor_overrides:
            new_floor = floor_overrides[rule.feed]
            if new_floor <= 0:
                continue
            rule = FeedRule(
                rule.feed,
                rule.kind,
                rule.window_start_et,
                rule.window_end_et,
                rule.lookback_minutes,
                new_floor,
            )
        rules.append(rule)
    return rules
