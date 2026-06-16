# Labeling Strategy for Financial ML

Reference documentation for Heber's approach to labeling flow alerts and options data.

## Triple-Barrier Method

The triple-barrier method (Lopez de Prado, 2018) assigns labels based on which barrier price hits first:

| Barrier | Condition | Label |
|---------|-----------|-------|
| **Upper (TP)** | Price rises +X% | +1 (success) |
| **Lower (SL)** | Price falls -X% | -1 (failure) |
| **Time Horizon** | Neither hit by deadline | 0 (expired) |

### Advantages Over Fixed-Horizon Returns

- **Captures path**: Fixed-horizon only sees endpoint; barriers capture the journey
- **Volatility-aware**: Barrier distances can scale with volatility
- **Risk-aligned**: Labels reflect actual trading outcomes (TP/SL hits)

### Heber Implementation

```python
# heber/watch/checker.py
class BarrierChecker:
    def check_all(self, watch: AlertWatch, snapshot: WatchSnapshot) -> WatchOutcome | None:
        # Upper barrier (TP)
        if snapshot.mark >= watch.target_price:
            return WatchOutcome(outcome="TP_HIT", ...)

        # Lower barrier (SL)
        if snapshot.mark <= watch.stop_price:
            return WatchOutcome(outcome="SL_HIT", ...)

        # Time barrier
        if now >= watch.window_end:
            return WatchOutcome(outcome="EXPIRED", ...)
```

## Meta-Labeling

Meta-labeling is a **two-stage approach** that improves precision without sacrificing recall.

### The Core Insight

Most ML classifiers face a recall/precision trade-off. In trading:

- High recall = catch opportunities, but many false positives (losing trades)
- High precision = fewer losses, but miss opportunities

Meta-labeling separates these concerns into two models.

### Two-Stage Architecture

```
Stage 1: Primary Model (or Signal)
├── Goal: High recall - catch all potential opportunities
├── Output: Direction prediction (+1 or -1)
└── Example: Flow alert signals from Unusual Whales

Stage 2: Meta Model (Filter)
├── Goal: High precision - filter out false positives
├── Input: Features + primary model's prediction
├── Output: P(primary prediction is correct)
└── Action: Only trade when P > threshold
```

### Meta-Label Construction

For each observation where the primary model makes a prediction:

| Primary Says | Actual Outcome | Meta-Label |
|--------------|----------------|------------|
| +1 (bullish) | Price up | 1 (true positive) |
| +1 (bullish) | Price down | 0 (false positive) |
| -1 (bearish) | Price down | 1 (true positive) |
| -1 (bearish) | Price up | 0 (false positive) |

The meta-model learns to distinguish reliable signals from noise.

### Application to Flow Alerts

Flow alerts serve as the **primary signal**:

- Unusual options activity detected
- Inherent direction from sweep/block characteristics
- High recall by design (flags all unusual activity)

The watch service provides **meta-labels**:

- `TP_HIT` = primary signal was correct → meta-label 1
- `SL_HIT` / `EXPIRED` = primary signal was wrong → meta-label 0

A meta-model trained on these labels learns:

- Which alert characteristics predict success
- When market conditions favor the signal
- Optimal filtering threshold for precision/recall

### Features for Meta-Model

When training a meta-model on flow alert outcomes, consider:

**Alert Characteristics:**

- Premium size, volume, OI ratio
- Days to expiry, moneyness (delta)
- Time of day, day of week
- Sweep vs block, bid vs ask side

**Market Context:**

- Underlying's recent volatility
- IV percentile, IV/RV ratio
- Market regime (trending vs mean-reverting)
- Sector performance

**Signal Quality:**

- Conviction score from UW
- Number of similar alerts (herding)
- Historical accuracy of similar patterns

## Trading-Time Metrics

Heber uses `exchange-calendars` to calculate trading-time-aware metrics:

### Why Trading Time Matters

Clock time includes non-trading hours (nights, weekends, holidays). A 24-hour window:

- Intraday: ~6.5 trading hours
- Overnight: ~17.5 non-trading hours

Labels based on clock time conflate "how long" with "how many trading sessions."

### Heber Implementation

```python
# heber/calendar/market.py
class MarketCalendar:
    def trading_minutes_until(self, start: datetime, end: datetime) -> int:
        """Count only minutes when market is open."""

    def add_trading_hours(self, dt: datetime, hours: float) -> datetime:
        """Add trading hours, skipping closed periods."""
```

The `trading_minutes_to_hit` field in `WatchOutcome` measures how many trading minutes elapsed before the barrier was hit.

## Point-in-Time Correctness

All labeling must respect the Zero-Leakage Firewall:

### Key Timestamps

| Field | Meaning |
|-------|---------|
| `ts_event` | When the event occurred |
| `ts_ingest` | When Heber received it |
| `ts_available` | When it's safe to use (ts_ingest + buffer) |

### As-Of Queries

When training, always filter: `WHERE ts_available <= training_cutoff`

This ensures you never use information that wasn't available at prediction time.

### Train/Test Split

Ensure train/test splits respect purge and embargo windows:

- **Purge window**: gap between last training label and first test sample
- **Embargo window**: additional buffer after test period to avoid indirect leakage

- **Purge**: Remove training samples whose label windows overlap test period
- **Embargo**: Additional buffer after test period to avoid indirect leakage

## References

- Lopez de Prado, M. (2018). *Advances in Financial Machine Learning*. Wiley.
- Lopez de Prado, M. (2020). "Trend-Scanning Labels." Working paper.
- MLFinLab documentation: <https://mlfinlab.readthedocs.io/>

## See Also

- [Watch Service](../heber/watch/) - Implementation of barrier labeling
- [HeberReader](sdk.md) - Zero-leakage enforcement via `read_asof()` predicate pushdown
- [Feature Store](../heber/feast/) - Feast integration for features
