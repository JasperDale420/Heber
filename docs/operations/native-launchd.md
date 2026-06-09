# Native LaunchD Pilot

This is the controlled path for moving low-risk Heber workers out of Docker.
It is not a full Docker replacement yet.

## Pilot Scope

The first native candidates are:

- `heber-dataflow-health`
- `heber-health-monitor`
- `heber-gold-poller`
- `heber-compactor`

Do not migrate `heber-consumer` in this first pilot. It owns the Redis consumer
group that writes Bronze and Silver, so running Docker and native copies at the
same time can split ingestion work or create confusing lake writes.

Do not migrate `heber-watch` until the monitoring workers have survived a real
trading-day check. It writes Gold labels and calls Data Gateway during market
hours, so it is a second-wave candidate.

## Install Without Starting

```bash
cd /Users/jacobmcmillan/Empire/Heber
scripts/install_native_launchd.sh dataflow-health
```

This copies the plist to `~/Library/LaunchAgents` but does not start it. Start
only after the matching Docker service is stopped.

## Start A Pilot Service

```bash
cd /Users/jacobmcmillan/Empire/Heber
docker compose stop heber-dataflow-health
scripts/install_native_launchd.sh --start dataflow-health
launchctl print gui/$(id -u)/com.empire.heber.dataflow-health
```

Logs are written under:

```text
/Users/jacobmcmillan/Empire/Heber/logs/native/
```

## Rollback

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.empire.heber.dataflow-health.plist
docker compose up -d heber-dataflow-health
```

Rollback is simple because the native service uses the same code and writes to
the same host lake path.

## Checkpoints

### Checkpoint 1: Immediate Startup

Run this right after starting a native service:

```bash
launchctl print gui/$(id -u)/com.empire.heber.dataflow-health
tail -100 logs/native/dataflow-health.err.log
tail -20 logs/native/dataflow-health.out.log
cat /Volumes/heber/data/ops/dataflow-health/latest.json
```

Move forward only if LaunchD shows the service running, logs do not show a crash
loop, and the latest JSON report is being refreshed.

### Checkpoint 2: Next Trading Day, June 1, 2026

Check after the market has been open long enough for fresh data to flow. The
minimum useful checks are:

```bash
docker exec data-gateway-redis redis-cli XLEN heber:events
docker exec data-gateway-redis redis-cli XLEN heber:events:dlq
curl -s http://localhost:9090/metrics | head
curl -s http://localhost:9091/metrics | head
cat /Volumes/heber/data/ops/dataflow-health/latest.json
```

Move forward only if the native report is fresh, Redis is reachable, DLQ is not
growing unexpectedly, and the Docker replacement service has stayed stopped.

### Checkpoint 3: One Full Trading Day

After one clean trading day, migrate `health-monitor`, then `compactor`, then
`gold-poller`. Move one service at a time and keep a rollback window between
them.

Do not migrate `heber-consumer` until all lower-risk workers have stayed clean
for at least one full trading day and the Monday check shows no ingestion lag or
DLQ growth.

## Critical-feed Discord alerting

The alarm is the **`alert-check`** service: a one-shot `heber alert-check` that
runs a single liveness cycle and posts a Discord alert when a must-flow feed
(flow_alerts, darkpool, oi_change, greek_exposure) goes dark or drops to a
trickle. It is scheduled via launchd `StartInterval` (every 5 min) rather than a
long-lived loop, so it runs as a fast, isolated process and is never starved by
the multi-tier `health-monitor`'s heavy Tier-2/Tier-3 sweeps. Cooldown/recovery
state lives on disk (`<data_root>/ops/alerts/state.json`), so throttling (one
alert per feed per hour) works across runs.

The runner sources `.env`, so set these there (no plist edit needed):

```
HEBER_ALERT_DISCORD_ENABLED=true
HEBER_ALERT_DISCORD_WEBHOOK_URL=<discord webhook>
# Focus the alarm on the UW feeds (bars/trades are healthy + heavy to read);
# floor 0 disables a feed. Add calibrated trickle floors here too.
HEBER_ALERT_FLOOR_OVERRIDES='{"bars":0,"trades":0,"flow_alerts":351}'
```

Install + start the alarm:

```
bash scripts/install_native_launchd.sh --start alert-check
```

- Verify the webhook end-to-end: `uv run heber alert-test`
- Run one cycle by hand: `uv run heber alert-check`
- Suggest trickle floors from healthy history: `uv run heber alert-calibrate`
  (scoped to the feeds you actually watch; disabled feeds are skipped)

The `health-monitor` service (Tier 1/2/3 data-quality checks) is independent and
optional — it is **not** required for alerting. Note its Tier-3 daily sweep can
stall on large un-pruned feed reads; run it only if you need those deeper checks.
