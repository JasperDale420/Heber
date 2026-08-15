#!/usr/bin/env bash
# Hard gate before Phase 2: raising the Docker VM's RAM and moving Heber onto a
# shared `empire-bus` network with Data-Gateway. Both recreate containers, and
# recreating data-gateway-redis costs an AOF reload during which the gateway
# cannot publish.
#
# Read-only. Exits non-zero if any gate fails; run it immediately before the
# change, not once the day before — the branch, the mount and the market all move.
#
# What is NOT at risk, verified rather than assumed: Redis runs `appendonly yes`
# with `appendfsync always`, so the stream, the consumer groups and their PELs
# survive a restart. The binding constraint is the *gateway* side — while Redis
# is down, data-gateway buffers into data_sink_failed_buffer_capacity (50,000)
# and drops beyond it. That is why the market-closed gate matters more than the
# stream's MAXLEN.
set -uo pipefail

DOCKER="${DOCKER_BIN:-/usr/local/bin/docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER=docker
REDIS="$DOCKER exec data-gateway-redis redis-cli"
VOLUME_SENTINEL="${HEBER_VOLUME_ROOT:-/Volumes/heber}/data/.heber-sentinel"
FAILOVER_BUFFER=50000          # Data-Gateway/gateway/config.py data_sink_failed_buffer_capacity
WORST_CASE_RELOAD_SECONDS=90   # observed AOF loads: 2.1s and 76.8s; budget the worst

fail=0
pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }

echo "Phase 2 preflight — $(date -u +%FT%TZ)"
echo

# --- 1. timing -------------------------------------------------------------
et="$(TZ=America/New_York date +%H%M)"
dow="$(TZ=America/New_York date +%u)"
echo "[1] Timing  (ET now: $(TZ=America/New_York date +'%F %H:%M %Z'))"
if [[ "$dow" -ge 6 ]]; then
  pass "weekend — market closed"
elif [[ "$et" > "0929" && "$et" < "1601" ]]; then
  bad "market is OPEN — a Redis restart now overflows the gateway failover buffer"
else
  pass "outside 09:30-16:00 ET"
fi
# deploy.sh refuses this window; the EOD Gold publish lands in it.
if [[ "$et" > "1624" && "$et" < "1646" ]]; then
  bad "inside the 16:25-16:45 ET EOD blackout"
else
  pass "outside the EOD blackout window"
fi
echo

# --- 2. lakehouse volume ---------------------------------------------------
echo "[2] Lakehouse volume"
if [[ -e "$VOLUME_SENTINEL" ]]; then
  pass "sentinel present ($VOLUME_SENTINEL)"
else
  bad "sentinel absent — volume unmounted or under repair; nothing should be recreated"
fi
echo

# --- 3. Redis ownership ----------------------------------------------------
# Data-Gateway-kairosprime/docker-compose.yml declares the SAME container_name
# and the same 127.0.0.1:6379 binding. Attaching a second instance to the
# empire-bus alias would make DNS nondeterministic, so prove which one is live
# from its compose labels rather than from its name.
echo "[3] Redis ownership"
proj="$($DOCKER inspect data-gateway-redis --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null)"
wd="$($DOCKER inspect data-gateway-redis --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' 2>/dev/null)"
if [[ "$proj" == "data-gateway" && "$wd" == *"/Empire/Data-Gateway" ]]; then
  pass "live instance belongs to project '$proj' ($wd)"
else
  bad "unexpected owner: project='$proj' workdir='$wd' — resolve before touching either compose file"
fi
if $DOCKER network inspect empire-bus >/dev/null 2>&1; then
  occupied="$($DOCKER network inspect empire-bus --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null)"
  [[ -n "${occupied// /}" ]] && warn "empire-bus already has: $occupied" || pass "empire-bus exists and is empty"
else
  warn "empire-bus does not exist yet — create it before deploying either compose file"
fi
echo

# --- 4. Phase 1A actually deployed -----------------------------------------
# The functional test, not the deployed_sha marker: that marker has lied before.
# If the gauge is being served, the running image contains the retry fix.
echo "[4] Phase 1A deployed (consumers survive the restart rather than crash-looping)"
for svc_port in "heber-consumer:9090:heber_consumer_last_xread_success_unixtime" \
                "heber-watch:9091:heber_watch_last_xread_success_unixtime"; do
  svc="${svc_port%%:*}"; rest="${svc_port#*:}"; port="${rest%%:*}"; metric="${rest#*:}"
  if curl -sf --max-time 5 "http://127.0.0.1:${port}/" 2>/dev/null | grep -q "^${metric} "; then
    pass "$svc serves $metric"
  else
    bad "$svc does NOT serve $metric — rebuild it before Phase 2, or it crash-loops through the reload"
  fi
done
echo

# --- 5. gateway buffer vs ingest rate --------------------------------------
# The actual data-loss path: Redis down => gateway cannot XADD => it buffers
# FAILOVER_BUFFER events and drops the rest.
echo "[5] Gateway failover headroom (buffer ${FAILOVER_BUFFER}, worst-case reload ${WORST_CASE_RELOAD_SECONDS}s)"
before="$($REDIS XINFO STREAM heber:events 2>/dev/null | grep -A1 entries-added | tail -1)"
sleep 15
after="$($REDIS XINFO STREAM heber:events 2>/dev/null | grep -A1 entries-added | tail -1)"
if [[ -n "$before" && -n "$after" && "$after" =~ ^[0-9]+$ && "$before" =~ ^[0-9]+$ ]]; then
  rate=$(( (after - before) / 15 ))
  need=$(( rate * WORST_CASE_RELOAD_SECONDS ))
  echo "        ingest ~${rate}/s => ~${need} events buffered during the reload"
  if (( need < FAILOVER_BUFFER / 2 )); then
    pass "well inside the buffer"
  elif (( need < FAILOVER_BUFFER )); then
    warn "inside the buffer but with little margin — prefer a quieter window"
  else
    bad "would overflow the ${FAILOVER_BUFFER}-event buffer and drop live data"
  fi
else
  bad "could not sample the ingest rate"
fi
echo

# --- 6. Redis memory -------------------------------------------------------
echo "[6] Redis memory"
used="$($REDIS INFO memory 2>/dev/null | grep -m1 '^used_memory:' | tr -d 'used_memory:\r')"
max="$($REDIS INFO memory 2>/dev/null | grep -m1 '^maxmemory:' | tr -d 'maxmemory:\r')"
if [[ -n "$used" && -n "$max" && "$max" -gt 0 ]]; then
  pct=$(( used * 100 / max ))
  if (( pct < 80 )); then pass "used ${pct}% of maxmemory"; else bad "used ${pct}% of maxmemory — evicting or close to it"; fi
else
  warn "could not read Redis memory"
fi
echo

# --- 7. consumer state ------------------------------------------------------
# Informational: PELs survive a restart, so a backlog does not block Phase 2 —
# but a PEL that never drains means those messages are stranded, not queued.
echo "[7] Consumer state (informational)"
$REDIS XINFO GROUPS heber:events 2>/dev/null | paste - - | awk '{printf "        %s\n", $0}'
echo

if (( fail )); then
  echo "VERDICT: BLOCKED — resolve the FAIL lines above before starting Phase 2."
  exit 1
fi
echo "VERDICT: CLEAR to proceed with Phase 2."
