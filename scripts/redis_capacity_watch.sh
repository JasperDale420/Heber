#!/usr/bin/env bash
# Sample what is happening around data-gateway-redis, so the cause of its
# repeated hard kills can be established from evidence instead of guessed at.
#
# Context: `docker inspect` reported 280 restarts with ExitCode=0 and
# OOMKilled=false, and the Redis log contains no shutdown lines at all — no
# SIGTERM, no "bye bye". A clean shutdown always logs; the absence means the
# process is being killed outright. Each restart replays a ~1.5 GB AOF for ~39
# seconds, during which every Heber consumer sees BusyLoadingError.
#
# Candidate causes, none yet established:
#   - VM memory reclaim: the Docker VM is 8 GB on a 48 GB host, container limits
#     total 13.5 GB, and Redis alone is configured maxmemory 4gb
#   - a Docker Desktop VM or daemon restart
#   - an external supervisor
#   - a disk/AOF fault (the log is full of "Asynchronous AOF fsync is taking too
#     long (disk is busy?)")
#
# The decisive signal is a kernel OOM-killer line inside the VM. IMPORTANT: the
# absence of one only means something if the probe that looks for it actually
# works, so the probe self-verifies on every run and says so in the log. Treat
# "probe unavailable" as a third outcome, never as "no OOM".
#
# Companion: redis_event_collector.sh records the kill timestamps (docker events
# is a blocking stream and cannot be sampled from a polling script).
set -uo pipefail
cd "$(dirname "$0")/.."

DOCKER="${DOCKER_BIN:-/usr/local/bin/docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER=docker

CONTAINER="${REDIS_CONTAINER:-data-gateway-redis}"
LOG_DIR="${HEBER_NATIVE_LOG_DIR:-$(pwd)/logs/native}"
LOG_FILE="${LOG_DIR}/redis-capacity-$(date -u +%F).jsonl"
mkdir -p "$LOG_DIR"

now() { date -u +%FT%TZ; }

emit() { printf '%s\n' "$1" >>"$LOG_FILE"; }

if ! "$DOCKER" info >/dev/null 2>&1; then
  emit "{\"ts\":\"$(now)\",\"event\":\"docker_unavailable\"}"
  exit 0
fi

# --- kernel OOM probe, with an explicit capability check -------------------
# nsenter into the VM's init namespace to read its dmesg ring buffer. If this
# cannot run, say so loudly: a silent failure here would read as "no OOM kills"
# and send the investigation down the wrong path.
probe_out=$("$DOCKER" run --rm --privileged --pid=host alpine \
  nsenter -t 1 -m -- dmesg -T 2>&1 | tail -400)
probe_status=$?

if [[ $probe_status -ne 0 || -z "$probe_out" ]]; then
  emit "{\"ts\":\"$(now)\",\"event\":\"oom_probe_unavailable\",\"detail\":$(printf '%s' "${probe_out:-no output}" | tail -1 | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))')}"
else
  oom_lines=$(printf '%s' "$probe_out" | grep -iE "out of memory|oom-kill|killed process" | tail -5)
  oom_count=$(printf '%s' "$oom_lines" | grep -c . || true)
  emit "{\"ts\":\"$(now)\",\"event\":\"oom_probe\",\"working\":true,\"oom_lines\":${oom_count:-0},\"sample\":$(printf '%s' "$oom_lines" | tail -1 | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().strip()))')}"
fi

# --- per-container memory ---------------------------------------------------
stats=$("$DOCKER" stats --no-stream --format '{{.Name}}|{{.MemUsage}}|{{.MemPerc}}' 2>/dev/null \
  | python3 -c '
import json, sys
rows = []
for line in sys.stdin:
    parts = line.strip().split("|")
    if len(parts) == 3:
        rows.append({"name": parts[0], "mem": parts[1], "pct": parts[2]})
print(json.dumps(rows))
')
emit "{\"ts\":\"$(now)\",\"event\":\"container_memory\",\"containers\":${stats:-[]}}"

# --- Redis internals --------------------------------------------------------
# rdb_last_bgsave_status / aof_last_write_status failing, or a large
# latest_fork_usec, point at disk rather than memory.
info=$("$DOCKER" exec "$CONTAINER" redis-cli INFO 2>/dev/null \
  | tr -d '\r' \
  | grep -E '^(used_memory|used_memory_rss|used_memory_peak|maxmemory|mem_fragmentation_ratio|rdb_last_bgsave_status|aof_last_write_status|aof_last_bgrewrite_status|latest_fork_usec|total_forks|evicted_keys|blocked_clients|connected_clients|instantaneous_ops_per_sec):' \
  | python3 -c '
import json, sys
out = {}
for line in sys.stdin:
    if ":" in line:
        k, v = line.strip().split(":", 1)
        out[k] = v
print(json.dumps(out))
')
# Assign the fallback separately: ${info:-{}} does not mean what it looks like,
# bash reads the default as a bare "{" and leaves a stray "}" behind.
[[ -z "$info" ]] && info='{}'
emit "{\"ts\":\"$(now)\",\"event\":\"redis_info\",\"info\":${info}}"

# --- uptime: a reset means it died since the last sample --------------------
uptime_s=$("$DOCKER" exec "$CONTAINER" redis-cli INFO server 2>/dev/null \
  | tr -d '\r' | awk -F: '/^uptime_in_seconds:/{print $2}')
restarts=$("$DOCKER" inspect "$CONTAINER" --format '{{.RestartCount}}' 2>/dev/null)
emit "{\"ts\":\"$(now)\",\"event\":\"redis_lifetime\",\"uptime_seconds\":${uptime_s:-null},\"restart_count\":${restarts:-null}}"
