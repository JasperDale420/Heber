#!/usr/bin/env bash
# Record every Docker lifecycle event for data-gateway-redis, with timestamps.
#
# This is the record of *when* Redis dies. It has to be its own long-running
# process because `docker events` is a blocking stream — it cannot be one step
# in a polling script. Run under launchd with KeepAlive so it is re-established
# after a Docker daemon restart.
#
# Pair with redis_capacity_watch.sh, which samples memory and Redis internals on
# an interval. Correlating a die/kill event here against the memory and AOF
# samples there is what distinguishes VM memory exhaustion from a disk/AOF fault
# from an external supervisor.
#
# Signals to look for in the output:
#   die with exitCode=137  -> SIGKILL, consistent with an OOM kill
#   die with exitCode=0    -> what is observed today; a clean exit code with no
#                             shutdown lines in the Redis log means the process
#                             did not get to run its shutdown path
#   kill                   -> something sent a signal; the event carries which
set -uo pipefail
cd "$(dirname "$0")/.."

DOCKER="${DOCKER_BIN:-/usr/local/bin/docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER=docker

CONTAINER="${REDIS_CONTAINER:-data-gateway-redis}"
LOG_DIR="${HEBER_NATIVE_LOG_DIR:-$(pwd)/logs/native}"
mkdir -p "$LOG_DIR"

now() { date -u +%FT%TZ; }

# Wait for the daemon rather than exiting: launchd would otherwise thrash
# restarting us during a Docker Desktop restart, which is one of the events we
# are here to capture.
until "$DOCKER" info >/dev/null 2>&1; do
  echo "$(now) waiting for docker daemon" >>"${LOG_DIR}/redis-events.log"
  sleep 10
done

echo "$(now) collector attached to ${CONTAINER}" >>"${LOG_DIR}/redis-events.log"

# One line per event with the fields that matter. Use .Action, not .Status —
# the latter is the pre-1.10 API field and errors out on current daemons.
# exitCode and the signal live in Actor.Attributes.
exec "$DOCKER" events \
  --filter "container=${CONTAINER}" \
  --filter "event=die" \
  --filter "event=kill" \
  --filter "event=stop" \
  --filter "event=start" \
  --filter "event=restart" \
  --filter "event=oom" \
  --format 'ts={{.Time}} action={{.Action}} exitCode={{index .Actor.Attributes "exitCode"}} signal={{index .Actor.Attributes "signal"}}' \
  >>"${LOG_DIR}/redis-events.log" 2>&1
