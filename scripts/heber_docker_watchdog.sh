#!/usr/bin/env bash
# Supervisor: bring back any Heber app container that is stopped or missing.
#
# Why this exists: docker-compose `restart: always` only restarts a container
# that CRASHES (non-zero exit). It does NOT recover a container that was simply
# stopped — e.g. left down across a deploy, or never re-`up`ed. That is exactly
# how the 2026-06-25 EOD ingest outage happened: heber-consumer sat stopped for
# ~3h (nothing watched it) straight through the 16:31 ET EOD publish, and the
# capped Redis stream evicted the unread feeds.
#
# This runs on a launchd StartInterval (see launchd/com.empire.heber.docker-watchdog.plist).
# It only touches compose when something is actually down, so in steady state it
# is a cheap read-only inspect and never races an in-flight deploy.
set -uo pipefail
cd "$(dirname "$0")/.."

DOCKER="${DOCKER_BIN:-/usr/local/bin/docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER=docker

SERVICES="heber-consumer heber-watch heber-catalog heber-gold-poller heber-compactor heber-dataflow-health heber-health-monitor"
now() { date -u +%FT%TZ; }

# Docker daemon not up yet (e.g. just after boot) -> nothing to do this tick.
if ! "$DOCKER" info >/dev/null 2>&1; then
  echo "$(now) docker daemon unavailable — skipping"
  exit 0
fi

down=""
unhealthy=""
for svc in $SERVICES; do
  state="$("$DOCKER" inspect -f '{{.State.Status}}' "$svc" 2>/dev/null || echo missing)"
  if [[ "$state" != "running" ]]; then
    down="${down:+$down }$svc"
    continue
  fi
  # Running is not enough: a wedged process stays "running" while its healthcheck
  # (e.g. heber-consumer's run-loop heartbeat) goes unhealthy. That is exactly how
  # the 2026-07-20 EOD ingest was lost — the consumer stalled through the 16:30
  # publish and nothing restarted it. Containers without a healthcheck report
  # "none" and are left alone.
  health="$("$DOCKER" inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$svc" 2>/dev/null || echo none)"
  [[ "$health" == "unhealthy" ]] && unhealthy="${unhealthy:+$unhealthy }$svc"
done

# Steady state: everything running and healthy. Do not touch docker (avoid racing a deploy).
[[ -z "$down" && -z "$unhealthy" ]] && exit 0

if [[ -n "$unhealthy" ]]; then
  echo "$(now) restarting unhealthy service(s): $unhealthy"
  # Plain restart (not compose up): the container exists and is running, its
  # process is just wedged. restart:always does not cover this case.
  # shellcheck disable=SC2086
  "$DOCKER" restart $unhealthy
fi

[[ -z "$down" ]] && exit 0

echo "$(now) reconciling down service(s): $down"
# --no-build: never rebuild here (the watchdog recovers, it does not deploy);
# starts stopped containers and recreates missing ones from existing images.
# shellcheck disable=SC2086
"$DOCKER" compose up -d --no-build $down
