#!/usr/bin/env bash
# Rebuild + redeploy Heber app containers to the current source tree.
#
# Why this exists: Heber images BAKE the source at build time (the Dockerfile
# COPYs heber/) and only the /data volume is mounted — so a committed code fix
# does NOT take effect on a plain `docker compose restart`, it needs a rebuild.
# That is exactly how heber-watch drifted ~3 weeks behind: it was restarted,
# never rebuilt. This rebuilds the named services (default: all app services),
# recreates them, waits for their healthchecks, and prints the deployed commit
# so running code is never silently stale.
#
#   scripts/deploy.sh                              # rebuild + redeploy all app services
#   scripts/deploy.sh heber-consumer heber-watch   # just these
#
# --no-deps keeps the blast radius to the named app services (a bare `up` also
# recreates dependencies like heber-catalog/heber-postgres).
set -euo pipefail
cd "$(dirname "$0")/.."

# --- Guardrail: don't rebuild during the UW EOD ingest window ---
# A rebuild recreates heber-consumer. If that lands across the ~16:31 ET EOD
# publish, the consumer is down while the ~114k-message burst arrives and the
# capped Redis stream evicts the earliest (unread) feeds — exactly how the
# 2026-06-25 oi_change/iv_rank/iv_term/historic_option_volume loss happened.
# Refuse 16:25–16:45 ET unless --force / HEBER_DEPLOY_FORCE=1.
FORCE="${HEBER_DEPLOY_FORCE:-0}"
NEWARGS=""
for a in "$@"; do
  if [[ "$a" == "--force" ]]; then FORCE=1; else NEWARGS="${NEWARGS:+$NEWARGS }$a"; fi
done
# shellcheck disable=SC2086 # service names are single words; intentional split
set -- $NEWARGS

et_num=$((10#$(TZ=America/New_York date +%H%M)))  # 10# forces base-10 (avoid octal on leading zeros)
if [[ "$FORCE" != "1" && "$et_num" -ge 1625 && "$et_num" -le 1645 ]]; then
  echo "⛔ Refusing to deploy during the UW EOD ingest window (16:25–16:45 ET)."
  echo "   Rebuilding now drops heber-consumer across the ~16:31 ET EOD publish and loses feeds."
  echo "   Re-run after 16:45 ET, or override: HEBER_DEPLOY_FORCE=1 $0 ${*:-}  (or pass --force)"
  exit 1
fi

DEFAULT_SERVICES="heber-consumer heber-watch heber-catalog heber-gold-poller heber-compactor heber-dataflow-health heber-health-monitor"
SERVICES="${*:-$DEFAULT_SERVICES}"

echo "Rebuilding + recreating: ${SERVICES}"
# shellcheck disable=SC2086
docker compose up -d --build --no-deps ${SERVICES}

echo "Waiting for containers to settle…"
failed=""
for svc in ${SERVICES}; do
  result="timeout"
  for _ in $(seq 1 45); do
    state="$(docker inspect -f '{{.State.Status}}' "${svc}" 2>/dev/null || echo missing)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${svc}" 2>/dev/null || echo none)"
    if [[ "${state}" != "running" ]]; then
      result="${state}"
      sleep 2
      continue
    fi
    if [[ "${health}" == "healthy" || "${health}" == "none" ]]; then
      result="ok"
      break
    fi
    result="${health}"  # starting / unhealthy
    sleep 2
  done
  if [[ "${result}" == "ok" ]]; then
    echo "  ✅ ${svc}"
  else
    echo "  ⚠️  ${svc} (${result})"
    failed="${failed} ${svc}"
  fi
done

sha="$(git rev-parse --short HEAD 2>/dev/null || echo '?')"
echo "Deployed ${sha}: $(git log -1 --format='%s' 2>/dev/null || echo '?')"
if [[ -n "${failed}" ]]; then
  echo "Not healthy:${failed}"
  echo "  check: docker compose logs --tail 50${failed}"
  exit 1
fi
