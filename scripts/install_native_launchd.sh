#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${HEBER_PROJECT_DIR:-/Users/jacobmcmillan/Empire/Heber}"
LAUNCHD_DIR="${PROJECT_DIR}/launchd"
AGENT_DIR="${HOME}/Library/LaunchAgents"
START_SERVICES=false

usage() {
  cat <<'USAGE'
Usage:
  scripts/install_native_launchd.sh [--start] service...

Services:
  dataflow-health
  health-monitor
  gold-poller
  compactor
  alert-check
  massive-daily
  massive-rest-backlog
  massive-ticker-meta
  massive-delisted-details

By default this installs plists without starting them. Pass --start to load and
kickstart the selected services after copying the plist files.
USAGE
}

if [[ $# -eq 0 ]]; then
  usage >&2
  exit 64
fi

services=()
for arg in "$@"; do
  case "${arg}" in
    --start)
      START_SERVICES=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    dataflow-health|health-monitor|gold-poller|compactor|alert-check|massive-daily|massive-rest-backlog|massive-ticker-meta|massive-delisted-details)
      services+=("${arg}")
      ;;
    *)
      echo "Unknown service or flag: ${arg}" >&2
      usage >&2
      exit 64
      ;;
  esac
done

if [[ ${#services[@]} -eq 0 ]]; then
  echo "No services selected." >&2
  usage >&2
  exit 64
fi

mkdir -p "${AGENT_DIR}"

uid="$(id -u)"
for service in "${services[@]}"; do
  label="com.empire.heber.${service}"
  source_plist="${LAUNCHD_DIR}/${label}.plist"
  target_plist="${AGENT_DIR}/${label}.plist"

  if [[ ! -f "${source_plist}" ]]; then
    echo "Missing plist: ${source_plist}" >&2
    exit 1
  fi

  cp "${source_plist}" "${target_plist}"
  echo "Installed ${target_plist}"

  if launchctl print "gui/${uid}/${label}" >/dev/null 2>&1; then
    launchctl bootout "gui/${uid}" "${target_plist}" >/dev/null 2>&1 || true
  fi

  if [[ "${START_SERVICES}" == "true" ]]; then
    launchctl bootstrap "gui/${uid}" "${target_plist}"
    launchctl enable "gui/${uid}/${label}"
    launchctl kickstart "gui/${uid}/${label}"
    echo "Started ${label}"
  else
    echo "Not started: ${label}. Re-run with --start to launch it."
  fi
done
