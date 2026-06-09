#!/usr/bin/env bash
set -euo pipefail

SERVICE="${1:-}"
PROJECT_DIR="${HEBER_PROJECT_DIR:-/Users/jacobmcmillan/Empire/Heber}"

export PATH="/Users/jacobmcmillan/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

if [[ -f "${PROJECT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_DIR}/.env"
  set +a
fi

HEBER_DATA_ROOT="${HEBER_DATA_ROOT:-/Volumes/heber/data}"
HEBER_VOLUME_ROOT="${HEBER_VOLUME_ROOT:-/Volumes/heber}"
HEBER_REDIS_URL="${HEBER_REDIS_URL:-redis://localhost:6379}"
DATA_GATEWAY_URL="${DATA_GATEWAY_URL:-http://localhost:8080}"
HEBER_POSTGRES_URL="${HEBER_POSTGRES_URL:-postgresql+asyncpg://heber:${POSTGRES_PASSWORD:-heber_dev_password}@localhost:5433/heber_catalog}"
HEBER_NATIVE_LOG_DIR="${HEBER_NATIVE_LOG_DIR:-${PROJECT_DIR}/logs/native}"

export HEBER_DATA_ROOT
export HEBER_VOLUME_ROOT
export HEBER_REDIS_URL
export DATA_GATEWAY_URL
export HEBER_POSTGRES_URL
export HEBER_GOLD_PATH="${HEBER_GOLD_PATH:-${HEBER_DATA_ROOT}/gold}"
export HEBER_HEALTH_CONSUMER_METRICS_URL="${HEBER_HEALTH_CONSUMER_METRICS_URL:-http://localhost:9090/metrics}"
export HEBER_HEALTH_WATCH_METRICS_URL="${HEBER_HEALTH_WATCH_METRICS_URL:-http://localhost:9091/metrics}"
export HEBER_HEALTH_REPORT_DIR="${HEBER_HEALTH_REPORT_DIR:-${HEBER_DATA_ROOT}/ops/dataflow-health}"
export PYTHONUNBUFFERED=1

mkdir -p "${HEBER_NATIVE_LOG_DIR}" "${HEBER_HEALTH_REPORT_DIR}"

if [[ ! -d "${HEBER_DATA_ROOT}" ]]; then
  echo "ERROR: HEBER_DATA_ROOT does not exist: ${HEBER_DATA_ROOT}" >&2
  exit 1
fi

cd "${PROJECT_DIR}"

case "${SERVICE}" in
  "dataflow-health")
    exec uv run python -m heber.ops.dataflow_health --loop --mode scheduled
    ;;
  "health-monitor")
    export HEBER_METRICS_PORT="${HEBER_METRICS_PORT:-9093}"
    exec uv run python -m heber.health_monitor
    ;;
  "gold-poller")
    export HEBER_METRICS_PORT="${HEBER_METRICS_PORT:-9092}"
    export HEBER_GOLD_POLLER_ENABLED="${HEBER_GOLD_POLLER_ENABLED:-true}"
    exec uv run python -m heber.gold_poller
    ;;
  "compactor")
    export HEBER_METRICS_PORT="${HEBER_METRICS_PORT:-9094}"
    exec uv run python -m heber.writer.compactor
    ;;
  "alert-check")
    # One-shot critical-feed liveness check; scheduled via launchd StartInterval.
    exec uv run heber alert-check
    ;;
  *)
    echo "Usage: $0 {dataflow-health|health-monitor|gold-poller|compactor|alert-check}" >&2
    exit 64
    ;;
esac
