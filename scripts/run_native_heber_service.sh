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
MASSIVE_ARCHIVE_ROOT="${MASSIVE_ARCHIVE_ROOT:-${HEBER_DATA_ROOT}/_vendor_raw/massive}"
MASSIVE_REST_BACKLOG_ROOT="${MASSIVE_REST_BACKLOG_ROOT:-${MASSIVE_ARCHIVE_ROOT}/massive_rest_backlog}"
MASSIVE_TICKER_META_ROOT="${MASSIVE_TICKER_META_ROOT:-${MASSIVE_ARCHIVE_ROOT}/massive_ticker_meta}"
MASSIVE_TICKER_META_WORKERS="${MASSIVE_TICKER_META_WORKERS:-4}"
MASSIVE_DELISTED_DETAILS_WORKERS="${MASSIVE_DELISTED_DETAILS_WORKERS:-4}"

export HEBER_DATA_ROOT
export HEBER_VOLUME_ROOT
export HEBER_REDIS_URL
export DATA_GATEWAY_URL
export HEBER_POSTGRES_URL
export HEBER_GOLD_PATH="${HEBER_GOLD_PATH:-${HEBER_DATA_ROOT}/gold}"
export HEBER_HEALTH_CONSUMER_METRICS_URL="${HEBER_HEALTH_CONSUMER_METRICS_URL:-http://localhost:9090/metrics}"
export HEBER_HEALTH_WATCH_METRICS_URL="${HEBER_HEALTH_WATCH_METRICS_URL:-http://localhost:9091/metrics}"
export HEBER_HEALTH_REPORT_DIR="${HEBER_HEALTH_REPORT_DIR:-${HEBER_DATA_ROOT}/ops/dataflow-health}"
export MASSIVE_ARCHIVE_ROOT
export MASSIVE_REST_BACKLOG_ROOT
export MASSIVE_TICKER_META_ROOT
export PYTHONUNBUFFERED=1

mkdir -p "${HEBER_NATIVE_LOG_DIR}"

# The data volume must be mounted before we create anything under it. When it is
# absent, mkdir-ing the report dir under /Volumes/heber either spams
# "Permission denied" against the root-owned mount placeholder (every launchd
# cycle) or split-brains the lakehouse onto the boot disk. Guard first.
if [[ ! -d "${HEBER_DATA_ROOT}" ]]; then
  echo "ERROR: HEBER_DATA_ROOT does not exist (volume not mounted?): ${HEBER_DATA_ROOT}" >&2
  exit 1
fi

mkdir -p "${HEBER_HEALTH_REPORT_DIR}"

cd "${PROJECT_DIR}"

# Sync dependencies explicitly. `uv run` does this implicitly on every launch,
# but silently: a broken lock (e.g. an unresolved `<<<<<<< HEAD` merge conflict
# in uv.lock) then just exits and relaunches every StartInterval with the parse
# error buried in stderr. Do it up front with a distinct, greppable diagnostic
# so the failure is alertable instead of looping quietly.
if ! uv sync 2>&1; then
  echo "HEBER_RUNNER_ERROR: uv sync failed for service '${SERVICE}' — dependency lock may be broken (check uv.lock for merge conflicts)." >&2
  exit 70
fi

source_massive_env() {
  if [[ -z "${MASSIVE_API_KEY:-}" && -f "/Users/jacobmcmillan/Empire/Data-Gateway/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "/Users/jacobmcmillan/Empire/Data-Gateway/.env"
    set +a
  fi
}

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
  "massive-daily")
    # One-shot Massive raw archive sync; scheduled after next-day flat-file publish.
    exec uv run heber massive-daily --archive-root "${MASSIVE_ARCHIVE_ROOT}"
    ;;
  "massive-rest-backlog")
    # One-shot Massive REST backlog sync; launchd restarts it until every dataset has a .done marker.
    source_massive_env
    exec uv run python scripts/massive_rest_backlog_sweep.py --out-root "${MASSIVE_REST_BACKLOG_ROOT}"
    ;;
  "massive-ticker-meta")
    # Per-ticker details and symbol-change events for the captured active/inactive equity universe.
    source_massive_env
    exec uv run python heber/backfill/massive/ticker_meta_sweep.py \
      --corp-root "${MASSIVE_ARCHIVE_ROOT}/massive_corp_actions" \
      --out "${MASSIVE_TICKER_META_ROOT}" \
      --workers "${MASSIVE_TICKER_META_WORKERS}"
    ;;
  "massive-delisted-details")
    # Historical ticker details for delisted equities as of their last listed day.
    source_massive_env
    exec uv run python heber/backfill/massive/delisted_details_sweep.py \
      --corp-root "${MASSIVE_ARCHIVE_ROOT}/massive_corp_actions" \
      --workers "${MASSIVE_DELISTED_DETAILS_WORKERS}"
    ;;
  *)
    echo "Usage: $0 {dataflow-health|health-monitor|gold-poller|compactor|alert-check|massive-daily|massive-rest-backlog|massive-ticker-meta|massive-delisted-details}" >&2
    exit 64
    ;;
esac
