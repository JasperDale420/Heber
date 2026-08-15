#!/usr/bin/env bash
# Arm the off-machine dead-man heartbeats by writing their ping URLs into .env.
#
# The URLs are capability secrets — anyone holding one can forge a heartbeat and
# keep a dead job looking alive. They are read from a prompt rather than passed
# as arguments so they never land in shell history, and never echoed back.
#
# A typo'd URL is the failure that matters: the ping silently 404s forever, the
# check never goes green, and you believe you are covered when you are not. So
# each URL is pinged for real and the result is reported before this exits.
set -uo pipefail

ENV_FILE="${HEBER_ENV_FILE:-/Users/jacobmcmillan/Empire/Heber/.env}"

[[ -f "$ENV_FILE" ]] || { echo "No .env at $ENV_FILE" >&2; exit 1; }

prompt_url() {
  # $1 = env key, $2 = what it watches
  local key="$1" what="$2" url
  if grep -q "^${key}=" "$ENV_FILE"; then
    echo "  ${key} is already set — edit ${ENV_FILE} by hand to change it." >&2
    return 1
  fi
  read -r -p "  Ping URL for ${what} (${key}), blank to skip: " url
  url="${url#"${url%%[![:space:]]*}"}"; url="${url%"${url##*[![:space:]]}"}"
  [[ -z "$url" ]] && { echo "  skipped." >&2; return 1; }
  [[ "$url" == http*://* ]] || { echo "  Not a URL — skipped." >&2; return 1; }
  printf '%s\n' "$url"
}

echo "Arming dead-man heartbeats. Nothing is printed back to the screen."
echo

alert_url="$(prompt_url HEBER_ALERT_HEARTBEAT_URL "the alert-check job")" || alert_url=""
flow_url="$(prompt_url HEBER_HEARTBEAT_URL "dataflow-health")" || flow_url=""

# Two jobs on one check is the classic mistake: whichever job still runs keeps
# the check green, so the dead one is never reported. That defeats the purpose.
if [[ -n "$alert_url" && -n "$flow_url" && "$alert_url" == "$flow_url" ]]; then
  echo "Both URLs are the same check. A live dataflow-health would then mask a dead" >&2
  echo "alert-check. Create two separate checks and re-run." >&2
  exit 1
fi

wrote=0
unverified=0
for pair in "HEBER_ALERT_HEARTBEAT_URL:$alert_url" "HEBER_HEARTBEAT_URL:$flow_url"; do
  key="${pair%%:*}"; url="${pair#*:}"
  [[ -z "$url" ]] && continue
  printf '%s=%s\n' "$key" "$url" >> "$ENV_FILE"
  wrote=$((wrote + 1))
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null || echo 000)"
  if [[ "$code" == "200" ]]; then
    echo "  ${key}: written, test ping accepted (HTTP 200) — the check should now be green."
  else
    unverified=$((unverified + 1))
    echo "  ${key}: WRITTEN BUT UNVERIFIED — test ping returned HTTP ${code}." >&2
    echo "    The URL is probably wrong. Fix it in ${ENV_FILE} — a check that is never" >&2
    echo "    pinged alerts forever, and a wrong URL looks identical to a dead job." >&2
  fi
done

echo
if (( wrote == 0 )); then
  echo "Nothing written."
elif (( unverified > 0 )); then
  echo "${wrote} URL(s) written to ${ENV_FILE}, ${unverified} of them UNVERIFIED — fix those before relying on this."
  exit 1
else
  echo "Done — ${wrote} URL(s) written and verified. alert-check pings every 5 minutes"
  echo "via launchd; nothing to restart."
  echo "Set each check to: period 5 min, grace 15 min (alert-check) / 20 min (dataflow-health)."
fi
