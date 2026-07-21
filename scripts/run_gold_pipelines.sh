#!/bin/bash
# Run all Gold feature pipelines for the last 7 days
# Intended to run daily at 2 AM ET via cron
#
# Crontab entry (ET timezone):
#   0 2 * * * /Users/jacobmcmillan/Empire/Heber/scripts/run_gold_pipelines.sh >> /tmp/gold_pipelines.log 2>&1
set -euo pipefail
# cron runs with a minimal PATH that omits ~/.local/bin where uv lives
export PATH="$HOME/.local/bin:$PATH"
cd /Users/jacobmcmillan/Empire/Heber
START=$(date -v-7d +%Y-%m-%d)
END=$(date +%Y-%m-%d)

echo "$(date): Running equity features pipeline..."
uv run python -m heber.features.pipelines.equity_features --start "$START" --end "$END" 2>&1

echo "$(date): Running market intel pipeline..."
uv run python -m heber.features.pipelines.market_intel_features --start "$START" --end "$END" 2>&1

echo "$(date): Done"
