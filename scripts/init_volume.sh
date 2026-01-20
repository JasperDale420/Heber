#!/bin/bash
# Initialize directory structure on external volume
# Run this once before starting Docker Compose

set -euo pipefail

VOLUME_ROOT="${HEBER_VOLUME_ROOT:-/Volumes/heber}"

echo "Initializing Heber storage on $VOLUME_ROOT..."

# Create directory structure
directories=(
    "data/bronze"
    "data/silver"
    "data/gold"
    "postgres/data"
    "clickhouse/data"
    "clickhouse/logs"
    "redis/data"
    "logs"
)

for dir in "${directories[@]}"; do
    full_path="$VOLUME_ROOT/$dir"
    if [ ! -d "$full_path" ]; then
        echo "Creating $full_path"
        mkdir -p "$full_path"
    else
        echo "Already exists: $full_path"
    fi
done

# Set permissions (needed for Docker containers)
echo "Setting permissions..."
chmod -R 755 "$VOLUME_ROOT/data"
chmod -R 700 "$VOLUME_ROOT/postgres"
chmod -R 755 "$VOLUME_ROOT/clickhouse"
chmod -R 755 "$VOLUME_ROOT/redis"

echo ""
echo "✓ Heber storage initialized at $VOLUME_ROOT"
echo ""
echo "Directory structure:"
ls -la "$VOLUME_ROOT"
echo ""
echo "Data directories:"
ls -la "$VOLUME_ROOT/data"
