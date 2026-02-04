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
    "elasticsearch/data"
    "minio/data"
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

# Clean macOS ._* resource fork files (AppleDouble) from volume directories.
# External drives (NTFS/exFAT) store extended attributes as ._* sidecar files.
# These cause "Operation not permitted" errors that crash postgres, redis, and
# clickhouse on startup. This MUST run after chmod since chmod itself creates
# ._* files on non-HFS+ filesystems.
echo "Cleaning macOS resource fork files..."
for dir in postgres redis clickhouse elasticsearch minio data; do
    dot_clean -m "$VOLUME_ROOT/$dir" 2>/dev/null || true
done
echo "Resource fork cleanup complete"

echo ""
echo "Heber storage initialized at $VOLUME_ROOT"
