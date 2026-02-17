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
HOST_OS="$(uname -s)"
if [ "$HOST_OS" = "Darwin" ]; then
    if command -v dot_clean >/dev/null 2>&1; then
        echo "Cleaning macOS resource fork files with dot_clean..."
        for dir in postgres redis clickhouse elasticsearch minio data; do
            target="$VOLUME_ROOT/$dir"
            if ! dot_clean -m "$target" 2>/dev/null; then
                echo "Warning: dot_clean failed for $target; continuing"
            fi
        done
        echo "Resource fork cleanup complete"
    else
        echo "Skipping dot_clean: not installed"
    fi

    # Belt-and-suspenders: remove any remaining ._* files that dot_clean missed
    # or that macOS recreated after chmod operations above.
    echo "Removing remaining ._* resource fork files..."
    removed_count=$(find "$VOLUME_ROOT" -name '._*' -type f -delete -print 2>/dev/null | wc -l | tr -d ' ')
    echo "Removed $removed_count resource fork files"
else
    echo "Skipping resource fork cleanup: host OS is $HOST_OS (macOS-only)"
fi

echo ""
echo "Heber storage initialized at $VOLUME_ROOT"
