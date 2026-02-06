#!/bin/bash
# =============================================================================
# Heber ClickHouse Backup Script per PRD §24.2
# =============================================================================
# Runs daily via cron at 02:00 UTC
# Remote destination is managed by clickhouse-backup config
# =============================================================================

set -euo pipefail

# Configuration
BACKUP_NAME="daily-$(date +%Y%m%d-%H%M%S)"
RETENTION_DAYS=7
BACKUP_CONFIG_PATH="${CLICKHOUSE_BACKUP_CONFIG:-/etc/clickhouse-backup/config.yml}"

echo "============================================"
echo "ClickHouse Backup Starting"
echo "============================================"
echo "Backup Name: ${BACKUP_NAME}"
echo "Backup Config: ${BACKUP_CONFIG_PATH}"
echo "Remote Destination: managed by clickhouse-backup config"
echo "Retention:   ${RETENTION_DAYS} days"
echo "============================================"

# Check if clickhouse-backup is installed
if ! command -v clickhouse-backup &> /dev/null; then
    echo "ERROR: clickhouse-backup not found. Install it first."
    exit 1
fi

# Create local backup
echo "Creating local backup..."
clickhouse-backup create "${BACKUP_NAME}"

# Upload to S3
echo "Uploading to S3..."
clickhouse-backup upload "${BACKUP_NAME}"

# Cleanup old backups (local)
echo "Cleaning up local backups older than ${RETENTION_DAYS} days..."
clickhouse-backup delete local --keep-backups-local=${RETENTION_DAYS}

# Cleanup old backups (remote)
echo "Cleaning up remote backups older than ${RETENTION_DAYS} days..."
clickhouse-backup delete remote --keep-backups-remote=${RETENTION_DAYS}

# Verify backup
echo "Verifying backup..."
if clickhouse-backup list remote | grep -q "${BACKUP_NAME}"; then
    echo "✅ Backup verified in remote storage"
else
    echo "❌ Backup verification failed!"
    exit 1
fi

REMOTE_ENTRY=$(clickhouse-backup list remote | grep "${BACKUP_NAME}" | head -n 1)

echo ""
echo "============================================"
echo "✅ ClickHouse Backup Complete"
echo "============================================"
echo "Backup: ${BACKUP_NAME}"
echo "Remote Entry: ${REMOTE_ENTRY}"
echo ""
