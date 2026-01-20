#!/bin/bash
# =============================================================================
# Heber ClickHouse Backup Script per PRD §24.2
# =============================================================================
# Runs daily via cron at 02:00 UTC
# Stores backups in S3 with 7-day retention
# =============================================================================

set -euo pipefail

# Configuration
BACKUP_NAME="daily-$(date +%Y%m%d-%H%M%S)"
S3_BUCKET="${HEBER_BACKUP_BUCKET:-heber-backups-prod}"
S3_PREFIX="clickhouse"
RETENTION_DAYS=7

echo "============================================"
echo "ClickHouse Backup Starting"
echo "============================================"
echo "Backup Name: ${BACKUP_NAME}"
echo "S3 Bucket:   ${S3_BUCKET}"
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
    echo "✅ Backup verified in S3"
else
    echo "❌ Backup verification failed!"
    exit 1
fi

echo ""
echo "============================================"
echo "✅ ClickHouse Backup Complete"
echo "============================================"
echo "Backup: ${BACKUP_NAME}"
echo "S3 Path: s3://${S3_BUCKET}/${S3_PREFIX}/${BACKUP_NAME}"
echo ""
