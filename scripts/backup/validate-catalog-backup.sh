#!/bin/bash
# =============================================================================
# Heber Catalog Backup Validation Script per PRD §24.4
# =============================================================================
# Runs monthly to validate Postgres backup restores correctly
# =============================================================================

set -euo pipefail

# Configuration
DB_INSTANCE="heber-catalog-prod"
TEST_INSTANCE="heber-catalog-backup-test"
REGION="${AWS_REGION:-us-east-1}"
VALIDATION_PASSED=0

cleanup_test_instance() {
    if aws rds describe-db-instances \
        --db-instance-identifier "${TEST_INSTANCE}" \
        --region "${REGION}" >/dev/null 2>&1; then
        echo "Deleting test instance ${TEST_INSTANCE}..."
        aws rds delete-db-instance \
            --db-instance-identifier "${TEST_INSTANCE}" \
            --skip-final-snapshot \
            --region "${REGION}" >/dev/null 2>&1 || true
        echo "Waiting for test instance deletion..."
        aws rds wait db-instance-deleted \
            --db-instance-identifier "${TEST_INSTANCE}" \
            --region "${REGION}" >/dev/null 2>&1 || true
    else
        echo "No test instance cleanup needed."
    fi
}

cleanup_on_exit() {
    local exit_code=$?
    set +e
    echo ""
    echo "Ensuring backup-validation test instance cleanup..."
    cleanup_test_instance

    if [ "${exit_code}" -ne 0 ] || [ "${VALIDATION_PASSED}" -ne 1 ]; then
        echo ""
        echo "============================================"
        echo "❌ Backup Validation Failed"
        echo "============================================"
        echo "Status: FAILED"
    fi

    trap - EXIT
    exit "${exit_code}"
}

trap cleanup_on_exit EXIT

echo "============================================"
echo "Catalog Backup Validation"
echo "============================================"
echo "Source Instance:  ${DB_INSTANCE}"
echo "Test Instance:    ${TEST_INSTANCE}"
echo "Region:           ${REGION}"
echo "============================================"

# Get latest snapshot
echo "Finding latest snapshot..."
LATEST_SNAPSHOT=$(aws rds describe-db-snapshots \
    --db-instance-identifier "${DB_INSTANCE}" \
    --query 'DBSnapshots | sort_by(@, &SnapshotCreateTime) | [-1].DBSnapshotIdentifier' \
    --output text \
    --region "${REGION}")

if [ -z "${LATEST_SNAPSHOT}" ] || [ "${LATEST_SNAPSHOT}" = "None" ]; then
    echo "❌ No snapshots found for ${DB_INSTANCE}"
    exit 1
fi

echo "Latest snapshot: ${LATEST_SNAPSHOT}"

# Delete existing test instance if exists
echo "Cleaning up any existing test instance..."
cleanup_test_instance

# Restore from snapshot
echo "Restoring from snapshot..."
aws rds restore-db-instance-from-db-snapshot \
    --db-instance-identifier "${TEST_INSTANCE}" \
    --db-snapshot-identifier "${LATEST_SNAPSHOT}" \
    --db-instance-class db.t3.small \
    --no-publicly-accessible \
    --region "${REGION}"

echo "Waiting for instance to be available (this may take 10-15 minutes)..."
aws rds wait db-instance-available \
    --db-instance-identifier "${TEST_INSTANCE}" \
    --region "${REGION}"

# Get endpoint
ENDPOINT=$(aws rds describe-db-instances \
    --db-instance-identifier "${TEST_INSTANCE}" \
    --query 'DBInstances[0].Endpoint.Address' \
    --output text \
    --region "${REGION}")

echo "Test instance available at: ${ENDPOINT}"

# Run validation queries
echo "Running validation queries..."
PGPASSWORD="${POSTGRES_PASSWORD}" psql -h "${ENDPOINT}" -U heber -d heber_catalog -c "
SELECT COUNT(*) as dataset_count FROM datasets;
SELECT COUNT(*) as partition_count FROM partitions;
SELECT MAX(created_at) as latest_partition FROM partitions;
"
VALIDATION_PASSED=1

echo ""
echo "============================================"
echo "✅ Backup Validation Complete"
echo "============================================"
echo "Snapshot: ${LATEST_SNAPSHOT}"
echo "Status: PASSED"
echo ""
