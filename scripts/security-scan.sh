#!/bin/bash
# =============================================================================
# Heber Vulnerability Scan Script per PRD §19.3
# =============================================================================
# Uses Trivy to scan container images for CVEs before push
# Requirement: trivy must be installed (brew install trivy / apt install trivy)
# =============================================================================

set -euo pipefail

IMAGE="${1:-ghcr.io/jacobmcmillan/heber:latest}"

echo "============================================"
echo "Vulnerability Scan: ${IMAGE}"
echo "============================================"

# Check if trivy is installed
if ! command -v trivy &> /dev/null; then
    echo "ERROR: trivy not found. Install it first:"
    echo "  macOS:  brew install trivy"
    echo "  Linux:  curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin"
    exit 1
fi

# Run vulnerability scan
echo "Scanning for vulnerabilities..."
trivy image \
    --config .trivy.yaml \
    --severity CRITICAL,HIGH \
    --exit-code 1 \
    --ignore-unfixed \
    "${IMAGE}"

SCAN_RESULT=$?

if [ ${SCAN_RESULT} -eq 0 ]; then
    echo ""
    echo "============================================"
    echo "✅ No CRITICAL or HIGH vulnerabilities found"
    echo "============================================"
else
    echo ""
    echo "============================================"
    echo "❌ Vulnerabilities detected! Fix before push."
    echo "============================================"
    exit 1
fi

# Additional checks
echo ""
echo "Running filesystem scan..."
trivy fs \
    --scanners secret,misconfig \
    --severity HIGH,CRITICAL \
    .

echo ""
echo "============================================"
echo "✅ All security scans passed"
echo "============================================"
