#!/bin/bash
# =============================================================================
# Heber Docker Push Script per PRD §19.4
# =============================================================================
# Pushes images to registry (default: ghcr.io)
# =============================================================================

set -euo pipefail

REGISTRY="${HEBER_REGISTRY:-ghcr.io/jacobmcmillan/heber}"
SERVICE="${1:-runtime}"
VERSION="${2:-}"

GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
SHA_TAG="sha-${GIT_SHA}"
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

echo "============================================"
echo "Pushing Heber Images"
echo "============================================"
echo "Registry: ${REGISTRY}"
echo "Service:  ${SERVICE}"
echo "SHA Tag:  ${SHA_TAG}"
echo "============================================"

# Push SHA-tagged image (immutable)
echo "Pushing ${REGISTRY}:${SHA_TAG}..."
docker push "${REGISTRY}:${SHA_TAG}"
docker push "${REGISTRY}:${SERVICE}-${SHA_TAG}"

# Push branch tag if main
if [ "${GIT_BRANCH}" = "main" ]; then
    echo "Pushing ${REGISTRY}:latest..."
    docker push "${REGISTRY}:latest"
    docker push "${REGISTRY}:${SERVICE}-latest"
fi

# Push semver tag if provided
if [ -n "${VERSION}" ]; then
    echo "Pushing ${REGISTRY}:${VERSION}..."
    docker push "${REGISTRY}:${VERSION}"
    docker push "${REGISTRY}:${SERVICE}-${VERSION}"
fi

echo ""
echo "============================================"
echo "Push Complete!"
echo "============================================"
