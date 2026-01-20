#!/bin/bash
# =============================================================================
# Heber Docker Build Script per PRD §19.5
# =============================================================================
# Image Tagging Strategy:
# - Git SHA (immutable): sha-<short_hash>
# - Semver (releases): v<major>.<minor>.<patch>
# - Branch tags: main -> latest
# =============================================================================

set -euo pipefail

# Configuration
REGISTRY="${HEBER_REGISTRY:-ghcr.io/jacobmcmillan/heber}"
SERVICE="${1:-runtime}"  # consumer, writer, compactor, catalog, backfill, hotloader
VERSION="${2:-}"  # Optional semver tag like v1.0.0

# Get git info
GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# Determine tags
SHA_TAG="sha-${GIT_SHA}"
FULL_IMAGE="${REGISTRY}:${SHA_TAG}"

echo "============================================"
echo "Building Heber Image"
echo "============================================"
echo "Service:    ${SERVICE}"
echo "Registry:   ${REGISTRY}"
echo "Git SHA:    ${GIT_SHA}"
echo "Branch:     ${GIT_BRANCH}"
echo "Build Date: ${BUILD_DATE}"
echo "============================================"

# Build the image targeting specific service stage
docker build \
    --target "${SERVICE}" \
    --build-arg BUILD_DATE="${BUILD_DATE}" \
    --build-arg GIT_SHA="${GIT_SHA}" \
    --build-arg VERSION="${VERSION:-dev}" \
    --label "org.opencontainers.image.created=${BUILD_DATE}" \
    --label "org.opencontainers.image.revision=${GIT_SHA}" \
    --label "org.opencontainers.image.version=${VERSION:-${SHA_TAG}}" \
    -t "${FULL_IMAGE}" \
    -t "${REGISTRY}:${SERVICE}-${SHA_TAG}" \
    .

echo "Built: ${FULL_IMAGE}"

# Tag with branch name (mutable)
if [ "${GIT_BRANCH}" = "main" ]; then
    docker tag "${FULL_IMAGE}" "${REGISTRY}:latest"
    docker tag "${FULL_IMAGE}" "${REGISTRY}:${SERVICE}-latest"
    echo "Tagged: ${REGISTRY}:latest"
fi

# Tag with semver if provided
if [ -n "${VERSION}" ]; then
    docker tag "${FULL_IMAGE}" "${REGISTRY}:${VERSION}"
    docker tag "${FULL_IMAGE}" "${REGISTRY}:${SERVICE}-${VERSION}"
    echo "Tagged: ${REGISTRY}:${VERSION}"
fi

echo ""
echo "============================================"
echo "Build Complete!"
echo "============================================"
echo "To push: ./scripts/docker-push.sh ${SERVICE} ${VERSION:-}"
echo ""
