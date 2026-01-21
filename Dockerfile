# =============================================================================
# Heber Multi-Stage Dockerfile per PRD §19
# =============================================================================
# Base image: python:3.11-slim-bookworm (Debian for compatibility)
# Security: Non-root user, minimal dependencies, no cache layers
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder - Install dependencies
# -----------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

# Copy only dependency files first (better layer caching)
COPY pyproject.toml README.md ./

# Install dependencies to a separate directory
RUN uv pip install --target=/build/deps -e .

# -----------------------------------------------------------------------------
# Stage 2: Runtime - Minimal production image
# -----------------------------------------------------------------------------
FROM python:3.11-slim-bookworm AS runtime

# Labels per OCI spec
LABEL org.opencontainers.image.source="https://github.com/jacobmcmillan/heber"
LABEL org.opencontainers.image.description="Heber Data Lakehouse"
LABEL org.opencontainers.image.licenses="Proprietary"

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/cache/apt/archives/*

WORKDIR /app

# Copy installed dependencies from builder
COPY --from=builder /build/deps /app/deps

# Copy application code
COPY heber/ /app/heber/
COPY features/ /app/features/

# Set Python path to include deps
ENV PYTHONPATH=/app/deps:/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Security: Create non-root user per PRD §19.3
RUN useradd --no-create-home --uid 65534 --gid 0 heber \
    && chown -R heber:0 /app

# Switch to non-root user
USER heber

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

# Default port
EXPOSE 8080

# Default command (overridden per service in docker-compose)
CMD ["python", "-m", "uvicorn", "heber.catalog.api:app", "--host", "0.0.0.0", "--port", "8080"]

# -----------------------------------------------------------------------------
# Stage 3 (optional): Consumer service
# -----------------------------------------------------------------------------
FROM runtime AS consumer
CMD ["python", "-m", "heber.bus.consumer"]

# -----------------------------------------------------------------------------
# Stage 4 (optional): Writer service
# -----------------------------------------------------------------------------
FROM runtime AS writer
CMD ["python", "-m", "heber.writer.service"]

# -----------------------------------------------------------------------------
# Stage 5 (optional): Compactor service
# -----------------------------------------------------------------------------
FROM runtime AS compactor
CMD ["python", "-m", "heber.writer.compaction"]

# -----------------------------------------------------------------------------
# Stage 6 (optional): Catalog API service
# -----------------------------------------------------------------------------
FROM runtime AS catalog
CMD ["python", "-m", "uvicorn", "heber.catalog.api:app", "--host", "0.0.0.0", "--port", "8080"]

# -----------------------------------------------------------------------------
# Stage 7 (optional): Backfill service
# -----------------------------------------------------------------------------
FROM runtime AS backfill
CMD ["python", "-m", "heber.backfill"]

# -----------------------------------------------------------------------------
# Stage 8 (optional): Hot Store Loader service
# -----------------------------------------------------------------------------
FROM runtime AS hotloader
CMD ["python", "-m", "heber.writer.hotstore"]
