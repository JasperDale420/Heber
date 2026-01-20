FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
RUN pip install uv

# Copy project files
COPY pyproject.toml .
COPY heber/ ./heber/
COPY features/ ./features/

# Install dependencies
RUN uv pip install --system -e .

# Create non-root user
RUN useradd -m -u 1000 heber && chown -R heber:heber /app
USER heber

# Default command (overridden in docker-compose)
CMD ["python", "-m", "uvicorn", "heber.catalog.api:app", "--host", "0.0.0.0", "--port", "8080"]
