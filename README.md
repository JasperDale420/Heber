# Heber Data Lakehouse

Centralized storage for market and intelligence data across all trading projects.

## Quick Start

```bash
# Initialize external volume directories
./scripts/init_volume.sh

# Start infrastructure
docker-compose up -d

# Run tests
uv run pytest tests/ -v
```

## Architecture

```
Data Gateway → Redis Streams → Heber Writer → Bronze/Silver/Gold (Parquet)
                                    ↓
                              Heber Catalog (Postgres)
                                    ↓
                              Hot Store (ClickHouse)
```

## Storage

All data is stored on the external volume `/Volumes/heber`:

- `data/` - Bronze/Silver/Gold Parquet files
- `postgres/` - Catalog database
- `clickhouse/` - Hot Store
- `redis/` - Event bus streams

## Services

- **heber-catalog**: REST API for dataset/instrument discovery
- **heber-consumer**: Redis → Lake writer
- **heber-compactor**: Periodic file compaction

## SDK Usage

```python
from heber.sdk import HeberClient

client = HeberClient()

# Read Silver data (point-in-time correct)
bars = client.read_asof("bars", asof_time="2025-01-15", instrument_keys=["equity:AAPL"])

# Write Gold features
client.write_gold("momentum_features", df=features, project="kairos", version="v1")
```

## Development

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) for dependency management
- Docker & Docker Compose

### Setup

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Set up pre-commit hooks
pre-commit install

# Verify hooks work
pre-commit run --all-files
```

### Code Quality

This project uses a comprehensive hygiene stack (largely LLM-generated code with automated enforcement):

| Tool | Purpose |
|------|---------|
| **ruff** | Python linting + formatting |
| **mypy** | Static type checking |
| **bandit** | Security vulnerability scanning |
| **detect-secrets** | Prevent accidental secret commits |
| **pytest-cov** | Test coverage (reports to `coverage.xml`) |

```bash
# Run all quality checks
pre-commit run --all-files

# Run tests with coverage
pytest tests/ -v --cov=heber --cov-report=term-missing --cov-report=xml
```

### CI/CD

GitHub Actions workflow (`.github/workflows/ci.yaml`) runs:

1. **Build** - Docker image creation
2. **Test** - Linting (ruff, mypy) + pytest with coverage
3. **Scan** - Trivy security scanning for vulnerabilities
4. **Push** - Container registry push (main branch only)
5. **Deploy** - Staging → Production (main branch only)

**Dependabot** auto-creates PRs for dependency updates weekly.

**SonarQube** expects `coverage.xml` at repo root (configure `sonar-project.properties` with your project key).
