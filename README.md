# Heber Data Lakehouse

Centralized storage and retrieval for market + intelligence data across all trading projects.

## Quick Start (Local)

```bash
# One-time: initialize external volume directories
./scripts/init_volume.sh

# Copy environment template
cp .env.example .env

# Start infrastructure + Heber services
docker compose up -d

# Run tests
uv run pytest tests/ -v
```

Local ports from `docker-compose.yml`:

- Catalog API: `http://localhost:8085`
- Postgres: `localhost:5433`
- Redis: `localhost:6380`
- ClickHouse: `localhost:8124` (HTTP), `localhost:9002` (native)
- lakeFS: `http://localhost:8000`
- MinIO: `http://localhost:19000` (S3), `http://localhost:19001` (console)
- Apicurio Registry: `http://localhost:18081`
- OpenMetadata: `http://localhost:8585`

## Architecture (Current)

```
Data Gateway -> Redis Streams -> heber-consumer -> Bronze (JSONL.gz) + Silver (Parquet)
                                            v
                                    heber-catalog (Postgres)
                                            v
                                      SDK + CLI
                                            v
                                Hot Store (ClickHouse)
```

## Storage Layout

All data is stored on the external volume (default: `/Volumes/heber`):

- `data/bronze/` - raw JSONL.gz (provider/feed/dt/hour)
- `data/silver/` - normalized Parquet (feed/instrument_type/dt[/hour])
- `data/gold/` - features/labels Parquet (dataset/project/version/dt)
- `postgres/` - catalog database
- `clickhouse/` - hot store
- `redis/` - event bus streams

## Services

- **heber-catalog**: FastAPI service for datasets, instruments, and feed mappings
- **heber-consumer**: Redis Streams consumer → Bronze/Silver writers
- **heber-compactor**: Parquet file compaction (Silver/Gold)
- **heber-watch**: Real-time flow alert tracking → TP/SL labels for ML
- **Hot Store**: ClickHouse for low-latency reads (sync helpers in `heber/hotstore/`)

### Watch Service

Tracks flow alert outcomes for ML labeling:

```bash
# Run locally
python -m heber.watch --help

# Or with Docker
docker compose up heber-watch
```

Environment variables:

- `HEBER_REDIS_URL` - Redis connection (default: `redis://localhost:6380`)
- `DATA_GATEWAY_URL` - Data Gateway for option quotes (default: `http://localhost:8000`)
- `HEBER_GOLD_PATH` - Gold layer output path

### ML Package (`heber/ml/`)

Meta-labeling infrastructure for predicting alert success:

```python
from heber.ml import (
    MetaLabelDatasetBuilder,  # Join features + outcomes
    MetaModelTrainer,          # Train LightGBM classifier
    MetaLabelScorer,           # Score new alerts
    AlertGate,                 # Filter low-probability alerts
)

# Build training dataset
builder = MetaLabelDatasetBuilder()
df = builder.build_from_parquet(start_date, end_date)

# Train model
trainer = MetaModelTrainer()
trainer.train(x_train, y_train, x_val, y_val)
trainer.save(Path("/models/meta_model"))

# Score alerts (optional integration)
scorer = MetaLabelScorer(config=InferenceConfig(model_path=Path("/models/meta_model")))
await scorer.initialize()
score = await scorer.score(alert)  # 0.0 - 1.0
```

**What runs automatically:**

| Component | Automatic? | Notes |
|-----------|------------|-------|
| Feature capture | ✅ Yes | Triggers on every alert via `AlertWatchConsumer` |
| Dataset building | ❌ No | Run manually for training |
| Model training | ❌ No | Run manually, logs to MLflow |
| Inference scoring | ⚙️ Optional | Enable `AlertGate` in consumer |

## SDK Usage

```python
from heber.sdk.client import HeberClient

client = HeberClient()

# Read Silver data (point-in-time correct)
bars = client.read_asof("bars", asof_time="2025-01-15", instrument_keys=["equity:AAPL"])

# Write Gold features
client.write_gold("momentum_features", df=features, project="kairos", version="v1")
```

## CLI Usage

```bash
heber info --verbose
heber datasets --layer silver
heber versions momentum_features
```

## Documentation

- `docs/architecture.md` - system overview, data flow, and layers
- `docs/catalog_api.md` - Catalog API reference
- `docs/data_contract.md` - EventEnvelope + feed schema contract
- `docs/schema_registry.md` - schema registry usage
- `docs/iceberg_migration.md` - Iceberg migration status
- `docs/hot_store.md` - Hot Store usage and sync notes
- `docs/configuration.md` - environment variables and local vs container settings
- `docs/sdk.md` - SDK usage and semantics
- `docs/operations/` - runbooks (deployment, monitoring, backup/DR)

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
3. **Scan** - Trivy security scanning for vulnerabilities and filesystem secrets/misconfigurations (fails on HIGH/CRITICAL findings)
4. **Push** - Container registry push (main branch only)
5. **Deploy** - Staging -> Production (main branch only)

**Dependabot** auto-creates PRs for dependency updates weekly.

**SonarQube** expects `coverage.xml` at repo root (configure `sonar-project.properties` with your project key).
