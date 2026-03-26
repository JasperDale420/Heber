#!/usr/bin/env bash
# Create minimal installable stubs for monorepo path dependencies (empire-core, empire-schemas).
# These exist at ../empire-core and ../empire-schemas locally but are not separate GitHub repos,
# so CI needs lightweight stubs that satisfy the import and install requirements.
set -euo pipefail

# empire-core stub
mkdir -p ../empire-core/empire_core
cat > ../empire-core/pyproject.toml << 'EOF'
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
[project]
name = "empire-core"
version = "0.0.0-ci"
requires-python = ">=3.11"
dependencies = [
    "structlog>=24.1.0",
    "httpx>=0.27",
    "tenacity>=8.2",
    "pydantic-settings>=2.0",
    "sqlalchemy>=2.0",
    "pandas>=2.0",
]
EOF

cat > ../empire-core/empire_core/__init__.py << 'EOF'
"""CI stub for empire-core."""
EOF

cat > ../empire-core/empire_core/logger.py << 'EOF'
"""CI stub — delegates to structlog."""
import structlog

def setup_logging(*a, **kw):
    pass

def get_logger(name=None):
    return structlog.get_logger(name)

def bind_context(**kw):
    pass

def clear_context():
    pass

def log_error(logger, exc, *a, **kw):
    logger.error(str(exc), exc_info=True)

def log_retry(logger, *a, **kw):
    logger.warning("retry", **kw)
EOF

cat > ../empire-core/empire_core/errors.py << 'EOF'
"""CI stub for EmpireError."""

class EmpireError(Exception):
    def __init__(self, message, code="UNKNOWN", details=None):
        super().__init__(message)
        self.code = code
        self.details = details or {}

    def to_dict(self):
        return {"error": True, "code": self.code, "message": str(self), "details": self.details}
EOF

cat > ../empire-core/empire_core/http_client.py << 'EOF'
"""CI stub for HTTP client."""
import httpx

def create_http_client(**kw):
    return httpx.Client(**kw)

def create_async_http_client(**kw):
    return httpx.AsyncClient(**kw)

def http_retry(fn):
    return fn

def raise_for_status(resp):
    resp.raise_for_status()
EOF

cat > ../empire-core/empire_core/config.py << 'EOF'
"""CI stub for config."""
EOF

# empire-schemas stub
mkdir -p ../empire-schemas/empire_schemas
cat > ../empire-schemas/pyproject.toml << 'EOF'
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
[project]
name = "empire-schemas"
version = "0.0.0-ci"
requires-python = ">=3.11"
dependencies = ["pydantic>=2.5"]
EOF

cat > ../empire-schemas/empire_schemas/__init__.py << 'EOF'
"""CI stub for empire-schemas."""
EOF

echo "CI stubs created for empire-core and empire-schemas"
