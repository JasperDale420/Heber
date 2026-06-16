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
version = "0.0.0.dev0"
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
import logging
import os
import sys

import structlog

_configured = False
_service_name = "unknown"

def _inject_service_name(logger, method_name, event_dict):
    if "service" not in event_dict:
        event_dict["service"] = _service_name
    return event_dict

def _rename_event_to_message(logger, method_name, event_dict):
    if "event" in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict

def _upcase_level(logger, method_name, event_dict):
    if "level" in event_dict:
        event_dict["level"] = event_dict["level"].upper()
    return event_dict

def setup_logging(service_name="unknown", level="INFO", *, force=False, **kw):
    global _configured, _service_name
    force = force or "PYTEST_CURRENT_TEST" in os.environ
    if _configured and not force:
        return

    _service_name = service_name
    level_name = os.environ.get("EMPIRE_LOG_LEVEL", level).upper()
    level_number = getattr(logging, level_name, logging.INFO)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _upcase_level,
            _inject_service_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _rename_event_to_message,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.basicConfig(format="%(message)s", handlers=[handler], level=level_number, force=force)
    logging.getLogger().setLevel(level_number)
    _configured = True

def get_logger(name=None):
    return structlog.get_logger(name)

def bind_context(**kw):
    structlog.contextvars.bind_contextvars(**kw)

def clear_context():
    structlog.contextvars.clear_contextvars()

def unbind_context(*keys):
    structlog.contextvars.unbind_contextvars(*keys)

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
version = "0.0.0.dev0"
requires-python = ">=3.11"
dependencies = ["pydantic>=2.5"]
EOF

cat > ../empire-schemas/empire_schemas/__init__.py << 'EOF'
"""CI stub for empire-schemas."""
EOF

echo "CI stubs created for empire-core and empire-schemas"
