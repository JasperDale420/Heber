"""Standardized Heber Exceptions.

Follows Empire Exception Protocol via empire-core.
"""

from enum import Enum

from empire_core.errors import EmpireError


class ErrorCode(str, Enum):
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"
    DB_ERROR = "DB_ERROR"
    INGESTION_ERROR = "INGESTION_ERROR"
    LAKE_ERROR = "LAKE_ERROR"


class HeberError(EmpireError):
    """Base exception for all Heber errors."""
