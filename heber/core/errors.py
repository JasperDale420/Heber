"""
Standardized Heber Exceptions.

Follows Empire Exception Protocol.
"""

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    CONFIG_ERROR = "CONFIG_ERROR"
    DB_ERROR = "DB_ERROR"
    INGESTION_ERROR = "INGESTION_ERROR"
    LAKE_ERROR = "LAKE_ERROR"


class HeberError(Exception):
    """Base exception for all Heber errors."""

    def __init__(
        self, message: str, code: str | ErrorCode = ErrorCode.UNKNOWN_ERROR, details: dict[str, Any] | None = None
    ):
        super().__init__(message)
        self.message = message
        self.code = str(code.value) if isinstance(code, Enum) else str(code)
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": True,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }
