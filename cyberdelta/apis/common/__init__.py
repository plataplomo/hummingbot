"""CyberDeltaEngine: APIs common package.

This package contains shared types, models, and interfaces used across all exchange APIs.
These are fundamental components that don't depend on specific exchange implementations.
"""

from __future__ import annotations

from .api_error import APIError, TransformationError
from .api_error_codes import APIErrorCode
from .api_error_response import APIErrorResponse
from .error_mapper_interface import IErrorMapper
from .types import MessageHandler


__all__ = [
    "APIError",
    "APIErrorCode",
    "APIErrorResponse",
    "IErrorMapper",
    "MessageHandler",
    "TransformationError",
]
