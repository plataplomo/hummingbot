"""
Models for API-related functionality in CyberDeltaEngine.

This package contains shared models used across different exchange APIs,
including error handling, configuration, and rate limiting models.
"""

from .api_error import APIError, TransformationError
from .api_error_codes import APIErrorCode
from .api_error_response import APIErrorResponse
from .exchange_api_config import ExchangeAPIConfig
from .rate_limiter_config import EndpointRateConfig, RateLimiterConfig

__all__ = [
    # Error handling
    "APIError",
    "APIErrorCode",
    "APIErrorResponse",
    "TransformationError",
    # Configuration
    "ExchangeAPIConfig",
    # Rate limiting
    "EndpointRateConfig",
    "RateLimiterConfig",
]
