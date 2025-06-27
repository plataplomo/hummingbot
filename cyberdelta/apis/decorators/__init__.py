"""API Decorators Package - CyberDeltaEngine.

This package contains decorators for type-safe and security-hardened API interactions.

Core Decorators:
- typed_api_method: Ultimate type-safe API method decorator
- dict_response: Single object responses
- list_response: Array responses
- auto_typed: Smart type detection from hints

Security Decorators:
- secure_transform: Security-first transformation with validation
- business_logic_validated: Financial constraints enforcement
- security_monitored: Real-time anomaly detection
- secure_mapped_response: End-to-end HTTP + security + mapping

Rate Limiting & Resilience:
- rate_limited: Token bucket rate limiting
- retry_on_failure: Exponential backoff retry
- CircuitBreaker: Prevent cascading failures
- Timeout: Operation timeout enforcement

Advanced Decorators:
- mapped_response: Automatic raw → domain model mapping
- validation_pipeline: Chainable validation steps
"""

from .rate_limiting_decorators import (
    CircuitBreaker,
    RateLimited,
    RetryOnFailure,
    Timeout,
    rate_limited,
    retry_on_failure,
)
from .security_decorators import (
    TransformationError,
    business_logic_validated,
    secure_mapped_response,
    secure_transform,
    security_monitored,
)
from .typed_responses import (
    TypedResponseError,
    auto_typed,
    dict_response,
    list_response,
    mapped_response,
    optional_response,
    typed_api_method,
    validated_response,
    validation_pipeline,
)


__all__ = [
    "CircuitBreaker",
    "RateLimited",
    "RetryOnFailure",
    "Timeout",
    "TransformationError",
    "TypedResponseError",
    "auto_typed",
    "business_logic_validated",
    "dict_response",
    "list_response",
    "mapped_response",
    "optional_response",
    # Rate Limiting & Resilience
    "rate_limited",
    "retry_on_failure",
    "secure_mapped_response",
    # Security Decorators
    "secure_transform",
    "security_monitored",
    # Type Safety Decorators
    "typed_api_method",
    "validated_response",
    "validation_pipeline",
]
