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

Advanced Decorators:
- mapped_response: Automatic raw → domain model mapping
- validation_pipeline: Chainable validation steps
"""

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
    # Type Safety Decorators
    "typed_api_method",
    "dict_response",
    "list_response",
    "optional_response",
    "auto_typed",
    "validated_response",
    "validation_pipeline",
    "mapped_response",
    "TypedResponseError",
    # Security Decorators
    "secure_transform",
    "business_logic_validated",
    "security_monitored",
    "secure_mapped_response",
    "TransformationError",
]
