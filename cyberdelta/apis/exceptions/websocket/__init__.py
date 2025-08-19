"""WebSocket error classes - structured hierarchy.

This package provides a comprehensive, well-organized error hierarchy for
all WebSocket error scenarios. The hierarchy is designed for better maintainability,
consistency, and debugging capabilities.

## Error Hierarchy Overview

```
WebSocketError (base for all WebSocket errors)
├── WebSocketDataValidationError (validation-time errors)
│   ├── PayloadValidationError (payload-specific validation)
│   ├── EnvelopeValidationError (envelope validation errors)
│   └── FieldValidationError (field-specific validation)
│
├── WebSocketSecurityValidationError (security validation errors)
│   ├── SecurityValidationError (general security violations)
│   ├── BlockedPatternFoundError (malicious pattern detection)
│   └── SizeSecurityError (DoS prevention size limits)
│
├── WebSocketConfigurationError (configuration-related errors)
│   └── Various configuration setup errors
│
└── WebSocketStreamError (runtime stream errors - existing hierarchy)
    └── Connection, authentication, and subscription errors
```

## Usage Examples

### Basic Error Creation

```python
from cyberdelta.apis.exceptions.websocket import (
    InvalidPayloadTypeError,
    PayloadSizeError,
    SecurityValidationError,
)

# Create validation errors directly
type_error = InvalidPayloadTypeError(
    context="order_request", actual_type=str, expected_type="dict"
)

size_error = PayloadSizeError(
    context="message_payload",
    actual_size=2048,
    constraint="at most",
    limit=1024,
    size_unit="bytes",
)
```

### Using the Error Factory (Recommended)

```python
from cyberdelta.apis.exceptions.websocket import (
    create_error_factory,
)

# Create factory for consistent error handling
factory = create_error_factory()

# Create related errors with shared correlation ID
validation_error = factory.create_invalid_payload_type_error(
    context="api_request", actual_type=list, expected_type="dict"
)

security_error = factory.create_blocked_pattern_found_error(
    pattern="<script>", content_preview="<script>alert('xss')</script>"
)

# Both errors share the same correlation ID for tracking
assert validation_error.correlation_id == security_error.correlation_id
```

### Error Categorization and Handling

```python
from cyberdelta.apis.exceptions.websocket import (
    WebSocketError,
    WebSocketDataValidationError,
    WebSocketSecurityValidationError,
)


def handle_websocket_error(error: Exception) -> None:
    if isinstance(error, WebSocketDataValidationError):
        # Handle validation-time errors
        logger.warning(f"Validation error: {error}")

    elif isinstance(error, WebSocketSecurityValidationError):
        # Handle security violations
        logger.error(f"Security violation: {error}")
        alert_security_team(error)

    elif isinstance(error, WebSocketStreamError):
        # Handle runtime stream errors
        logger.info(f"Stream error: {error}")
    # All WebSocket errors provide troubleshooting guidance
    if isinstance(error, WebSocketError):
        logger.info(
            f"Troubleshooting: {error.get_troubleshooting_guide()}"
        )
```
"""

from __future__ import annotations

# Import base error classes
from .base import (
    WebSocketConfigurationError,
    WebSocketDataValidationError,
    WebSocketError,
    WebSocketSecurityValidationError,
)
from .envelope_validation import (
    EmptyRoutingKeyError,
    EnvelopeValidationError,
    EnvelopeValidationFailedError,
    FieldValidationError,
    InvalidFieldTypeError,
    InvalidFormatError,
    InvalidItemTypeError,
    InvalidNumericValueError,
    InvalidRoutingKeyFormatError,
    InvalidTimestampError,
    NumericRangeError,
    RoutingKeyValidationError,
    UnexpectedFieldsError,
)

# Import factory functions
from .factory import (
    WebSocketErrorFactory,
    create_correlated_factory,
    create_error_factory,
    get_suggested_action,
    is_critical_error_code,
    is_retryable_error_code,
)

# Import validation error classes from split modules
from .payload_validation import (
    InvalidPayloadTypeError,
    MissingRequiredFieldsError,
    PayloadNoneError,
    PayloadSizeError,
    PayloadValidationError,
)

# Import security error classes
from .security import (
    ArrayLengthExceedsLimitError,
    BlockedPatternFoundError,
    MessageSizeExceedsLimitError,
    MessageSizeValidationFailedError,
    NestingDepthExceedsLimitError,
    ObjectKeysExceedLimitError,
    SecurityValidationError,
    SizeSecurityError,
    StringLengthExceedsLimitError,
)

# Import stream and configuration error classes
from .stream import (
    AuthenticationErrorMismatchError,
    BurstSizeTooLargeError,
    EnvelopeValidatorNotSetError,
    RateLimitError,
    SuccessErrorMismatchError,
    UnsupportedAlgorithmError,
    WebSocketAuthenticationError,
    WebSocketConnectionError,
    WebSocketContextCreationError,
    WebSocketFieldValidationError,
    WebSocketInvalidChannelError,
    WebSocketMessageFormatError,
    WebSocketSecurityError,
    WebSocketSequenceError,
    WebSocketSequenceValidationError,
    WebSocketStreamError,
    WebSocketStreamInterruptedError,
    WebSocketSubscriptionError,
    WebSocketSubscriptionLimitError,
    WebSocketTransformerError,
    WebSocketValidationError,
)


__all__ = [
    "ArrayLengthExceedsLimitError",
    "AuthenticationErrorMismatchError",
    "BlockedPatternFoundError",
    "BurstSizeTooLargeError",
    "EmptyRoutingKeyError",
    "EnvelopeValidationError",
    "EnvelopeValidationFailedError",
    "EnvelopeValidatorNotSetError",
    "FieldValidationError",
    "InvalidFieldTypeError",
    "InvalidFormatError",
    "InvalidItemTypeError",
    "InvalidNumericValueError",
    "InvalidPayloadTypeError",
    "InvalidRoutingKeyFormatError",
    "InvalidTimestampError",
    "MessageSizeExceedsLimitError",
    "MessageSizeValidationFailedError",
    "MissingRequiredFieldsError",
    "NestingDepthExceedsLimitError",
    "NumericRangeError",
    "ObjectKeysExceedLimitError",
    "PayloadNoneError",
    "PayloadSizeError",
    "PayloadValidationError",
    "RateLimitError",
    "RoutingKeyValidationError",
    "SecurityValidationError",
    "SizeSecurityError",
    "StringLengthExceedsLimitError",
    "SuccessErrorMismatchError",
    "UnexpectedFieldsError",
    "UnsupportedAlgorithmError",
    "WebSocketAuthenticationError",
    "WebSocketConfigurationError",
    "WebSocketConnectionError",
    "WebSocketContextCreationError",
    "WebSocketDataValidationError",
    "WebSocketError",
    "WebSocketErrorFactory",
    "WebSocketFieldValidationError",
    "WebSocketInvalidChannelError",
    "WebSocketMessageFormatError",
    "WebSocketSecurityError",
    "WebSocketSecurityValidationError",
    "WebSocketSequenceError",
    "WebSocketSequenceValidationError",
    "WebSocketStreamError",
    "WebSocketStreamInterruptedError",
    "WebSocketSubscriptionError",
    "WebSocketSubscriptionLimitError",
    "WebSocketTransformerError",
    "WebSocketValidationError",
    "create_correlated_factory",
    "create_error_factory",
    "get_suggested_action",
    "is_critical_error_code",
    "is_retryable_error_code",
]
