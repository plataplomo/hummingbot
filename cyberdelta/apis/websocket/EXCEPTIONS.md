# WebSocket Error Hierarchy

This document provides comprehensive documentation for the WebSocket error system, including usage examples and migration guidance.

## Error Hierarchy Overview

```
WebSocketError (base for all WebSocket errors)
├── WebSocketDataValidationError (validation-time errors)
│   ├── PayloadValidationError (payload-specific validation)
│   │   ├── InvalidPayloadTypeError (type mismatches)
│   │   ├── PayloadSizeError (size constraint violations)
│   │   │   └── PayloadTooLargeError (backward compatibility)
│   │   ├── PayloadNoneError (None when value required)
│   │   └── MissingRequiredFieldsError (missing required fields)
│   │
│   └── EnvelopeValidationError (envelope-specific validation)
│       ├── RoutingKeyValidationError (routing key validation base)
│       │   ├── EmptyRoutingKeyError (empty/None routing keys)
│       │   └── InvalidRoutingKeyFormatError (invalid key formats)
│       └── EnvelopeValidationFailedError (exchange validation failures)
│
├── WebSocketSecurityValidationError (security validation errors)
│   ├── SecurityValidationError (general security violations)
│   ├── BlockedPatternFoundError (malicious pattern detection)
│   └── SizeSecurityError (DoS prevention size limits)
│       ├── MessageSizeExceedsLimitError (message size limits)
│       ├── MessageSizeValidationFailedError (size validation failures)
│       ├── NestingDepthExceedsLimitError (object nesting limits)
│       ├── ObjectKeysExceedLimitError (object key count limits)
│       ├── ArrayLengthExceedsLimitError (array length limits)
│       └── StringLengthExceedsLimitError (string length limits)
│
└── WebSocketStreamError (runtime stream errors - existing hierarchy)
    ├── WebSocketConnectionError (connection issues)
    ├── WebSocketAuthenticationError (authentication failures)
    ├── WebSocketSubscriptionError (subscription problems)
    │   ├── WebSocketSubscriptionLimitError (subscription limits)
    │   └── WebSocketInvalidChannelError (invalid channels)
    ├── WebSocketValidationError (runtime validation)
    ├── WebSocketMessageFormatError (message format issues)
    ├── WebSocketStreamInterruptedError (stream interruptions)
    ├── WebSocketSequenceError (sequence problems)
    └── WebSocketSecurityError (runtime security violations)
```

## Key Features

### Base WebSocketError Features
- **Automatic error ID generation** for unique tracking
- **Correlation ID support** for linking related errors
- **Timestamp tracking** for chronological analysis
- **Categorization** for monitoring and alerting
- **Serialization** for structured logging
- **Troubleshooting guidance** for operations

### Exception Categories

1. **Validation-Time Errors** (`WebSocketDataValidationError`)
   - Occur during message processing before stream layer
   - Include payload validation, type checking, size validation
   - Used for rejecting invalid messages early

2. **Security Validation Errors** (`WebSocketSecurityValidationError`)
   - Security violations during validation phase
   - DoS prevention, pattern blocking, size limits
   - Separate from runtime security errors

3. **Runtime Stream Errors** (`WebSocketStreamError`)
   - Existing hierarchy for stream-level problems
   - Connection, authentication, subscription issues
   - Include error codes and recovery strategies

## Usage Examples

### 1. Basic Exception Creation

```python
from cyberdelta.apis.websocket.ws_exceptions import (
    InvalidPayloadTypeError,
    PayloadSizeError,
    SecurityValidationError,
    MessageSizeExceedsLimitError
)

# Create validation errors directly
type_error = InvalidPayloadTypeError(
    context="order_request",
    actual_type=str,
    expected_type="dict"
)

size_error = PayloadSizeError(
    context="message_payload",
    actual_size=2048,
    constraint="at most",
    limit=1024,
    size_unit="bytes"
)
```

### 2. Using the Exception Factory (Recommended)

```python
from cyberdelta.apis.websocket.ws_exception_factory import create_exception_factory

# Create factory for consistent error handling
factory = create_exception_factory()

# Create related errors with shared correlation ID
validation_error = factory.create_invalid_payload_type_error(
    context="api_request",
    actual_type=list,
    expected_type="dict"
)

security_error = factory.create_blocked_pattern_found_error(
    pattern="<script>",
    content_preview="<script>alert('xss')</script>"
)

# Both errors share the same correlation ID for tracking
assert validation_error.correlation_id == security_error.correlation_id
```

### 3. Error Categorization and Handling

```python
from cyberdelta.apis.websocket.exceptions import WebSocketError

def handle_websocket_error(error: Exception) -> None:
    if isinstance(error, WebSocketDataValidationError):
        # Handle validation-time errors
        logger.warning(f"Validation error: {error.category}")

    elif isinstance(error, WebSocketSecurityValidationError):
        # Handle security violations
        logger.error(f"Security violation: {error.security_type}")
        alert_security_team(error.violation_details)

    elif isinstance(error, WebSocketStreamError):
        # Handle runtime stream errors
        logger.info(f"Stream error: {error.code}")

    # All WebSocket errors provide troubleshooting guidance
    if isinstance(error, WebSocketError):
        logger.info(f"Troubleshooting: {error.get_troubleshooting_guide()}")
```

### 4. Batch Error Creation

```python
from cyberdelta.apis.websocket.exceptions import create_error_factory

factory = create_error_factory()

# Create multiple related errors for comprehensive validation
errors = factory.create_batch_validation_errors([
    {
        "type": "invalid_payload_type",
        "params": {
            "context": "order",
            "actual_type": str,
            "expected_type": "dict"
        }
    },
    {
        "type": "missing_required_fields",
        "params": {
            "context": "order",
            "missing_fields": ["symbol", "quantity"]
        }
    }
])

# All errors share correlation ID for tracking
correlation_id = errors[0].correlation_id
```

### 5. Error Serialization for Logging

```python
# All WebSocket exceptions can be serialized for structured logging
try:
    process_websocket_message(data)
except WebSocketError as e:
    # Serialize exception for logging/monitoring
    log_data = e.to_dict()
    logger.error("WebSocket processing failed", extra=log_data)

    # Log data includes:
    # - error_id: unique identifier
    # - category: error classification
    # - message: human-readable message
    # - timestamp: when error occurred
    # - correlation_id: for tracking related errors
    # - exception_type: specific exception class
    # - cause: original exception if chained
```

### 6. Correlation Tracking

```python
from cyberdelta.apis.websocket.ws_exception_factory import (
    create_exception_factory,
    create_correlated_factory
)

# Create factory with correlation ID
factory = create_exception_factory()

# Process multiple related operations
validation_error = factory.create_payload_size_error(...)

# Create correlated factory for related errors
correlated_factory = create_correlated_factory(validation_error)
security_error = correlated_factory.create_blocked_pattern_found_error(...)

# Both errors share the same correlation ID
assert validation_error.correlation_id == security_error.correlation_id
```

## Integration with Error Codes

All exceptions integrate with the WebSocket error code system for categorization:

```python
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode

# Error codes provide categorization and suggested actions
error_code = WebSocketErrorCode.MESSAGE_TOO_LARGE
print(f"Category: {error_code.get_category()}")
print(f"Retryable: {error_code.is_retryable()}")
print(f"Critical: {error_code.is_critical()}")
print(f"Action: {error_code.get_suggested_action()}")

# Factory automatically maps validation types to error codes
from cyberdelta.apis.websocket.ws_exception_factory import (
    get_error_code_for_validation_type,
    get_error_code_for_security_type
)

validation_code = get_error_code_for_validation_type('invalid_payload_type')
security_code = get_error_code_for_security_type('blocked_pattern')
```

## Migration from Legacy Exceptions

Legacy exceptions from `ws_validators.py`, `ws_envelope.py`, and `ws_security.py` are being consolidated into this unified hierarchy. Use the exception factory for creating new exceptions to ensure consistency.

### Before (Deprecated)

```python
# OLD - scattered across multiple files
from cyberdelta.apis.websocket.ws_validators import PayloadSizeError
from cyberdelta.apis.websocket.ws_security import MessageSizeExceedsLimitError

# Direct creation with inconsistent interfaces
error1 = PayloadSizeError("context", 100, "at most", 50)
error2 = MessageSizeExceedsLimitError(2048, 1024)
```

### After (Recommended)

```python
# NEW - unified hierarchy with factory
from cyberdelta.apis.websocket.ws_exception_factory import create_exception_factory

factory = create_exception_factory()

# Consistent creation with correlation tracking
error1 = factory.create_payload_size_error(
    context="validation",
    actual_size=100,
    constraint="at most",
    limit=50
)

error2 = factory.create_message_size_exceeds_limit_error(
    message_size=2048,
    limit=1024
)

# Both errors share correlation ID and have consistent interfaces
```

## Error Factory Methods

The `WebSocketErrorFactory` provides specialized creation methods:

### Payload Validation
- `create_invalid_payload_type_error()`
- `create_payload_size_error()`
- `create_payload_too_large_error()`
- `create_payload_none_error()`
- `create_missing_required_fields_error()`

### Envelope Validation
- `create_envelope_validation_error()`
- `create_empty_routing_key_error()`
- `create_invalid_routing_key_format_error()`
- `create_envelope_validation_failed_error()`

### Security Validation
- `create_security_validation_error()`
- `create_blocked_pattern_found_error()`
- `create_message_size_exceeds_limit_error()`
- `create_nesting_depth_exceeds_limit_error()`
- `create_object_keys_exceed_limit_error()`
- `create_array_length_exceeds_limit_error()`
- `create_string_length_exceeds_limit_error()`

### Batch Operations
- `create_batch_validation_errors()` - Create multiple related errors

### Utility Methods
- `get_error_category()` - Get error category from code
- `get_suggested_action()` - Get suggested action for code
- `is_retryable_error()` - Check if error is retryable
- `is_critical_error()` - Check if error is critical

## Best Practices

1. **Use the Exception Factory**: Always use `create_exception_factory()` instead of direct instantiation
2. **Correlation Tracking**: Use correlation IDs to track related errors across operations
3. **Proper Categorization**: Use the right exception hierarchy for the context (validation vs security vs stream)
4. **Error Code Integration**: Leverage error codes for automated handling and monitoring
5. **Structured Logging**: Use `to_dict()` method for consistent logging format
6. **Troubleshooting**: Use `get_troubleshooting_guide()` for operational guidance

## Error Code Mappings

The factory automatically maps exception types to appropriate error codes:

### Validation Error Codes
- `invalid_payload_type` → `PAYLOAD_TYPE_MISMATCH`
- `payload_size` → `MESSAGE_TOO_LARGE`
- `missing_required_fields` → `MISSING_REQUIRED_FIELD`

### Envelope Error Codes
- `envelope_validation` → `VALIDATION_FAILED`
- `empty_routing_key` → `INVALID_FIELD_VALUE`
- `invalid_routing_key_format` → `INVALID_FIELD_VALUE`
- `envelope_validation_failed` → `VALIDATION_FAILED`

### Security Error Codes
- `blocked_pattern` → `INJECTION_DETECTED`
- `message_size` → `MESSAGE_TOO_LARGE`
- `nesting_depth` → `OVERFLOW_DETECTED`
- `resource_limits` → `RESOURCE_EXHAUSTED`

## Public API

The module exports 33 exception classes through `__all__`:

```python
from cyberdelta.apis.websocket.ws_exceptions import *

# All exception classes are available for import
# See __all__ list in ws_exceptions.py for complete list
```

For factory usage:

```python
from cyberdelta.apis.websocket.ws_exception_factory import (
    create_exception_factory,
    create_correlated_factory,
    get_error_code_for_validation_type,
    get_error_code_for_security_type
)
```
