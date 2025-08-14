"""WebSocket-specific exception classes.

Unified exception hierarchy for all WebSocket error scenarios.
This module consolidates exceptions from multiple files into a single,
well-organized hierarchy for better maintainability and consistency.

## Exception Hierarchy Overview

```
WebSocketException (base for all WebSocket errors)
├── WebSocketDataValidationError (validation-time errors)
│   └── PayloadValidationError (payload-specific validation)
│       ├── InvalidPayloadTypeError (type mismatches)
│       ├── PayloadSizeError (size constraint violations)
│       │   └── PayloadTooLargeError (backward compatibility)
│       ├── PayloadNoneError (None when value required)
│       └── MissingRequiredFieldsError (missing required fields)
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

## Usage Examples

### Basic Exception Creation

```python
from cyberdelta.apis.websocket.ws_exceptions import (
    InvalidPayloadTypeError,
    PayloadSizeError,
    SecurityValidationError,
    MessageSizeExceedsLimitError,
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

### Using the Exception Factory (Recommended)

```python
from cyberdelta.apis.websocket.ws_exception_factory import (
    create_exception_factory,
)

# Create factory for consistent error handling
factory = create_exception_factory()

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
from cyberdelta.apis.websocket.ws_exceptions import WebSocketException


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
    if isinstance(error, WebSocketException):
        logger.info(
            f"Troubleshooting: {error.get_troubleshooting_guide()}"
        )
```

### Batch Error Creation

```python
from cyberdelta.apis.websocket.ws_exception_factory import (
    create_exception_factory,
)

factory = create_exception_factory()

# Create multiple related errors for comprehensive validation
errors = factory.create_batch_validation_errors([
    {
        "type": "invalid_payload_type",
        "params": {
            "context": "order",
            "actual_type": str,
            "expected_type": "dict",
        },
    },
    {
        "type": "missing_required_fields",
        "params": {
            "context": "order",
            "missing_fields": ["symbol", "quantity"],
        },
    },
])

# All errors share correlation ID for tracking
correlation_id = errors[0].correlation_id
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
```

## Migration from Legacy Exceptions

Legacy exceptions from ws_validators.py, ws_envelope.py, and ws_security.py
are being consolidated into this unified hierarchy. Use the exception factory
for creating new exceptions to ensure consistency.

```python
# OLD (deprecated) - scattered across multiple files
from cyberdelta.apis.websocket.ws_validators import (
    PayloadSizeError,
)  # OLD
from cyberdelta.apis.websocket.ws_security import (
    MessageSizeExceedsLimitError,
)  # OLD

# NEW (recommended) - unified hierarchy with factory
from cyberdelta.apis.websocket.ws_exception_factory import (
    create_exception_factory,
)

factory = create_exception_factory()
error = factory.create_payload_size_error(...)  # NEW
```
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from cyberdelta.apis.common.error_foundation import (
    ErrorSeverity,
    WebSocketRecoveryStrategy,
)
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext

# Constants for error handling
MAX_RAW_MESSAGE_SIZE = 1000
RAW_MESSAGE_SAMPLE_SIZE = 200


# ============================================================================
# Base Exception Classes
# ============================================================================


class WebSocketException(Exception):
    """Base class for all WebSocket exceptions.

    Provides common functionality for error tracking, correlation,
    and troubleshooting across the entire WebSocket module.

    Features:
    - Automatic error ID generation for unique tracking
    - Correlation ID support for linking related errors
    - Timestamp tracking for chronological analysis
    - Categorization for monitoring and alerting
    - Serialization for structured logging
    - Troubleshooting guidance for operations

    Example:
        ```python
        # Direct creation (not recommended - use factory instead)
        error = WebSocketException(
            message="Connection failed", correlation_id="req-12345"
        )

        # Access common properties
        print(f"Error ID: {error.error_id}")
        print(f"Category: {error.category}")
        print(f"Correlation: {error.correlation_id}")

        # Serialize for logging
        log_data = error.to_dict()
        logger.error("WebSocket error", extra=log_data)

        # Get troubleshooting guidance
        print(error.get_troubleshooting_guide())
        ```

    Note:
        Use ws_exception_factory.create_exception_factory() for creating
        exceptions instead of direct instantiation to ensure consistency.
    """

    def __init__(
        self,
        message: str,
        error_id: str | None = None,
        correlation_id: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize base WebSocket exception.

        Args:
            message: Human-readable error message
            error_id: Unique identifier for this error instance
            correlation_id: ID to correlate related errors
            cause: Original exception that caused this error
        """
        super().__init__(message)
        self.error_id = error_id or str(uuid4())
        self.correlation_id = correlation_id
        self.timestamp = time.time()
        self.cause = cause

    @property
    def category(self) -> str:
        """Get error category for classification."""
        return self.__class__.__name__.replace("WebSocket", "").replace("Error", "")

    def to_dict(self) -> dict[str, Any]:
        """Serialize exception for logging/monitoring."""
        return {
            "error_id": self.error_id,
            "category": self.category,
            "message": str(self),
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "exception_type": self.__class__.__name__,
            "cause": str(self.cause) if self.cause else None,
        }

    def get_troubleshooting_guide(self) -> str:
        """Get user-friendly troubleshooting information."""
        return (
            f"Error {self.category}: {self}. Check logs for correlation_id: {self.correlation_id}"
        )


class WebSocketDataValidationError(WebSocketException):
    """Base class for all data validation-related WebSocket errors.

    Consolidates payload, envelope, field, and value validation errors
    under a single hierarchy for consistent handling.

    This hierarchy covers validation errors that occur during message
    processing before the message reaches the stream processing layer.

    Subclasses:
    - PayloadValidationError: Payload-specific validation issues
    - InvalidPayloadTypeError: Type mismatches in payload data
    - PayloadSizeError: Size constraint violations
    - PayloadNoneError: None values when data is required
    - MissingRequiredFieldsError: Missing required fields

    Example:
        ```python
        from cyberdelta.apis.websocket.ws_exception_factory import (
            create_exception_factory,
        )

        factory = create_exception_factory()

        # Create payload validation errors
        type_error = factory.create_invalid_payload_type_error(
            context="order_processing", actual_type=str, expected_type="dict"
        )

        size_error = factory.create_payload_size_error(
            context="message_validation",
            actual_size=2048,
            constraint="at most",
            limit=1024,
        )

        # Handle validation errors
        if isinstance(error, WebSocketDataValidationError):
            logger.warning(
                f"Validation failed in {error.context}: {error.validation_type}"
            )
        ```

    Note: This is separate from WebSocketValidationError (which inherits from
    WebSocketStreamError) to distinguish validation-time errors from runtime
    stream validation errors.
    """

    def __init__(
        self,
        message: str,
        context: str,
        validation_type: str,
        **kwargs: Any,
    ) -> None:
        """Initialize data validation error.

        Args:
            message: Error message
            context: Validation context (e.g., "envelope", "payload")
            validation_type: Type of validation that failed
            **kwargs: Additional arguments passed to base class
        """
        super().__init__(message, **kwargs)
        self.context = context
        self.validation_type = validation_type

    def get_troubleshooting_guide(self) -> str:
        """Get validation-specific troubleshooting information."""
        return (
            f"Validation Error in {self.context}: {self.validation_type} validation failed. "
            f"Message: {self}. Check data format and requirements."
        )


# ============================================================================
# Payload Validation Errors (Step 22)
# ============================================================================


class PayloadValidationError(WebSocketDataValidationError):
    """Base class for payload validation errors.

    Consolidates all payload-related validation errors including
    type errors, size errors, and missing field errors.
    """

    def __init__(
        self,
        message: str,
        payload_type: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize payload validation error.

        Args:
            message: Error message
            payload_type: Type of payload that failed validation
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            context="payload",
            validation_type="payload_validation",
            **kwargs,
        )
        self.payload_type = payload_type


class InvalidPayloadTypeError(PayloadValidationError, TypeError):
    """Unified error for invalid payload types.

    Consolidates InvalidPayloadTypeError from ws_validators.py
    and similar type validation errors.
    """

    def __init__(
        self,
        context: str,
        actual_type: type,
        expected_type: str,
        **kwargs: Any,
    ) -> None:
        """Initialize invalid payload type error.

        Args:
            context: Context where validation failed
            actual_type: Actual type that was provided
            expected_type: Expected type description
            **kwargs: Additional arguments
        """
        message = (
            f"Invalid payload type in {context}: "
            f"expected {expected_type}, got {actual_type.__name__}"
        )
        super().__init__(
            message=message,
            payload_type=actual_type.__name__,
            **kwargs,
        )
        self.actual_type = actual_type
        self.expected_type = expected_type


class PayloadSizeError(PayloadValidationError):
    """Unified error for payload size violations.

    Consolidates PayloadSizeError from ws_validators.py and
    PayloadTooLargeError from ws_envelope.py.
    """

    def __init__(
        self,
        context: str,
        actual_size: int,
        constraint: str,
        limit: int,
        size_unit: str = "bytes",
        **kwargs: Any,
    ) -> None:
        """Initialize payload size error.

        Args:
            context: Context where size validation failed
            actual_size: Actual size that was provided
            constraint: Constraint type ("at least", "at most", "exactly")
            limit: Size limit that was violated
            size_unit: Unit of size measurement
            **kwargs: Additional arguments
        """
        message = (
            f"Payload size violation in {context}: "
            f"size {actual_size} {size_unit}, expected {constraint} {limit} {size_unit}"
        )
        super().__init__(
            message=message,
            payload_type="size_constraint",
            **kwargs,
        )
        self.actual_size = actual_size
        self.constraint = constraint
        self.limit = limit
        self.size_unit = size_unit


class PayloadTooLargeError(PayloadSizeError):
    """Specific error for payload exceeding maximum size.

    Backward compatibility alias for PayloadSizeError with "at most" constraint.
    """

    def __init__(
        self,
        payload_type: str,
        size: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize payload too large error.

        Args:
            payload_type: Type of payload (dict, list, etc.)
            size: Actual size
            limit: Maximum allowed size
            **kwargs: Additional arguments
        """
        super().__init__(
            context=payload_type,
            actual_size=size,
            constraint="at most",
            limit=limit,
            size_unit="items" if payload_type in ("dict", "list") else "bytes",
            **kwargs,
        )


class PayloadNoneError(PayloadValidationError):
    """Error for None payload when value is required.

    Migrated from ws_envelope.py for consistency.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize payload None error."""
        super().__init__(
            message="Payload cannot be None",
            payload_type="None",
            **kwargs,
        )


class MissingRequiredFieldsError(PayloadValidationError):
    """Error for missing required fields in payload.

    Migrated from ws_validators.py for consistency.
    """

    def __init__(
        self,
        context: str,
        missing_fields: list[str],
        **kwargs: Any,
    ) -> None:
        """Initialize missing required fields error.

        Args:
            context: Context where fields are missing
            missing_fields: List of missing field names
            **kwargs: Additional arguments
        """
        fields_str = ", ".join(missing_fields)
        message = f"Missing required fields in {context}: {fields_str}"
        super().__init__(
            message=message,
            payload_type="required_fields",
            **kwargs,
        )
        self.missing_fields = missing_fields


# ============================================================================
# Envelope Validation Errors (Step 27 - Migrated from ws_envelope.py)
# ============================================================================


class EnvelopeValidationError(WebSocketDataValidationError):
    """Base class for envelope-specific validation errors.

    Migrated from ws_envelope.py to unified hierarchy.
    Provides structured information about envelope validation failures,
    including envelope data and validation context.
    """

    def __init__(
        self,
        message: str,
        envelope_data: dict[str, Any] | None = None,
        validation_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize envelope validation error.

        Args:
            message: Error message describing the validation failure
            envelope_data: The envelope data that failed validation
            validation_context: Additional context about the validation failure
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            context="envelope",
            validation_type="envelope_structure",
            **kwargs,
        )
        self.envelope_data = envelope_data
        self.validation_context = validation_context or {}

    def get_troubleshooting_guide(self) -> str:
        """Get envelope-specific troubleshooting information."""
        base_guide = super().get_troubleshooting_guide()
        return (
            f"{base_guide} Check envelope structure, required fields, and data types. "
            f"Validation context: {self.validation_context}"
        )


class RoutingKeyValidationError(EnvelopeValidationError):
    """Base class for routing key validation errors."""

    def __init__(
        self,
        message: str,
        routing_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize routing key validation error.

        Args:
            message: Error message
            routing_key: The invalid routing key
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            validation_context={"routing_key": routing_key},
            **kwargs,
        )
        self.routing_key = routing_key


class EmptyRoutingKeyError(RoutingKeyValidationError):
    """Raised when routing key is empty or None.

    Migrated from ws_envelope.py to unified hierarchy.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize empty routing key error."""
        super().__init__(
            message="Routing key cannot be empty or None",
            routing_key=None,
            **kwargs,
        )


class InvalidRoutingKeyFormatError(RoutingKeyValidationError):
    """Raised when routing key has invalid format.

    Migrated from ws_envelope.py to unified hierarchy.
    """

    def __init__(self, routing_key: str, **kwargs: Any) -> None:
        """Initialize invalid routing key format error.

        Args:
            routing_key: The invalid routing key
            **kwargs: Additional arguments
        """
        super().__init__(
            message=f"Invalid routing key format: {routing_key}",
            routing_key=routing_key,
            **kwargs,
        )


class EnvelopeValidationFailedError(EnvelopeValidationError):
    """Raised when envelope validation fails for a specific exchange.

    Migrated from ws_envelope.py to unified hierarchy.
    Wraps validation errors with exchange context.
    """

    def __init__(
        self,
        exchange_name: str,
        original_error: str,
        **kwargs: Any,
    ) -> None:
        """Initialize envelope validation failed error.

        Args:
            exchange_name: Name of the exchange where validation failed
            original_error: Original error message
            **kwargs: Additional arguments
        """
        message = f"Envelope validation failed for {exchange_name}: {original_error}"
        super().__init__(
            message=message,
            validation_context={"exchange_name": exchange_name, "original_error": original_error},
            **kwargs,
        )
        self.exchange_name = exchange_name
        self.original_error = original_error


# ============================================================================
# Field and Format Validation Errors (Step 28 - Migrated from ws_validators.py)
# ============================================================================


class FieldValidationError(WebSocketDataValidationError):
    """Base class for field-specific validation errors.
    
    Migrated from ws_validators.py to unified hierarchy.
    Handles field-level validation failures including type mismatches,
    format violations, and constraint violations.
    """
    
    def __init__(
        self,
        message: str,
        field_name: str | None = None,
        field_value: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize field validation error.
        
        Args:
            message: Error message
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            context=f"field:{field_name}" if field_name else "field",
            validation_type="field_validation",
            **kwargs,
        )
        self.field_name = field_name
        self.field_value = field_value


class InvalidItemTypeError(FieldValidationError, TypeError):
    """Raised when an item in a list has an invalid type.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        index: int,
        actual_type: type[Any],
        expected_type: type[Any],
        **kwargs: Any,
    ) -> None:
        """Initialize invalid item type error.
        
        Args:
            context: Context where the error occurred
            index: Index of the invalid item
            actual_type: Actual type of the item
            expected_type: Expected type of the item
            **kwargs: Additional arguments
        """
        message = (
            f"{context} payload item {index} has invalid type: "
            f"{actual_type.__name__}. Expected {expected_type.__name__}."
        )
        super().__init__(
            message=message,
            field_name=f"item[{index}]",
            field_value=None,
            **kwargs,
        )
        self.index = index
        self.actual_type = actual_type
        self.expected_type = expected_type


class InvalidFieldTypeError(FieldValidationError, TypeError):
    """Raised when a field has an invalid type.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        actual_type: type[Any],
        expected_type: str,
        **kwargs: Any,
    ) -> None:
        """Initialize invalid field type error.
        
        Args:
            context: Context where the error occurred
            actual_type: Actual type of the field
            expected_type: Expected type description
            **kwargs: Additional arguments
        """
        message = f"Invalid {context} type: {actual_type.__name__}. Expected {expected_type}."
        super().__init__(
            message=message,
            field_name=context,
            **kwargs,
        )
        self.actual_type = actual_type
        self.expected_type = expected_type


class UnexpectedFieldsError(FieldValidationError):
    """Raised when unexpected fields are found in payload.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        unexpected_fields: list[str],
        allowed_fields: list[str],
        **kwargs: Any,
    ) -> None:
        """Initialize unexpected fields error.
        
        Args:
            context: Context where the error occurred
            unexpected_fields: List of unexpected field names
            allowed_fields: List of allowed field names
            **kwargs: Additional arguments
        """
        message = (
            f"Unexpected fields in {context}: {unexpected_fields}. "
            f"Allowed fields: {allowed_fields}"
        )
        super().__init__(
            message=message,
            **kwargs,
        )
        self.unexpected_fields = unexpected_fields
        self.allowed_fields = allowed_fields


class InvalidFormatError(FieldValidationError):
    """Raised when a value doesn't match the expected format pattern.
    
    Migrated from ws_validators.py to unified hierarchy.
    Note: This is WebSocket-specific. General field validation uses
    cyberdelta.exceptions.field_validation.InvalidFormatError.
    """
    
    def __init__(
        self,
        context: str,
        value: str,
        pattern: str,
        **kwargs: Any,
    ) -> None:
        """Initialize invalid format error.
        
        Args:
            context: Context where the error occurred
            value: Value that doesn't match the pattern
            pattern: Expected pattern
            **kwargs: Additional arguments
        """
        message = f"Invalid {context} format: '{value}' doesn't match pattern '{pattern}'"
        super().__init__(
            message=message,
            field_name=context,
            field_value=value,
            **kwargs,
        )
        self.value = value
        self.pattern = pattern


class InvalidNumericValueError(FieldValidationError):
    """Raised when a numeric value is invalid.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        value: Any,
        reason: str,
        **kwargs: Any,
    ) -> None:
        """Initialize invalid numeric value error.
        
        Args:
            context: Context where the error occurred
            value: Invalid numeric value
            reason: Reason why the value is invalid
            **kwargs: Additional arguments
        """
        message = f"Invalid {context} numeric value: {value}. {reason}"
        super().__init__(
            message=message,
            field_name=context,
            field_value=value,
            **kwargs,
        )
        self.value = value
        self.reason = reason


class NumericRangeError(FieldValidationError):
    """Raised when a numeric value is outside the allowed range.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        value: float | int,
        min_value: float | int | None = None,
        max_value: float | int | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize numeric range error.
        
        Args:
            context: Context where the error occurred
            value: Value outside the range
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            **kwargs: Additional arguments
        """
        if min_value is not None and max_value is not None:
            message = f"{context} value {value} outside range [{min_value}, {max_value}]"
        elif min_value is not None:
            message = f"{context} value {value} below minimum {min_value}"
        elif max_value is not None:
            message = f"{context} value {value} above maximum {max_value}"
        else:
            message = f"{context} value {value} outside allowed range"
            
        super().__init__(
            message=message,
            field_name=context,
            field_value=value,
            **kwargs,
        )
        self.value = value
        self.min_value = min_value
        self.max_value = max_value


class InvalidTimestampError(FieldValidationError):
    """Raised when a timestamp value is invalid.
    
    Migrated from ws_validators.py to unified hierarchy.
    """
    
    def __init__(
        self,
        context: str,
        value: Any,
        reason: str,
        **kwargs: Any,
    ) -> None:
        """Initialize invalid timestamp error.
        
        Args:
            context: Context where the error occurred
            value: Invalid timestamp value
            reason: Reason why the timestamp is invalid
            **kwargs: Additional arguments
        """
        message = f"Invalid {context} timestamp: {value}. {reason}"
        super().__init__(
            message=message,
            field_name=context,
            field_value=value,
            **kwargs,
        )
        self.value = value
        self.reason = reason


# ============================================================================
# Security Validation Errors (Step 23)
# ============================================================================


class WebSocketSecurityValidationError(WebSocketException):
    """Base class for all security validation-related WebSocket errors.

    Consolidates authentication, authorization, blocked patterns,
    and security validation errors under a unified hierarchy.

    Note: This is separate from WebSocketSecurityError (which inherits from
    WebSocketStreamError) to distinguish validation-time security errors
    from runtime stream security errors.
    """

    def __init__(
        self,
        message: str,
        security_type: str,
        violation_details: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize security validation error.

        Args:
            message: Error message
            security_type: Type of security violation
            violation_details: Details about the violation
            **kwargs: Additional arguments
        """
        super().__init__(message, **kwargs)
        self.security_type = security_type
        self.violation_details = violation_details or {}

    def get_troubleshooting_guide(self) -> str:
        """Get security-specific troubleshooting information."""
        return (
            f"Security Validation Error ({self.security_type}): {self}. "
            f"Review security policies and data validation. "
            f"Violation details: {self.violation_details}"
        )


class SecurityValidationError(WebSocketSecurityValidationError, ValueError):
    """Unified error for security validation failures.

    Consolidates SecurityValidationError from ws_security.py with
    enhanced structure and hierarchy integration.
    """

    def __init__(
        self,
        message: str,
        violation_type: str,
        message_data: dict[str, Any] | None = None,
        security_context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize security validation error.

        Args:
            message: Error message
            violation_type: Type of security violation
            message_data: Data that triggered the violation
            security_context: Security context information
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            security_type=violation_type,
            violation_details={
                "violation_type": violation_type,
                "message_data": message_data,
                "security_context": security_context,
            },
            **kwargs,
        )
        self.violation_type = violation_type
        self.message_data = message_data
        self.security_context = security_context


class BlockedPatternFoundError(WebSocketSecurityValidationError):
    """Error for blocked pattern detection.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        pattern: str,
        content_preview: str,
        **kwargs: Any,
    ) -> None:
        """Initialize blocked pattern found error.

        Args:
            pattern: The blocked pattern that was found
            content_preview: Preview of content containing the pattern
            **kwargs: Additional arguments
        """
        message = f"Blocked pattern '{pattern}' found in content"
        super().__init__(
            message=message,
            security_type="blocked_pattern",
            violation_details={
                "pattern": pattern,
                "content_preview": content_preview,
            },
            **kwargs,
        )
        self.pattern = pattern
        self.content_preview = content_preview


class SizeSecurityError(WebSocketSecurityValidationError):
    """Base class for size-related security errors.

    Consolidates all size limit violations to prevent resource
    exhaustion and DoS attacks.
    """

    def __init__(
        self,
        message: str,
        size_type: str,
        actual_size: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize size security error.

        Args:
            message: Error message
            size_type: Type of size limit (message, nesting, string, etc.)
            actual_size: Actual size that exceeded the limit
            limit: The size limit that was exceeded
            **kwargs: Additional arguments
        """
        super().__init__(
            message=message,
            security_type=f"size_limit_{size_type}",
            violation_details={
                "size_type": size_type,
                "actual_size": actual_size,
                "limit": limit,
            },
            **kwargs,
        )
        self.size_type = size_type
        self.actual_size = actual_size
        self.limit = limit


class MessageSizeExceedsLimitError(SizeSecurityError):
    """Error for message size exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        message_size: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize message size exceeds limit error.

        Args:
            message_size: Actual message size in bytes
            limit: Maximum allowed size in bytes
            **kwargs: Additional arguments
        """
        message = f"Message size {message_size} bytes exceeds limit of {limit} bytes"
        super().__init__(
            message=message,
            size_type="message",
            actual_size=message_size,
            limit=limit,
            **kwargs,
        )


class MessageSizeValidationFailedError(SizeSecurityError):
    """Error for message size validation failure.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        original_error: str,
        **kwargs: Any,
    ) -> None:
        """Initialize message size validation failed error.

        Args:
            original_error: Description of the original validation error
            **kwargs: Additional arguments
        """
        message = f"Message size validation failed: {original_error}"
        super().__init__(
            message=message,
            size_type="validation",
            actual_size=0,  # Unknown size when validation fails
            limit=0,  # Unknown limit when validation fails
            **kwargs,
        )
        self.original_error = original_error


class NestingDepthExceedsLimitError(SizeSecurityError):
    """Error for nesting depth exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        current_depth: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize nesting depth exceeds limit error.

        Args:
            current_depth: Actual nesting depth
            limit: Maximum allowed nesting depth
            **kwargs: Additional arguments
        """
        message = f"Message nesting depth {current_depth} exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="nesting_depth",
            actual_size=current_depth,
            limit=limit,
            **kwargs,
        )


class ObjectKeysExceedLimitError(SizeSecurityError):
    """Error for object key count exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        key_count: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize object keys exceed limit error.

        Args:
            key_count: Actual number of object keys
            limit: Maximum allowed number of keys
            **kwargs: Additional arguments
        """
        message = f"Object has {key_count} keys, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="object_keys",
            actual_size=key_count,
            limit=limit,
            **kwargs,
        )


class ArrayLengthExceedsLimitError(SizeSecurityError):
    """Error for array length exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        array_length: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize array length exceeds limit error.

        Args:
            array_length: Actual array length
            limit: Maximum allowed array length
            **kwargs: Additional arguments
        """
        message = f"Array has {array_length} items, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="array_length",
            actual_size=array_length,
            limit=limit,
            **kwargs,
        )


class StringLengthExceedsLimitError(SizeSecurityError):
    """Error for string length exceeding limits.

    Migrated from ws_security.py for consistency.
    """

    def __init__(
        self,
        string_length: int,
        limit: int,
        **kwargs: Any,
    ) -> None:
        """Initialize string length exceeds limit error.

        Args:
            string_length: Actual string length
            limit: Maximum allowed string length
            **kwargs: Additional arguments
        """
        message = f"String has {string_length} characters, exceeds limit of {limit}"
        super().__init__(
            message=message,
            size_type="string_length",
            actual_size=string_length,
            limit=limit,
            **kwargs,
        )


# ============================================================================
# Connection Errors
# ============================================================================


class WebSocketConnectionError(WebSocketStreamError):
    """Error related to WebSocket connection issues."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize connection error.

        Args:
            message: Error message
            context: Stream error context
            code: Specific error code (defaults to CONNECTION_LOST)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.CONNECTION_LOST

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


# ============================================================================
# Authentication Errors
# ============================================================================


class WebSocketAuthenticationError(WebSocketStreamError):
    """Authentication-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        code: WebSocketErrorCode | None = None,
        is_permanent: bool = False,
        cause: Exception | None = None,
    ) -> None:
        """Initialize authentication error.

        Args:
            message: Error message
            context: Stream error context
            code: Specific error code (defaults to AUTH_FAILED)
            is_permanent: Whether this is a permanent auth failure
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.AUTH_FAILED

        # Permanent auth failures cannot be retried
        if is_permanent:
            recovery_strategy = WebSocketRecoveryStrategy.NONE
            severity = ErrorSeverity.CRITICAL
        else:
            recovery_strategy = WebSocketRecoveryStrategy.EXPONENTIAL_BACKOFF
            severity = ErrorSeverity.ERROR

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=severity,
            recovery_strategy=recovery_strategy,
            cause=cause,
        )


# ============================================================================
# Subscription Errors
# ============================================================================


class WebSocketSubscriptionError(WebSocketStreamError):
    """Subscription-related WebSocket error."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        channel: str | None = None,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize subscription error.

        Args:
            message: Error message
            context: Stream error context
            channel: Channel that failed to subscribe
            code: Specific error code (defaults to SUBSCRIPTION_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.SUBSCRIPTION_FAILED

        # Update context with channel if provided
        if channel:
            context.channel = channel

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.RESUBSCRIBE_SINGLE,
            cause=cause,
        )


class WebSocketSubscriptionLimitError(WebSocketSubscriptionError):
    """Subscription limit exceeded."""

    def __init__(
        self,
        context: StreamErrorContext,
        limit: int,
        current: int,
        channel: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize subscription limit error.

        Args:
            context: Stream error context
            limit: Maximum allowed subscriptions
            current: Current number of subscriptions
            channel: Channel that couldn't be subscribed
            cause: Original exception if any
        """
        message = f"Subscription limit exceeded: {current}/{limit}"
        if channel:
            message += f" when subscribing to {channel}"

        context.extra_context["subscription_limit"] = limit
        context.extra_context["current_subscriptions"] = current

        super().__init__(
            message=message,
            context=context,
            channel=channel,
            code=WebSocketErrorCode.SUBSCRIPTION_LIMIT_EXCEEDED,
            cause=cause,
        )


class WebSocketInvalidChannelError(WebSocketSubscriptionError):
    """Invalid channel for subscription."""

    def __init__(
        self,
        channel: str,
        context: StreamErrorContext,
        reason: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize invalid channel error.

        Args:
            channel: Invalid channel name
            context: Stream error context
            reason: Reason why channel is invalid
            cause: Original exception if any
        """
        message = f"Invalid channel: {channel}"
        if reason:
            message += f" ({reason})"

        context.extra_context["invalid_channel"] = channel
        if reason:
            context.extra_context["invalid_reason"] = reason

        super().__init__(
            message=message,
            context=context,
            channel=channel,
            code=WebSocketErrorCode.SUBSCRIPTION_INVALID_CHANNEL,
            cause=cause,
        )


# ============================================================================
# Validation Errors
# ============================================================================


class WebSocketValidationError(WebSocketStreamError):
    """Data validation error in WebSocket stream."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        field: str | None = None,
        value: object = None,
        code: WebSocketErrorCode | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize validation error.

        Args:
            message: Error message
            context: Stream error context
            field: Field that failed validation
            value: Invalid value
            code: Specific error code (defaults to VALIDATION_FAILED)
            cause: Original exception if any
        """
        if code is None:
            code = WebSocketErrorCode.VALIDATION_FAILED

        # Store validation details
        self._field = field
        self._value = value
        if field:
            context.extra_context["validation_field"] = field
        if value is not None:
            context.extra_context["invalid_value"] = str(value)

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )

    @property
    def field(self) -> str | None:
        """Get the field that failed validation."""
        return self._field

    @property
    def value(self) -> object:
        """Get the invalid value."""
        return self._value


class WebSocketMessageFormatError(WebSocketValidationError):
    """Invalid message format in WebSocket stream."""

    def __init__(
        self,
        context: StreamErrorContext,
        expected_format: str,
        actual_format: str | None = None,
        raw_message: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize message format error.

        Args:
            context: Stream error context
            expected_format: Expected message format
            actual_format: Actual format received
            raw_message: Raw message if available
            cause: Original exception if any
        """
        message = f"Invalid message format. Expected: {expected_format}"
        if actual_format:
            message += f", Got: {actual_format}"

        context.extra_context["expected_format"] = expected_format
        if actual_format:
            context.extra_context["actual_format"] = actual_format
        if raw_message and len(raw_message) < MAX_RAW_MESSAGE_SIZE:  # Don't store huge messages
            context.extra_context["raw_message_sample"] = raw_message[:RAW_MESSAGE_SAMPLE_SIZE]

        super().__init__(
            message=message,
            context=context,
            code=WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
            cause=cause,
        )


# ============================================================================
# Stream Errors
# ============================================================================


class WebSocketStreamInterruptedError(WebSocketStreamError):
    """WebSocket stream was interrupted."""

    def __init__(
        self,
        context: StreamErrorContext,
        reason: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize stream interrupted error.

        Args:
            context: Stream error context
            reason: Reason for interruption
            cause: Original exception if any
        """
        message = "WebSocket stream interrupted"
        if reason:
            message += f": {reason}"

        if reason:
            context.extra_context["interruption_reason"] = reason

        super().__init__(
            message=message,
            code=WebSocketErrorCode.STREAM_INTERRUPTED,
            context=context,
            severity=ErrorSeverity.ERROR,
            recovery_strategy=WebSocketRecoveryStrategy.RECONNECT_SAME,
            cause=cause,
        )


class WebSocketSequenceError(WebSocketStreamError):
    """Message sequence error in WebSocket stream."""

    def __init__(
        self,
        context: StreamErrorContext,
        expected_seq: int | None = None,
        actual_seq: int | None = None,
        gap_size: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize sequence error.

        Args:
            context: Stream error context
            expected_seq: Expected sequence number
            actual_seq: Actual sequence number received
            gap_size: Size of sequence gap
            cause: Original exception if any
        """
        if gap_size is not None and gap_size > 0:
            message = f"WebSocket sequence gap detected: {gap_size} messages missing"
            code = WebSocketErrorCode.SEQUENCE_GAP
        else:
            message = "WebSocket sequence out of order"
            code = WebSocketErrorCode.SEQUENCE_OUT_OF_ORDER

        if expected_seq is not None and actual_seq is not None:
            message += f" (expected: {expected_seq}, got: {actual_seq})"

        # Update context
        if expected_seq is not None:
            context.expected_sequence = expected_seq
        if actual_seq is not None:
            context.sequence_number = actual_seq

        super().__init__(
            message=message,
            code=code,
            context=context,
            severity=ErrorSeverity.WARNING,
            recovery_strategy=WebSocketRecoveryStrategy.IMMEDIATE_RETRY,
            cause=cause,
        )


# ============================================================================
# Rate Limiting Errors
# ============================================================================


# ============================================================================
# Protocol Errors
# ============================================================================


# ============================================================================
# Security Errors
# ============================================================================


class WebSocketSecurityError(WebSocketStreamError):
    """Security violation in WebSocket communication."""

    def __init__(
        self,
        message: str,
        context: StreamErrorContext,
        security_type: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        """Initialize security error.

        Args:
            message: Error message
            context: Stream error context
            security_type: Type of security violation
            cause: Original exception if any
        """
        if security_type:
            context.extra_context["security_type"] = security_type

        super().__init__(
            message=message,
            code=WebSocketErrorCode.SECURITY_VIOLATION,
            context=context,
            severity=ErrorSeverity.CRITICAL,
            recovery_strategy=WebSocketRecoveryStrategy.NONE,  # No retry for security
            cause=cause,
        )


# ============================================================================
# Configuration and Validation Errors
# ============================================================================


class WebSocketConfigurationError(Exception):
    """Base class for WebSocket configuration errors."""

    def __init__(
        self, component: str, issue: str, available_options: list[str] | None = None
    ) -> None:
        """Initialize configuration error.

        Args:
            component: The component that has configuration issues
            issue: Description of the configuration issue
            available_options: List of available options if applicable
        """
        self.component = component
        self.issue = issue
        self.available_options = available_options or []

        message = f"Configuration error in {component}: {issue}"
        if self.available_options:
            message += f". Available options: {', '.join(self.available_options)}"

        super().__init__(message)


# ============================================================================
# Configuration and Setup Errors
# ============================================================================


class EnvelopeValidatorNotSetError(WebSocketConfigurationError):
    """Raised when envelope validator is not set in router."""

    def __init__(self) -> None:
        """Initialize envelope validator not set error."""
        super().__init__(
            component="WebSocketRouter",
            issue="Envelope validator not set. Call set_envelope_validator() first",
        )


class BurstSizeTooLargeError(WebSocketConfigurationError):
    """Raised when burst size exceeds maximum allowed value."""

    def __init__(self, burst_size: int, max_burst_size: int) -> None:
        """Initialize burst size too large error.

        Args:
            burst_size: The requested burst size
            max_burst_size: Maximum allowed burst size
        """
        self.burst_size = burst_size
        self.max_burst_size = max_burst_size
        super().__init__(
            component="RateLimiter",
            issue=f"Burst size {burst_size} exceeds maximum {max_burst_size}",
        )


class UnsupportedAlgorithmError(WebSocketConfigurationError):
    """Raised when an unsupported rate limiting algorithm is specified."""

    def __init__(self, algorithm: str, supported_algorithms: list[str]) -> None:
        """Initialize unsupported algorithm error.

        Args:
            algorithm: The unsupported algorithm name
            supported_algorithms: List of supported algorithms
        """
        self.algorithm = algorithm
        super().__init__(
            component="RateLimiter",
            issue=f"Unsupported algorithm: {algorithm}",
            available_options=supported_algorithms,
        )


class RateLimitError(WebSocketException):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: float | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize rate limit error.

        Args:
            message: Error message
            retry_after: Seconds to wait before retrying
            **kwargs: Additional arguments for base class
        """
        self.retry_after = retry_after
        super().__init__(message, **kwargs)
        if retry_after:
            self.troubleshooting_guide = f"Rate limit exceeded. Please wait {retry_after:.1f} seconds before retrying."


class SuccessErrorMismatchError(WebSocketConfigurationError):
    """Raised when success flag and error presence don't match."""

    def __init__(self, success: bool, has_error: bool) -> None:
        """Initialize success/error mismatch error.

        Args:
            success: Success flag value
            has_error: Whether error is present
        """
        self.success = success
        self.has_error = has_error
        issue = f"Success flag is {success} but error {'is' if has_error else 'is not'} present"
        super().__init__(component="WebSocketResponse", issue=issue)


class AuthenticationErrorMismatchError(WebSocketConfigurationError):
    """Raised when authentication flag and auth error don't match."""

    def __init__(self, authenticated: bool, auth_error: str | None) -> None:
        """Initialize authentication error mismatch error.

        Args:
            authenticated: Authentication flag value
            auth_error: Authentication error message if present
        """
        self.authenticated = authenticated
        self.auth_error = auth_error
        issue = f"Authenticated flag is {authenticated} but auth_error {'is' if auth_error else 'is not'} present"
        super().__init__(component="AuthenticationResponse", issue=issue)


class WebSocketContextCreationError(TypeError):
    """Error when WebSocket context cannot be created."""

    def __init__(self, context_type: str, protocol_requirement: str) -> None:
        """Initialize context creation error.

        Args:
            context_type: The context type that failed
            protocol_requirement: The protocol requirement that was not met
        """
        self.context_type = context_type
        self.protocol_requirement = protocol_requirement

        message = (
            f"Context {context_type} does not provide {protocol_requirement} method "
            f"and is not a StreamErrorContext. Expected WebSocketContextProtocol "
            f"with {protocol_requirement} method or StreamErrorContext instance."
        )
        super().__init__(message)


class WebSocketTransformerError(ValueError):
    """Error when WebSocket transformer cannot handle context parameters."""

    def __init__(self, transformer_name: str, context_provided: bool) -> None:
        """Initialize transformer error.

        Args:
            transformer_name: Name of the transformer that failed
            context_provided: Whether context was provided to the transformer
        """
        self.transformer_name = transformer_name
        self.context_provided = context_provided

        if context_provided:
            message = (
                f"Transformer {transformer_name} does not accept context parameters "
                f"but context was provided. Either update transformer to accept context "
                f"or call without context."
            )
        else:
            message = f"Transformer {transformer_name} failed to process payload."

        super().__init__(message)


class WebSocketSequenceValidationError(ValueError):
    """Error for WebSocket sequence validation failures."""

    def __init__(
        self, validation_type: str, current_seq: int | None = None, expected_seq: int | None = None
    ) -> None:
        """Initialize sequence validation error.

        Args:
            validation_type: Type of validation that failed
            current_seq: Current sequence number
            expected_seq: Expected sequence number
        """
        self.validation_type = validation_type
        self.current_seq = current_seq
        self.expected_seq = expected_seq

        if validation_type == "expected_requires_sequence":
            message = "Expected sequence requires sequence number"
        elif validation_type == "expected_greater_than_current":
            message = "Expected sequence must be greater than current sequence"
        else:
            message = f"Sequence validation failed: {validation_type}"

        super().__init__(message)


class WebSocketFieldValidationError(ValueError):
    """Error for WebSocket field validation failures."""

    def __init__(self, field_name: str, constraint: str, value: object = None) -> None:
        """Initialize field validation error.

        Args:
            field_name: Name of the field that failed validation
            constraint: The constraint that was violated
            value: The invalid value
        """
        self.field_name = field_name
        self.constraint = constraint
        self.value = value

        if constraint == "non_negative":
            message = f"{field_name} must be non-negative"
        elif constraint == "sequence_non_negative":
            message = "Sequence number must be non-negative"
        elif constraint == "message_size_non_negative":
            message = "Message size must be non-negative"
        elif constraint == "timestamp_non_negative":
            message = "Timestamp must be non-negative"
        elif constraint == "timestamp_future":
            message = f"Timestamp {value} is in the future"
        else:
            message = f"{field_name} violates constraint: {constraint}"

        super().__init__(message)


# ============================================================================
# Public API Exports
# ============================================================================

__all__ = [
    # Base exception classes
    "WebSocketException",
    "WebSocketDataValidationError",
    "WebSocketSecurityValidationError",
    # Payload validation exceptions
    "PayloadValidationError",
    "InvalidPayloadTypeError",
    "PayloadSizeError",
    "PayloadTooLargeError",
    "PayloadNoneError",
    "MissingRequiredFieldsError",
    # Envelope validation exceptions (migrated from ws_envelope.py)
    "EnvelopeValidationError",
    "RoutingKeyValidationError",
    "EmptyRoutingKeyError",
    "InvalidRoutingKeyFormatError",
    "EnvelopeValidationFailedError",
    # Field and format validation exceptions (migrated from ws_validators.py)
    "FieldValidationError",
    "InvalidItemTypeError",
    "InvalidFieldTypeError",
    "UnexpectedFieldsError",
    "InvalidFormatError",
    "InvalidNumericValueError",
    "NumericRangeError",
    "InvalidTimestampError",
    # Security validation exceptions
    "SecurityValidationError",
    "BlockedPatternFoundError",
    "SizeSecurityError",
    "MessageSizeExceedsLimitError",
    "MessageSizeValidationFailedError",
    "NestingDepthExceedsLimitError",
    "ObjectKeysExceedLimitError",
    "ArrayLengthExceedsLimitError",
    "StringLengthExceedsLimitError",
    # Stream runtime exceptions (existing hierarchy)
    "WebSocketConnectionError",
    "WebSocketAuthenticationError",
    "WebSocketSubscriptionError",
    "WebSocketSubscriptionLimitError",
    "WebSocketInvalidChannelError",
    "WebSocketValidationError",
    "WebSocketMessageFormatError",
    "WebSocketStreamInterruptedError",
    "WebSocketSequenceError",
    "WebSocketSecurityError",
    # Configuration and setup exceptions
    "WebSocketConfigurationError",
    "EnvelopeValidatorNotSetError",
    "BurstSizeTooLargeError",
    "UnsupportedAlgorithmError",
    "RateLimitError",
    "SuccessErrorMismatchError",
    "AuthenticationErrorMismatchError",
    "WebSocketContextCreationError",
    "WebSocketTransformerError",
    "WebSocketSequenceValidationError",
    "WebSocketFieldValidationError",
]
