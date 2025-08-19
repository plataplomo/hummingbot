"""WebSocket Error Factory.

This module provides a centralized factory for creating WebSocket errors
with standardized error codes, context information, and correlation tracking.

The factory ensures consistent exception creation across the entire WebSocket
module while maintaining type safety and providing enhanced debugging capabilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext

# Import the error codes
from cyberdelta.apis.enums.websocket import WebSocketErrorCode

from .base import (
    WebSocketDataValidationError,
    WebSocketError,
)
from .envelope_validation import (
    EmptyRoutingKeyError,
    EnvelopeValidationError,
    EnvelopeValidationFailedError,
    InvalidRoutingKeyFormatError,
)

# Import exceptions from the new structure
from .payload_validation import (
    InvalidPayloadTypeError,
    MissingRequiredFieldsError,
    PayloadNoneError,
    PayloadSizeError,
)
from .security import (
    BlockedPatternFoundError,
    SecurityValidationError,
)


# Error code mappings for consistent categorization
VALIDATION_ERROR_CODE_MAP = {
    "invalid_payload_type": WebSocketErrorCode.INVALID_MESSAGE_TYPE,
    "payload_size": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "payload_none": WebSocketErrorCode.INVALID_MESSAGE_TYPE,
    "missing_required_fields": WebSocketErrorCode.MISSING_REQUIRED_FIELD,
    "envelope_validation": WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
    "empty_routing_key": WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
    "invalid_routing_key_format": WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
    "envelope_validation_failed": WebSocketErrorCode.MESSAGE_VALIDATION_FAILED,
    "invalid_item_type": WebSocketErrorCode.INVALID_MESSAGE_TYPE,
    "invalid_field_type": WebSocketErrorCode.INVALID_MESSAGE_TYPE,
    "unexpected_fields": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "invalid_format": WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
    "invalid_numeric_value": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "numeric_range": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "invalid_timestamp": WebSocketErrorCode.INVALID_TIMESTAMP,
}

SECURITY_ERROR_CODE_MAP = {
    "security_validation": WebSocketErrorCode.SECURITY_VIOLATION,
    "blocked_pattern": WebSocketErrorCode.INJECTION_DETECTED,
    "message_size_exceeds": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "message_size_validation": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "nesting_depth_exceeds": WebSocketErrorCode.OVERFLOW_DETECTED,
    "object_keys_exceed": WebSocketErrorCode.OVERFLOW_DETECTED,
    "array_length_exceeds": WebSocketErrorCode.OVERFLOW_DETECTED,
    "string_length_exceeds": WebSocketErrorCode.OVERFLOW_DETECTED,
}


class WebSocketErrorFactory:
    """Factory for creating WebSocket errors with consistent error tracking.

    Provides centralized error creation with automatic correlation ID generation,
    error code mapping, and enhanced context tracking for debugging and monitoring.
    """

    def __init__(self, correlation_id: str | None = None) -> None:
        """Initialize exception factory.

        Args:
            correlation_id: Shared correlation ID for related errors.
                           If None, a new correlation ID will be generated.
        """
        self.correlation_id = correlation_id or str(uuid4())

    def _get_error_code(
        self, error_type: str, error_maps: dict[str, WebSocketErrorCode]
    ) -> WebSocketErrorCode | None:
        """Get error code for error type from mapping.

        Args:
            error_type: Type of error to look up
            error_maps: Dictionary mapping error types to error codes

        Returns:
            WebSocketErrorCode if found, None otherwise
        """
        return error_maps.get(error_type)

    # ============================================================================
    # Payload Validation Errors
    # ============================================================================

    def create_invalid_payload_type_error(
        self,
        context: str,
        actual_type: type,
        expected_type: str,
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> InvalidPayloadTypeError:
        """Create InvalidPayloadTypeError with factory correlation.

        Args:
            context: Context where validation failed
            actual_type: Actual type that was provided
            expected_type: Expected type description
            stream_context: Stream error context
            error_id: Unique error identifier

        Returns:
            InvalidPayloadTypeError instance
        """
        return InvalidPayloadTypeError(
            context=context,
            actual_type=actual_type,
            expected_type=expected_type,
            stream_context=stream_context,
            error_id=error_id,
            correlation_id=self.correlation_id,
        )

    def create_payload_size_error(
        self,
        context: str,
        actual_size: int,
        constraint: str,
        limit: int,
        size_unit: str = "bytes",
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> PayloadSizeError:
        """Create PayloadSizeError with factory correlation.

        Args:
            context: Context where size validation failed
            actual_size: Actual size that was provided
            constraint: Constraint type ("at least", "at most", "exactly")
            limit: Size limit that was violated
            size_unit: Unit of size measurement
            stream_context: Stream error context
            error_id: Unique error identifier

        Returns:
            PayloadSizeError instance
        """
        return PayloadSizeError(
            context=context,
            actual_size=actual_size,
            constraint=constraint,
            limit=limit,
            size_unit=size_unit,
            stream_context=stream_context,
            error_id=error_id,
            correlation_id=self.correlation_id,
        )

    def create_payload_too_large_error(
        self,
        payload_type: str,
        size: int,
        limit: int,
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> PayloadSizeError:
        """Create PayloadSizeError for too large payloads.

        This method is kept for backward compatibility but returns PayloadSizeError.

        Args:
            payload_type: Type of payload (dict, list, etc.)
            size: Actual size
            limit: Maximum allowed size
            stream_context: Stream error context
            error_id: Unique error identifier

        Returns:
            PayloadSizeError instance with 'at most' constraint
        """
        return PayloadSizeError(
            context=payload_type,
            actual_size=size,
            constraint="at most",
            limit=limit,
            size_unit="items" if payload_type in {"dict", "list"} else "bytes",
            stream_context=stream_context,
            error_id=error_id,
            correlation_id=self.correlation_id,
        )

    def create_payload_none_error(
        self,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> PayloadNoneError:
        """Create PayloadNoneError with factory correlation.

        Args:
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            PayloadNoneError instance
        """
        return PayloadNoneError(
            context=context,
            error_id=error_id,
            correlation_id=self.correlation_id,
        )

    def create_missing_required_fields_error(
        self,
        context: str,
        missing_fields: list[str],
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> MissingRequiredFieldsError:
        """Create MissingRequiredFieldsError with factory correlation.

        Args:
            context: Context where fields are missing
            missing_fields: List of missing field names
            stream_context: Stream error context
            error_id: Unique error identifier

        Returns:
            MissingRequiredFieldsError instance
        """
        return MissingRequiredFieldsError(
            context=context,
            missing_fields=missing_fields,
            stream_context=stream_context,
            error_id=error_id,
            correlation_id=self.correlation_id,
        )

    # ============================================================================
    # Envelope Validation Errors
    # ============================================================================

    def create_envelope_validation_error(
        self,
        message: str,
        envelope_data: dict[str, Any] | None = None,
        validation_context: dict[str, Any] | None = None,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> EnvelopeValidationError:
        """Create EnvelopeValidationError with factory correlation.

        Args:
            message: Error message describing the validation failure
            envelope_data: The envelope data that failed validation
            validation_context: Additional context about the validation failure
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            EnvelopeValidationError instance
        """
        return EnvelopeValidationError(
            message=message,
            envelope_data=envelope_data,
            validation_context=validation_context,
            context=context,
            error_id=error_id,
        )

    def create_empty_routing_key_error(
        self,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> EmptyRoutingKeyError:
        """Create EmptyRoutingKeyError with factory correlation.

        Args:
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            EmptyRoutingKeyError instance
        """
        return EmptyRoutingKeyError(
            context=context,
            error_id=error_id,
        )

    def create_invalid_routing_key_format_error(
        self,
        routing_key: str,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> InvalidRoutingKeyFormatError:
        """Create InvalidRoutingKeyFormatError with factory correlation.

        Args:
            routing_key: The invalid routing key
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            InvalidRoutingKeyFormatError instance
        """
        return InvalidRoutingKeyFormatError(
            routing_key=routing_key,
            context=context,
            error_id=error_id,
        )

    def create_envelope_validation_failed_error(
        self,
        exchange_name: str,
        original_error: str,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> EnvelopeValidationFailedError:
        """Create EnvelopeValidationFailedError with factory correlation.

        Args:
            exchange_name: Name of the exchange where validation failed
            original_error: Original error message
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            EnvelopeValidationFailedError instance
        """
        return EnvelopeValidationFailedError(
            exchange_name=exchange_name,
            original_error=original_error,
            context=context,
            error_id=error_id,
        )

    # ============================================================================
    # Security Validation Errors
    # ============================================================================

    def create_security_validation_error(
        self,
        message: str,
        violation_type: str,
        message_data: dict[str, Any] | None = None,
        security_context: dict[str, Any] | None = None,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> SecurityValidationError:
        """Create SecurityValidationError with factory correlation.

        Args:
            message: Error message
            violation_type: Type of security violation
            message_data: Data that triggered the violation
            security_context: Security context information
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            SecurityValidationError instance
        """
        return SecurityValidationError(
            message=message,
            violation_type=violation_type,
            message_data=message_data,
            security_context=security_context,
            context=context,
            error_id=error_id,
        )

    def create_blocked_pattern_found_error(
        self,
        pattern: str,
        content_preview: str,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
    ) -> BlockedPatternFoundError:
        """Create BlockedPatternFoundError with factory correlation.

        Args:
            pattern: The blocked pattern that was found
            content_preview: Preview of content containing the pattern
            context: Stream error context
            error_id: Unique error identifier

        Returns:
            BlockedPatternFoundError instance
        """
        return BlockedPatternFoundError(
            pattern=pattern,
            content_preview=content_preview,
            context=context,
            error_id=error_id,
        )

    # ============================================================================
    # Utility Methods
    # ============================================================================

    def create_batch_validation_errors(
        self, error_specs: list[dict[str, Any]]
    ) -> list[WebSocketError]:
        """Create multiple validation errors with shared correlation ID.

        Args:
            error_specs: List of error specifications, each containing:
                - type: Error type name
                - params: Parameters for error creation

        Returns:
            List of validation errors with shared correlation ID

        Example:
            >>> factory = WebSocketErrorFactory()
            >>> errors = factory.create_batch_validation_errors([
            ...     {
            ...         "type": "invalid_payload_type",
            ...         "params": {
            ...             "context": "order",
            ...             "actual_type": str,
            ...             "expected_type": "dict",
            ...         },
            ...     },
            ...     {
            ...         "type": "missing_required_fields",
            ...         "params": {
            ...             "context": "order",
            ...             "missing_fields": ["symbol", "quantity"],
            ...         },
            ...     },
            ... ])
        """
        errors: list[WebSocketError] = []
        for spec in error_specs:
            error_type = spec["type"]
            params = spec.get("params", {})

            error: WebSocketError
            if error_type == "invalid_payload_type":
                error = self.create_invalid_payload_type_error(**params)
            elif error_type == "payload_size":
                error = self.create_payload_size_error(**params)
            elif error_type == "missing_required_fields":
                error = self.create_missing_required_fields_error(**params)
            elif error_type == "security_validation":
                error = self.create_security_validation_error(**params)
            elif error_type == "blocked_pattern":
                error = self.create_blocked_pattern_found_error(**params)
            else:
                # Fallback to generic validation error
                error = WebSocketDataValidationError(
                    message=f"Validation error: {error_type}",
                    correlation_id=self.correlation_id,
                )

            errors.append(error)

        return errors


def create_error_factory(correlation_id: str | None = None) -> WebSocketErrorFactory:
    """Create a new exception factory instance.

    Args:
        correlation_id: Optional correlation ID for related errors

    Returns:
        WebSocketErrorFactory instance
    """
    return WebSocketErrorFactory(correlation_id=correlation_id)


def create_correlated_factory(base_error: WebSocketError) -> WebSocketErrorFactory:
    """Create factory with correlation ID from existing error.

    Args:
        base_error: Existing error to extract correlation ID from

    Returns:
        WebSocketErrorFactory with shared correlation ID
    """
    return WebSocketErrorFactory(correlation_id=base_error.correlation_id)


# Utility functions for error code operations
def is_retryable_error_code(code: WebSocketErrorCode) -> bool:
    """Check if error code represents a retryable error.

    Returns:
        True if the error code indicates a retryable condition.
    """
    retryable_codes = {
        WebSocketErrorCode.CONNECTION_LOST,
        WebSocketErrorCode.CONNECTION_TIMEOUT,
        WebSocketErrorCode.MESSAGE_TIMEOUT,
        WebSocketErrorCode.MESSAGE_TOO_LARGE,  # Can be retried with smaller message
    }
    return code in retryable_codes


def is_critical_error_code(code: WebSocketErrorCode) -> bool:
    """Check if error code represents a critical error.

    Returns:
        True if the error code indicates a critical condition.
    """
    critical_codes = {
        WebSocketErrorCode.AUTH_FAILED,
        WebSocketErrorCode.AUTH_UNAUTHORIZED,
        WebSocketErrorCode.VALIDATION_FAILED,
        WebSocketErrorCode.SECURITY_VIOLATION,
    }
    return code in critical_codes


def get_suggested_action(code: WebSocketErrorCode) -> str:
    """Get suggested action for error code.

    Returns:
        String containing the suggested action for handling this error code.
    """
    action_map = {
        WebSocketErrorCode.CONNECTION_LOST: "Reconnect to WebSocket",
        WebSocketErrorCode.CONNECTION_TIMEOUT: "Retry with increased timeout",
        WebSocketErrorCode.AUTH_FAILED: "Check authentication credentials",
        WebSocketErrorCode.AUTH_UNAUTHORIZED: "Verify user permissions",
        WebSocketErrorCode.INVALID_MESSAGE_TYPE: "Check payload format and types",
        WebSocketErrorCode.MESSAGE_TOO_LARGE: "Reduce message size",
        WebSocketErrorCode.VALIDATION_FAILED: "Review security policies",
        WebSocketErrorCode.SECURITY_VIOLATION: "Remove blocked content",
    }
    return action_map.get(code, "Contact support for assistance")
