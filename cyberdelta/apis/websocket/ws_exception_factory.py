"""WebSocket Exception Factory.

This module provides a centralized factory for creating WebSocket exceptions
with standardized error codes, context information, and correlation tracking.

The factory ensures consistent exception creation across the entire WebSocket
module while maintaining type safety and providing enhanced debugging capabilities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_exceptions import (
    ArrayLengthExceedsLimitError,
    BlockedPatternFoundError,
    EmptyRoutingKeyError,
    # Envelope validation exceptions (migrated from ws_envelope.py)
    EnvelopeValidationError,
    EnvelopeValidationFailedError,
    InvalidPayloadTypeError,
    InvalidRoutingKeyFormatError,
    # Field and format validation exceptions (migrated from ws_validators.py)
    FieldValidationError,
    InvalidItemTypeError,
    InvalidFieldTypeError,
    UnexpectedFieldsError,
    InvalidFormatError,
    InvalidNumericValueError,
    NumericRangeError,
    InvalidTimestampError,
    MessageSizeExceedsLimitError,
    MissingRequiredFieldsError,
    NestingDepthExceedsLimitError,
    ObjectKeysExceedLimitError,
    PayloadNoneError,
    PayloadSizeError,
    PayloadTooLargeError,
    SecurityValidationError,
    StringLengthExceedsLimitError,
    WebSocketDataValidationError,
    WebSocketException,
)


if TYPE_CHECKING:
    pass


class WebSocketExceptionFactory:
    """Factory for creating standardized WebSocket exceptions.

    This factory provides:
    - Consistent error code integration
    - Standardized error messages
    - Correlation ID tracking
    - Context information attachment
    - Type-safe exception creation
    """

    def __init__(self, correlation_id: str | None = None) -> None:
        """Initialize exception factory.

        Args:
            correlation_id: Optional correlation ID for tracking related errors
        """
        self.correlation_id = correlation_id or str(uuid4())

    # ========================================================================
    # Payload Validation Exceptions
    # ========================================================================

    def create_invalid_payload_type_error(
        self,
        context: str,
        actual_type: type,
        expected_type: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.PAYLOAD_TYPE_MISMATCH,
        **kwargs: Any,
    ) -> InvalidPayloadTypeError:
        """Create invalid payload type error with standardized format.

        Args:
            context: Context where validation failed
            actual_type: Actual type that was provided
            expected_type: Expected type description
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidPayloadTypeError instance
        """
        return InvalidPayloadTypeError(
            context=context,
            actual_type=actual_type,
            expected_type=expected_type,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_payload_size_error(
        self,
        context: str,
        actual_size: int,
        constraint: str,
        limit: int,
        size_unit: str = "bytes",
        error_code: WebSocketErrorCode = WebSocketErrorCode.MESSAGE_TOO_LARGE,
        **kwargs: Any,
    ) -> PayloadSizeError:
        """Create payload size error with standardized format.

        Args:
            context: Context where size validation failed
            actual_size: Actual size that was provided
            constraint: Constraint type ("at least", "at most", "exactly")
            limit: Size limit that was violated
            size_unit: Unit of size measurement
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured PayloadSizeError instance
        """
        return PayloadSizeError(
            context=context,
            actual_size=actual_size,
            constraint=constraint,
            limit=limit,
            size_unit=size_unit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_payload_too_large_error(
        self,
        payload_type: str,
        size: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.MESSAGE_TOO_LARGE,
        **kwargs: Any,
    ) -> PayloadTooLargeError:
        """Create payload too large error for backward compatibility.

        Args:
            payload_type: Type of payload (dict, list, etc.)
            size: Actual size
            limit: Maximum allowed size
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured PayloadTooLargeError instance
        """
        return PayloadTooLargeError(
            payload_type=payload_type,
            size=size,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_payload_none_error(
        self,
        error_code: WebSocketErrorCode = WebSocketErrorCode.PAYLOAD_MISSING_REQUIRED,
        **kwargs: Any,
    ) -> PayloadNoneError:
        """Create payload None error.

        Args:
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured PayloadNoneError instance
        """
        return PayloadNoneError(
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_missing_required_fields_error(
        self,
        context: str,
        missing_fields: list[str],
        error_code: WebSocketErrorCode = WebSocketErrorCode.MISSING_REQUIRED_FIELD,
        **kwargs: Any,
    ) -> MissingRequiredFieldsError:
        """Create missing required fields error.

        Args:
            context: Context where fields are missing
            missing_fields: List of missing field names
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured MissingRequiredFieldsError instance
        """
        return MissingRequiredFieldsError(
            context=context,
            missing_fields=missing_fields,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    # ========================================================================
    # Security Validation Exceptions
    # ========================================================================

    def create_security_validation_error(
        self,
        message: str,
        violation_type: str,
        message_data: dict[str, Any] | None = None,
        security_context: dict[str, Any] | None = None,
        error_code: WebSocketErrorCode = WebSocketErrorCode.SECURITY_VIOLATION,
        **kwargs: Any,
    ) -> SecurityValidationError:
        """Create security validation error.

        Args:
            message: Error message
            violation_type: Type of security violation
            message_data: Data that triggered the violation
            security_context: Security context information
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured SecurityValidationError instance
        """
        return SecurityValidationError(
            message=message,
            violation_type=violation_type,
            message_data=message_data,
            security_context=security_context,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_blocked_pattern_found_error(
        self,
        pattern: str,
        content_preview: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INJECTION_DETECTED,
        **kwargs: Any,
    ) -> BlockedPatternFoundError:
        """Create blocked pattern found error.

        Args:
            pattern: The blocked pattern that was found
            content_preview: Preview of content containing the pattern
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured BlockedPatternFoundError instance
        """
        return BlockedPatternFoundError(
            pattern=pattern,
            content_preview=content_preview,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_message_size_exceeds_limit_error(
        self,
        message_size: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.MESSAGE_TOO_LARGE,
        **kwargs: Any,
    ) -> MessageSizeExceedsLimitError:
        """Create message size exceeds limit error.

        Args:
            message_size: Actual message size in bytes
            limit: Maximum allowed size in bytes
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured MessageSizeExceedsLimitError instance
        """
        return MessageSizeExceedsLimitError(
            message_size=message_size,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_nesting_depth_exceeds_limit_error(
        self,
        current_depth: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.OVERFLOW_DETECTED,
        **kwargs: Any,
    ) -> NestingDepthExceedsLimitError:
        """Create nesting depth exceeds limit error.

        Args:
            current_depth: Actual nesting depth
            limit: Maximum allowed nesting depth
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured NestingDepthExceedsLimitError instance
        """
        return NestingDepthExceedsLimitError(
            current_depth=current_depth,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_object_keys_exceed_limit_error(
        self,
        key_count: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.RESOURCE_EXHAUSTED,
        **kwargs: Any,
    ) -> ObjectKeysExceedLimitError:
        """Create object keys exceed limit error.

        Args:
            key_count: Actual number of object keys
            limit: Maximum allowed number of keys
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured ObjectKeysExceedLimitError instance
        """
        return ObjectKeysExceedLimitError(
            key_count=key_count,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_array_length_exceeds_limit_error(
        self,
        array_length: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.RESOURCE_EXHAUSTED,
        **kwargs: Any,
    ) -> ArrayLengthExceedsLimitError:
        """Create array length exceeds limit error.

        Args:
            array_length: Actual array length
            limit: Maximum allowed array length
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured ArrayLengthExceedsLimitError instance
        """
        return ArrayLengthExceedsLimitError(
            array_length=array_length,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_string_length_exceeds_limit_error(
        self,
        string_length: int,
        limit: int,
        error_code: WebSocketErrorCode = WebSocketErrorCode.RESOURCE_EXHAUSTED,
        **kwargs: Any,
    ) -> StringLengthExceedsLimitError:
        """Create string length exceeds limit error.

        Args:
            string_length: Actual string length
            limit: Maximum allowed string length
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured StringLengthExceedsLimitError instance
        """
        return StringLengthExceedsLimitError(
            string_length=string_length,
            limit=limit,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    # ========================================================================
    # Error Code Integration
    # ========================================================================

    def get_error_category(self, error_code: WebSocketErrorCode) -> str:
        """Get error category from error code.

        Args:
            error_code: WebSocket error code

        Returns:
            Error category string
        """
        return error_code.get_category()

    def get_suggested_action(self, error_code: WebSocketErrorCode) -> str:
        """Get suggested action for error code.

        Args:
            error_code: WebSocket error code

        Returns:
            Suggested action string
        """
        return error_code.get_suggested_action()

    def is_retryable_error(self, error_code: WebSocketErrorCode) -> bool:
        """Check if error is retryable.

        Args:
            error_code: WebSocket error code

        Returns:
            True if error is retryable
        """
        return error_code.is_retryable()

    def is_critical_error(self, error_code: WebSocketErrorCode) -> bool:
        """Check if error is critical.

        Args:
            error_code: WebSocket error code

        Returns:
            True if error is critical
        """
        return error_code.is_critical()

    # ========================================================================
    # Envelope Validation Exceptions (Step 27)
    # ========================================================================

    def create_envelope_validation_error(
        self,
        message: str,
        envelope_data: dict[str, Any] | None = None,
        validation_context: dict[str, Any] | None = None,
        error_code: WebSocketErrorCode = WebSocketErrorCode.VALIDATION_FAILED,
        **kwargs: Any,
    ) -> EnvelopeValidationError:
        """Create envelope validation error.

        Args:
            message: Error message describing the validation failure
            envelope_data: The envelope data that failed validation
            validation_context: Additional context about the validation failure
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured EnvelopeValidationError instance
        """
        return EnvelopeValidationError(
            message=message,
            envelope_data=envelope_data,
            validation_context=validation_context,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_empty_routing_key_error(
        self,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> EmptyRoutingKeyError:
        """Create empty routing key error.

        Args:
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured EmptyRoutingKeyError instance
        """
        return EmptyRoutingKeyError(
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_invalid_routing_key_format_error(
        self,
        routing_key: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> InvalidRoutingKeyFormatError:
        """Create invalid routing key format error.

        Args:
            routing_key: The invalid routing key
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidRoutingKeyFormatError instance
        """
        return InvalidRoutingKeyFormatError(
            routing_key=routing_key,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_envelope_validation_failed_error(
        self,
        exchange_name: str,
        original_error: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.VALIDATION_FAILED,
        **kwargs: Any,
    ) -> EnvelopeValidationFailedError:
        """Create envelope validation failed error.

        Args:
            exchange_name: Name of the exchange where validation failed
            original_error: Original error message
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured EnvelopeValidationFailedError instance
        """
        return EnvelopeValidationFailedError(
            exchange_name=exchange_name,
            original_error=original_error,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    # ========================================================================
    # Field Validation Exceptions (Step 28)
    # ========================================================================

    def create_invalid_item_type_error(
        self,
        context: str,
        index: int,
        actual_type: type[Any],
        expected_type: type[Any],
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> InvalidItemTypeError:
        """Create invalid item type error.

        Args:
            context: Context where the error occurred
            index: Index of the invalid item
            actual_type: Actual type of the item
            expected_type: Expected type of the item
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidItemTypeError instance
        """
        return InvalidItemTypeError(
            context=context,
            index=index,
            actual_type=actual_type,
            expected_type=expected_type,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_invalid_field_type_error(
        self,
        context: str,
        actual_type: type[Any],
        expected_type: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> InvalidFieldTypeError:
        """Create invalid field type error.

        Args:
            context: Context where the error occurred
            actual_type: Actual type of the field
            expected_type: Expected type description
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidFieldTypeError instance
        """
        return InvalidFieldTypeError(
            context=context,
            actual_type=actual_type,
            expected_type=expected_type,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_unexpected_fields_error(
        self,
        context: str,
        unexpected_fields: list[str],
        allowed_fields: list[str],
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> UnexpectedFieldsError:
        """Create unexpected fields error.

        Args:
            context: Context where the error occurred
            unexpected_fields: List of unexpected field names
            allowed_fields: List of allowed field names
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured UnexpectedFieldsError instance
        """
        return UnexpectedFieldsError(
            context=context,
            unexpected_fields=unexpected_fields,
            allowed_fields=allowed_fields,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_invalid_format_error(
        self,
        context: str,
        value: str,
        pattern: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
        **kwargs: Any,
    ) -> InvalidFormatError:
        """Create invalid format error.

        Args:
            context: Context where the error occurred
            value: Value that doesn't match the pattern
            pattern: Expected pattern
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidFormatError instance
        """
        return InvalidFormatError(
            context=context,
            value=value,
            pattern=pattern,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_invalid_numeric_value_error(
        self,
        context: str,
        value: Any,
        reason: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> InvalidNumericValueError:
        """Create invalid numeric value error.

        Args:
            context: Context where the error occurred
            value: Invalid numeric value
            reason: Reason why the value is invalid
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidNumericValueError instance
        """
        return InvalidNumericValueError(
            context=context,
            value=value,
            reason=reason,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_numeric_range_error(
        self,
        context: str,
        value: float | int,
        min_value: float | int | None = None,
        max_value: float | int | None = None,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> NumericRangeError:
        """Create numeric range error.

        Args:
            context: Context where the error occurred
            value: Value outside the range
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured NumericRangeError instance
        """
        return NumericRangeError(
            context=context,
            value=value,
            min_value=min_value,
            max_value=max_value,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    def create_invalid_timestamp_error(
        self,
        context: str,
        value: Any,
        reason: str,
        error_code: WebSocketErrorCode = WebSocketErrorCode.INVALID_FIELD_VALUE,
        **kwargs: Any,
    ) -> InvalidTimestampError:
        """Create invalid timestamp error.

        Args:
            context: Context where the error occurred
            value: Invalid timestamp value
            reason: Reason why the timestamp is invalid
            error_code: WebSocket error code for categorization
            **kwargs: Additional arguments

        Returns:
            Configured InvalidTimestampError instance
        """
        return InvalidTimestampError(
            context=context,
            value=value,
            reason=reason,
            error_id=str(uuid4()),
            correlation_id=self.correlation_id,
            **kwargs,
        )

    # ========================================================================
    # Batch Exception Creation
    # ========================================================================

    def create_batch_validation_errors(
        self,
        validation_errors: list[dict[str, Any]],
        correlation_id: str | None = None,
    ) -> list[WebSocketDataValidationError]:
        """Create multiple validation errors with shared correlation ID.

        Args:
            validation_errors: List of error specifications
            correlation_id: Override correlation ID for this batch

        Returns:
            List of configured validation errors
        """
        batch_correlation_id = correlation_id or self.correlation_id
        errors: list[WebSocketDataValidationError] = []

        for error_spec in validation_errors:
            error_type = error_spec.get("type")
            error_params = error_spec.get("params", {}).copy()  # Copy to avoid modifying original

            # Create a temporary factory with the batch correlation ID
            temp_factory = WebSocketExceptionFactory(correlation_id=batch_correlation_id)

            if error_type == "invalid_payload_type":
                errors.append(temp_factory.create_invalid_payload_type_error(**error_params))
            elif error_type == "payload_size":
                errors.append(temp_factory.create_payload_size_error(**error_params))
            elif error_type == "payload_too_large":
                errors.append(temp_factory.create_payload_too_large_error(**error_params))
            elif error_type == "payload_none":
                errors.append(temp_factory.create_payload_none_error(**error_params))
            elif error_type == "missing_required_fields":
                errors.append(temp_factory.create_missing_required_fields_error(**error_params))
            # Envelope validation errors
            elif error_type == "envelope_validation":
                errors.append(temp_factory.create_envelope_validation_error(**error_params))
            elif error_type == "empty_routing_key":
                errors.append(temp_factory.create_empty_routing_key_error(**error_params))
            elif error_type == "invalid_routing_key_format":
                errors.append(temp_factory.create_invalid_routing_key_format_error(**error_params))
            elif error_type == "envelope_validation_failed":
                errors.append(temp_factory.create_envelope_validation_failed_error(**error_params))
            else:
                # Default to generic WebSocketDataValidationError
                errors.append(
                    WebSocketDataValidationError(
                        message=error_spec.get("message", "Validation error"),
                        context=error_spec.get("context", "unknown"),
                        validation_type=error_spec.get("validation_type", "generic"),
                        error_id=str(uuid4()),
                        correlation_id=batch_correlation_id,
                    )
                )

        return errors


# ============================================================================
# Convenience Functions
# ============================================================================


def create_exception_factory(correlation_id: str | None = None) -> WebSocketExceptionFactory:
    """Create a new exception factory instance.

    Args:
        correlation_id: Optional correlation ID for tracking related errors

    Returns:
        Configured WebSocketExceptionFactory instance
    """
    return WebSocketExceptionFactory(correlation_id=correlation_id)


def create_correlated_factory(base_exception: WebSocketException) -> WebSocketExceptionFactory:
    """Create a factory that uses the same correlation ID as an existing exception.

    Args:
        base_exception: Exception to copy correlation ID from

    Returns:
        Factory configured with the same correlation ID
    """
    return WebSocketExceptionFactory(correlation_id=base_exception.correlation_id)


# ============================================================================
# Standard Error Code Mappings
# ============================================================================

VALIDATION_ERROR_CODE_MAP = {
    "invalid_payload_type": WebSocketErrorCode.PAYLOAD_TYPE_MISMATCH,
    "payload_size": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "payload_too_large": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "payload_none": WebSocketErrorCode.PAYLOAD_MISSING_REQUIRED,
    "missing_required_fields": WebSocketErrorCode.MISSING_REQUIRED_FIELD,
    "invalid_field_type": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "format_validation": WebSocketErrorCode.INVALID_MESSAGE_FORMAT,
    # Envelope validation error codes (Step 27)
    "envelope_validation": WebSocketErrorCode.VALIDATION_FAILED,
    "empty_routing_key": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "invalid_routing_key_format": WebSocketErrorCode.INVALID_FIELD_VALUE,
    "envelope_validation_failed": WebSocketErrorCode.VALIDATION_FAILED,
}

SECURITY_ERROR_CODE_MAP = {
    "blocked_pattern": WebSocketErrorCode.INJECTION_DETECTED,
    "message_size": WebSocketErrorCode.MESSAGE_TOO_LARGE,
    "nesting_depth": WebSocketErrorCode.OVERFLOW_DETECTED,
    "object_keys": WebSocketErrorCode.RESOURCE_EXHAUSTED,
    "array_length": WebSocketErrorCode.RESOURCE_EXHAUSTED,
    "string_length": WebSocketErrorCode.RESOURCE_EXHAUSTED,
    "security_validation": WebSocketErrorCode.SECURITY_VIOLATION,
}


def get_error_code_for_validation_type(validation_type: str) -> WebSocketErrorCode:
    """Get appropriate error code for validation type.

    Args:
        validation_type: Type of validation that failed

    Returns:
        Appropriate WebSocket error code
    """
    return VALIDATION_ERROR_CODE_MAP.get(validation_type, WebSocketErrorCode.VALIDATION_FAILED)


def get_error_code_for_security_type(security_type: str) -> WebSocketErrorCode:
    """Get appropriate error code for security violation type.

    Args:
        security_type: Type of security violation

    Returns:
        Appropriate WebSocket error code
    """
    return SECURITY_ERROR_CODE_MAP.get(security_type, WebSocketErrorCode.SECURITY_VIOLATION)
