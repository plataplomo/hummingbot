"""WebSocket envelope and field validation exception classes.

This module contains all exceptions related to envelope validation and
field validation during WebSocket message processing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .base import WebSocketDataValidationError


if TYPE_CHECKING:
    from cyberdelta.apis.models.websocket import StreamErrorContext


# ============================================================================
# Envelope Validation Errors
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
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize envelope validation error.

        Args:
            message: Error message describing the validation failure
            envelope_data: The envelope data that failed validation
            validation_context: Additional context about the validation failure
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        super().__init__(
            message=message,
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.envelope_data = envelope_data
        self.validation_context = validation_context or {}

    def get_troubleshooting_guide(self) -> str:
        """Get envelope-specific troubleshooting information.

        Returns:
            String containing troubleshooting guidance for envelope errors.
        """
        base_guide = super().get_troubleshooting_guide()
        return (
            f"{base_guide}\n"
            f"Envelope Troubleshooting:\n"
            f"- Check envelope structure and required fields\n"
            f"- Validate data types and formats\n"
            f"- Review validation context: {self.validation_context}"
        )


class RoutingKeyValidationError(EnvelopeValidationError):
    """Base class for routing key validation errors."""

    def __init__(
        self,
        message: str,
        routing_key: str | None = None,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize routing key validation error.

        Args:
            message: Error message
            routing_key: The invalid routing key
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        super().__init__(
            message=message,
            validation_context={"routing_key": routing_key},
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.routing_key = routing_key


class EmptyRoutingKeyError(RoutingKeyValidationError):
    """Raised when routing key is empty or None.

    Migrated from ws_envelope.py to unified hierarchy.
    """

    def __init__(
        self,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize empty routing key error."""
        super().__init__(
            message="Routing key cannot be empty or None",
            routing_key=None,
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )


class InvalidRoutingKeyFormatError(RoutingKeyValidationError):
    """Raised when routing key has invalid format.

    Migrated from ws_envelope.py to unified hierarchy.
    """

    def __init__(
        self,
        routing_key: str,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize invalid routing key format error.

        Args:
            routing_key: The invalid routing key
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        super().__init__(
            message=f"Invalid routing key format: {routing_key}",
            routing_key=routing_key,
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
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
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize envelope validation failed error.

        Args:
            exchange_name: Name of the exchange where validation failed
            original_error: Original error message
            context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        message = f"Envelope validation failed for {exchange_name}: {original_error}"
        super().__init__(
            message=message,
            validation_context={"exchange_name": exchange_name, "original_error": original_error},
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.exchange_name = exchange_name
        self.original_error = original_error


# ============================================================================
# Field and Format Validation Errors
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
        field_value: object | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize field validation error.

        Args:
            message: Error message
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            context: Stream error context
        """
        super().__init__(
            message=message,
            context=context,
        )
        self.field_name = field_name
        self.field_value = field_value


class InvalidItemTypeError(FieldValidationError, TypeError):
    """Raised when an item in a list has an invalid type.

    Migrated from ws_validators.py to unified hierarchy.
    """

    def __init__(
        self,
        error_context: str,
        index: int,
        actual_type: type[object],
        expected_type: type[object],
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize invalid item type error.

        Args:
            error_context: Context where the error occurred
            index: Index of the invalid item
            actual_type: Actual type of the item
            expected_type: Expected type of the item
            context: Stream error context
        """
        message = (
            f"{error_context} payload item {index} has invalid type: "
            f"{actual_type.__name__}. Expected {expected_type.__name__}."
        )
        super().__init__(
            message=message,
            field_name=f"item[{index}]",
            field_value=None,
            context=context,
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
        error_context: str,
        field_name: str,
        actual_type: type[object],
        expected_type: type[object],
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize invalid field type error.

        Args:
            error_context: Context where the error occurred
            field_name: Name of the field
            actual_type: Actual type of the field
            expected_type: Expected type of the field
            context: Stream error context
        """
        message = (
            f"{error_context} field '{field_name}' has invalid type: "
            f"{actual_type.__name__}. Expected {expected_type.__name__}."
        )
        super().__init__(
            message=message,
            field_name=field_name,
            context=context,
        )
        self.actual_type = actual_type
        self.expected_type = expected_type


class UnexpectedFieldsError(FieldValidationError):
    """Raised when unexpected fields are found in payload.

    Migrated from ws_validators.py to unified hierarchy.
    """

    def __init__(
        self,
        error_context: str,
        unexpected_fields: list[str],
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize unexpected fields error.

        Args:
            error_context: Context where the error occurred
            unexpected_fields: List of unexpected field names
            context: Stream error context
        """
        fields_str = ", ".join(unexpected_fields)
        message = f"{error_context} contains unexpected fields: {fields_str}"
        super().__init__(
            message=message,
            field_name="unexpected_fields",
            field_value=fields_str,
            context=context,
        )
        self.unexpected_fields = unexpected_fields


class InvalidFormatError(FieldValidationError):
    """Raised when a field has an invalid format.

    Migrated from ws_validators.py to unified hierarchy.
    Enhanced to be WebSocket-specific rather than generic.
    """

    def __init__(
        self,
        error_context: str,
        field_name: str,
        field_value: str,
        expected_format: str,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize invalid format error.

        Args:
            error_context: Context where the error occurred
            field_name: Name of the field
            field_value: Invalid field value
            expected_format: Expected format description
            context: Stream error context
        """
        message = (
            f"{error_context} field '{field_name}' has invalid format: "
            f"'{field_value}'. Expected format: {expected_format}"
        )
        super().__init__(
            message=message,
            field_name=field_name,
            field_value=field_value,
            context=context,
        )
        self.expected_format = expected_format


class InvalidNumericValueError(FieldValidationError):
    """Raised when a numeric field has an invalid value.

    Migrated from ws_validators.py to unified hierarchy.
    """

    def __init__(
        self,
        error_context: str,
        field_name: str,
        field_value: str,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize invalid numeric value error.

        Args:
            error_context: Context where the error occurred
            field_name: Name of the field
            field_value: Invalid field value
            context: Stream error context
        """
        message = f"{error_context} field '{field_name}' has invalid numeric value: '{field_value}'"
        super().__init__(
            message=message,
            field_name=field_name,
            field_value=field_value,
            context=context,
        )


class NumericRangeError(FieldValidationError):
    """Raised when a numeric field is outside the allowed range.

    Migrated from ws_validators.py to unified hierarchy.
    """

    def __init__(
        self,
        error_context: str,
        field_name: str,
        field_value: float,
        min_value: float | None = None,
        max_value: float | None = None,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize numeric range error.

        Args:
            error_context: Context where the error occurred
            field_name: Name of the field
            field_value: Field value that's out of range
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            context: Stream error context
        """
        range_desc: list[str] = []
        if min_value is not None:
            range_desc.append(f"min: {min_value}")
        if max_value is not None:
            range_desc.append(f"max: {max_value}")
        range_str = ", ".join(range_desc)

        message = (
            f"{error_context} field '{field_name}' value {field_value} "
            f"is out of range ({range_str})"
        )
        super().__init__(
            message=message,
            field_name=field_name,
            field_value=field_value,
            context=context,
        )
        self.min_value = min_value
        self.max_value = max_value


class InvalidTimestampError(FieldValidationError):
    """Raised when a timestamp field has an invalid value.

    Migrated from ws_validators.py to unified hierarchy.
    """

    def __init__(
        self,
        error_context: str,
        field_name: str,
        field_value: str,
        context: StreamErrorContext | None = None,
    ) -> None:
        """Initialize invalid timestamp error.

        Args:
            error_context: Context where the error occurred
            field_name: Name of the timestamp field
            field_value: Invalid timestamp value
            context: Stream error context
        """
        message = f"{error_context} field '{field_name}' has invalid timestamp: '{field_value}'"
        super().__init__(
            message=message,
            field_name=field_name,
            field_value=field_value,
            context=context,
        )
