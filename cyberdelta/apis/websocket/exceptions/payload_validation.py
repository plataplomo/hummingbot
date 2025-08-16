"""WebSocket payload validation exception classes.

This module contains all exceptions related to payload validation during
WebSocket message processing, including type validation, size validation,
and required field validation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import WebSocketDataValidationError


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext


class PayloadValidationError(WebSocketDataValidationError):
    """Base class for payload validation errors.

    Consolidates all payload-related validation errors including
    type errors, size errors, and missing field errors.
    """

    def __init__(
        self,
        message: str,
        payload_type: str | None = None,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize payload validation error.

        Args:
            message: Error message
            payload_type: Type of payload that failed validation
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
        self.payload_type = payload_type


class InvalidPayloadTypeError(PayloadValidationError, TypeError):
    """Error for invalid payload types.

    Consolidates InvalidPayloadTypeError from ws_validators.py
    and similar type validation errors.
    """

    def __init__(
        self,
        context: str,
        actual_type: type,
        expected_type: str,
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize invalid payload type error.

        Args:
            context: Context where validation failed
            actual_type: Actual type that was provided
            expected_type: Expected type description
            stream_context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        message = (
            f"Invalid payload type in {context}: "
            f"expected {expected_type}, got {actual_type.__name__}"
        )
        super().__init__(
            message=message,
            payload_type=actual_type.__name__,
            context=stream_context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.actual_type = actual_type
        self.expected_type = expected_type


class PayloadSizeError(PayloadValidationError):
    """Error for payload size violations.

    Used for all payload size constraint violations including minimum,
    maximum, and exact size requirements.
    """

    def __init__(
        self,
        context: str,
        actual_size: int,
        constraint: str,
        limit: int,
        size_unit: str = "bytes",
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize payload size error.

        Args:
            context: Context where size validation failed
            actual_size: Actual size that was provided
            constraint: Constraint type ("at least", "at most", "exactly")
            limit: Size limit that was violated
            size_unit: Unit of size measurement
            stream_context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        message = (
            f"Payload size violation in {context}: "
            f"size {actual_size} {size_unit}, expected {constraint} {limit} {size_unit}"
        )
        super().__init__(
            message=message,
            payload_type="size_constraint",
            context=stream_context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.actual_size = actual_size
        self.constraint = constraint
        self.limit = limit
        self.size_unit = size_unit


class PayloadNoneError(PayloadValidationError):
    """Error for None payload when value is required.

    Migrated from ws_envelope.py for consistency.
    """

    def __init__(
        self,
        context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize payload None error."""
        super().__init__(
            message="Payload cannot be None",
            payload_type="None",
            context=context,
            error_id=error_id,
            correlation_id=correlation_id,
        )


class MissingRequiredFieldsError(PayloadValidationError):
    """Error for missing required fields in payload.

    Migrated from ws_validators.py for consistency.
    """

    def __init__(
        self,
        context: str,
        missing_fields: list[str],
        stream_context: StreamErrorContext | None = None,
        error_id: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize missing required fields error.

        Args:
            context: Context where fields are missing
            missing_fields: List of missing field names
            stream_context: Stream error context
            error_id: Unique error identifier
            correlation_id: Correlation identifier for related errors
        """
        fields_str = ", ".join(missing_fields)
        message = f"Missing required fields in {context}: {fields_str}"
        super().__init__(
            message=message,
            payload_type="required_fields",
            context=stream_context,
            error_id=error_id,
            correlation_id=correlation_id,
        )
        self.missing_fields = missing_fields
