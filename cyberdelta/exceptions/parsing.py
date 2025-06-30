"""Parsing and conversion exceptions for CyberDelta.

These exceptions handle errors that occur during parsing and conversion
of raw data values, particularly in validators for raw types.
"""

from typing import Any


class ParsingError(ValueError):
    """Base class for parsing-related errors."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        value: Any = None,
        expected_type: str | None = None,
        **metadata: Any,
    ) -> None:
        """Initialize parsing error.

        Args:
            message: Human-readable error description
            field_name: Name of the field being parsed
            value: The value that failed parsing
            expected_type: Expected type or format
            **metadata: Additional error context
        """
        super().__init__(message)
        self.field_name = field_name
        self.value = value
        self.expected_type = expected_type
        self.metadata = metadata


class DateTimeParsingError(ParsingError):
    """Raised when datetime parsing returns None or fails."""

    def __init__(
        self,
        field_name: str,
        value: Any,
        reason: str = "parse_datetime_utc returned None",
    ) -> None:
        """Initialize datetime parsing error.

        Args:
            field_name: Name of the field
            value: The value that failed parsing
            reason: Reason for parsing failure
        """
        message = f"Field {field_name}: {reason} for '{value}'"
        super().__init__(
            message=message,
            field_name=field_name,
            value=value,
            expected_type="datetime",
            reason=reason,
        )


class TimestampYearRangeError(ParsingError):
    """Raised when timestamp year is outside acceptable range."""

    def __init__(
        self,
        field_name: str,
        value: Any,
        year: int,
        min_year: int,
        max_year: int,
        context: str | None = None,
    ) -> None:
        """Initialize timestamp year range error.

        Args:
            field_name: Name of the field
            value: The timestamp value
            year: The actual year
            min_year: Minimum allowed year
            max_year: Maximum allowed year
            context: Optional context (e.g., "funding rate")
        """
        if context:
            message = (
                f"Field {field_name}: Timestamp '{value}' results in an implausible year "
                f"({year}) for {context} context (expected {min_year}-{max_year})."
            )
        else:
            message = (
                f"Field {field_name}: Timestamp '{value}' results in an implausible year "
                f"({year}) for this context."
            )
        
        super().__init__(
            message=message,
            field_name=field_name,
            value=value,
            year=year,
            min_year=min_year,
            max_year=max_year,
            context=context,
        )


class TimestampFormatError(ParsingError):
    """Raised when timestamp has invalid format."""

    def __init__(
        self,
        field_name: str,
        value: Any,
        expected_format: str,
        details: str | None = None,
    ) -> None:
        """Initialize timestamp format error.

        Args:
            field_name: Name of the field
            value: The timestamp value
            expected_format: Expected format description
            details: Optional error details
        """
        if details:
            message = f"Field {field_name}: Invalid {expected_format} timestamp value '{value}'. Details: {details}"
        else:
            message = f"Field {field_name}: Expected {expected_format} timestamp, got {type(value).__name__}."
        
        super().__init__(
            message=message,
            field_name=field_name,
            value=value,
            expected_type=expected_format,
            details=details,
        )


class NonNullableFieldError(ParsingError):
    """Raised when a non-nullable field receives None."""

    def __init__(self, field_name: str) -> None:
        """Initialize non-nullable field error.

        Args:
            field_name: Name of the field
        """
        message = f"Field {field_name}: Value cannot be None."
        super().__init__(
            message=message,
            field_name=field_name,
            value=None,
            expected_type="non-null value",
        )


class EmptyStringError(ParsingError):
    """Raised when an empty string is provided where non-empty is required."""

    def __init__(self, field_name: str, context: str | None = None) -> None:
        """Initialize empty string error.

        Args:
            field_name: Name of the field
            context: Optional context message
        """
        if context:
            message = f"{field_name}: {context}"
        else:
            message = f"Field {field_name}: String cannot be empty"
        
        super().__init__(
            message=message,
            field_name=field_name,
            value="",
            expected_type="non-empty string",
        )


class ClientIdFormatError(ParsingError):
    """Raised when clientId has invalid format."""

    def __init__(self, field_name: str = "clientId", reason: str = "raw value must be a string or integer") -> None:
        """Initialize client ID format error.

        Args:
            field_name: Name of the field (default: clientId)
            reason: Reason for the error
        """
        message = f"{field_name}: {reason}"
        super().__init__(
            message=message,
            field_name=field_name,
            expected_type="string or integer",
            reason=reason,
        )


class MsgpackSerializationError(ParsingError):
    """Raised when msgpack serialization fails."""

    def __init__(self, details: str, original_error: Exception | None = None) -> None:
        """Initialize msgpack serialization error.

        Args:
            details: Error details
            original_error: The original exception
        """
        message = f"Failed to serialize action payload: {details}"
        super().__init__(
            message=message,
            expected_type="msgpack-serializable",
            details=details,
            original_error=original_error,
        )


class ActionHashError(ParsingError):
    """Raised when action hash computation fails."""

    def __init__(self, details: str, original_error: Exception | None = None) -> None:
        """Initialize action hash error.

        Args:
            details: Error details
            original_error: The original exception
        """
        message = f"Failed to compute action hash: {details}"
        super().__init__(
            message=message,
            expected_type="hashable",
            details=details,
            original_error=original_error,
        )