"""Core parsing exceptions for CyberDelta utilities.

These exceptions are used by the utils.parsing module and other foundational
code that cannot depend on API-specific exceptions.
"""


class ParsingError(Exception):
    """Base class for parsing-related errors."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        value: object = None,
        expected_type: str | None = None,
        **metadata: object,
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
        value: object,
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


class TimestampFormatError(ParsingError):
    """Raised when timestamp has invalid format."""

    def __init__(
        self,
        field_name: str,
        value: object,
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
            message = (
                f"Field {field_name}: Invalid {expected_format} timestamp value '{value}'. "
                f"Details: {details}"
            )
        else:
            message = f"Field {field_name}: Invalid {expected_format} timestamp value '{value}'"

        super().__init__(
            message=message,
            field_name=field_name,
            value=value,
            expected_type=expected_format,
            details=details,
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
