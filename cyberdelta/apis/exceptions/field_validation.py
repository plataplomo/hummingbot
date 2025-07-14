"""API-specific field validation exceptions.

This module contains field validation exceptions that are used exclusively
within API model validation and need to be imported by API modules.
"""

from typing import Any


class FieldError(Exception):
    """Base class for field-related errors."""

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        source_value: object = None,
        source_data: dict[str, Any] | None = None,
        code: str | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize field error."""
        super().__init__(message)
        self.field_name = field_name
        self.source_value = source_value
        self.source_data = source_data
        self.code = code
        self.original_exception = original_exception


class DecimalFiniteError(ValueError, FieldError):
    """Raised when decimal value is not finite."""

    def __init__(
        self,
        field_name: str,
        value: object,
        *,
        context: str | None = None,
    ) -> None:
        """Initialize decimal finite error."""
        message = f"Field '{field_name}' must be finite"
        if context:
            message = f"{message} {context}"

        super().__init__(message)
        FieldError.__init__(
            self,
            message,
            field_name=field_name,
            source_value=value,
            code="DECIMAL_NOT_FINITE",
        )


class TypeFieldError(TypeError, FieldError):
    """Raised when field has incorrect type."""

    def __init__(
        self,
        field_name: str,
        expected_type: str,
        actual_type: str,
    ) -> None:
        """Initialize type field error."""
        message = f"Field '{field_name}' expected {expected_type}, got {actual_type}"

        super().__init__(message)
        FieldError.__init__(
            self,
            message,
            field_name=field_name,
            code="INCORRECT_TYPE",
        )


class ListFieldError(ValueError, FieldError):
    """Raised when list field validation fails."""

    def __init__(
        self,
        field_name: str,
        reason: str,
        *,
        list_length: int | None = None,
        element_index: int | None = None,
    ) -> None:
        """Initialize list field error."""
        message = f"List field '{field_name}' validation failed: {reason}"
        if list_length is not None:
            message = f"{message} (length: {list_length})"
        if element_index is not None:
            message = f"{message} (at index {element_index})"

        super().__init__(message)
        FieldError.__init__(
            self,
            message,
            field_name=field_name,
            code="LIST_VALIDATION_FAILED",
        )


class EmptyStringFieldError(ValueError, FieldError):
    """Raised when an empty string is provided where non-empty is required."""

    def __init__(
        self,
        field_name: str,
        *,
        context: str | None = None,
    ) -> None:
        """Initialize empty string field error."""
        if context:
            message = f"Field '{field_name}': {context}"
        else:
            message = f"Field '{field_name}': String cannot be empty"

        super().__init__(message)
        FieldError.__init__(
            self,
            message,
            field_name=field_name,
            source_value="",
            code="EMPTY_STRING_NOT_ALLOWED",
        )
