"""WebSocket field validation errors.

This module provides specialized validation error classes for WebSocket field validation.
Separated to avoid circular imports.
"""

from __future__ import annotations


class WebSocketFieldValidationError(ValueError):
    """Error for WebSocket field validation failures."""

    def __init__(
        self,
        field_name: str,
        field_value: object,
        validation_error: str,
    ) -> None:
        """Initialize field validation error.

        Args:
            field_name: Name of the field that failed validation
            field_value: Value that failed validation
            validation_error: Description of the validation failure
        """
        self.field_name = field_name
        self.field_value = field_value
        self.validation_error = validation_error

        message = f"Field '{field_name}' validation failed: {validation_error}"
        if field_value is not None:
            message += f" (value: {field_value})"

        super().__init__(message)
