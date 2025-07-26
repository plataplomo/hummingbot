"""Common utilities for service argument models.

This module contains shared validation functions and imports used across
different service arg models. No model classes should be defined here.
"""

from cyberdelta.apis.exceptions.field_validation import (
    EmptyStringFieldError,
    TypeFieldError,
)


def validate_api_str_field(
    value: object,
    *,
    field_name: str,
    max_length: int | None = None,
    allow_empty: bool = True,
) -> str:
    """Validate string field using API-specific exceptions.

    Args:
        value: Value to validate
        field_name: Name of the field being validated
        max_length: Maximum allowed length
        allow_empty: Whether empty strings are allowed

    Returns:
        Validated string value

    Raises:
        TypeFieldError: If value is not a string or exceeds max_length
        EmptyStringFieldError: If value is empty and allow_empty is False
    """
    if not isinstance(value, str):
        raise TypeFieldError(
            field_name=field_name,
            expected_type="str",
            actual_type=type(value).__name__,
        )

    if not allow_empty and not value.strip():
        raise EmptyStringFieldError(field_name=field_name)

    if max_length is not None and len(value) > max_length:
        raise TypeFieldError(
            field_name=field_name,
            expected_type=f"string with max length {max_length}",
            actual_type=f"string with length {len(value)}",
        )

    try:
        value.encode("utf-8", "strict")
    except UnicodeEncodeError as e:
        raise TypeFieldError(
            field_name=field_name,
            expected_type="valid UTF-8 string",
            actual_type="string with invalid UTF-8",
        ) from e

    return value
