"""Common configuration types for CyberDeltaEngine.

This module contains shared Pydantic types and validators used across
configuration models to avoid circular import issues.
"""

from decimal import Decimal
from typing import Annotated

from pydantic import BeforeValidator, ValidationInfo

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


def _parse_yaml_input_to_required_decimal(
    v: str | float | Decimal,
    info: ValidationInfo,
) -> Decimal:
    """Pydantic 'before' validator to parse input to a required, finite Decimal.

    Args:
        v: The value to parse (string, float, or Decimal)
        info: Pydantic validation information

    Returns:
        A finite Decimal value parsed from the input.

    Raises:
        ValueError: If the value cannot be parsed to a Decimal or is not finite.
    """
    field_name = info.field_name or "decimal_field"
    # allow_none=False because this is for fields that are expected to be Decimal.
    # Optionality of the field itself is handled by Pydantic's Optional[ConfigDecimal] typing.
    parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
    # Note: parsed cannot be None when allow_none=False
    if not parsed.is_finite():
        msg = f"Field '{field_name}': Decimal value must be finite, got '{v}'."
        raise ValueError(msg)
    return parsed


def _validate_string_for_literal_check(v: str | float | bool, info: ValidationInfo) -> str:
    """Pydantic 'before' validator to ensure v is a string before Literal check.

    Args:
        v: The value to validate (string, float, or bool)
        info: Pydantic validation information

    Returns:
        A validated non-empty string value.
    """
    return validate_str_field(
        v,
        field_name=info.field_name or "literal_str_field",
        allow_empty=False,
    )


def _validate_non_empty_string(v: str | float | bool, info: ValidationInfo) -> str:
    """Pydantic 'before' validator for non-empty string fields.

    Args:
        v: The value to validate (string, float, or bool)
        info: Pydantic validation information

    Returns:
        A validated non-empty string value.
    """
    return validate_str_field(v, field_name=info.field_name or "string_field", allow_empty=False)


# Annotated types for common validation patterns
ConfigDecimal = Annotated[Decimal, BeforeValidator(_parse_yaml_input_to_required_decimal)]
StringForLiteral = Annotated[str, BeforeValidator(_validate_string_for_literal_check)]
NonEmptyConfigString = Annotated[str, BeforeValidator(_validate_non_empty_string)]
