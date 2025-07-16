"""Decimal parsing utilities for financial data handling.

This module provides safe decimal parsing utilities that ensure
financial precision and follow project rules for decimal handling.
"""

from decimal import Decimal, InvalidOperation

from cyberdelta.apis.base.validation_context_domain import ValidationContext
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def _validate_decimal_finite(value: Decimal, field_name: str, context: str) -> None:
    """Validate that a decimal value is finite."""
    if not value.is_finite():
        raise APIError(
            message=f"Non-finite decimal value for {field_name} in {context}: {value}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )


def _prepare_value_string(value: str | float | Decimal, field_name: str, context: str) -> str:
    """Convert value to string for safe decimal parsing."""
    if isinstance(value, str):
        # Remove whitespace
        value_str = value.strip()
        if not value_str:
            raise APIError(
                message=(f"Empty string cannot be parsed as decimal for {field_name} in {context}"),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
        return value_str
    if isinstance(value, float):
        # Convert numbers to string to avoid float precision issues
        return str(value)
    raise APIError(
        message=(f"Cannot parse {type(value).__name__} as decimal for {field_name} in {context}"),
        code=APIErrorCode.INVALID_RESPONSE.value,
    )


def safe_parse_decimal(
    value: str | float | Decimal | None,
    validation_context: ValidationContext | None = None,
) -> Decimal | None:
    """Safely parse a value to Decimal with comprehensive error handling.

    Args:
        value: Value to parse (string, int, float, or Decimal)
        validation_context: Validation context with null and range policies

    Returns:
        Parsed Decimal value or None if allowed

    Raises:
        APIError: If parsing fails or value is invalid
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    # Handle None values according to context policy
    if value is None:
        if validation_context.null_policy.value == "allow":
            return None
        if validation_context.null_policy.value == "default_zero":
            return Decimal(0)
        raise APIError(
            message=(
                f"Cannot parse None as decimal for {validation_context.field_name} "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    # Handle already-decimal values
    if isinstance(value, Decimal):
        _validate_decimal_finite(
            value, validation_context.field_name, validation_context.context_description
        )
        return value

    # Convert to string for safe parsing
    try:
        value_str = _prepare_value_string(
            value, validation_context.field_name, validation_context.context_description
        )

        # Parse using Decimal constructor
        decimal_value = Decimal(value_str)

        # Validate the result
        _validate_decimal_finite(
            decimal_value, validation_context.field_name, validation_context.context_description
        )

        logger.debug(
            "decimal_parse_success",
            field_name=validation_context.field_name,
            context=validation_context.context_description,
            original_value=str(value),
            parsed_value=str(decimal_value),
        )

    except InvalidOperation as e:
        logger.exception(
            "decimal_parse_failed",
            field_name=validation_context.field_name,
            context=validation_context.context_description,
            value=str(value),
            error=str(e),
        )
        raise APIError(
            message=(
                f"Invalid decimal format for {validation_context.field_name} "
                f"in {validation_context.context_description}: {value}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e
    except (ValueError, TypeError) as e:
        logger.exception(
            "decimal_parse_error",
            field_name=validation_context.field_name,
            context=validation_context.context_description,
            value=str(value),
            error=str(e),
        )
        raise APIError(
            message=(
                f"Failed to parse decimal for {validation_context.field_name} "
                f"in {validation_context.context_description}: {value}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e
    else:
        return decimal_value


def validate_positive_decimal(
    value: Decimal,
    validation_context: ValidationContext | None = None,
) -> Decimal:
    """Validate that a decimal value is positive according to validation context.

    Args:
        value: Decimal value to validate
        validation_context: Validation context with range policy

    Returns:
        Validated decimal value

    Raises:
        APIError: If value is not positive
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    # Apply range validation according to policy
    if validation_context.range_policy.value == "non_negative":
        if value < Decimal(0):
            raise APIError(
                message=(
                    f"{validation_context.field_name} must be non-negative "
                    f"in {validation_context.context_description}, got: {value}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
    elif validation_context.range_policy.value == "positive":
        if value <= Decimal(0):
            raise APIError(
                message=(
                    f"{validation_context.field_name} must be positive "
                    f"in {validation_context.context_description}, got: {value}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )
    elif validation_context.range_policy.value == "financial_positive" and value <= Decimal(
        "0.00000001"
    ):
        raise APIError(
            message=(
                f"{validation_context.field_name} must be financially positive (> 0.00000001) "
                f"in {validation_context.context_description}, got: {value}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    return value


def validate_decimal_precision(
    value: Decimal,
    max_decimal_places: int,
    field_name: str = "value",
    context: str = "unknown",
) -> Decimal:
    """Validate that a decimal value has acceptable precision.

    Args:
        value: Decimal value to validate
        max_decimal_places: Maximum allowed decimal places
        field_name: Name of the field for error messages
        context: Context description for error messages

    Returns:
        Validated decimal value

    Raises:
        APIError: If precision exceeds limit
    """
    # Get the number of decimal places
    _sign, _digits, exponent = value.as_tuple()

    # Check for special values (NaN, Infinity) where exponent is not an integer
    if not isinstance(exponent, int):
        raise APIError(
            message=(
                f"{field_name} contains special value in {context}: {value} (exponent: {exponent})"
            ),
            code=APIErrorCode.INVALID_REQUEST.value,
        )

    if exponent < 0:  # Negative exponent means decimal places
        decimal_places = -exponent
        if decimal_places > max_decimal_places:
            raise APIError(
                message=(
                    f"{field_name} has too many decimal places in {context}: "
                    f"{decimal_places} > {max_decimal_places}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

    return value


def format_decimal_for_exchange(
    value: Decimal,
    decimal_places: int,
    field_name: str = "value",
    context: str = "unknown",
) -> str:
    """Format a decimal value for exchange API submission.

    Args:
        value: Decimal value to format
        decimal_places: Number of decimal places to format to
        field_name: Name of the field for error messages
        context: Context description for error messages

    Returns:
        Formatted decimal string

    Raises:
        APIError: If formatting fails
    """
    try:
        # Create format string (e.g., "0.00000000" for 8 decimal places)
        format_str = "0." + "0" * decimal_places
        formatted = value.quantize(Decimal(format_str))

        # Convert to string and remove trailing zeros for cleaner output
        result = str(formatted).rstrip("0").rstrip(".")

        # Ensure we have at least one digit after decimal for non-integers
        if "." not in result and decimal_places > 0:
            result += ".0"

        logger.debug(
            "decimal_format_success",
            field_name=field_name,
            context=context,
            original_value=str(value),
            formatted_value=result,
            decimal_places=decimal_places,
        )

    except (InvalidOperation, ValueError) as e:
        logger.exception(
            "decimal_format_failed",
            field_name=field_name,
            context=context,
            value=str(value),
            decimal_places=decimal_places,
            error=str(e),
        )
        raise APIError(
            message=f"Failed to format decimal {field_name} in {context}: {value}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e
    else:
        return result
