"""Response formatting utilities for Hyperliquid API responses.

This module provides utilities for formatting and transforming API responses
into internal domain models, extracted to improve code organization and reusability.
"""

from decimal import Decimal
from typing import Any, TypeGuard

from cyberdelta.apis.base.validation_contexts import ValidationContext
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import InvalidBatchResponseError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import CancelOrderResultStatus
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_decimal_value


logger = get_logger(__name__)


def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if value is a dict[str, Any].

    This helps pyright understand that after this check,
    the value is definitely a dict with string keys.

    Returns:
        bool: True if value is a dict, False otherwise.
    """
    return isinstance(value, dict)


def format_cancel_order_result(
    cancel_status: dict[str, Any],
    symbol: Symbol,
    order_id: str | None = None,
) -> CancelOrderResult:
    """Format cancellation status into CancelOrderResult.

    Args:
        cancel_status: Processed cancellation status from status processing
        symbol: Trading symbol
        order_id: Optional order ID

    Returns:
        Formatted cancel order result
    """
    if "success" in cancel_status:
        return CancelOrderResult(
            status=CancelOrderResultStatus.SUCCESS,
            success=True,
            symbol=symbol,
            order_id=order_id,
            message="Order canceled successfully",
        )

    if "canceled" in cancel_status:
        return CancelOrderResult(
            status=CancelOrderResultStatus.SUCCESS,
            success=True,
            symbol=symbol,
            order_id=order_id,
            message="Order was already canceled",
        )

    if "error" in cancel_status:
        error_message = str(cancel_status["error"])
        return CancelOrderResult(
            status=CancelOrderResultStatus.FAILED,
            success=False,
            symbol=symbol,
            order_id=order_id,
            message=f"Failed to cancel order: {error_message}",
        )

    # Unknown status
    return CancelOrderResult(
        status=CancelOrderResultStatus.FAILED,
        success=False,
        symbol=symbol,
        order_id=order_id,
        message=f"Unknown cancellation status: {cancel_status}",
    )


def format_batch_cancel_results(
    batch_statuses: list[dict[str, Any]],
    symbols: list[Symbol],
    order_ids: list[str] | None = None,
) -> list[CancelOrderResult]:
    """Format batch cancellation statuses into CancelOrderResult list.

    Args:
        batch_statuses: List of processed cancellation statuses
        symbols: List of trading symbols
        order_ids: Optional list of order IDs

    Returns:
        List of formatted cancel order results

    Raises:
        InvalidBatchResponseError: If array lengths don't match
    """
    if len(batch_statuses) != len(symbols):
        raise InvalidBatchResponseError(
            operation="batch cancel results formatting",
            expected_data=f"{len(symbols)} statuses to match {len(symbols)} symbols",
            response_data={"statuses_count": len(batch_statuses), "symbols_count": len(symbols)},
        )

    if order_ids and len(order_ids) != len(symbols):
        raise InvalidBatchResponseError(
            operation="batch cancel results formatting",
            expected_data=f"{len(symbols)} order IDs to match {len(symbols)} symbols",
            response_data={"order_ids_count": len(order_ids), "symbols_count": len(symbols)},
        )

    results: list[CancelOrderResult] = []
    for i, (status, symbol) in enumerate(zip(batch_statuses, symbols, strict=False)):
        order_id = order_ids[i] if order_ids else None
        result = format_cancel_order_result(status, symbol, order_id)
        results.append(result)

    return results


def validate_decimal_field(
    value: object,
    validation_context: ValidationContext | None = None,
) -> Decimal | None:
    """Validate and parse a decimal field from API response.

    Args:
        value: Value to validate and parse
        validation_context: Validation context with null and precision policies

    Returns:
        Parsed decimal value or None if allowed

    Raises:
        APIError: If value is invalid
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()

    if value is None:
        if validation_context.null_policy.value == "allow":
            return None
        raise APIError(
            message=(
                f"Missing required field '{validation_context.field_name}' "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    try:
        # Type check for parse_decimal_value compatibility
        if not isinstance(value, (Decimal, str, float)):
            _raise_invalid_decimal_type_error(value, validation_context.field_name)
        # Use helper function for proper type handling
        return _parse_validated_decimal(value)
    except (ValueError, TypeError) as e:
        logger.exception(
            "decimal_field_validation_failed",
            field_name=validation_context.field_name,
            context=validation_context.context_description,
            value=str(value),
            error=str(e),
        )
        raise APIError(
            message=(
                f"Invalid {validation_context.field_name} "
                f"in {validation_context.context_description}: {value}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        ) from e


def validate_string_field(
    value: object,
    validation_context: ValidationContext | None = None,
) -> str | None:
    """Validate a string field from API response.

    Args:
        value: Value to validate
        validation_context: Validation context with null and string policies

    Returns:
        Validated string or None if allowed

    Raises:
        APIError: If value is invalid
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()
    if value is None:
        if validation_context.null_policy.value == "allow":
            return None
        raise APIError(
            message=(
                f"Missing required field '{validation_context.field_name}' "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    if not isinstance(value, str):
        raise APIError(
            message=(
                f"Field '{validation_context.field_name}' must be string "
                f"in {validation_context.context_description}, "
                f"got {type(value).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    min_length = getattr(validation_context, "min_length", 0)
    if len(value) < min_length:
        raise APIError(
            message=(
                f"Field '{validation_context.field_name}' too short "
                f"in {validation_context.context_description}: "
                f"minimum {min_length} characters"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    return value


def validate_integer_field(
    value: object,
    validation_context: ValidationContext | None = None,
) -> int | None:
    """Validate an integer field from API response.

    Args:
        value: Value to validate
        validation_context: Validation context with null and range policies

    Returns:
        Validated integer or None if allowed

    Raises:
        APIError: If value is invalid
    """
    # Use default context if none provided
    if validation_context is None:
        validation_context = ValidationContext()
    if value is None:
        if validation_context.null_policy.value == "allow":
            return None
        raise APIError(
            message=(
                f"Missing required field '{validation_context.field_name}' "
                f"in {validation_context.context_description}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    if not isinstance(value, int):
        raise APIError(
            message=(
                f"Field '{validation_context.field_name}' must be integer "
                f"in {validation_context.context_description}, "
                f"got {type(value).__name__}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    min_value = getattr(validation_context, "min_value", None)
    if min_value is not None and value < min_value:
        raise APIError(
            message=(
                f"Field '{validation_context.field_name}' too small "
                f"in {validation_context.context_description}: "
                f"minimum {min_value}"
            ),
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    return value


def safe_extract_nested_field(
    data: dict[str, Any],
    field_path: list[str],
    context: str,
    default: object = None,
) -> object:
    """Safely extract a nested field from dictionary data.

    Args:
        data: Dictionary to extract from
        field_path: List of keys representing the path to the field
        context: Context description for error messages
        default: Default value if field is not found

    Returns:
        Extracted value or default
    """
    current: object = data
    for i, key in enumerate(field_path):
        if not _is_dict_str_any(current):
            logger.debug(
                "nested_field_not_found",
                context=context,
                field_path=".".join(field_path[: i + 1]),
                available_keys="not_dict",
            )
            return default

        # Now pyright knows current is dict[str, Any]
        if key not in current:
            available_keys: list[str] = list(current.keys())
            logger.debug(
                "nested_field_not_found",
                context=context,
                field_path=".".join(field_path[: i + 1]),
                available_keys=available_keys,
            )
            return default

        # Extract the value from the dict
        current = current[key]

    return current


def _parse_validated_decimal(value: object) -> Decimal | None:
    """Parse a value that has been validated to be a decimal type.

    Args:
        value: Value that is guaranteed to be Decimal | str | float

    Returns:
        Parsed decimal value

    Raises:
        TypeError: If value is not a decimal-compatible type (internal error).
    """
    # Double-check type for mypy (should already be validated by caller)
    if not isinstance(value, (Decimal, str, float)):
        msg = f"Internal error: expected decimal-compatible type, got {type(value)}"
        raise TypeError(msg)
    return parse_decimal_value(value)


def _raise_invalid_decimal_type_error(value: object, field_name: str) -> None:
    """Raise TypeError for invalid decimal field type.

    Args:
        value: The invalid value
        field_name: Name of the field for error context

    Raises:
        TypeError: Always raises with field-specific error message.
    """
    msg = f"Invalid type for decimal field '{field_name}': {type(value)}"
    raise TypeError(msg)


def create_batch_response_error(
    operation: str,
    expected_count: int,
    actual_count: int,
    details: str = "",
) -> InvalidBatchResponseError:
    """Create a standardized batch response error.

    Args:
        operation: Description of the batch operation
        expected_count: Expected number of responses
        actual_count: Actual number of responses
        details: Additional error details

    Returns:
        Configured InvalidBatchResponseError
    """
    message = (
        f"Batch {operation} response count mismatch: expected {expected_count}, got {actual_count}"
    )

    if details:
        message += f". {details}"

    return InvalidBatchResponseError(
        operation=operation,
        expected_data=f"{expected_count} responses",
    )
