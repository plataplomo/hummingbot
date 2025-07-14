"""Common mapper utilities for Hyperliquid mappers.

This module contains shared utility functions used across multiple Hyperliquid mappers
to reduce code duplication and ensure consistency.
"""

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import MissingRequiredFieldError, UnknownEnumError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide


logger = get_logger(__name__)


def validate_side_value(hl_side: str) -> None:
    """Validate order side value.

    Args:
        hl_side: Raw side string from Hyperliquid ("B" or "A")

    Raises:
        UnknownEnumError: If side cannot be mapped
    """
    if hl_side not in {"B", "A"}:
        raise UnknownEnumError(
            enum_type="Hyperliquid order side", value=hl_side, valid_values=["B", "A"]
        )


def map_side_to_internal(hl_side: str) -> OrderSide:
    """Maps a Hyperliquid order side string to internal OrderSide enum.

    Args:
        hl_side: Raw side string from Hyperliquid ("B" or "A")

    Returns:
        OrderSide: Mapped internal enum value

    Raises:
        UnknownEnumError: If side cannot be mapped
    """
    try:
        # Validate side value first
        validate_side_value(hl_side)

        if hl_side == "B":
            return OrderSide.BUY
        if hl_side == "A":
            return OrderSide.SELL

    except Exception as e:
        if isinstance(e, TransformationError):
            raise
        raise UnknownEnumError(
            enum_type="Hyperliquid order side", value=hl_side, valid_values=["B", "A"]
        ) from e
    else:
        # This should not be reached due to validation above, but for type safety
        return OrderSide.BUY


def validate_trade_data(price: object, quantity: object, context: str) -> tuple[object, object]:
    """Validate trade price and quantity data.

    Args:
        price: Raw price value
        quantity: Raw quantity value
        context: Context for error messages

    Returns:
        tuple[object, object]: Validated price and quantity

    Raises:
        MissingRequiredFieldError: If required fields are missing
    """
    missing_fields: list[str] = []
    if price is None:
        missing_fields.append("price")
    if quantity is None:
        missing_fields.append("quantity")

    if missing_fields:
        raise MissingRequiredFieldError(missing_fields, context)

    return price, quantity


def ensure_not_none(
    value: object | None,
    field_name: str,
    context: str,
    additional_info: dict[str, object] | None = None,
) -> object:
    """Ensure a value is not None.

    This is a generic utility for the common pattern of checking if a value is None
    and raising an appropriate error.

    Args:
        value: The value to check
        field_name: Name of the field being checked
        context: Context for error messages
        additional_info: Additional information to include in error logs

    Returns:
        The value if not None

    Raises:
        MissingRequiredFieldError: If value is None
    """
    if value is None:
        logger.error(
            "required_field_none",
            field=field_name,
            context=context,
            additional_info=additional_info,
            message=f"Required field '{field_name}' is None in {context}",
        )
        raise MissingRequiredFieldError([field_name], context)
    return value
