"""Order validation utilities for Hyperliquid trading operations.

This module provides exchange-specific validation functions for order parameters,
extracted from the monolithic trading service to improve maintainability and testability.
"""

from decimal import Decimal

from cyberdelta.apis.exceptions import InvalidEnumValueError
from cyberdelta.apis.models.service_args_models import PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderType, TimeInForce


logger = get_logger(__name__)

# Constants
HYPERLIQUID_MAX_BATCH_SIZE = 50  # Maximum orders allowed in a single batch request


def validate_place_order_params(args: PlaceOrderArgs, current_method: str) -> None:
    """Validate order parameters for Hyperliquid exchange.

    Validates business logic constraints specific to Hyperliquid,
    ensuring order parameters are compatible with exchange requirements.

    Args:
        args: Order placement parameters
        current_method: Name of calling method for error context

    Raises:
        ValueError: If order parameters are invalid for Hyperliquid
    """
    _validate_order_type(args.order_type, current_method)
    _validate_time_in_force(args.time_in_force, current_method)
    _validate_price_requirements(args, current_method)
    _validate_stop_price_requirements(args, current_method)
    _validate_quantity_precision(args.quantity, current_method)


def validate_orders_list(orders: list[PlaceOrderArgs], current_method: str) -> None:
    """Validate a list of orders for batch operations.

    Args:
        orders: List of order placement parameters
        current_method: Name of calling method for error context

    Raises:
        ValueError: If orders list is invalid
    """
    if not orders:
        error_msg = f"[{current_method}] Orders list cannot be empty"
        logger.error("empty_orders_list", method=current_method, message=error_msg)
        raise ValueError(error_msg)

    if len(orders) > HYPERLIQUID_MAX_BATCH_SIZE:
        error_msg = (
            f"[{current_method}] Too many orders in batch: {len(orders)}. "
            f"Maximum is {HYPERLIQUID_MAX_BATCH_SIZE}"
        )
        logger.error("batch_size_exceeded", method=current_method, count=len(orders))
        raise ValueError(error_msg)

    # Validate each order individually
    for i, order in enumerate(orders):
        try:
            validate_place_order_params(order, current_method)
        except ValueError as e:
            error_msg = f"[{current_method}] Order {i} validation failed: {e}"
            logger.exception(
                "batch_order_validation_failed", method=current_method, order_index=i, error=str(e)
            )
            raise ValueError(error_msg) from e


def validate_batch_orders(orders: list[PlaceOrderArgs], current_method: str) -> None:
    """Validate batch order constraints beyond individual order validation.

    Args:
        orders: List of order placement parameters
        current_method: Name of calling method for error context

    Raises:
        ValueError: If batch constraints are violated
    """
    validate_orders_list(orders, current_method)

    # Check for duplicate symbols in batch (Hyperliquid constraint)
    symbols = [order.symbol for order in orders]
    duplicate_symbols: set[str] = set()
    seen_symbols: set[str] = set()

    for symbol in symbols:
        if symbol in seen_symbols:
            duplicate_symbols.add(symbol)
        seen_symbols.add(symbol)

    if duplicate_symbols:
        error_msg = (
            f"[{current_method}] Duplicate symbols in batch not allowed: "
            f"{', '.join(duplicate_symbols)}"
        )
        logger.error(
            "duplicate_symbols_in_batch", method=current_method, symbols=list(duplicate_symbols)
        )
        raise ValueError(error_msg)


def map_time_in_force_to_hyperliquid(tif: TimeInForce) -> str:
    """Map internal TimeInForce enum values to Hyperliquid-specific format.

    Args:
        tif: Internal time in force enum

    Returns:
        Hyperliquid-compatible time in force string

    Raises:
        ValueError: If time in force is not supported
    """
    mapping = {
        TimeInForce.GTC: "Gtc",
        TimeInForce.IOC: "Ioc",
        TimeInForce.ALO: "Alo",  # Add Liquidity Only for Hyperliquid
    }

    if tif not in mapping:
        supported_values = [t.value for t in mapping]
        raise InvalidEnumValueError(
            parameter_name="time_in_force",
            value=tif.value,
            valid_values=supported_values,
            enum_type="TimeInForce",
        )

    return mapping[tif]


def _validate_order_type(order_type: OrderType, current_method: str) -> None:
    """Validate that order type is supported by Hyperliquid."""
    supported_order_types = [
        OrderType.LIMIT,
        OrderType.MARKET,
        OrderType.STOP_MARKET,
        OrderType.STOP_LIMIT,
    ]

    if order_type not in supported_order_types:
        supported_values = [ot.value for ot in supported_order_types]
        error_msg = (
            f"[{current_method}] Order type {order_type.value} is not supported "
            f"by Hyperliquid. Supported types: {supported_values}"
        )
        logger.error(
            "unsupported_order_type",
            method=current_method,
            order_type=order_type.value,
            supported_types=supported_values,
        )
        raise ValueError(error_msg)


def _validate_time_in_force(tif: TimeInForce, current_method: str) -> None:
    """Validate that time in force is supported by Hyperliquid."""
    if tif == TimeInForce.FOK:
        error_msg = (
            f"[{current_method}] TimeInForce FOK is not supported by Hyperliquid. "
            f"Supported values: GTC, IOC, ALO"
        )
        logger.error("unsupported_time_in_force", method=current_method, tif=tif.value)
        raise ValueError(error_msg)


def _validate_price_requirements(args: PlaceOrderArgs, current_method: str) -> None:
    """Validate price requirements for different order types."""
    if args.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and args.price is None:
        error_msg = f"[{current_method}] Price is required for {args.order_type.value} orders"
        logger.error("missing_price", method=current_method, order_type=args.order_type.value)
        raise ValueError(error_msg)

    if args.price is not None and args.price <= Decimal(0):
        error_msg = f"[{current_method}] Price must be positive, got: {args.price}"
        logger.error("invalid_price", method=current_method, price=str(args.price))
        raise ValueError(error_msg)


def _validate_stop_price_requirements(args: PlaceOrderArgs, current_method: str) -> None:
    """Validate stop price requirements for stop orders."""
    if args.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT} and args.stop_price is None:
        error_msg = f"[{current_method}] Stop price is required for {args.order_type.value} orders"
        logger.error("missing_stop_price", method=current_method, order_type=args.order_type.value)
        raise ValueError(error_msg)

    if args.stop_price is not None and args.stop_price <= Decimal(0):
        error_msg = f"[{current_method}] Stop price must be positive, got: {args.stop_price}"
        logger.error("invalid_stop_price", method=current_method, stop_price=str(args.stop_price))
        raise ValueError(error_msg)


def _validate_quantity_precision(quantity: Decimal, current_method: str) -> None:
    """Validate quantity precision and bounds."""
    if quantity <= Decimal(0):
        error_msg = f"[{current_method}] Quantity must be positive, got: {quantity}"
        logger.error("invalid_quantity", method=current_method, quantity=str(quantity))
        raise ValueError(error_msg)

    # Check for finite decimal (no infinity/NaN)
    if not quantity.is_finite():
        error_msg = f"[{current_method}] Quantity must be finite, got: {quantity}"
        logger.error("non_finite_quantity", method=current_method, quantity=str(quantity))
        raise ValueError(error_msg)
