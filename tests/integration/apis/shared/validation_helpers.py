"""Common validation helpers extracted from existing tests."""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.spot_balance import SpotBalance


def assert_valid_spot_balance(balance: SpotBalance) -> None:
    """Common spot balance assertions extracted from existing tests."""
    # Basic type validation
    assert isinstance(balance.total_quantity, Decimal)
    assert isinstance(balance.available_quantity, Decimal)

    # Value validation
    assert balance.total_quantity >= Decimal(0)
    assert balance.available_quantity >= Decimal(0)
    assert balance.total_quantity >= balance.available_quantity

    # Precision validation
    total_precision = (
        len(str(balance.total_quantity).split(".")[-1]) if "." in str(balance.total_quantity) else 0
    )
    available_precision = (
        len(str(balance.available_quantity).split(".")[-1])
        if "." in str(balance.available_quantity)
        else 0
    )

    assert total_precision <= 18
    assert available_precision <= 18

    # Backpack-specific validation if present
    if balance.bp_details:
        bp_details = balance.bp_details
        if bp_details.open_order_quantity is not None:
            assert isinstance(bp_details.open_order_quantity, Decimal)
            assert bp_details.open_order_quantity >= Decimal(0)


def assert_valid_derivative_position(position: DerivativePosition) -> None:
    """Common derivative position assertions."""
    assert isinstance(position.size, Decimal)
    assert position.size.is_finite()
    if position.size != Decimal(0):
        assert position.entry_price is not None
        assert position.entry_price > Decimal(0)


def assert_valid_order_lifecycle(order: Order) -> None:
    """Common order lifecycle assertions."""
    assert order.quantity_filled <= order.quantity_requested
    if order.quantity_filled > Decimal(0):
        assert order.average_fill_price is not None
        assert order.average_fill_price > Decimal(0)
