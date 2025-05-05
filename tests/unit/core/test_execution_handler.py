from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.core.models import (
    ExecutionStatus,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,  # Added import
    TimeInForce,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Define SizedOpportunity locally or import if moved
@dataclass
class SizedOpportunity:
    opportunity: ArbitrageOpportunity
    long_size: Decimal
    short_size: Decimal
    expected_profit: Decimal


# Ensure Decimal is used for price/quantity in Order creation
def create_mock_order(
    side: OrderSide = OrderSide.BUY,
    price: Decimal | None = Decimal("30000"),
    quantity: Decimal = Decimal("1.0"),
    filled: Decimal = Decimal("0.0"),
    status: OrderStatus = OrderStatus.OPEN,
) -> Order:
    return Order(
        symbol="BTC-PERP",
        id="mock_order_id",  # Added default ID
        type=OrderType.LIMIT if price else OrderType.MARKET,  # Added type based on price
        side=side,
        price=price,
        quantity=quantity,
        filled_quantity=filled,
        status=status,
        # timestamp=datetime.now(UTC), # Assuming timestamp is set internally or handled by Order
    )


# Update SizedOpportunity usage if necessary
# sized_opportunity = SizedOpportunity(
#     opportunity=mock_opportunity,
#     long_size=Decimal("0.5"), # Use Decimal
#     short_size=Decimal("0.5"), # Use Decimal
#     expected_profit=Decimal("5.0") # Use Decimal
# )


def mock_get_config(key: str, default: Any = None) -> object | None:
    """Mock function for Config.get."""
    config_values = {
        "exchanges.mock_hl.enabled": True,
    }
    return config_values.get(key, default)


def create_mock_order_with_details(
    client_order_id: str,
    symbol: str,
    side: OrderSide,
    status: OrderStatus,
    price: Decimal,
    quantity: Decimal,
    filled_quantity: Decimal,
    created_at: datetime,
    updated_at: datetime,
    time_in_force: TimeInForce,
    exchange: str,
    triggered_at: datetime | None,
    strategy_name: str | None,
    signal_id: str | None,
) -> Order:
    return Order(
        client_order_id=client_order_id,
        symbol=symbol,
        side=side,
        status=status,
        price=price,
        quantity_requested=quantity,
        quantity_filled=filled_quantity,
        created_at=created_at,
        updated_at=updated_at,
        time_in_force=time_in_force,
        exchange=exchange,
        triggered_at=triggered_at,
        strategy_name=strategy_name,
        signal_id=signal_id,
    )


def test_execution_handler():
    # Create mock Order
    mock_order = Order(
        client_order_id="test_order_123",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        status=OrderStatus.OPEN,
        price=Decimal("50000.0"),
        quantity_requested=Decimal("0.1"),
        quantity_filled=Decimal("0.0"),
        time_in_force=TimeInForce.GTC,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        exchange="mock_hl",
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )

    # Create a mock Order object
    mock_order = Order(
        client_order_id="test_order_123",
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        status=OrderStatus.OPEN,
        price=Decimal("50000.0"),
        quantity_requested=Decimal("0.1"),
        quantity_filled=Decimal("0.0"),
        time_in_force=TimeInForce.GTC,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        exchange="mock_hl",
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )

    # Assertions based on expected behavior after compensation
    assert exec_result.status == ExecutionStatus.SUCCESS
    assert exec_result.order_id == original_order_id
    mock_hl_api.place_order.assert_called_once()  # Only initial order placement

    assert exec_result.status == ExecutionStatus.FAILED
    assert exec_result.error_message is not None
    assert exec_result.order_id == original_order_id
    mock_hl_api.place_order.assert_called_once()  # Only initial order placement
