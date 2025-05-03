from dataclasses import dataclass
from decimal import Decimal

from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,  # Added import
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
