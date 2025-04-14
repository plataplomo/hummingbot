from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.core.models import (
    ArbitrageOpportunity,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)


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
        # order_id="order123", # Assuming ID is set internally or not needed for mock init
        side=side,
        # order_type=OrderType.LIMIT if price else OrderType.MARKET, # Assuming type is set internally
        price=price,
        quantity=quantity,
        filled_quantity=filled,
        status=status,
        # timestamp=datetime.now(UTC), # Assuming timestamp is set internally
    )


# Update SizedOpportunity usage if necessary
# sized_opportunity = SizedOpportunity(
#     opportunity=mock_opportunity,
#     long_size=Decimal("0.5"), # Use Decimal
#     short_size=Decimal("0.5"), # Use Decimal
#     expected_profit=Decimal("5.0") # Use Decimal
# )
