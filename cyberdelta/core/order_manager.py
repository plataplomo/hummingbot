"""OrderManager: Business Logic Layer for Order State Management.

Handles all order state mutation, fill reconciliation, average fill price calculation,
and status transitions.
This is the only place where order state mutation, snapping, and status transitions occur.

- Applies new fills/trades to an order
- Updates quantity_filled, average_fill_price, and status
- Handles rounding/overfill (snapping)
- Emits logs or warnings for inconsistencies
- Ensures all state transitions are explicit and testable

Usage:
    OrderManager.apply_fill(order, trade)
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from cyberdelta.core.models.market.order import Order
    from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.enums import OrderStatus


logger = logging.getLogger(__name__)


class OrderManager:
    """Handles business logic for updating Order state in response to new fills/trades.

    This is the only place where order state mutation, snapping, and status transitions occur.
    """

    @staticmethod
    def apply_fill(order: "Order", trade: "Trade") -> None:
        """Apply a new fill/trade to the order.

        Updates filled quantity, average fill price, and status.

        Handles overfill (snapping) and logs inconsistencies.

        Args:
            order (Order): The order to update.
            trade (Trade): The new fill/trade to apply.

        """
        # Validate trade matches order
        if trade.order_id and order.exchange_order_id and trade.order_id != order.exchange_order_id:
            logger.warning("Trade order_id does not match order.exchange_order_id")
        if trade.client_order_id and trade.client_order_id != order.client_order_id:
            logger.warning("Trade client_order_id does not match order.client_order_id")
        if trade.symbol != order.symbol or (trade.side and trade.side != order.side):
            logger.warning("Trade details mismatch order details")

        # Update filled quantity and average fill price
        current_total_value = (order.average_fill_price or Decimal(0)) * order.quantity_filled
        new_total_value = current_total_value + (trade.price * trade.quantity)
        new_quantity_filled = order.quantity_filled + trade.quantity

        if new_quantity_filled > 0:
            order.average_fill_price = new_total_value / new_quantity_filled
        else:
            order.average_fill_price = None

        order.quantity_filled = new_quantity_filled
        order.trades.append(trade)
        order.updated_at = (
            datetime.now(order.created_at.tzinfo) if order.created_at.tzinfo else datetime.now()
        )

        # Handle overfill (snapping)
        tolerance = Decimal("1e-9")
        if order.quantity_filled > order.quantity_requested:
            if (order.quantity_filled - order.quantity_requested) > tolerance:
                logger.error(
                    f"Order {order.client_order_id}: quantity_filled ({order.quantity_filled}) > "
                    f"quantity_requested ({order.quantity_requested})",
                )
                # Optionally raise or snap
                order.quantity_filled = order.quantity_requested
            else:
                logger.warning(
                    f"Order {order.client_order_id}: Snapping slightly overfilled qty "
                    f"{order.quantity_filled} to requested {order.quantity_requested}.",
                )
                order.quantity_filled = order.quantity_requested

        # Status transitions
        if order.status not in [
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        ]:
            if abs(order.quantity_filled - order.quantity_requested) < tolerance:
                order.status = OrderStatus.FILLED
            elif order.quantity_filled > 0:
                order.status = OrderStatus.PARTIALLY_FILLED

        # Additional consistency checks/logging as needed
