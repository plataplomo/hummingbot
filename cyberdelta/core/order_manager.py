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

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.core.models.market.order import Order
    from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.enums import OrderStatus


logger = get_logger(__name__)


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
            logger.warning(
                "trade_order_id_mismatch",
                trade_order_id=trade.order_id,
                order_exchange_id=order.exchange_order_id,
                order_client_id=order.client_order_id,
                symbol=order.symbol,
                action="validation_warning",
                message="Trade order_id does not match order.exchange_order_id",
            )
        if trade.client_order_id and trade.client_order_id != order.client_order_id:
            logger.warning(
                "trade_client_order_id_mismatch",
                trade_client_order_id=trade.client_order_id,
                order_client_order_id=order.client_order_id,
                order_exchange_id=order.exchange_order_id,
                symbol=order.symbol,
                action="validation_warning",
                message="Trade client_order_id does not match order.client_order_id",
            )
        if trade.symbol != order.symbol or (trade.side and trade.side != order.side):
            logger.warning(
                "trade_details_mismatch",
                trade_symbol=trade.symbol,
                order_symbol=order.symbol,
                trade_side=trade.side.value if trade.side else None,
                order_side=order.side.value,
                order_client_id=order.client_order_id,
                action="validation_warning",
                message="Trade details mismatch order details",
            )

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
            datetime.now(order.created_at.tzinfo) if order.created_at.tzinfo else datetime.now(UTC)
        )

        # Handle overfill (snapping)
        tolerance = Decimal("1e-9")
        if order.quantity_filled > order.quantity_requested:
            if (order.quantity_filled - order.quantity_requested) > tolerance:
                logger.error(
                    "order_overfill_error",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    symbol=order.symbol,
                    quantity_filled=float(order.quantity_filled),
                    quantity_requested=float(order.quantity_requested),
                    overfill_amount=float(order.quantity_filled - order.quantity_requested),
                    action="snapping_to_requested",
                    message=(
                        f"Order {order.client_order_id}: quantity_filled "
                        f"({order.quantity_filled}) > quantity_requested "
                        f"({order.quantity_requested})"
                    ),
                )
                # Optionally raise or snap
                order.quantity_filled = order.quantity_requested
            else:
                logger.warning(
                    "order_slight_overfill_snapped",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    symbol=order.symbol,
                    quantity_filled=float(order.quantity_filled),
                    quantity_requested=float(order.quantity_requested),
                    overfill_amount=float(order.quantity_filled - order.quantity_requested),
                    tolerance=float(tolerance),
                    action="snapping_to_requested",
                    message=(
                        f"Order {order.client_order_id}: Snapping slightly "
                        f"overfilled qty {order.quantity_filled} to requested "
                        f"{order.quantity_requested}."
                    ),
                )
                order.quantity_filled = order.quantity_requested

        # Status transitions
        previous_status = order.status
        if order.status not in {
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.FAILED,
        }:
            if abs(order.quantity_filled - order.quantity_requested) < tolerance:
                order.status = OrderStatus.FILLED
                logger.info(
                    "order_status_transition_to_filled",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    symbol=order.symbol,
                    previous_status=previous_status.value,
                    new_status=order.status.value,
                    quantity_filled=float(order.quantity_filled),
                    quantity_requested=float(order.quantity_requested),
                    average_fill_price=float(order.average_fill_price)
                    if order.average_fill_price
                    else None,
                    trade_id=trade.id,
                    trade_price=float(trade.price),
                    trade_quantity=float(trade.quantity),
                    message=(f"Order {order.client_order_id} fully filled with trade {trade.id}"),
                )
            elif order.quantity_filled > 0:
                order.status = OrderStatus.PARTIALLY_FILLED
                logger.info(
                    "order_status_transition_to_partially_filled",
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    symbol=order.symbol,
                    previous_status=previous_status.value,
                    new_status=order.status.value,
                    quantity_filled=float(order.quantity_filled),
                    quantity_requested=float(order.quantity_requested),
                    fill_percentage=float((order.quantity_filled / order.quantity_requested) * 100),
                    average_fill_price=float(order.average_fill_price)
                    if order.average_fill_price
                    else None,
                    trade_id=trade.id,
                    trade_price=float(trade.price),
                    trade_quantity=float(trade.quantity),
                    message=(
                        f"Order {order.client_order_id} partially filled "
                        f"({order.quantity_filled}/{order.quantity_requested}) "
                        f"with trade {trade.id}"
                    ),
                )

        # Log successful fill application
        logger.debug(
            "fill_applied_to_order",
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id,
            symbol=order.symbol,
            trade_id=trade.id,
            trade_price=float(trade.price),
            trade_quantity=float(trade.quantity),
            order_status=order.status.value,
            quantity_filled=float(order.quantity_filled),
            quantity_requested=float(order.quantity_requested),
            average_fill_price=float(order.average_fill_price)
            if order.average_fill_price
            else None,
            num_trades=len(order.trades),
            message=(
                f"Applied fill {trade.id} to order "
                f"{order.client_order_id}: {trade.quantity} @ {trade.price}"
            ),
        )
