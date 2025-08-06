"""Order tracking and state management.

This module tracks active orders and provides order state management
functionality following CODING_STANDARDS.md.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class OrderTracker:
    """Tracks active orders and manages order state.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Returns copies to prevent external mutation
    - Explicit state tracking
    - NO assumptions about order lifecycle
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize order tracker with configuration.

        Args:
            config: Application settings for any tracking limits
        """
        self.config = config
        self._active_orders: dict[str, Order] = {}
        self._order_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

    def track_order(self, order: Order, signal_id: str | None = None) -> None:
        """Track a new active order.

        Args:
            order: Order to track
            signal_id: Optional signal ID that generated the order
        """
        if order.exchange_order_id:
            self._active_orders[order.exchange_order_id] = order
            self._order_count += 1
            self._last_activity = datetime.now(UTC)

            logger.info(
                "order_tracked",
                order_id=order.exchange_order_id,
                client_order_id=order.client_order_id,
                signal_id=signal_id,
                symbol=order.symbol.value,
                exchange=order.exchange.value,
                status=order.status.value,
            )

    def update_order_status(self, order_id: str, new_status: OrderStatus) -> bool:
        """Update the status of a tracked order.

        Args:
            order_id: Exchange order ID
            new_status: New order status

        Returns:
            True if order was found and updated, False otherwise
        """
        order = self._active_orders.get(order_id)
        if not order:
            logger.warning("status_update_for_unknown_order", order_id=order_id)
            return False

        old_status = order.status
        order.status = new_status
        order.updated_at = datetime.now(UTC)

        logger.info(
            "order_status_updated",
            order_id=order_id,
            old_status=old_status.value,
            new_status=new_status.value,
        )

        # Update success/error counts based on status
        if new_status == OrderStatus.FILLED:
            self._success_count += 1
        elif new_status in {OrderStatus.CANCELED, OrderStatus.REJECTED}:
            self._error_count += 1

        # Remove from active tracking if terminal state
        if new_status in {OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED}:
            del self._active_orders[order_id]
            logger.debug(
                "order_removed_from_tracking",
                order_id=order_id,
                final_status=new_status.value,
            )

        self._last_activity = datetime.now(UTC)
        return True

    def get_order(self, order_id: str) -> Order | None:
        """Get a tracked order by ID.

        Args:
            order_id: Exchange order ID

        Returns:
            Order if found, None otherwise
        """
        return self._active_orders.get(order_id)

    def remove_order(self, order_id: str) -> bool:
        """Remove an order from tracking.

        Args:
            order_id: Exchange order ID

        Returns:
            True if order was found and removed, False otherwise
        """
        if order_id in self._active_orders:
            del self._active_orders[order_id]
            self._last_activity = datetime.now(UTC)
            logger.debug("order_removed_from_tracking", order_id=order_id)
            return True
        return False

    def get_active_order_count(self) -> int:
        """Get count of active orders.

        Returns:
            Number of orders currently being tracked
        """
        return len(self._active_orders)

    def get_active_orders(self) -> dict[str, Order]:
        """Get copy of active orders dictionary.

        Returns:
            Copy of active orders for read-only access

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns copy to prevent external mutation
        """
        return self._active_orders.copy()

    def get_statistics(self) -> dict[str, Any]:
        """Get tracking statistics.

        Returns:
            Dictionary with tracking metrics
        """
        return {
            "active_orders": len(self._active_orders),
            "total_orders": self._order_count,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
            "success_rate": (
                self._success_count / self._order_count if self._order_count > 0 else 0.0
            ),
        }

    def increment_error_count(self) -> None:
        """Increment error count for tracking metrics."""
        self._error_count += 1
        self._last_activity = datetime.now(UTC)
