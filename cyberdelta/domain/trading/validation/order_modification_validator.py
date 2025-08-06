"""Order modification and cancellation validation.

This module validates order modifications and cancellations against exchange rules
using validated AppSettings configuration.
"""

from __future__ import annotations

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class OrderModificationValidator:
    """Validates order modifications and cancellations from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Check modification constraints from config
    - NO assumptions about allowed modifications
    - All monetary values as Decimal, NOT float
    """

    @staticmethod
    async def validate_modification(
        original_order: Order,
        modified_order: Order,
        exchange_config: ExchangeSpecificConfig,
    ) -> list[str]:
        """Validate order modification request.

        Args:
            original_order: Original order being modified
            modified_order: Modified order parameters
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of modification violations

        Note:
        - Check modification constraints from config
        - NO assumptions about allowed modifications
        """
        violations: list[str] = []

        # Check if modifications are allowed by exchange config
        if (
            exchange_config.allow_order_modifications is not None
            and not exchange_config.allow_order_modifications
        ):
            violations.append(f"Order modifications not allowed on {original_order.exchange}")
            return violations

        # Check modification-specific constraints
        violations.extend(
            OrderModificationValidator._check_price_change(
                original_order,
                modified_order,
                exchange_config,
            ),
        )

        violations.extend(
            OrderModificationValidator._check_quantity_change(
                original_order,
                modified_order,
                exchange_config,
            ),
        )

        return violations

    @staticmethod
    def _check_price_change(
        original_order: Order,
        modified_order: Order,
        exchange_config: ExchangeSpecificConfig,
    ) -> list[str]:
        """Check price change constraints.

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        if exchange_config.max_price_change_pct is not None:
            max_change = exchange_config.max_price_change_pct
            if original_order.price and modified_order.price:
                price_change = (
                    abs(modified_order.price - original_order.price) / original_order.price * 100
                )
                if price_change > max_change:
                    violations.append(
                        f"Price change {price_change:.2f}% exceeds maximum "
                        f"{max_change}% for modifications",
                    )

        return violations

    @staticmethod
    def _check_quantity_change(
        original_order: Order,
        modified_order: Order,
        exchange_config: ExchangeSpecificConfig,
    ) -> list[str]:
        """Check quantity change constraints.

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        if exchange_config.max_quantity_change_pct is not None:
            max_change = exchange_config.max_quantity_change_pct
            quantity_change = (
                abs(modified_order.quantity_requested - original_order.quantity_requested)
                / original_order.quantity_requested
                * 100
            )
            if quantity_change > max_change:
                violations.append(
                    f"Quantity change {quantity_change:.2f}% exceeds maximum "
                    f"{max_change}% for modifications",
                )

        return violations

    @staticmethod
    def validate_cancellation(order: Order, exchange_config: ExchangeSpecificConfig) -> list[str]:
        """Validate order cancellation request.

        Args:
            order: Order to cancel
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of cancellation violations

        Note:
        - Check cancellation constraints from config
        - NO assumptions about cancellation rules
        """
        violations: list[str] = []

        # Check if cancellations are allowed
        if (
            exchange_config.allow_order_cancellations is not None
            and not exchange_config.allow_order_cancellations
        ):
            violations.append(f"Order cancellations not allowed on {order.exchange}")

        # Check order status constraints
        if exchange_config.cancellable_statuses is not None:
            cancellable_statuses = exchange_config.cancellable_statuses
            if order.status and order.status.value not in cancellable_statuses:
                violations.append(
                    f"Order status {order.status.value} not cancellable, "
                    f"allowed: {cancellable_statuses}",
                )

        return violations
