"""Exchange-specific constraint validation using configuration.

This module validates orders against exchange-specific rules like min/max sizes,
tick sizes, and lot sizes using validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderType
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class ExchangeValidator:
    """Validates orders against exchange-specific constraints from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL validation rules from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about exchange behavior
    """

    @staticmethod
    def validate(order: Order, exchange_config: ExchangeSpecificConfig) -> list[str]:
        """Validate order against exchange-specific constraints.

        Args:
            order: Order to validate
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of exchange constraint violations

        Note:
        - Check min/max from config.exchanges[exchange].min_order_size
        - Check tick size from config.exchanges[exchange].tick_size
        - Check lot size from config.exchanges[exchange].lot_size
        - ALL constraints from configuration
        """
        violations: list[str] = []

        # Check order value constraints
        violations.extend(ExchangeValidator._check_order_value_constraints(order, exchange_config))

        # Check price alignment constraints
        violations.extend(ExchangeValidator._check_price_alignment(order, exchange_config))

        # Check quantity alignment constraints
        violations.extend(ExchangeValidator._check_quantity_alignment(order, exchange_config))

        # Check quantity limits
        violations.extend(ExchangeValidator._check_quantity_limits(order, exchange_config))

        return violations

    @staticmethod
    def _check_order_value_constraints(
        order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check order value against min/max constraints.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # For market orders without a price, we cannot calculate order value
        # These need special handling or should be validated differently
        if order.order_type == OrderType.MARKET and not order.price:
            # Market orders should be validated by quantity limits, not value
            # Skip value validation for market orders
            logger.debug(
                "skipping_value_validation_for_market_order",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                exchange=order.exchange.value,
            )
            return violations

        # For limit orders, price must be specified
        if order.order_type == OrderType.LIMIT and not order.price:
            violations.append("Limit order must have a price specified")
            return violations

        # Calculate order value only when we have a valid price
        if not order.price:
            violations.append(
                f"Cannot calculate order value without price for {order.order_type.value} order"
            )
            return violations

        order_value = order.quantity_requested * order.price

        # Check minimum order size from config
        if exchange_config.min_order_size is not None:
            min_size = Decimal(str(exchange_config.min_order_size))
            if order_value < min_size:
                violations.append(
                    f"Order value ${order_value} below minimum ${min_size} "
                    f"for {order.exchange.value}"
                )

        # Check maximum order size from config
        if exchange_config.max_order_size is not None:
            max_size = Decimal(str(exchange_config.max_order_size))
            if order_value > max_size:
                violations.append(
                    f"Order value ${order_value} exceeds maximum ${max_size} "
                    f"for {order.exchange.value}"
                )

        return violations

    @staticmethod
    def _check_price_alignment(order: Order, exchange_config: ExchangeSpecificConfig) -> list[str]:
        """Check price alignment against tick size.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # Market orders don't have price constraints
        if order.order_type == OrderType.MARKET:
            return violations

        # For limit orders, check tick size alignment
        if exchange_config.tick_size is not None and order.price:
            tick_size = Decimal(str(exchange_config.tick_size))
            if tick_size > 0:
                price_remainder = order.price % tick_size
                if price_remainder != 0:
                    violations.append(
                        f"Price {order.price} not aligned to tick size {tick_size} "
                        f"for {order.exchange}"
                    )

        return violations

    @staticmethod
    def _check_quantity_alignment(
        order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check quantity alignment against lot size.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.lot_size is not None:
            lot_size = Decimal(str(exchange_config.lot_size))
            if lot_size > 0:
                quantity_remainder = order.quantity_requested % lot_size
                if quantity_remainder != 0:
                    violations.append(
                        f"Quantity {order.quantity_requested} not aligned to lot size {lot_size} "
                        f"for {order.exchange}"
                    )

        return violations

    @staticmethod
    def _check_quantity_limits(order: Order, exchange_config: ExchangeSpecificConfig) -> list[str]:
        """Check quantity against min/max limits.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # Check minimum quantity from config
        if exchange_config.min_quantity is not None:
            min_quantity = exchange_config.min_quantity
            if order.quantity_requested < min_quantity:
                violations.append(
                    f"Quantity {order.quantity_requested} below minimum {min_quantity} "
                    f"for {order.exchange}"
                )

        # Check maximum quantity from config
        if exchange_config.max_quantity is not None:
            max_quantity = exchange_config.max_quantity
            if order.quantity_requested > max_quantity:
                violations.append(
                    f"Quantity {order.quantity_requested} exceeds maximum {max_quantity} "
                    f"for {order.exchange}"
                )

        return violations
