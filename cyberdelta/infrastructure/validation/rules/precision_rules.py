"""Precision validation rules for price and quantity alignment.

This module implements validation rules that check order price and quantity
against exchange-specific precision requirements (tick size, lot size).

These rules consolidate precision validation that was previously scattered
across ExchangeValidator and other validators.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderType, ValidationCategory
from cyberdelta.models.validation import ValidationResult


if TYPE_CHECKING:
    from cyberdelta.infrastructure.validation.validation_context import ValidationContext
    from cyberdelta.models.market.order import Order

logger = get_logger(__name__)


class PricePrecisionRule:
    """Validates order price against tick size requirements.

    This rule ensures that order prices are aligned to the exchange's
    tick size, which is critical for order matching engines.

    Consolidates tick size validation from ExchangeValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Tick size from config.exchanges[exchange].tick_size
    - NO hardcoded precision values
    - Uses Decimal for all calculations
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize price precision rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "price_precision"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - PRECISION checks run first."""
        return ValidationCategory.PRECISION

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Price precision is always checked, even for reduce-only."""
        return False

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order price precision against tick size.

        Args:
            order: Order to validate
            context: Validation context with configuration

        Returns:
            ValidationResult with any violations

        Note:
            - Market orders without price are skipped
            - Limit orders must have price aligned to tick size
            - Inspired by Nautilus pattern of strict precision validation
        """
        violations: list[str] = []

        # Market orders don't have price constraints
        if order.order_type == OrderType.MARKET:
            logger.debug(
                "price_precision_skipped_market_order",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
            )
            return ValidationResult(violations=[])

        # Limit orders must have a price
        if order.order_type == OrderType.LIMIT and order.price is None:
            violations.append("Limit order must have a price specified")
            return ValidationResult(violations=violations)

        # Skip if no price to validate
        if order.price is None:
            return ValidationResult(violations=[])

        # Check positive price (Nautilus pattern)
        if order.price <= 0:
            violations.append(
                f"Order denied: Price must be positive for {order.symbol.value}, got {order.price}"
            )

        # Check tick size alignment if configured
        tick_size = context.get_tick_size()
        if tick_size is not None and tick_size > 0:
            tick_decimal = Decimal(str(tick_size))

            if not self._is_precision_valid(order.price, tick_decimal):
                violations.append(
                    f"Order denied: Price {order.price} not aligned to tick size {tick_size} "
                    f"for {order.exchange.value}"
                )

                # Suggest aligned price for user feedback
                aligned_price = self._align_to_tick(order.price, tick_decimal)
                logger.debug(
                    "price_precision_violation",
                    order_id=order.exchange_order_id,
                    original_price=order.price,
                    tick_size=tick_size,
                    suggested_price=aligned_price,
                )

        return ValidationResult(violations=violations)

    def _is_precision_valid(self, price: Decimal, tick_size: Decimal) -> bool:
        """Check if price aligns with tick size.

        Args:
            price: Price to check
            tick_size: Required tick size

        Returns:
            True if price is properly aligned
        """
        return (price % tick_size) == Decimal(0)

    def _align_to_tick(self, price: Decimal, tick_size: Decimal) -> Decimal:
        """Align price to nearest valid tick.

        Args:
            price: Original price
            tick_size: Tick size to align to

        Returns:
            Price aligned to tick size
        """
        return (price // tick_size) * tick_size


class QuantityPrecisionRule:
    """Validates order quantity against lot size requirements.

    This rule ensures that order quantities are aligned to the exchange's
    lot size (minimum tradeable unit).

    Consolidates lot size validation from ExchangeValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Lot size from config.exchanges[exchange].lot_size
    - NO hardcoded precision values
    - Uses Decimal for all calculations
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize quantity precision rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "quantity_precision"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - PRECISION checks run first."""
        return ValidationCategory.PRECISION

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Quantity precision is always checked, even for reduce-only."""
        return False

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order quantity precision against lot size.

        Args:
            order: Order to validate
            context: Validation context with configuration

        Returns:
            ValidationResult with any violations

        Note:
            - All order types must respect lot size
            - Quantity must be positive
            - Inspired by Nautilus pattern of strict precision validation
        """
        violations: list[str] = []

        # Check positive quantity (fundamental check)
        if order.quantity_requested <= 0:
            violations.append(
                f"Order denied: Quantity must be positive for {order.symbol.value}, "
                f"got {order.quantity_requested}"
            )

        # Check lot size alignment if configured
        lot_size = context.get_lot_size()
        if lot_size is not None and lot_size > 0:
            lot_decimal = Decimal(str(lot_size))

            if not self._is_precision_valid(order.quantity_requested, lot_decimal):
                violations.append(
                    f"Order denied: Quantity {order.quantity_requested} not aligned to "
                    f"lot size {lot_size} for {order.exchange.value}"
                )

                # Suggest aligned quantity for user feedback
                aligned_quantity = self._align_to_lot(order.quantity_requested, lot_decimal)
                logger.debug(
                    "quantity_precision_violation",
                    order_id=order.exchange_order_id,
                    original_quantity=order.quantity_requested,
                    lot_size=lot_size,
                    suggested_quantity=aligned_quantity,
                )

        return ValidationResult(violations=violations)

    def _is_precision_valid(self, quantity: Decimal, lot_size: Decimal) -> bool:
        """Check if quantity aligns with lot size.

        Args:
            quantity: Quantity to check
            lot_size: Required lot size

        Returns:
            True if quantity is properly aligned
        """
        return (quantity % lot_size) == Decimal(0)

    def _align_to_lot(self, quantity: Decimal, lot_size: Decimal) -> Decimal:
        """Align quantity to nearest valid lot.

        Args:
            quantity: Original quantity
            lot_size: Lot size to align to

        Returns:
            Quantity aligned to lot size (rounded down)
        """
        return (quantity // lot_size) * lot_size
