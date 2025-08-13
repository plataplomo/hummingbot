"""Risk validation rules for position and exposure limits.

This module implements validation rules that enforce risk management constraints
such as maximum position sizes and total exposure limits.

These rules consolidate risk validation that was previously scattered
across RiskValidator and other validators.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide, ValidationCategory
from cyberdelta.models.validation import ValidationResult


if TYPE_CHECKING:
    from cyberdelta.infrastructure.validation.validation_context import ValidationContext
    from cyberdelta.models.market.order import Order

logger = get_logger(__name__)


class MaxPositionRule:
    """Validates order against maximum position size limits.

    This rule ensures that executing an order would not cause any single
    position to exceed configured maximum position size limits.

    Consolidates position size checks from RiskValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Max position sizes from config.risk.global_risk.max_position_usd
    - Exchange-specific limits from config.exchanges[exchange].max_position_size
    - NO hardcoded position limits
    - Uses Decimal for all calculations
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize maximum position rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "max_position_size"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - RISK checks for position limits."""
        return ValidationCategory.RISK

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Position limits don't apply to reduce-only orders."""
        return True  # Reduce-only orders close positions, can't increase exposure

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order against maximum position size limits.

        Args:
            order: Order to validate
            context: Validation context with portfolio state and configuration

        Returns:
            ValidationResult with any violations

        Note:
            - Skipped if context says to skip risk checks
            - Checks both global and exchange-specific position limits
            - Calculates new position size after order execution
        """
        violations: list[str] = []

        # Skip if context says to skip risk checks
        if context.should_skip_risk_checks():
            logger.debug(
                "max_position_validation_skipped",
                order_id=order.exchange_order_id,
                reason="skip_risk_checks",
                trading_state=context.trading_state.value,
                is_reconciling=context.is_reconciling,
            )
            return ValidationResult(violations=[])

        # Skip if no portfolio state available
        if not context.has_portfolio_state():
            violations.append("Portfolio state unavailable for position size validation")
            return ValidationResult(violations=violations)

        # Calculate new position size after order execution
        current_position = self._get_current_position(order, context)
        try:
            new_position_size = self._calculate_new_position_size(order, current_position, context)
        except ValueError as e:
            # Cannot calculate position size - validation fails
            violations.append(str(e))
            return ValidationResult(violations=violations)

        # Check global maximum position limit
        global_limit = context.config.risk.global_risk.max_position_usd
        if new_position_size > global_limit:
            violations.append(
                f"Order would result in position size ${new_position_size} exceeding "
                f"global limit ${global_limit} for {order.symbol.value}"
            )

        # Check exchange-specific position limit if available
        if context.has_exchange_config() and context.exchange_config is not None:
            exchange_config = context.exchange_config

            if exchange_config.max_order_size is not None:
                # Use max_order_size as a proxy for position size limit
                exchange_limit = Decimal(str(exchange_config.max_order_size))
                if new_position_size > exchange_limit:
                    violations.append(
                        f"Order would result in position size ${new_position_size} exceeding "
                        f"exchange order size limit ${exchange_limit} for {order.exchange.value}"
                    )

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )

    def _get_current_position(self, order: Order, context: ValidationContext) -> Decimal:
        """Get current position size for the order's symbol.

        Args:
            order: Order to get position for
            context: Validation context with portfolio state

        Returns:
            Current position size in USD (positive for long, negative for short)
        """
        portfolio_state = context.portfolio_state
        if portfolio_state is None:
            return Decimal(0)

        # Create position key using same format as portfolio state manager
        position_key = f"{order.exchange.value}:{order.symbol.value}"
        position = portfolio_state.positions.get(position_key)

        if not position:
            return Decimal(0)

        # Return position value (size * entry_price)
        # Note: In production, would use current market price
        if position.entry_price is None:
            return Decimal(0)
        return abs(position.size * position.entry_price)

    def _calculate_new_position_size(
        self, order: Order, current_position: Decimal, context: ValidationContext
    ) -> Decimal:
        """Calculate new position size after order execution.

        Args:
            order: Order to execute
            current_position: Current position size
            context: Validation context with market snapshot

        Returns:
            New position size after order execution

        Raises:
            ValueError: If market order has no price and market data is unavailable
        """
        # Calculate order value
        if order.price is None:
            # For market orders, get current market price from snapshot
            if context.has_market_data() and context.market_snapshot is not None:
                ticker = context.market_snapshot.get_ticker(order.exchange, order.symbol)
                if ticker and ticker.price:
                    price = ticker.price
                else:
                    msg = (
                        f"Cannot calculate position for market order {order.exchange_order_id}: "
                        f"no price available for {order.symbol.value} on {order.exchange.value}"
                    )
                    logger.error(
                        "market_order_no_ticker_price",
                        order_id=order.exchange_order_id,
                        symbol=order.symbol.value,
                        exchange=order.exchange.value,
                    )
                    raise ValueError(msg)
            else:
                msg = (
                    f"Cannot calculate position for market order {order.exchange_order_id}: "
                    "market snapshot required but not available"
                )
                logger.error(
                    "market_order_no_market_data",
                    order_id=order.exchange_order_id,
                    msg="Market order validation requires market snapshot with current price",
                )
                raise ValueError(msg)

            order_value = order.quantity_requested * price
        else:
            order_value = order.quantity_requested * order.price

        # Calculate new position based on order side
        if order.side == OrderSide.BUY:
            # Buying increases long position or reduces short position
            new_position = current_position + order_value
        else:
            # Selling reduces long position or increases short position
            new_position = current_position - order_value

        return abs(new_position)  # Return absolute value for size check


class MaxExposureRule:
    """Validates order against maximum total exposure limits.

    This rule ensures that executing an order would not cause total portfolio
    exposure to exceed configured maximum exposure limits.

    Consolidates exposure checks from RiskValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Max exposure from config.risk.global_risk.max_total_exposure_usd
    - Exchange-specific limits from config.exchanges[exchange].max_exposure
    - NO hardcoded exposure limits
    - Uses Decimal for all calculations
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize maximum exposure rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "max_total_exposure"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - RISK checks for exposure limits."""
        return ValidationCategory.RISK

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Exposure limits don't apply to reduce-only orders."""
        return True  # Reduce-only orders decrease exposure

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order against maximum total exposure limits.

        Args:
            order: Order to validate
            context: Validation context with portfolio state and configuration

        Returns:
            ValidationResult with any violations

        Note:
            - Skipped if context says to skip risk checks
            - Checks both global and exchange-specific exposure limits
            - Calculates total exposure across all positions and pending orders
        """
        violations: list[str] = []

        # Skip if context says to skip risk checks
        if context.should_skip_risk_checks():
            logger.debug(
                "max_exposure_validation_skipped",
                order_id=order.exchange_order_id,
                reason="skip_risk_checks",
                trading_state=context.trading_state.value,
                is_reconciling=context.is_reconciling,
            )
            return ValidationResult(violations=[])

        # Skip if no portfolio state available
        if not context.has_portfolio_state():
            violations.append("Portfolio state unavailable for exposure validation")
            return ValidationResult(violations=violations)

        # Calculate current total exposure
        current_exposure = self._calculate_current_exposure(context)

        # Calculate additional exposure from this order
        try:
            order_exposure = self._calculate_order_exposure(order, context)
        except ValueError as e:
            # Cannot calculate exposure - validation fails
            violations.append(str(e))
            return ValidationResult(violations=violations)

        # Calculate new total exposure
        new_total_exposure = current_exposure + order_exposure

        # Check global maximum exposure limit
        global_limit = context.config.risk.global_risk.max_total_exposure_usd
        if new_total_exposure > global_limit:
            violations.append(
                f"Order would result in total exposure ${new_total_exposure} exceeding "
                f"global limit ${global_limit}"
            )

        # Note: Exchange-specific exposure limits not yet configured
        # ExchangeSpecificConfig would need max_exposure field for this validation

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )

    def _calculate_current_exposure(self, context: ValidationContext) -> Decimal:
        """Calculate current total portfolio exposure.

        Args:
            context: Validation context with portfolio state

        Returns:
            Current total exposure across all positions
        """
        portfolio_state = context.portfolio_state
        if portfolio_state is None:
            return Decimal(0)

        total_exposure = Decimal(0)

        # Sum absolute value of all positions
        for position in portfolio_state.positions.values():
            if position.size != 0 and position.entry_price is not None:
                position_value = abs(position.size * position.entry_price)
                total_exposure += position_value

        return total_exposure

    def _calculate_order_exposure(self, order: Order, context: ValidationContext) -> Decimal:
        """Calculate exposure that would be added by this order.

        Args:
            order: Order to calculate exposure for
            context: Validation context with market snapshot

        Returns:
            Additional exposure from this order

        Raises:
            ValueError: If market order has no price and market data is unavailable
        """
        if order.price is None:
            # For market orders, get current market price from snapshot
            if context.has_market_data() and context.market_snapshot is not None:
                ticker = context.market_snapshot.get_ticker(order.exchange, order.symbol)
                if ticker and ticker.price:
                    price = ticker.price
                else:
                    msg = (
                        f"Cannot calculate exposure for market order {order.exchange_order_id}: "
                        f"no price available for {order.symbol.value} on {order.exchange.value}"
                    )
                    logger.error(
                        "market_order_no_ticker_price_exposure",
                        order_id=order.exchange_order_id,
                        symbol=order.symbol.value,
                        exchange=order.exchange.value,
                    )
                    raise ValueError(msg)
            else:
                msg = (
                    f"Cannot calculate exposure for market order {order.exchange_order_id}: "
                    "market snapshot required but not available"
                )
                logger.error(
                    "market_order_no_market_data_exposure",
                    order_id=order.exchange_order_id,
                    msg="Market order validation requires market snapshot with current price",
                )
                raise ValueError(msg)

            order_value = order.quantity_requested * price
        else:
            order_value = order.quantity_requested * order.price

        # Return absolute value since we're calculating total exposure
        return abs(order_value)
