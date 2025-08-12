"""Business logic validation rules for balance and limits.

This module implements validation rules that check business constraints
like balance availability and position limits.

These rules consolidate business validation that was previously scattered
across PortfolioValidator and other validators.
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName, OrderSide, ValidationCategory
from cyberdelta.models.validation import ValidationResult
from cyberdelta.symbols import bp_symbol, hl_symbol


if TYPE_CHECKING:
    from cyberdelta.infrastructure.validation.validation_context import ValidationContext
    from cyberdelta.models.market.order import Order
    from cyberdelta.symbols.models import Symbol

logger = get_logger(__name__)


class BalanceValidationRule:
    """Validates order against available balance constraints.

    This rule ensures sufficient balance is available for the order,
    consolidating balance checks from PortfolioValidator.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO assumptions about asset naming conventions
    - Uses Symbol objects from cyberdelta.symbols
    - ALL balance checks from portfolio state
    - NO hardcoded currency assumptions
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize balance validation rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "balance_check"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - BALANCE checks for sufficient funds."""
        return ValidationCategory.BALANCE

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Balance checks may be bypassed for reduce-only orders."""
        return True  # Reduce-only orders close positions, don't require new funds

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order against available balance.

        Args:
            order: Order to validate
            context: Validation context with portfolio state

        Returns:
            ValidationResult with any violations

        Note:
            - Skipped if context indicates to skip balance checks
            - Handles buy/sell orders differently
            - Uses exchange-specific symbol resolution
        """
        violations: list[str] = []

        # Skip if context says to skip balance checks
        if context.should_skip_balance_checks():
            logger.debug(
                "balance_validation_skipped",
                order_id=order.exchange_order_id,
                reason="skip_balance_checks",
                is_reduce_only=context.is_reduce_only,
                is_reconciling=context.is_reconciling,
            )
            return ValidationResult(violations=[])

        # Skip if no portfolio state available
        if not context.has_portfolio_state():
            violations.append("Portfolio state unavailable for balance validation")
            return ValidationResult(violations=violations)

        # Validate based on order side
        if order.side == OrderSide.BUY:
            violations.extend(await self._validate_buy_balance(order, context))
        elif order.side == OrderSide.SELL:
            violations.extend(await self._validate_sell_balance(order, context))

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )

    async def _validate_buy_balance(self, order: Order, context: ValidationContext) -> list[str]:
        """Validate balance for buy order.

        Args:
            order: Buy order to validate
            context: Validation context

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        # Get quote asset symbol (what we need to spend)
        quote_symbol = self._get_quote_symbol(order)

        # Get balance from portfolio state
        portfolio_state = context.portfolio_state
        if portfolio_state is None:
            violations.append("Portfolio state required for balance validation")
            return violations

        # Create balance key using same format as portfolio state manager
        balance_key = f"{order.exchange.value}:{quote_symbol.value}"
        balance = portfolio_state.balances.get(balance_key)

        # Check if order has price (required for buy orders)
        if order.price is None:
            violations.append("Cannot validate buy order without price")
            return violations

        required_amount = order.quantity_requested * order.price

        if not balance:
            violations.append(
                f"No {quote_symbol.value} balance found on {order.exchange.value} for buy order"
            )
        elif balance.available_quantity < required_amount:
            violations.append(
                f"Insufficient {quote_symbol.value} balance: need {required_amount}, "
                f"available {balance.available_quantity}"
            )

        return violations

    async def _validate_sell_balance(self, order: Order, context: ValidationContext) -> list[str]:
        """Validate balance for sell order.

        Args:
            order: Sell order to validate
            context: Validation context

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        # Get base asset symbol (what we're selling)
        base_symbol = self._get_base_symbol(order)

        # Get balance from portfolio state
        portfolio_state = context.portfolio_state
        if portfolio_state is None:
            violations.append("Portfolio state required for balance validation")
            return violations

        # Create balance key using same format as portfolio state manager
        balance_key = f"{order.exchange.value}:{base_symbol.value}"
        balance = portfolio_state.balances.get(balance_key)

        if not balance:
            violations.append(
                f"No {base_symbol.value} balance found on {order.exchange.value} for sell order"
            )
        elif balance.available_quantity < order.quantity_requested:
            violations.append(
                f"Insufficient {base_symbol.value} balance: need {order.quantity_requested}, "
                f"available {balance.available_quantity}"
            )

        return violations

    def _get_quote_symbol(self, order: Order) -> Symbol:
        """Get quote asset symbol from order.

        Args:
            order: Order to extract quote symbol from

        Returns:
            Quote asset Symbol object

        Raises:
            ValueError: If symbol has no quote asset

        Note:
            - Uses symbol's built-in quote_asset property
            - Creates new symbol using exchange-specific factory
        """
        # Get quote asset from symbol's components
        quote_asset_str = order.symbol.quote_asset
        if quote_asset_str is None:
            # Symbol doesn't have a quote asset (shouldn't happen for trading pairs)
            msg = f"Symbol {order.symbol.value} on {order.exchange.value} has no quote asset"
            raise ValueError(msg)

        # Use exchange-specific symbol factory
        if order.exchange == ExchangeName.HYPERLIQUID:
            return hl_symbol(quote_asset_str)
        # Currently only two exchanges - BACKPACK is the only other option
        return bp_symbol(quote_asset_str)

    def _get_base_symbol(self, order: Order) -> Symbol:
        """Get base asset symbol from order.

        Args:
            order: Order to extract base symbol from

        Returns:
            Base asset Symbol object

        Note:
            - Uses symbol's built-in base_asset property
            - Creates new symbol using exchange-specific factory
        """
        # Get base asset from symbol's components
        base_asset_str = order.symbol.base_asset

        # Use exchange-specific symbol factory
        if order.exchange == ExchangeName.HYPERLIQUID:
            return hl_symbol(base_asset_str)
        # Currently only two exchanges - BACKPACK is the only other option
        return bp_symbol(base_asset_str)


class OrderValueLimitsRule:
    """Validates order value against configured min/max limits.

    This rule consolidates order value validation that was previously
    done in both RiskValidator and ExchangeValidator with different limits.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Min/max values from configuration
    - NO hardcoded limits
    - Uses exchange-specific and global limits
    """

    def __init__(self, enabled: bool = True) -> None:
        """Initialize order value limits rule.

        Args:
            enabled: Whether this rule is enabled
        """
        self._enabled = enabled

    @property
    def name(self) -> str:
        """Rule name for identification."""
        return "order_value_limits"

    @property
    def category(self) -> ValidationCategory:
        """Rule category - LIMITS for min/max value checks."""
        return ValidationCategory.LIMITS

    @property
    def enabled(self) -> bool:
        """Whether rule is enabled."""
        return self._enabled

    @property
    def bypass_on_reduce_only(self) -> bool:
        """Value limits always apply, even for reduce-only orders."""
        return False

    async def validate(self, order: Order, context: ValidationContext) -> ValidationResult:
        """Validate order value against configured limits.

        Args:
            order: Order to validate
            context: Validation context with configuration

        Returns:
            ValidationResult with any violations

        Note:
            - Checks both global and exchange-specific limits
            - Requires price for value calculation
        """
        violations: list[str] = []

        # Skip if no price to calculate value
        if order.price is None:
            # This is not a violation - market orders don't have prices
            logger.debug(
                "order_value_limits_skipped_no_price",
                order_id=order.exchange_order_id,
                order_type=order.order_type.value,
            )
            return ValidationResult(violations=[])

        order_value = order.quantity_requested * order.price

        # Check global limits
        violations.extend(self._check_global_limits(order_value, context))

        # Check exchange-specific limits
        violations.extend(self._check_exchange_limits(order_value, order, context))

        return ValidationResult(
            violations=violations,
            category=self.category,
            rule_name=self.name,
        )

    def _check_global_limits(self, order_value: Decimal, context: ValidationContext) -> list[str]:
        """Check global validation limits.

        Args:
            order_value: Calculated order value
            context: Validation context with configuration

        Returns:
            List of violations if any
        """
        violations: list[str] = []
        validation_config = context.config.validation

        # Global minimum trade value
        if hasattr(validation_config, "min_trade_value"):
            min_value = validation_config.min_trade_value
            if order_value < min_value:
                violations.append(f"Order value ${order_value} below global minimum ${min_value}")

        # Global maximum trade value
        if hasattr(validation_config, "max_trade_value"):
            max_value = validation_config.max_trade_value
            if order_value > max_value:
                violations.append(f"Order value ${order_value} exceeds global maximum ${max_value}")

        return violations

    def _check_exchange_limits(
        self, order_value: Decimal, order: Order, context: ValidationContext
    ) -> list[str]:
        """Check exchange-specific limits.

        Args:
            order_value: Calculated order value
            order: Order being validated
            context: Validation context with configuration

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        if not context.has_exchange_config() or context.exchange_config is None:
            return violations

        exchange_config = context.exchange_config

        # Exchange minimum order size
        if (
            hasattr(exchange_config, "min_order_size")
            and exchange_config.min_order_size is not None
        ):
            min_size = Decimal(str(exchange_config.min_order_size))
            if order_value < min_size:
                violations.append(
                    f"Order value ${order_value} below exchange minimum "
                    f"${min_size} for {order.exchange.value}"
                )

        # Exchange maximum order size
        if (
            hasattr(exchange_config, "max_order_size")
            and exchange_config.max_order_size is not None
        ):
            max_size = Decimal(str(exchange_config.max_order_size))
            if order_value > max_size:
                violations.append(
                    f"Order value ${order_value} exceeds exchange maximum "
                    f"${max_size} for {order.exchange.value}"
                )

        return violations
