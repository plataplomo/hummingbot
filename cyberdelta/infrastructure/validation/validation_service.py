"""Unified validation service that consolidates all order validation logic.

This service replaces the existing scattered validation approach with a single,
extensible framework that can handle all types of order validation.

Inspired by Nautilus Trader's pre-trade risk management patterns, this service
provides a centralized entry point for all validation with clear categorization
and execution order.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import TradingState, ValidationCategory

# Import all validation rule implementations
from cyberdelta.infrastructure.validation.rules.business_rules import (
    BalanceValidationRule,
    OrderValueLimitsRule,
)
from cyberdelta.infrastructure.validation.rules.market_rules import (
    LiquidityRule,
    MarketStatusRule,
)
from cyberdelta.infrastructure.validation.rules.precision_rules import (
    PricePrecisionRule,
    QuantityPrecisionRule,
)
from cyberdelta.infrastructure.validation.rules.risk_rules import (
    MaxExposureRule,
    MaxPositionRule,
)
from cyberdelta.infrastructure.validation.validation_context import ValidationContext
from cyberdelta.infrastructure.validation.validation_registry import ValidationRegistry
from cyberdelta.models.validation import OrderDenied, ValidationResult


if TYPE_CHECKING:
    from cyberdelta.config.models import AppSettings
    from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
    from cyberdelta.models.market.market_snapshot import MarketSnapshot
    from cyberdelta.models.market.order import Order
    from cyberdelta.models.portfolio.state import PortfolioState

logger = get_logger(__name__)


class ValidationService:
    """Centralized validation service for all order validation logic.

    This service consolidates validation from multiple existing validators:
    - OrderValidator -> PricePrecisionRule, QuantityPrecisionRule
    - RiskValidator -> MaxPositionRule, MaxExposureRule
    - PortfolioValidator -> BalanceValidationRule
    - ExchangeValidator -> OrderValueLimitsRule (precision rules)
    - MarketValidator -> MarketStatusRule, LiquidityRule

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL validation parameters from AppSettings
    - NO hardcoded values or assumptions
    - Uses typed models throughout
    - Fail-fast approach for critical violations
    - Comprehensive logging for debugging and monitoring

    Execution Order (by ValidationCategory):
    1. PRECISION - Price/quantity alignment (fail fast)
    2. LIMITS - Min/max value constraints
    3. BALANCE - Sufficient funds available
    4. RISK - Position and exposure limits
    5. MARKET - Market status and liquidity
    6. STATE - System state and trading hours
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize validation service.

        Args:
            config: Application configuration containing all validation parameters

        Note:
            - Registers all validation rules during initialization
            - Rules are organized by category for ordered execution
            - Configuration is passed to context for each validation
        """
        self.config = config
        self.registry = ValidationRegistry()
        self._register_validation_rules()

        logger.info(
            "validation_service_initialized",
            total_rules=len(self.registry.get_rules()),
            categories=[cat.value for cat in ValidationCategory],
        )

    def _register_validation_rules(self) -> None:
        """Register all validation rules with the registry.

        Rules are registered by category and will be executed in the order
        defined by ValidationCategory enum.

        Note:
            - Each rule can be enabled/disabled via configuration
            - Rules include bypass logic for reduce-only orders
            - Registry maintains rules by category for efficient execution
        """
        # PRECISION rules (executed first for fail-fast)
        self.registry.register(PricePrecisionRule(enabled=True))
        self.registry.register(QuantityPrecisionRule(enabled=True))

        # LIMITS rules (basic constraints)
        self.registry.register(OrderValueLimitsRule(enabled=True))

        # BALANCE rules (fund availability)
        self.registry.register(BalanceValidationRule(enabled=True))

        # RISK rules (position and exposure management)
        self.registry.register(MaxPositionRule(enabled=True))
        self.registry.register(MaxExposureRule(enabled=True))

        # MARKET rules (market conditions)
        self.registry.register(MarketStatusRule(enabled=True))
        # TradingHoursRule removed - crypto markets are 24/7
        self.registry.register(LiquidityRule(enabled=True))

        # STATE rules would be added here if needed
        # (currently handled via TradingState in context)

        logger.debug(
            "validation_rules_registered",
            precision_rules=len(self.registry.get_rules(ValidationCategory.PRECISION)),
            limits_rules=len(self.registry.get_rules(ValidationCategory.LIMITS)),
            balance_rules=len(self.registry.get_rules(ValidationCategory.BALANCE)),
            risk_rules=len(self.registry.get_rules(ValidationCategory.RISK)),
            market_rules=len(self.registry.get_rules(ValidationCategory.MARKET)),
        )

    async def validate_order(
        self,
        order: Order,
        portfolio_state: PortfolioState | None = None,
        market_snapshot: MarketSnapshot | None = None,
        trading_state: TradingState = TradingState.ACTIVE,
        is_reconciling: bool = False,
        is_reduce_only: bool = False,
    ) -> ValidationResult:
        """Validate an order against all registered validation rules.

        Args:
            order: Order to validate
            portfolio_state: Current portfolio state for balance/position validation
            market_snapshot: Current market data for price/liquidity validation
            trading_state: Current system trading state
            is_reconciling: Whether system is in reconciliation mode
            is_reduce_only: Whether order is reduce-only (closing positions)

        Returns:
            ValidationResult containing all violations found across all rules

        Note:
            - Executes rules in category order (PRECISION first, STATE last)
            - Stops on first critical violation if configured
            - Returns comprehensive result with all violations
            - Logs detailed information for monitoring and debugging
        """
        # Order validation (removed unreachable None check as Order is typed as non-optional)

        start_time = datetime.now(UTC)

        # Build validation context
        context = self._build_context(
            portfolio_state=portfolio_state,
            market_snapshot=market_snapshot,
            trading_state=trading_state,
            is_reconciling=is_reconciling,
            is_reduce_only=is_reduce_only,
            order_exchange=order.exchange,
        )

        logger.info(
            "order_validation_started",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            side=order.side.value,
            quantity=order.quantity_requested,
            price=order.price,
            trading_state=trading_state.value,
            is_reduce_only=is_reduce_only,
            is_reconciling=is_reconciling,
        )

        # Execute validation rules by category
        all_violations: list[str] = []
        category_results: dict[str, ValidationResult] = {}

        # Execute each category in order
        for category in ValidationCategory:
            category_result = await self._run_category(order, context, category)
            category_results[category.value] = category_result

            if category_result.violations:
                all_violations.extend(category_result.violations)

                # Stop on critical precision errors (fail-fast)
                if category == ValidationCategory.PRECISION and category_result.violations:
                    logger.warning(
                        "validation_failed_precision_critical",
                        order_id=order.exchange_order_id,
                        violations=category_result.violations,
                        msg="Stopping validation due to critical precision errors",
                    )
                    break

        # Create final result
        final_result = ValidationResult(
            violations=all_violations,
            validation_type="PRE_TRADE_RISK_CHECK",
            timestamp=start_time,
        )

        # Log validation completion
        validation_duration_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000

        if final_result.is_valid:
            logger.info(
                "order_validation_passed",
                order_id=order.exchange_order_id,
                duration_ms=validation_duration_ms,
                categories_checked=len(category_results),
            )
        else:
            logger.warning(
                "order_validation_failed",
                order_id=order.exchange_order_id,
                duration_ms=validation_duration_ms,
                total_violations=len(all_violations),
                violations=all_violations,
                category_breakdown={
                    cat: len(result.violations)
                    for cat, result in category_results.items()
                    if result.violations
                },
            )

        return final_result

    async def validate_order_cancellation(
        self,
        order: Order,
        portfolio_state: PortfolioState | None = None,
        market_snapshot: MarketSnapshot | None = None,
        trading_state: TradingState = TradingState.ACTIVE,
        is_reconciling: bool = False,
    ) -> ValidationResult:
        """Validate order cancellation request.

        Args:
            order: Order to cancel
            portfolio_state: Current portfolio state
            market_snapshot: Current market data
            trading_state: Current system trading state
            is_reconciling: Whether system is in reconciliation mode

        Returns:
            ValidationResult indicating if cancellation is allowed

        Note:
            - Simpler validation than order placement
            - Mainly checks order state and system conditions
            - Most validation rules don't apply to cancellation
        """
        start_time = datetime.now(UTC)
        violations: list[str] = []

        # Basic order state checks
        if order.status in {OrderStatus.FILLED, OrderStatus.CANCELED}:
            violations.append(f"Cannot cancel order in {order.status.value} state")

        if order.exchange_order_id is None:
            violations.append("Cannot cancel order without exchange order ID")

        # System state checks
        if trading_state == TradingState.HALTED:
            violations.append("Order cancellation not allowed while trading is halted")

        # Log cancellation validation
        logger.debug(
            "order_cancellation_validation",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            status=order.status.value,
            trading_state=trading_state.value,
            violations_count=len(violations),
        )

        return ValidationResult(
            violations=violations,
            validation_type="ORDER_CANCELLATION_CHECK",
            timestamp=start_time,
        )

    def _build_context(
        self,
        portfolio_state: PortfolioState | None,
        market_snapshot: MarketSnapshot | None,
        trading_state: TradingState,
        is_reconciling: bool,
        is_reduce_only: bool,
        order_exchange: str,
    ) -> ValidationContext:
        """Build validation context for rule execution.

        Args:
            portfolio_state: Portfolio state for balance/position validation
            market_snapshot: Market data for price/liquidity validation
            trading_state: Current system trading state
            is_reconciling: Whether system is in reconciliation mode
            is_reduce_only: Whether order is reduce-only
            order_exchange: Exchange name for configuration lookup

        Returns:
            ValidationContext with all necessary data for validation rules

        Note:
            - Looks up exchange-specific configuration
            - Creates timezone-aware timestamp
            - Validates context consistency in __post_init__
        """
        # Get exchange-specific configuration
        exchange_config: ExchangeSpecificConfig | None = None
        if hasattr(self.config, "exchanges") and order_exchange in self.config.exchanges:
            exchange_config = self.config.exchanges[order_exchange]

        return ValidationContext(
            config=self.config,
            exchange_config=exchange_config,
            market_snapshot=market_snapshot,
            portfolio_state=portfolio_state,
            trading_state=trading_state,
            timestamp=datetime.now(UTC),
            is_reconciling=is_reconciling,
            is_reduce_only=is_reduce_only,
        )

    async def _run_category(
        self, order: Order, context: ValidationContext, category: ValidationCategory
    ) -> ValidationResult:
        """Execute all validation rules for a specific category.

        Args:
            order: Order to validate
            context: Validation context
            category: Category of rules to execute

        Returns:
            ValidationResult with violations from all rules in the category

        Note:
            - Only executes enabled rules
            - Handles rule exceptions gracefully
            - Applies bypass logic for reduce-only orders
        """
        category_violations: list[str] = []
        rules = self.registry.get_enabled_rules(category)

        if not rules:
            logger.debug(
                "validation_category_no_rules",
                category=category.value,
                order_id=order.exchange_order_id,
            )
            return ValidationResult(violations=[])

        logger.debug(
            "validation_category_started",
            category=category.value,
            rule_count=len(rules),
            order_id=order.exchange_order_id,
        )

        # Execute each rule in the category
        for rule in rules:
            try:
                # Check if rule should be bypassed for reduce-only orders
                if context.is_reduce_only and rule.bypass_on_reduce_only:
                    logger.debug(
                        "validation_rule_bypassed_reduce_only",
                        rule_name=rule.name,
                        category=category.value,
                        order_id=order.exchange_order_id,
                    )
                    continue

                # Execute the rule
                rule_result = await rule.validate(order, context)

                if rule_result.violations:
                    category_violations.extend(rule_result.violations)
                    logger.debug(
                        "validation_rule_violations",
                        rule_name=rule.name,
                        category=category.value,
                        violations=rule_result.violations,
                        order_id=order.exchange_order_id,
                    )

            except Exception as e:
                # Rule execution failed - treat as violation
                violation_msg = f"Validation rule {rule.name} failed: {e!s}"
                category_violations.append(violation_msg)
                logger.exception(
                    "validation_rule_error",
                    rule_name=rule.name,
                    category=category.value,
                    error=str(e),
                    order_id=order.exchange_order_id,
                    msg="Rule execution failed",
                )

        return ValidationResult(
            violations=category_violations,
            category=category,
            validation_type=f"{category.value.upper()}_CHECK",
        )

    async def create_order_denied_event(
        self, order: Order, validation_result: ValidationResult, context: ValidationContext
    ) -> OrderDenied:
        """Create an OrderDenied event from validation failure.

        Args:
            order: Order that was denied
            validation_result: Result containing violations
            context: Validation context for additional details

        Returns:
            OrderDenied event for downstream processing

        Note:
            - Inspired by Nautilus Trader's OrderDenied event pattern
            - Contains human-readable reason and debugging details
            - Can be used for event sourcing and audit trails
        """
        # Create comprehensive reason from all violations
        reason = "; ".join(validation_result.violations)

        # Determine primary category for the denial
        primary_category = validation_result.category or ValidationCategory.STATE

        return OrderDenied(
            order_id=order.exchange_order_id or f"temp_{order.symbol.value}",
            reason=reason,
            validation_category=primary_category,
            timestamp=validation_result.timestamp,
            details={
                "symbol": order.symbol.value,
                "side": order.side.value,
                "quantity": str(order.quantity_requested),
                "price": str(order.price) if order.price else "market",
                "exchange": order.exchange.value,
                "trading_state": context.trading_state.value,
                "is_reduce_only": context.is_reduce_only,
                "is_reconciling": context.is_reconciling,
                "violation_count": len(validation_result.violations),
                "validation_type": validation_result.validation_type,
            },
            trading_state=context.trading_state.value,
        )
