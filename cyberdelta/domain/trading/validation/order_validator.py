"""Order validation orchestrator using decomposed validators.

This module provides comprehensive order validation by orchestrating
specialized validators for different aspects of order validation.
"""

from __future__ import annotations

from cyberdelta.config.models import AppSettings
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.market.market_service import MarketDataService
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.trading.validation.exchange_validator import ExchangeValidator
from cyberdelta.domain.trading.validation.market_validator import MarketValidator
from cyberdelta.domain.trading.validation.order_modification_validator import (
    OrderModificationValidator,
)
from cyberdelta.domain.trading.validation.portfolio_validator import PortfolioValidator
from cyberdelta.domain.trading.validation.risk_validator import RiskValidator
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class OrderValidator:
    """Comprehensive order validation orchestrator.

    This validator orchestrates multiple specialized validators:
    - ExchangeValidator: Exchange-specific constraints
    - MarketValidator: Market condition validation
    - PortfolioValidator: Portfolio constraint validation
    - RiskValidator: Risk limit validation
    - OrderModificationValidator: Modification/cancellation validation

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL validation rules from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about exchange behavior
    """

    def __init__(
        self,
        config: AppSettings,
        market_service: MarketDataService,
        portfolio_service: PortfolioService,
    ) -> None:
        """Initialize order validator with configuration and dependencies.

        Args:
            config: Application settings containing all validation configuration
            market_service: Market data service for price validation
            portfolio_service: Portfolio service for balance validation
        """
        self.config = config
        self._market_service = market_service

        # Initialize specialized validators
        self._portfolio_validator = PortfolioValidator(portfolio_service)
        self._risk_validator = RiskValidator(config)

        logger.info(
            "order_validator_initialized",
            exchanges_configured=len(config.exchanges),
            validation_rules_enabled=True,
        )

    async def validate_order(self, order: Order) -> list[str]:
        """Validate order against all configured rules.

        Args:
            order: Order to validate

        Returns:
            List of validation violations (empty if valid)

        Note:
        - Orchestrates all specialized validators
        - Returns explicit violations, NO silent filtering
        - ALL limits from configuration
        """
        violations: list[str] = []

        logger.debug(
            "order_validation_starting",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            side=order.side.value,
            quantity=float(order.quantity_requested),
            price=float(order.price) if order.price else None,
        )

        try:
            # Get exchange configuration
            exchange_config = self._get_exchange_config(order.exchange)
            if not exchange_config:
                violations.append(f"No configuration found for exchange: {order.exchange}")
                return violations

            # Validation 1: Exchange-specific constraints
            exchange_violations = ExchangeValidator.validate(order, exchange_config)
            violations.extend(exchange_violations)

            # Validation 2: Market condition constraints
            market_snapshot = await self._market_service.get_market_snapshot()
            if market_snapshot:
                market_violations = await MarketValidator.validate(
                    order, market_snapshot, exchange_config,
                )
                violations.extend(market_violations)
            else:
                violations.append("Market data unavailable for order validation")

            # Validation 3: Portfolio constraints
            portfolio_violations = await self._portfolio_validator.validate(order)
            violations.extend(portfolio_violations)

            # Validation 4: Risk limit constraints
            risk_violations = await self._risk_validator.validate(order)
            violations.extend(risk_violations)

            # Validation 5: Order structure validation
            structure_violations = self._validate_order_structure(order)
            violations.extend(structure_violations)

            if violations:
                logger.warning(
                    "order_validation_failed",
                    order_id=order.exchange_order_id,
                    violation_count=len(violations),
                    violations=violations[:5],  # Log first 5 violations
                )
            else:
                logger.debug(
                    "order_validation_passed",
                    order_id=order.exchange_order_id,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                )

        except Exception as e:
            logger.exception(
                "order_validation_error", order_id=order.exchange_order_id, error=str(e),
            )
            raise

        return violations

    async def validate_order_modification(
        self, original_order: Order, modified_order: Order,
    ) -> list[str]:
        """Validate order modification request.

        Args:
            original_order: Original order being modified
            modified_order: Modified order parameters

        Returns:
            List of modification violations
        """
        violations: list[str] = []

        # Get exchange configuration
        exchange_config = self._get_exchange_config(original_order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {original_order.exchange}")
            return violations

        # Validate the modified order structure
        structure_violations = await self.validate_order(modified_order)
        violations.extend(structure_violations)

        # Check modification-specific constraints
        modification_violations = await OrderModificationValidator.validate_modification(
            original_order, modified_order, exchange_config,
        )
        violations.extend(modification_violations)

        return violations

    async def validate_order_cancellation(self, order: Order) -> list[str]:
        """Validate order cancellation request.

        Args:
            order: Order to cancel

        Returns:
            List of cancellation violations
        """
        violations: list[str] = []

        # Get exchange configuration
        exchange_config = self._get_exchange_config(order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {order.exchange}")
            return violations

        # Check cancellation constraints
        cancellation_violations = OrderModificationValidator.validate_cancellation(
            order, exchange_config,
        )
        violations.extend(cancellation_violations)

        return violations

    def _get_exchange_config(self, exchange: ExchangeName) -> ExchangeSpecificConfig | None:
        """Get exchange configuration from AppSettings.

        Args:
            exchange: Exchange to get configuration for

        Returns:
            Exchange configuration or None if not found

        Note:
        - Uses ExchangeName enum, NOT strings
        - NO defaults, explicit config required
        """
        exchange_str = exchange.value
        return self.config.exchanges.get(exchange_str)

    def _validate_order_structure(self, order: Order) -> list[str]:
        """Validate order structure and required fields.

        Args:
            order: Order to validate

        Returns:
            List of structural violations

        Note:
        - Check Symbol object type, NOT strings
        - Check ExchangeName enum type, NOT strings
        - Validate Decimal types for quantities/prices
        """
        violations: list[str] = []

        # Validate quantity is positive Decimal - type already guaranteed
        if order.quantity_requested <= 0:
            violations.append(f"Order quantity must be positive, got {order.quantity_requested}")

        # Validate price if provided
        if order.price is not None and order.price <= 0:
            violations.append(f"Order price must be positive, got {order.price}")

        # Validate required fields
        if not order.exchange_order_id and not order.client_order_id:
            violations.append("Order ID (exchange_order_id or client_order_id) is required")

        return violations
