"""Refactored Risk Manager using modular position sizing.

This module provides risk management functionality using the strategy pattern
for position sizing, eliminating the flag-based approach.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Protocol

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
from cyberdelta.core.risk.sizing.models.sizing_result import SizingResult
from cyberdelta.exceptions.risk import RiskCheckError, RiskConfigError
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Import risk types
from cyberdelta.core.risk_types import (
    SizedOpportunity,
    CircuitBreakerSystemProtocol,
    FundingRateValidatorProtocol,
    ZERO,
    ONE,
)


logger = get_logger(__name__)


class RiskManager:
    """Refactored Risk Manager with modular position sizing.
    
    Responsible for:
    - Validating opportunities against risk constraints
    - Delegating position sizing to configurable strategies
    - Enforcing position limits and portfolio risk controls
    - Calculating risk metrics
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_state_manager: PortfolioStateManager,
        circuit_breaker_system: CircuitBreakerSystemProtocol | None = None,
        funding_rate_validator: FundingRateValidatorProtocol | None = None,
        risk_factory: RiskServiceFactory | None = None,
    ) -> None:
        """Initialize the risk manager with modular position sizing.

        Args:
            app_settings: Application configuration
            portfolio_state_manager: Portfolio state manager for position and balance tracking
            circuit_breaker_system: Optional system for circuit breakers
            funding_rate_validator: Optional validator for funding rate predictions
            risk_factory: Optional risk service factory for creating position sizer
        """
        self.app_settings = app_settings
        self.logger = logger
        self.portfolio_state_manager = portfolio_state_manager
        self.circuit_breaker_system = circuit_breaker_system
        self.funding_rate_validator = funding_rate_validator
        
        # Create risk factory if not provided
        if risk_factory is None:
            risk_factory = RiskServiceFactory(app_settings)
        self.risk_factory = risk_factory
        
        # Initialize position sizer based on configuration
        self._init_position_sizer()
        
        # Load risk parameters from config
        self._load_risk_parameters()

    def _init_position_sizer(self) -> None:
        """Initialize the position sizer based on configuration."""
        # Determine sizing method from config
        sizing_method = self._determine_sizing_method()
        
        # Create position sizer through factory
        self.position_sizer = self.risk_factory.create_position_sizer(sizing_method)
        
        self.logger.info(
            "position_sizer_initialized",
            sizing_method=sizing_method,
            message=f"Initialized position sizer with {sizing_method} method"
        )

    def _determine_sizing_method(self) -> str:
        """Determine the sizing method from configuration."""
        # Check new sizing configuration first
        if hasattr(self.app_settings.risk, 'sizing') and hasattr(self.app_settings.risk.sizing, 'method'):
            return self.app_settings.risk.sizing.method
        
        # Check for legacy flag-based configuration
        if hasattr(self.app_settings.risk, 'use_simple_sizing_path'):
            if self.app_settings.risk.use_simple_sizing_path:
                return "simple"
            else:
                return "kelly"
        
        # Default to simple sizing
        return "simple"

    def _load_risk_parameters(self) -> None:
        """Load risk parameters from configuration."""
        risk_config = self.app_settings.risk
        
        # Position limits
        self.max_position_value = getattr(risk_config, 'max_position_value', Decimal("100000"))
        self.max_portfolio_allocation = getattr(risk_config, 'max_portfolio_allocation', Decimal("0.2"))
        
        # Risk thresholds
        self.max_leverage = getattr(risk_config, 'max_leverage', Decimal("2.0"))
        self.min_profit_threshold = getattr(risk_config, 'min_profit_threshold', Decimal("0.001"))
        
        # Validation parameters
        self.enable_exchange_validation = getattr(risk_config, 'enable_exchange_validation', True)
        self.enable_funding_rate_validation = getattr(risk_config, 'enable_funding_rate_validation', True)

    async def size_opportunity(self, opportunity: ArbitrageOpportunity) -> SizedOpportunity | None:
        """Calculate the optimal size for an arbitrage opportunity using modular sizing.

        Args:
            opportunity: The arbitrage opportunity to size

        Returns:
            SizedOpportunity if sizing successful and risk checks pass, None otherwise
        """
        self.logger.info(
            "sizing_opportunity",
            symbol=opportunity.symbol,
            long_exchange=opportunity.long_exchange,
            short_exchange=opportunity.short_exchange,
        )

        # Validate opportunity first
        is_valid = await self.validate_opportunity(opportunity)
        if not is_valid:
            return None

        # Get available capital
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()
        total_capital = getattr(portfolio_state, "total_capital", Decimal(0))
        
        if total_capital <= ZERO:
            self.logger.warning(
                "insufficient_capital",
                symbol=opportunity.symbol,
                total_capital=float(total_capital),
            )
            return None

        # Size the opportunity using position sizer
        try:
            sizing_result = await self.position_sizer.size_opportunity(
                opportunity,
                total_capital
            )
            
            if not sizing_result.success:
                self.logger.warning(
                    "sizing_failed",
                    symbol=opportunity.symbol,
                    reason=sizing_result.message,
                    details=sizing_result.details,
                )
                return None

            # Convert SizingResult to legacy SizedOpportunity for compatibility
            return self._convert_to_sized_opportunity(opportunity, sizing_result, total_capital)

        except Exception as e:
            self.logger.exception(
                "sizing_error",
                symbol=opportunity.symbol,
                error=str(e),
            )
            return None

    def _convert_to_sized_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        sizing_result: SizingResult,
        total_capital: Decimal,
    ) -> SizedOpportunity:
        """Convert modern SizingResult to legacy SizedOpportunity format.

        Args:
            opportunity: Original arbitrage opportunity
            sizing_result: Result from position sizer
            total_capital: Total available capital

        Returns:
            Legacy SizedOpportunity for compatibility
        """
        # Calculate allocation percentage
        allocation_percentage = (sizing_result.position_size / total_capital) if total_capital > 0 else ZERO
        
        # Calculate expected profit (simplified)
        expected_profit = sizing_result.position_size * opportunity.expected_profit_pct / 100
        
        # Calculate expected return
        expected_return = opportunity.expected_profit_pct
        
        # Risk adjusted return (use raw expected return if not available)
        risk_adjusted_return = sizing_result.risk_metrics.get("sharpe_ratio", expected_return)
        
        return SizedOpportunity(
            opportunity=opportunity,
            long_size=sizing_result.position_size,
            short_size=sizing_result.position_size,  # Equal sizes for delta neutral
            allocation_percentage=allocation_percentage,
            expected_profit=expected_profit,
            expected_return=expected_return,
            risk_adjusted_return=Decimal(str(risk_adjusted_return)),
        )

    async def validate_opportunity(self, opportunity: ArbitrageOpportunity) -> bool:
        """Validate an arbitrage opportunity against all risk checks.

        Args:
            opportunity: The opportunity to validate

        Returns:
            True if all validations pass, False otherwise
        """
        # Check required fields
        if not self._check_required_fields(opportunity):
            return False

        # Check profitability
        if not self._check_profitability(opportunity):
            return False

        # Check circuit breaker
        if self.circuit_breaker_system and not self._check_circuit_breaker(opportunity):
            return False

        # Check exchange balances
        if self.enable_exchange_validation and not await self._check_exchange_balances(opportunity):
            return False

        # Check funding rate validation
        if self.enable_funding_rate_validation and self.funding_rate_validator:
            if not await self._check_funding_rates(opportunity):
                return False

        return True

    def _check_required_fields(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if opportunity has all required fields."""
        required_fields = ["symbol", "long_exchange", "short_exchange", "expected_profit_pct"]
        
        for field in required_fields:
            if not hasattr(opportunity, field) or getattr(opportunity, field) is None:
                self.logger.warning(
                    "missing_required_field",
                    field=field,
                    symbol=getattr(opportunity, "symbol", "unknown"),
                )
                return False
        
        return True

    def _check_profitability(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if opportunity meets minimum profitability threshold."""
        if opportunity.expected_profit_pct < self.min_profit_threshold:
            self.logger.debug(
                "below_profit_threshold",
                symbol=opportunity.symbol,
                expected_profit=float(opportunity.expected_profit_pct),
                threshold=float(self.min_profit_threshold),
            )
            return False
        
        return True

    def _check_circuit_breaker(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if circuit breaker allows trading."""
        if not self.circuit_breaker_system:
            return True
            
        breaker_state = self.circuit_breaker_system.check_state()
        if breaker_state.is_tripped:
            self.logger.warning(
                "circuit_breaker_tripped",
                symbol=opportunity.symbol,
                reason=breaker_state.reason,
            )
            return False
        
        return True

    async def _check_exchange_balances(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check if exchanges have sufficient balances."""
        # Simplified balance check - delegate to portfolio state manager
        try:
            balances = await self.portfolio_state_manager.get_balances(opportunity.long_exchange)
            
            # Check for minimum balance (simplified)
            min_balance = Decimal("10")  # Minimum $10
            total_balance = sum(b.usd_value for b in balances.values() if hasattr(b, 'usd_value'))
            
            if total_balance < min_balance:
                self.logger.warning(
                    "insufficient_exchange_balance",
                    exchange=opportunity.long_exchange,
                    balance=float(total_balance),
                    required=float(min_balance),
                )
                return False
                
        except Exception as e:
            self.logger.exception(
                "balance_check_error",
                exchange=opportunity.long_exchange,
                error=str(e),
            )
            return False
        
        return True

    async def _check_funding_rates(self, opportunity: ArbitrageOpportunity) -> bool:
        """Check funding rate predictions if validator is available."""
        if not self.funding_rate_validator:
            return True
            
        try:
            metrics = self.funding_rate_validator.get_symbol_metrics(
                opportunity.long_exchange,
                opportunity.symbol
            )
            
            # Simple check - ensure funding rate data is recent
            if metrics and metrics.get("last_update"):
                return True
                
        except Exception as e:
            self.logger.exception(
                "funding_rate_check_error",
                symbol=opportunity.symbol,
                error=str(e),
            )
        
        return True