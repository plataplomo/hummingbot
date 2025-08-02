"""Risk Manager - Breaking Change Version (No Backwards Compatibility).

This shows what a clean, modern API would look like without legacy support.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory
from cyberdelta.core.risk.sizing.models.sizing_result import SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity

logger = get_logger(__name__)


class RiskValidator(Protocol):
    """Protocol for risk validation services."""
    
    async def validate(self, opportunity: ArbitrageOpportunity) -> bool:
        """Validate an opportunity against risk rules."""
        ...


class RiskManager:
    """Modern Risk Manager with clean API.
    
    Breaking changes:
    1. No legacy configuration support
    2. Returns SizingResult instead of SizedOpportunity
    3. Requires explicit risk_factory
    4. No backwards compatibility parameters
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_state_manager: PortfolioStateManager,
        risk_factory: RiskServiceFactory,  # Required, not optional
    ) -> None:
        """Initialize with modern architecture only.
        
        Args:
            app_settings: Application configuration (must have risk.sizing.method)
            portfolio_state_manager: Portfolio state manager
            risk_factory: Risk service factory (required)
        """
        self.app_settings = app_settings
        self.portfolio_state_manager = portfolio_state_manager
        self.risk_factory = risk_factory
        
        # Direct access to modern config - no fallbacks
        self.sizing_method = app_settings.risk.sizing.method
        self.risk_params = app_settings.risk.sizing
        
        # Create position sizer
        self.position_sizer = risk_factory.create_position_sizer(self.sizing_method)
        
        # Create validators through factory
        self.validators = self._create_validators()
        
        logger.info(
            "risk_manager_initialized",
            sizing_method=self.sizing_method,
            validators=len(self.validators)
        )

    def _create_validators(self) -> list[RiskValidator]:
        """Create risk validators from factory."""
        # In a full implementation, these would come from risk_factory
        return []

    async def size_opportunity(
        self, 
        opportunity: ArbitrageOpportunity
    ) -> SizingResult | None:
        """Size an opportunity using modern API.
        
        Args:
            opportunity: The arbitrage opportunity
            
        Returns:
            SizingResult directly (not SizedOpportunity)
            None if validation fails
        """
        # Validate first
        if not await self.validate(opportunity):
            return None
        
        # Get capital
        portfolio = await self.portfolio_state_manager.get_portfolio_summary()
        available_capital = portfolio.total_capital
        
        if available_capital <= Decimal(0):
            logger.warning("insufficient_capital", symbol=opportunity.symbol)
            return None
        
        # Size directly - no conversion needed
        return await self.position_sizer.size_opportunity(
            opportunity,
            available_capital
        )

    async def validate(self, opportunity: ArbitrageOpportunity) -> bool:
        """Validate using modern validator chain."""
        for validator in self.validators:
            if not await validator.validate(opportunity):
                return False
        return True
        
    async def get_risk_metrics(self) -> dict:
        """Get current risk metrics."""
        return {
            "sizing_method": self.sizing_method,
            "performance": self.position_sizer.get_performance_stats(),
            "exposure": await self._calculate_exposure(),
        }
    
    async def _calculate_exposure(self) -> dict:
        """Calculate portfolio exposure metrics."""
        # Delegate to exposure calculator from factory
        calculator = self.risk_factory.create_exposure_calculator()
        portfolio_state = await self.portfolio_state_manager.get_portfolio_summary()
        return calculator.calculate_portfolio_exposure(portfolio_state)