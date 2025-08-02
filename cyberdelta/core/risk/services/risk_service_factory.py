"""Risk service factory for portfolio integration."""

from __future__ import annotations

from cyberdelta.config import AppSettings
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig, SymbolPatterns
from cyberdelta.core.risk.exposure.portfolio_exposure import PortfolioExposureCalculator
from cyberdelta.core.risk.sizing.orchestrator.position_sizer import PositionSizer
from cyberdelta.core.risk.sizing.strategies.simple_sizer import SimpleSizer
from cyberdelta.core.risk.sizing.strategies.kelly_criterion_sizer import KellyCriterionSizer
from cyberdelta.core.risk.sizing.strategies.production_kelly_sizer import ProductionKellySizer
from cyberdelta.core.risk.utils.risk_metrics_calculator import RiskMetricsCalculator
from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState
from cyberdelta.core.portfolio.config.risk_parameters import MarketDataProvider


class RiskServiceFactory:
    """Factory for creating risk services for portfolio integration."""

    def __init__(self, config: AppSettings | None = None):
        """Initialize risk service factory."""
        # Store config directly - services will handle None config appropriately
        self.config = config
        self._services: dict[str, object] = {}
        self._portfolio_state: PortfolioState | None = None
        self._market_data_provider: MarketDataProvider | None = None

    def create_exposure_calculator(self) -> PortfolioExposureCalculator:
        """Create portfolio exposure calculator."""
        if "exposure_calculator" not in self._services:
            calculator = PortfolioExposureCalculator()
            self._services["exposure_calculator"] = calculator
            return calculator
        
        # Return the stored service - we know it's the right type
        stored_service = self._services["exposure_calculator"]
        assert isinstance(stored_service, PortfolioExposureCalculator)
        return stored_service

    def create_position_sizer(self, sizing_method: str = "simple") -> PositionSizer:
        """Create position sizer with specified method.
        
        Args:
            sizing_method: Sizing method - "simple", "kelly", or "production_kelly"
            
        Returns:
            Position sizer instance
        """
        if self.config is None:
            raise ValueError("Config is required for position sizer creation")
            
        service_key = f"position_sizer_{sizing_method}"
        
        if service_key not in self._services:
            if sizing_method == "kelly":
                sizer: SimpleSizer | KellyCriterionSizer | ProductionKellySizer = KellyCriterionSizer(self.config)
            elif sizing_method == "production_kelly":
                sizer = ProductionKellySizer(
                    self.config,
                    portfolio_state=self._portfolio_state,
                    market_data_provider=self._market_data_provider
                )
            else:
                sizer = SimpleSizer(self.config)
            
            position_sizer = PositionSizer(sizer, self.config)
            self._services[service_key] = position_sizer
            return position_sizer
        
        # Return the stored service - we know it's the right type
        stored_service = self._services[service_key]
        assert isinstance(stored_service, PositionSizer)
        return stored_service

    def create_risk_metrics_calculator(self) -> RiskMetricsCalculator:
        """Create risk metrics calculator."""
        if "risk_metrics_calculator" not in self._services:
            calculator = RiskMetricsCalculator()
            self._services["risk_metrics_calculator"] = calculator
            return calculator
        
        # Return the stored service - we know it's the right type
        stored_service = self._services["risk_metrics_calculator"]
        assert isinstance(stored_service, RiskMetricsCalculator)
        return stored_service

    async def initialize_all(self) -> None:
        """Initialize all risk services."""
        # Risk services are currently synchronous, but we provide async interface for consistency
        pass

    async def shutdown_all(self) -> None:
        """Shutdown all risk services."""
        # Risk services don't need explicit shutdown, but we provide async interface for consistency
        self._services.clear()
    
    def set_portfolio_state(self, portfolio_state: PortfolioState) -> None:
        """Set portfolio state for production Kelly sizing.
        
        Args:
            portfolio_state: Current portfolio state
        """
        self._portfolio_state = portfolio_state
    
    def set_market_data_provider(self, provider: MarketDataProvider) -> None:
        """Set market data provider for production sizing.
        
        Args:
            provider: Market data provider instance
        """
        self._market_data_provider = provider