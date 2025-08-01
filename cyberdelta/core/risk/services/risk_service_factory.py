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
from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState
from cyberdelta.core.portfolio.config.risk_parameters import MarketDataProvider


class RiskServiceFactory:
    """Factory for creating risk services for portfolio integration."""

    def __init__(self, config: AppSettings | None = None):
        """Initialize risk service factory."""
        if config is None:
            # Create minimal SmartSymbolsConfig for default AppSettings
            patterns = SymbolPatterns(
                hyperliquid={"perp": "{symbol}-USD"},
                backpack={"perp": "{symbol}_USDC"}
            )
            symbols_config = SmartSymbolsConfig(
                list=["BTC", "ETH"],
                patterns=patterns
            )
            config = AppSettings(symbols=symbols_config)
        self.config = config
        self._services: dict[str, object] = {}
        self._portfolio_state: PortfolioState | None = None
        self._market_data_provider: MarketDataProvider | None = None

    def create_exposure_calculator(self) -> PortfolioExposureCalculator:
        """Create portfolio exposure calculator."""
        if "exposure_calculator" not in self._services:
            self._services["exposure_calculator"] = PortfolioExposureCalculator()
        return self._services["exposure_calculator"]

    def create_position_sizer(self, sizing_method: str = "simple") -> PositionSizer:
        """Create position sizer with specified method.
        
        Args:
            sizing_method: Sizing method - "simple", "kelly", or "production_kelly"
            
        Returns:
            Position sizer instance
        """
        service_key = f"position_sizer_{sizing_method}"
        
        if service_key not in self._services:
            if sizing_method == "kelly":
                sizer = KellyCriterionSizer(self.config)
            elif sizing_method == "production_kelly":
                sizer = ProductionKellySizer(
                    self.config,
                    portfolio_state=self._portfolio_state,
                    market_data_provider=self._market_data_provider
                )
            else:
                sizer = SimpleSizer(self.config)
            
            self._services[service_key] = PositionSizer(sizer, self.config)
        
        return self._services[service_key]

    def create_risk_metrics_calculator(self) -> RiskMetricsCalculator:
        """Create risk metrics calculator."""
        if "risk_metrics_calculator" not in self._services:
            self._services["risk_metrics_calculator"] = RiskMetricsCalculator()
        return self._services["risk_metrics_calculator"]

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