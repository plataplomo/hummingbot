"""Unified factory coordinating clean portfolio and risk modules."""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.services.analytics.performance_analytics import PerformanceAnalyticsService
    from cyberdelta.core.portfolio.services.event_dispatcher import EventDispatcher
    from cyberdelta.core.portfolio.services.market_data.market_data_service import RealMarketDataService

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import (
    PortfolioRiskCoordinator,
    PortfolioWithRiskModel
)
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.portfolio.services import PortfolioServiceFactory
from cyberdelta.core.risk.services.risk_service_factory import RiskServiceFactory


class UnifiedServiceFactory(BaseModel):
    """Coordinates portfolio and risk service factories with clean boundaries."""

    config: Any = Field(..., description="Configuration object")

    model_config = ConfigDict(extra="forbid", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize service instances after Pydantic validation."""
        # Extract config sections - handle both nested and flat configs
        if hasattr(self.config, 'portfolio_config'):
            portfolio_config = self.config.portfolio_config
        elif hasattr(self.config, 'portfolio'):
            portfolio_config = self.config.portfolio
        else:
            portfolio_config = self.config
            
        if hasattr(self.config, 'risk_config'):
            risk_config = self.config.risk_config
        elif hasattr(self.config, 'risk'):
            risk_config = self.config.risk
        else:
            risk_config = self.config

        # Separate factories for each module
        self.portfolio_factory = PortfolioServiceFactory(portfolio_config)
        self.risk_factory = RiskServiceFactory(risk_config)
        
        # API clients for exchange integration (will be set during initialization)
        self._api_clients: dict[str, Any] | None = None
        
        # Market data provider will be created during initialization
        self._market_data_provider: RealMarketDataService | None = None

        # Integration layer - the core coordinator
        self.coordinator = PortfolioRiskCoordinator(
            portfolio_factory=self.portfolio_factory,
            risk_factory=self.risk_factory
        )

        # Service instances (initialized during startup)
        self._portfolio_manager: PortfolioStateManager | None = None
        self._performance_analytics: PerformanceAnalyticsService | None = None
        self._risk_analytics: Any | None = None  # TODO: Add proper type
        self._exposure_analytics: Any | None = None  # TODO: Add proper type
        self._event_dispatcher: EventDispatcher | None = None
        
        # Initialization state
        self._initialized = False

    async def initialize_all(self, api_clients: dict[str, Any] | None = None) -> None:
        """Initialize both modules in correct order.
        
        Args:
            api_clients: Optional dictionary of exchange API clients for market data
        """
        if self._initialized:
            return
            
        # Store API clients for market data service
        self._api_clients = api_clients
            
        try:
            # Phase 1: Portfolio module initialization (state management foundation)
            await self.portfolio_factory.initialize_all()

            # Phase 2: Risk module initialization (needs portfolio for risk calculations)
            await self.risk_factory.initialize_all()
            
            # Phase 2.5: Create market data service if API clients provided
            if self._api_clients:
                self._market_data_provider = self.portfolio_factory.create_market_data_service(self._api_clients)
                # Update coordinator with market data provider
                self.coordinator.market_data_provider = self._market_data_provider

            # Phase 3: Create service instances
            self._portfolio_manager = self.portfolio_factory.create_portfolio_state_manager()
            self._performance_analytics = self.portfolio_factory.create_performance_analytics()
            # Create risk calculators instead of analytics services
            self._risk_analytics = self.risk_factory.create_risk_metrics_calculator()
            self._exposure_analytics = self.risk_factory.create_exposure_calculator()
            
            # Get event dispatcher if available
            try:
                self._event_dispatcher = self.portfolio_factory.create_event_dispatcher()
            except Exception:
                self._event_dispatcher = None

            # Phase 4: Initialize individual services
            services_to_init = [
                self._portfolio_manager,
                self._performance_analytics,
                self._risk_analytics,
                self._exposure_analytics
            ]
            
            if self._event_dispatcher:
                services_to_init.append(self._event_dispatcher)

            for service in services_to_init:
                if service and hasattr(service, 'initialize'):
                    await service.initialize()

            self._initialized = True
            
        except Exception as e:
            # Cleanup on failure
            await self._cleanup_on_failure()
            raise RuntimeError(f"Failed to initialize unified service factory: {e}") from e

    async def _cleanup_on_failure(self) -> None:
        """Cleanup services if initialization fails."""
        try:
            await self.risk_factory.shutdown_all()
        except Exception:
            pass
        
        try:
            await self.portfolio_factory.shutdown_all()
        except Exception:
            pass

    # Clean interfaces for production components
    def get_portfolio_manager(self) -> PortfolioStateManager:
        """Pure portfolio state management."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        if self._portfolio_manager is None:
            raise RuntimeError("Portfolio manager not created - initialization may have failed")
        return self._portfolio_manager

    def get_performance_analytics(self) -> PerformanceAnalyticsService | None:
        """Get performance analytics service."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self._performance_analytics

    def get_risk_analytics(self) -> Any | None:
        """Get risk analytics service.""" 
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self._risk_analytics

    def get_exposure_analytics(self) -> Any | None:
        """Get exposure analytics service."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self._exposure_analytics

    def get_event_dispatcher(self) -> EventDispatcher | None:
        """Get event dispatcher service."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self._event_dispatcher

    def get_risk_coordinator(self) -> PortfolioRiskCoordinator:
        """Clean portfolio-risk coordination."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self.coordinator

    async def get_portfolio_with_risk_assessment(self) -> PortfolioWithRiskModel:
        """Integrated portfolio + risk view."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return await self.coordinator.get_current_portfolio_with_risk_assessment()

    # Factory method access (for creating new instances)
    def get_portfolio_factory(self) -> PortfolioServiceFactory:
        """Get portfolio service factory."""
        return self.portfolio_factory

    def get_risk_factory(self) -> RiskServiceFactory:
        """Get risk service factory."""
        return self.risk_factory
    
    def get_market_data_provider(self) -> RealMarketDataService | None:
        """Get market data provider."""
        if not self._initialized:
            raise RuntimeError("UnifiedServiceFactory not initialized - call initialize_all() first")
        return self._market_data_provider

    # Health and status methods
    def is_initialized(self) -> bool:
        """Check if factory is initialized."""
        return self._initialized

    async def health_check(self) -> dict[str, Any]:
        """Comprehensive health check of all services."""
        if not self._initialized:
            return {"status": "not_initialized", "services": {}}

        health_status = {
            "status": "healthy",
            "services": {},
            "timestamp": asyncio.get_event_loop().time()
        }

        # Check portfolio manager
        if self._portfolio_manager:
            try:
                portfolio_state = await self._portfolio_manager.get_portfolio_summary()
                health_status["services"]["portfolio_manager"] = {
                    "status": "healthy",
                    "total_capital": float(portfolio_state.total_capital),
                    "position_count": len([p for positions in portfolio_state.positions.values() for p in positions])
                }
            except Exception as e:
                health_status["services"]["portfolio_manager"] = {"status": "unhealthy", "error": str(e)}
                health_status["status"] = "degraded"
        else:
            health_status["services"]["portfolio_manager"] = {"status": "not_initialized"}

        # Check performance analytics
        if self._performance_analytics:
            try:
                if hasattr(self._performance_analytics, 'health_check'):
                    perf_health = await self._performance_analytics.health_check()
                    health_status["services"]["performance_analytics"] = perf_health
                else:
                    health_status["services"]["performance_analytics"] = {"status": "healthy", "note": "no health check method"}
            except Exception as e:
                health_status["services"]["performance_analytics"] = {"status": "unhealthy", "error": str(e)}
                health_status["status"] = "degraded"
        else:
            health_status["services"]["performance_analytics"] = {"status": "not_initialized"}

        # Check risk analytics
        try:
            if hasattr(self._risk_analytics, 'health_check'):
                risk_health = await self._risk_analytics.health_check()
                health_status["services"]["risk_analytics"] = risk_health
            else:
                health_status["services"]["risk_analytics"] = {"status": "healthy", "note": "no health check method"}
        except Exception as e:
            health_status["services"]["risk_analytics"] = {"status": "unhealthy", "error": str(e)}
            health_status["status"] = "degraded"

        # Check coordinator
        try:
            portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
            health_status["services"]["coordinator"] = {
                "status": "healthy",
                "risk_score": portfolio_with_risk.risk_assessment.risk_score,
                "leverage": float(portfolio_with_risk.risk_assessment.leverage_ratio)
            }
        except Exception as e:
            health_status["services"]["coordinator"] = {"status": "unhealthy", "error": str(e)}
            health_status["status"] = "degraded"

        return health_status

    async def shutdown_all(self) -> None:
        """Shutdown in reverse order."""
        if not self._initialized:
            return

        try:
            # Shutdown individual services first
            services_to_shutdown = [
                self._event_dispatcher,
                self._exposure_analytics, 
                self._risk_analytics,
                self._performance_analytics,
                self._portfolio_manager
            ]

            for service in services_to_shutdown:
                if service and hasattr(service, 'shutdown'):
                    try:
                        await service.shutdown()
                    except Exception:
                        # Log error but continue shutdown
                        pass

            # Shutdown factories in reverse order
            await self.risk_factory.shutdown_all()
            await self.portfolio_factory.shutdown_all()

            # Reset state
            self._portfolio_manager = None
            self._performance_analytics = None
            self._risk_analytics = None
            self._exposure_analytics = None
            self._event_dispatcher = None
            self._initialized = False

        except Exception as e:
            # Log error but don't re-raise during shutdown
            pass