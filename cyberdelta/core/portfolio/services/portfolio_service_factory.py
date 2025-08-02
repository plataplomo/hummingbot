"""Portfolio service factory for creating REAL portfolio services - NO MOCKS."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional
from datetime import datetime, UTC

from cyberdelta.config import AppSettings
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig, SymbolPatterns
from cyberdelta.core.portfolio.managers.portfolio_state_manager import PortfolioStateManager
from cyberdelta.core.portfolio.services.event_dispatcher import EventDispatcher
from cyberdelta.core.portfolio.services.analytics.performance_analytics import PerformanceAnalyticsService
from cyberdelta.core.portfolio.services.analytics.reporting_service import ReportingService
from cyberdelta.core.portfolio.services.metrics.exposure_metrics import ExposureMetricsService
from cyberdelta.core.portfolio.services.reconciliation_service import PortfolioReconciliationService
from cyberdelta.core.portfolio.services.exchange_data_service import ExchangeDataService
from cyberdelta.core.portfolio.services.validation.portfolio_validation_coordinator import (
    PortfolioValidationCoordinator
)
from cyberdelta.core.portfolio.services.cache.cache_service import MemoryCacheService
from cyberdelta.core.portfolio.services.monitoring.health_check_orchestrator import HealthCheckOrchestrator
from cyberdelta.core.portfolio.services.market_data import RealMarketDataService
from cyberdelta.core.portfolio.state.async_state_container import AsyncStateContainer
from cyberdelta.core.portfolio.protocols import (
    StateContainerProtocol,
    ValidationServiceProtocol,
    MetricsCollectorProtocol
)
from cyberdelta.core.portfolio.portfolio_types.infrastructure import EventType
from cyberdelta.config.structlog_config import get_logger

logger = get_logger(__name__)


class PortfolioServiceFactory:
    """Factory for creating REAL portfolio services following Week 4 specifications."""

    def __init__(self, config: Any):
        """Initialize portfolio service factory with real dependencies.
        
        Args:
            config: AppSettings or config object with required attributes
        """
        if config is None:
            raise ValueError("Config is required for PortfolioServiceFactory")
            
        self.app_settings = config
            
        self._services: Dict[str, Any] = {}
        self._initialized = False
        
        # Core dependencies
        self._state_container: Optional[StateContainerProtocol] = None
        self._validation_service: Optional[ValidationServiceProtocol] = None
        self._metrics_collector: Optional[MetricsCollectorProtocol] = None
        self._cache_service: Optional[MemoryCacheService[str, Any]] = None
        self._health_service: Optional[HealthCheckOrchestrator] = None
        
        # API clients for exchange integration
        self._api_clients: Optional[Dict[str, Any]] = None

    def create_portfolio_state_manager(self) -> PortfolioStateManager:
        """Create REAL portfolio state manager with proper dependencies."""
        if "portfolio_state_manager" not in self._services:
            # Ensure dependencies are created
            if not self._state_container:
                self._state_container = self._create_state_container()
            if not self._validation_service:
                self._validation_service = self._create_validation_service()
            if not self._metrics_collector:
                self._metrics_collector = self._create_metrics_collector()
                
            # Create REAL PortfolioStateManager with proper dependencies
            manager = PortfolioStateManager(
                app_settings=self.app_settings,
                state_container=self._state_container,
                validation_service=self._validation_service,
                metrics_collector=self._metrics_collector
            )
            
            self._services["portfolio_state_manager"] = manager
        return self._services["portfolio_state_manager"]

    def create_performance_analytics(self) -> PerformanceAnalyticsService:
        """Create performance analytics service."""
        if "performance_analytics" not in self._services:
            # Use default base currency since portfolio_tracker was removed
            base_currency = 'USDC'
            self._services["performance_analytics"] = PerformanceAnalyticsService(
                base_currency=base_currency
            )
        return self._services["performance_analytics"]

    def create_risk_analytics(self) -> ReportingService:
        """Create risk analytics service."""
        if "risk_analytics" not in self._services:
            self._services["risk_analytics"] = ReportingService()
        return self._services["risk_analytics"]

    def create_exposure_analytics(self) -> ExposureMetricsService:
        """Create exposure analytics service."""
        if "exposure_analytics" not in self._services:
            # Use default base currency since portfolio_tracker was removed
            base_currency = 'USDC'
            self._services["exposure_analytics"] = ExposureMetricsService(
                base_currency=base_currency
            )
        return self._services["exposure_analytics"]

    def create_event_dispatcher(self) -> EventDispatcher:
        """Create REAL event dispatcher with proper event handling."""
        if "event_dispatcher" not in self._services:
            # Create REAL EventDispatcher
            dispatcher = EventDispatcher()
            self._services["event_dispatcher"] = dispatcher
        return self._services["event_dispatcher"]
    
    def get_portfolio_manager(self) -> PortfolioStateManager:
        """Get portfolio state manager (alias for create method)."""
        return self.create_portfolio_state_manager()
    
    def get_performance_analytics(self) -> PerformanceAnalyticsService:
        """Get performance analytics service (alias for create method)."""
        return self.create_performance_analytics()
    
    def get_risk_analytics(self) -> ReportingService:
        """Get risk analytics service."""
        return self.create_risk_analytics()
    
    def get_exposure_analytics(self) -> ExposureMetricsService:
        """Get exposure analytics service."""
        return self.create_exposure_analytics()
    
    def get_event_dispatcher(self) -> EventDispatcher:
        """Get event dispatcher (alias for create method)."""
        return self.create_event_dispatcher()

    async def initialize_all(self, api_clients: Optional[Dict[str, Any]] = None) -> None:
        """Initialize all portfolio services in correct dependency order."""
        if self._initialized:
            return
            
        logger.info("Starting PortfolioServiceFactory initialization")
        self._api_clients = api_clients
            
        try:
            # Phase 1: Initialize infrastructure services
            await self._initialize_infrastructure_services()
            
            # Phase 2: Initialize data services
            await self._initialize_data_services()
            
            # Phase 3: Initialize analytics services
            await self._initialize_analytics_services()
            
            # Phase 4: Initialize portfolio manager
            await self._initialize_portfolio_manager()
            
            # Phase 5: Initialize event system
            await self._initialize_event_system()
            
            self._initialized = True
            logger.info("PortfolioServiceFactory initialization complete")
            
        except Exception as e:
            # Cleanup on failure
            await self._cleanup_on_initialization_failure()
            raise RuntimeError(f"Failed to initialize portfolio services: {e}") from e

    async def _initialize_infrastructure_services(self) -> None:
        """Initialize infrastructure layer services."""
        logger.info("Initializing infrastructure services")
        
        # State container
        if not self._state_container:
            self._state_container = self._create_state_container()
        if hasattr(self._state_container, 'initialize'):
            await self._state_container.initialize()
            
        # Cache service
        if not self._cache_service:
            # Create cache service without portfolio_tracker config
            self._cache_service = MemoryCacheService[str, Any](app_settings=self.app_settings)
            if hasattr(self._cache_service, 'start'):
                await self._cache_service.start()
                    
        # Health service
        if not self._health_service:
            self._health_service = HealthCheckOrchestrator()
            if hasattr(self._health_service, 'start'):
                await self._health_service.start()

    async def _initialize_data_services(self) -> None:
        """Initialize data layer services."""
        logger.info("Initializing data services")
        
        # Validation service
        if not self._validation_service:
            self._validation_service = self._create_validation_service()
        if hasattr(self._validation_service, 'initialize'):
            await self._validation_service.initialize()
            
        # Exchange data service
        if "exchange_data_service" not in self._services:
            service = ExchangeDataService(service_name="portfolio_exchange_data")
            
            # Register all available API clients generically
            if self._api_clients:
                for exchange_id, api_client in self._api_clients.items():
                    service.register_api_client(exchange_id, api_client)
            
            self._services["exchange_data_service"] = service
        if hasattr(self._services["exchange_data_service"], 'initialize'):
            await self._services["exchange_data_service"].initialize()

    async def _initialize_analytics_services(self) -> None:
        """Initialize analytics layer services."""
        logger.info("Initializing analytics services")
        
        # Create analytics services
        self.create_performance_analytics()
        self.create_risk_analytics()
        self.create_exposure_analytics()
        
        # Initialize them
        for service_name in ["performance_analytics", "risk_analytics", "exposure_analytics"]:
            service = self._services.get(service_name)
            if service and hasattr(service, 'initialize'):
                await service.initialize()

    async def _initialize_portfolio_manager(self) -> None:
        """Initialize portfolio manager."""
        logger.info("Initializing portfolio manager")
        
        manager = self.create_portfolio_state_manager()
        if hasattr(manager, 'initialize'):
            await manager.initialize()

    async def _initialize_event_system(self) -> None:
        """Initialize event system with handler registration."""
        logger.info("Initializing event system")
        
        dispatcher = self.create_event_dispatcher()
        
        # Initialize dispatcher
        if hasattr(dispatcher, 'start'):
            await dispatcher.start()
        elif hasattr(dispatcher, 'initialize'):
            await dispatcher.initialize()
            
        # Register event handlers for portfolio manager
        await self._register_event_handlers()

    async def _register_event_handlers(self) -> None:
        """Register event handlers for portfolio events."""
        dispatcher = self._services.get("event_dispatcher")
        manager = self._services.get("portfolio_state_manager")
        
        if not dispatcher or not manager:
            return
            
        # Register handlers if manager has integrated event handling
        if hasattr(manager, 'handle_balance_update'):
            await dispatcher.register_handler(EventType.BALANCE_UPDATED, manager.handle_balance_update)
        if hasattr(manager, 'handle_position_update'):
            await dispatcher.register_handler(EventType.POSITION_UPDATED, manager.handle_position_update)
        if hasattr(manager, 'handle_trade_execution'):
            await dispatcher.register_handler(EventType.TRADE_PROCESSED, manager.handle_trade_execution)
        if hasattr(manager, 'handle_order_fill'):
            await dispatcher.register_handler(EventType.ORDER_FILLED, manager.handle_order_fill)
            
        logger.info("Event handlers registered")

    async def shutdown_all(self) -> None:
        """Shutdown all portfolio services in reverse dependency order."""
        if not self._initialized:
            return
            
        logger.info("Shutting down PortfolioServiceFactory")
            
        try:
            # Shutdown services in reverse order
            shutdown_order = [
                "event_dispatcher",
                "portfolio_state_manager",
                "exposure_analytics",
                "risk_analytics",
                "performance_analytics",
                "exchange_data_service"
            ]
            
            for service_name in shutdown_order:
                if service_name in self._services:
                    service = self._services[service_name]
                    try:
                        if hasattr(service, 'stop'):
                            await service.stop()
                        elif hasattr(service, 'shutdown'):
                            await service.shutdown()
                    except Exception as e:
                        logger.exception(f"Error shutting down {service_name}: {e}")
            
            # Shutdown core dependencies
            if self._health_service and hasattr(self._health_service, 'stop'):
                await self._health_service.stop()
                
            if self._cache_service and hasattr(self._cache_service, 'stop'):
                await self._cache_service.stop()
                
            if self._metrics_collector and hasattr(self._metrics_collector, 'shutdown'):
                await self._metrics_collector.shutdown()
                
            if self._validation_service and hasattr(self._validation_service, 'shutdown'):
                await self._validation_service.shutdown()
                
            if self._state_container and hasattr(self._state_container, 'shutdown'):
                await self._state_container.shutdown()
            
            self._services.clear()
            self._initialized = False
            logger.info("PortfolioServiceFactory shutdown complete")
            
        except Exception as e:
            logger.exception("Error during shutdown")
            # Ensure cleanup even if shutdown fails
            self._services.clear()
            self._initialized = False

    def create_exchange_service(self) -> ExchangeDataService:
        """Create exchange data service."""
        if "exchange_service" not in self._services:
            service = ExchangeDataService(service_name="portfolio_exchange_data")
            self._services["exchange_service"] = service
        return self._services["exchange_service"]

    def get_exchange_data_service(self) -> ExchangeDataService:
        """Get exchange data service."""
        return self._services.get("exchange_data_service") or self.create_exchange_service()

    def create_validation_service(self) -> PortfolioValidationCoordinator:
        """Create REAL validation service."""
        if "validation_service" not in self._services:
            # Ensure state container is created
            if not self._state_container:
                self._state_container = self._create_state_container()
            # Create REAL validation coordinator
            validator = PortfolioValidationCoordinator(self.app_settings, self._state_container)
            self._services["validation_service"] = validator
        return self._services["validation_service"]
        
    def _create_state_container(self) -> StateContainerProtocol:
        """Create state container for portfolio data persistence."""
        return AsyncStateContainer(state_id="portfolio_state_container")
        
    def _create_validation_service(self) -> ValidationServiceProtocol:
        """Create validation service for portfolio data validation."""
        if not self._state_container:
            self._state_container = self._create_state_container()
        return PortfolioValidationCoordinator(self.app_settings, self._state_container)
        
    def _create_metrics_collector(self) -> MetricsCollectorProtocol | None:
        """Create metrics collector for performance tracking."""
        # Return None for now - metrics collector is optional
        return None
        
    async def _cleanup_on_initialization_failure(self) -> None:
        """Cleanup services if initialization fails."""
        try:
            await self.shutdown_all()
        except Exception:
            # Best effort cleanup
            pass
            
    def is_initialized(self) -> bool:
        """Check if factory is initialized."""
        return self._initialized

    def create_reconciliation_service(self) -> PortfolioReconciliationService:
        """Create portfolio reconciliation service."""
        if "reconciliation_service" not in self._services:
            service = PortfolioReconciliationService(service_factory=self)
            self._services["reconciliation_service"] = service
        return self._services["reconciliation_service"]
        
    def get_validation_service(self) -> PortfolioValidationCoordinator:
        """Get validation service (alias for create method)."""
        return self.create_validation_service()
    
    def create_market_data_service(self, api_clients: dict[str, Any] | None = None) -> RealMarketDataService:
        """Create market data service for historical prices and volatility."""
        if "market_data_service" not in self._services:
            service = RealMarketDataService(
                app_settings=self.app_settings,
                cache_service=self._cache_service,
                api_clients=api_clients or self._api_clients
            )
            self._services["market_data_service"] = service
        return self._services["market_data_service"]
    
    def get_market_data_service(self) -> RealMarketDataService:
        """Get market data service (alias for create method)."""
        return self.create_market_data_service()
        
    def get_cache_service(self) -> Optional[MemoryCacheService[str, Any]]:
        """Get cache service."""
        return self._cache_service
        
    def get_health_service(self) -> Optional[HealthCheckOrchestrator]:
        """Get health service."""
        return self._health_service
        
    def get_state_container(self) -> Optional[StateContainerProtocol]:
        """Get state container."""
        return self._state_container
        
    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check on all services."""
        if not self._initialized:
            return {
                "status": "unhealthy",
                "reason": "Service factory not initialized",
                "timestamp": datetime.now(UTC).isoformat()
            }

        health_status = {
            "status": "healthy",
            "services": {},
            "timestamp": datetime.now(UTC).isoformat()
        }

        # Check each service
        for service_name, service in self._services.items():
            try:
                if hasattr(service, 'health_check'):
                    service_health = await service.health_check()
                    health_status["services"][service_name] = service_health
                else:
                    health_status["services"][service_name] = {"status": "healthy", "note": "No health check available"}
            except Exception as e:
                health_status["services"][service_name] = {"status": "unhealthy", "error": str(e)}
                health_status["status"] = "degraded"

        return health_status