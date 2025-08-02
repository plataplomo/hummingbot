"""Service factory for creating focused services."""

from __future__ import annotations

from typing import Any

# Analytics services are in the analytics module
from cyberdelta.core.analytics.services.performance_analytics import PerformanceAnalyticsService
from cyberdelta.core.analytics.services.reporting_service import ReportingService
# Metrics services are in the analytics module
from cyberdelta.core.analytics.metrics.pnl_metrics import PnLMetricsService
from cyberdelta.core.analytics.metrics.exposure_metrics import ExposureMetricsService
from cyberdelta.core.analytics.metrics.performance_metrics import PerformanceMetricsService
# Config services need to be imported from correct location
# from cyberdelta.core.config.config_loader import ConfigLoaderService
# from cyberdelta.core.config.config_validator import ConfigValidatorService
# from cyberdelta.core.config.config_persistence import ConfigPersistenceService


class PortfolioServiceFactory:
    """Creates and manages portfolio services."""

    def __init__(self) -> None:
        self._services: dict[str, Any] = {}

    # Analytics Services
    def create_performance_analytics(self) -> PerformanceAnalyticsService:
        """Create performance analytics service."""
        if "performance_analytics" not in self._services:
            service = PerformanceAnalyticsService()
            self._services["performance_analytics"] = service
        
        service_from_cache = self._services["performance_analytics"]
        assert isinstance(service_from_cache, PerformanceAnalyticsService)
        return service_from_cache

    def create_reporting_service(self) -> ReportingService:
        """Create reporting service."""
        if "reporting" not in self._services:
            service = ReportingService()
            self._services["reporting"] = service
            
        service_from_cache = self._services["reporting"]
        assert isinstance(service_from_cache, ReportingService)
        return service_from_cache

    # Config Services - commented out as these services don't exist yet
    # def create_config_loader(self) -> ConfigLoaderService:
    #     """Create config loader service."""
    #     pass
    #
    # def create_config_validator(self) -> ConfigValidatorService:
    #     """Create config validator service."""
    #     pass
    #
    # def create_config_persistence(self) -> ConfigPersistenceService:
    #     """Create config persistence service."""
    #     pass

    # Metrics Services
    def create_pnl_metrics(self) -> PnLMetricsService:
        """Create P&L metrics service."""
        if "pnl_metrics" not in self._services:
            service = PnLMetricsService()
            self._services["pnl_metrics"] = service
            
        service_from_cache = self._services["pnl_metrics"]
        assert isinstance(service_from_cache, PnLMetricsService)
        return service_from_cache

    def create_exposure_metrics(self) -> ExposureMetricsService:
        """Create exposure metrics service."""
        if "exposure_metrics" not in self._services:
            service = ExposureMetricsService()
            self._services["exposure_metrics"] = service
            
        service_from_cache = self._services["exposure_metrics"]
        assert isinstance(service_from_cache, ExposureMetricsService)
        return service_from_cache

    def create_performance_metrics(self) -> PerformanceMetricsService:
        """Create performance metrics service."""
        if "performance_metrics" not in self._services:
            service = PerformanceMetricsService()
            self._services["performance_metrics"] = service
            
        service_from_cache = self._services["performance_metrics"]
        assert isinstance(service_from_cache, PerformanceMetricsService)
        return service_from_cache

    # Service Management
    async def initialize_all(self) -> None:
        """Initialize all created services."""
        for service in self._services.values():
            if hasattr(service, 'initialize'):
                await service.initialize()

    async def shutdown_all(self) -> None:
        """Shutdown all services."""
        for service in self._services.values():
            if hasattr(service, 'shutdown'):
                await service.shutdown()

    def get_service(self, service_name: str) -> Any | None:
        """Get a service by name."""
        return self._services.get(service_name)

    def list_services(self) -> list[str]:
        """List all created service names."""
        return list(self._services.keys())

    def get_service_status(self) -> dict[str, Any]:
        """Get status of all services."""
        status = {}
        for name, service in self._services.items():
            if hasattr(service, 'get_service_status'):
                status[name] = service.get_service_status()
            else:
                status[name] = {"name": name, "status": "unknown"}
        return status