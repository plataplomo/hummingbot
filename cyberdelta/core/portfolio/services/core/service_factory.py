"""Service factory for creating focused services."""

from __future__ import annotations

from typing import Any

from ..analytics.performance_analytics import PerformanceAnalyticsService
from ..analytics.reporting_service import ReportingService
from ..config.config_loader import ConfigLoaderService
from ..config.config_validator import ConfigValidatorService
from ..config.config_persistence import ConfigPersistenceService
from ..metrics.pnl_metrics import PnLMetricsService
from ..metrics.exposure_metrics import ExposureMetricsService
from ..metrics.performance_metrics import PerformanceMetricsService


class PortfolioServiceFactory:
    """Creates and manages portfolio services."""

    def __init__(self) -> None:
        self._services: dict[str, Any] = {}

    # Analytics Services
    def create_performance_analytics(self) -> PerformanceAnalyticsService:
        """Create performance analytics service."""
        if "performance_analytics" not in self._services:
            self._services["performance_analytics"] = PerformanceAnalyticsService()
        return self._services["performance_analytics"]

    def create_reporting_service(self) -> ReportingService:
        """Create reporting service."""
        if "reporting" not in self._services:
            self._services["reporting"] = ReportingService()
        return self._services["reporting"]

    # Config Services
    def create_config_loader(self) -> ConfigLoaderService:
        """Create config loader service."""
        if "config_loader" not in self._services:
            self._services["config_loader"] = ConfigLoaderService()
        return self._services["config_loader"]

    def create_config_validator(self) -> ConfigValidatorService:
        """Create config validator service."""
        if "config_validator" not in self._services:
            self._services["config_validator"] = ConfigValidatorService()
        return self._services["config_validator"]

    def create_config_persistence(self) -> ConfigPersistenceService:
        """Create config persistence service."""
        if "config_persistence" not in self._services:
            self._services["config_persistence"] = ConfigPersistenceService()
        return self._services["config_persistence"]

    # Metrics Services
    def create_pnl_metrics(self) -> PnLMetricsService:
        """Create P&L metrics service."""
        if "pnl_metrics" not in self._services:
            self._services["pnl_metrics"] = PnLMetricsService()
        return self._services["pnl_metrics"]

    def create_exposure_metrics(self) -> ExposureMetricsService:
        """Create exposure metrics service."""
        if "exposure_metrics" not in self._services:
            self._services["exposure_metrics"] = ExposureMetricsService()
        return self._services["exposure_metrics"]

    def create_performance_metrics(self) -> PerformanceMetricsService:
        """Create performance metrics service."""
        if "performance_metrics" not in self._services:
            self._services["performance_metrics"] = PerformanceMetricsService()
        return self._services["performance_metrics"]

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