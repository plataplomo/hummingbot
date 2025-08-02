"""Portfolio services for various support functions."""

from cyberdelta.core.infrastructure.services.base_service import BaseService as BasePortfolioService, ServiceConfiguration
from cyberdelta.core.infrastructure.cache.cache_service import CacheService
from cyberdelta.core.infrastructure.concurrency.concurrency_manager import ConcurrencyManager
from cyberdelta.core.integrations.currency import CurrencyConversionService, FXRate

# New focused monitoring services (recommended)
from cyberdelta.core.monitoring.health import (
    AlertLifecycleService,
    AlertSeverity,
    AlertThreshold,
    AlertThresholdManager,
    ApplicationMetrics,
    BaseHealthCheckMixin,
    HealthAlert,
    HealthAlertCoordinator,
    HealthCheckDetails,
    HealthCheckOrchestrator,
    HealthCheckResult,
    HealthCheckable,
    HealthMetricsCollector,
    HealthStatus,
    PortfolioMetrics,
    SystemMetrics,
    create_health_check_decorator,
)

# New focused persistence services
from cyberdelta.core.data_management.persistence import (
    BackupManager,
    PersistenceConfig,
    PersistenceStats,
    PydanticJSONSerializer,
    SimplePersistenceManager,
    StateSerializer,
    TypedStatePersistenceError,
    create_persistence_config,
    create_persistence_manager,
)

from .portfolio_service_factory import PortfolioServiceFactory
from cyberdelta.core.integrations.pricing import PriceService
from .reconciliation import (
    ReconciliationOrchestrator,
    ReconciliationResult,
)

# New type-safe services from clean break refactor
from cyberdelta.core.infrastructure.middleware.type_validation import TypeValidator
from .validation_middleware import ValidationMiddleware


__all__ = [
    # Core services
    "BasePortfolioService",
    "ServiceConfiguration",
    "CacheService",
    "ConcurrencyManager",
    "CurrencyConversionService",
    "FXRate",
    "PortfolioServiceFactory",
    "PriceService",
    "ReconciliationOrchestrator",
    "ReconciliationResult",
    "TypeValidator",
    "ValidationMiddleware",
    # New focused monitoring services
    "AlertLifecycleService",
    "AlertSeverity",
    "AlertThreshold", 
    "AlertThresholdManager",
    "ApplicationMetrics",
    "BaseHealthCheckMixin",
    "HealthAlert",
    "HealthAlertCoordinator",
    "HealthCheckDetails",
    "HealthCheckOrchestrator",
    "HealthCheckResult",
    "HealthCheckable",
    "HealthMetricsCollector",
    "HealthStatus",
    "PortfolioMetrics",
    "SystemMetrics",
    "create_health_check_decorator",
    # New focused persistence services
    "BackupManager",
    "PersistenceConfig",
    "PersistenceStats",
    "PydanticJSONSerializer",
    "SimplePersistenceManager",
    "StateSerializer",
    "TypedStatePersistenceError",
    "create_persistence_config",
    "create_persistence_manager",
]
