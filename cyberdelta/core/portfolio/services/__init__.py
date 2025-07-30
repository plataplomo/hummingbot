"""Portfolio services for various support functions."""

from .base.base_service import BasePortfolioService, ServiceConfiguration
from .cache import CacheService
from .concurrency_manager import ConcurrencyManager
from .currency import CurrencyConversionService, FXRate

# New focused monitoring services (recommended)
from .monitoring import (
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
from .persistence import (
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
from .pricing import PriceService
from .reconciliation import (
    ReconciliationOrchestrator,
    ReconciliationResult,
)
from .symbol import (
    CacheEntry,
    SymbolCacheService,
    SymbolMetadata,
    SymbolMetadataService,
    SymbolNormalizationService,
    SymbolParsingService,
)

# New type-safe services from clean break refactor
from .type_validation import TypeValidator
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
    # Symbol services
    "CacheEntry",
    "SymbolCacheService",
    "SymbolMetadata",
    "SymbolMetadataService",
    "SymbolNormalizationService",
    "SymbolParsingService",
]
