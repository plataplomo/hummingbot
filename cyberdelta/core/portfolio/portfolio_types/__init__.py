"""Portfolio type definitions and data models."""

from .calculation_types import (
    ExposureResult,
    PerformanceMetrics,
    PortfolioExposureResult,
    PortfolioUnrealizedPnLResult,
    RealizedPnLResult,
    UnrealizedPnLResult,
)
from .domain_models import (
    ConfigurationData,
    ErrorContext,
    MetricsData,
    OperationContext,
    ValidationContext,
)
from .manager_protocols import (
    BalanceManagerProtocol,
    OrderManagerProtocol,
    PortfolioManagerProtocol as PortfolioStateManagerProtocol,
    PositionManagerProtocol,
)
from .portfolio_models import (
    ComponentHealth,
    ExchangeSummary,
    HealthStatus,
    PortfolioComponentType,
    PortfolioSnapshot,
    PortfolioUpdate,
    RiskParameters,
    TradingSession,
)
from .service_protocols import (
    CacheServiceProtocol,
    PersistenceServiceProtocol,
    PriceServiceProtocol,
    SymbolServiceProtocol,
)
from .state_types import (
    StateChange,
    StateChangeType,
    StateContainer,
    StateSnapshot,
    StateSummary,
)


__all__ = [
    # Manager protocols
    "BalanceManagerProtocol",
    # Service protocols
    "CacheServiceProtocol",
    "ComponentHealth",
    # Domain models
    "ConfigurationData",
    "ErrorContext",
    "ExchangeSummary",
    # Calculation types
    "ExposureResult",
    "HealthStatus",
    "MetricsData",
    "OperationContext",
    "OrderManagerProtocol",
    "PerformanceMetrics",
    "PersistenceServiceProtocol",
    "PortfolioComponentType",
    "PortfolioExposureResult",
    # Portfolio models
    "PortfolioSnapshot",
    "PortfolioStateManagerProtocol",
    "PortfolioUnrealizedPnLResult",
    "PortfolioUpdate",
    "PositionManagerProtocol",
    "PriceServiceProtocol",
    "RealizedPnLResult",
    "RiskParameters",
    # State types
    "StateChange",
    "StateChangeType",
    "StateContainer",
    "StateSnapshot",
    "StateSummary",
    "SymbolServiceProtocol",
    "TradingSession",
    "UnrealizedPnLResult",
    "ValidationContext",
]
