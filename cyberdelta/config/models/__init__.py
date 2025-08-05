"""Configuration Models Package for CyberDeltaEngine.

This package contains all Pydantic models for application configuration.
The models are now organized into logical modules for better maintainability.
"""

# Keep backward compatibility by importing AppSettings
from .app_config import AppSettings

# Import new modular configs
from .config_types import ConfigDecimal, NonEmptyConfigString, StringForLiteral
from .exchange_config import AddressActionSafetyNetConfig, ExchangeSpecificConfig
from .execution_config import ExecutionCompensationSettings, ExecutionSettings
from .fee_config import FeeStructureConfig
from .funding_strategy_models import (
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from .general_config import GeneralSettings
from .market_data_config import (
    MarketDataAggregationSettings,
    MarketDataCacheSettings,
    MarketDataFetchSettings,
    MarketDataSettings,
)
from .monitoring_config import MonitoringSettings
from .portfolio_config import (
    PortfolioCacheSettings,
    PortfolioCalculationSettings,
    PortfolioStateSettings,
    PortfolioValidationSettings,
)
from .risk_config import (
    CheckerSettings,
    CheckerThresholds,
    EnhancedRiskSettings,
    GlobalRiskSettings,
    RiskSettings,
    SizingSettings,
)
from .safety_config import (
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    PositionReconciliationSettings,
    SafetySystemsSettings,
)
from .simulation_config import SimulationSettings


__all__ = [
    # Original exports for backward compatibility
    "AppSettings",
    "ConfigDecimal",
    "NonEmptyConfigString",
    "StringForLiteral",
    "StrategiesSettings",
    "StrategyConfigHLPerpBPSpot",
    "StrategyParamsHLPerpBPSpot",
    # New modular exports
    "ExchangeSpecificConfig",
    "ExecutionSettings",
    "ExecutionCompensationSettings",
    "FeeStructureConfig",
    "GeneralSettings",
    "AddressActionSafetyNetConfig",
    "MarketDataSettings",
    "MarketDataCacheSettings",
    "MarketDataFetchSettings",
    "MarketDataAggregationSettings",
    "MonitoringSettings",
    "PortfolioCacheSettings",
    "PortfolioCalculationSettings",
    "PortfolioStateSettings",
    "PortfolioValidationSettings",
    "EnhancedRiskSettings",
    "RiskSettings",
    "GlobalRiskSettings",
    "CheckerSettings",
    "CheckerThresholds",
    "SizingSettings",
    "SafetySystemsSettings",
    "CircuitBreakerSettings",
    "PositionReconciliationSettings",
    "BalanceMonitoringSettings",
    "SimulationSettings",
]
