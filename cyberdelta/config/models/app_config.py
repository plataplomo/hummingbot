"""Main application configuration model.

This module combines all configuration models into the main AppSettings model
used throughout the application.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, computed_field

# Import all configuration components
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.execution_config import ExecutionSettings
from cyberdelta.config.models.funding_strategy_models import StrategiesSettings
from cyberdelta.config.models.general_config import GeneralSettings
from cyberdelta.config.models.monitoring_config import MonitoringSettings
from cyberdelta.config.models.portfolio_config import (
    PortfolioCalculationSettings,
    PortfolioStateSettings,
    PortfolioValidationSettings,
)
from cyberdelta.config.models.risk_config import EnhancedRiskSettings
from cyberdelta.config.models.safety_config import SafetySystemsSettings
from cyberdelta.config.models.simulation_config import SimulationSettings
from cyberdelta.config.models.smart_symbol_models import SmartSymbolsConfig


if TYPE_CHECKING:
    from cyberdelta.config.models.smart_symbol_generator import SmartSymbolGenerator


class AppSettings(BaseModel):
    """Main application configuration model.

    This is the root configuration object that combines all subsystem configurations.
    It is loaded from config.yaml and validated using Pydantic.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Core settings
    general: GeneralSettings = Field(
        default_factory=GeneralSettings, description="General application settings"
    )

    # Exchange configurations
    exchanges: dict[str, ExchangeSpecificConfig] = Field(
        default_factory=dict, description="Exchange-specific configurations"
    )

    # Trading and execution
    strategies: StrategiesSettings = Field(
        default_factory=StrategiesSettings, description="Trading strategy settings"
    )
    risk: EnhancedRiskSettings = Field(
        default_factory=EnhancedRiskSettings, description="Risk management settings"
    )
    execution: ExecutionSettings = Field(
        default_factory=ExecutionSettings, description="Order execution settings"
    )

    # Safety and monitoring
    safety_systems: SafetySystemsSettings = Field(
        default_factory=SafetySystemsSettings, description="Safety systems configuration"
    )
    monitoring: MonitoringSettings = Field(
        default_factory=MonitoringSettings, description="Monitoring and alerting settings"
    )

    # Portfolio management
    calculation: PortfolioCalculationSettings = Field(
        default_factory=PortfolioCalculationSettings, description="Portfolio calculation settings"
    )
    validation: PortfolioValidationSettings = Field(
        default_factory=PortfolioValidationSettings, description="Portfolio validation settings"
    )
    state: PortfolioStateSettings = Field(
        default_factory=PortfolioStateSettings, description="Portfolio state management settings"
    )

    # Simulation settings (for paper trading/safe mode)
    simulation: SimulationSettings = Field(
        default_factory=SimulationSettings, description="Simulation mode settings for paper trading"
    )

    # Symbol configuration
    symbols: SmartSymbolsConfig = Field(
        default_factory=SmartSymbolsConfig, description="Smart symbol configuration"
    )

    @property
    @computed_field
    def symbol_generator(self) -> SmartSymbolGenerator:
        """Get the symbol generator for this configuration."""
        # Lazy import to avoid circular dependency
        from cyberdelta.config.models.smart_symbol_generator import SmartSymbolGenerator

        return SmartSymbolGenerator(self.symbols)

    def get_exchange_config(self, exchange_name: str) -> ExchangeSpecificConfig | None:
        """Get configuration for a specific exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            Exchange configuration if found, None otherwise
        """
        return self.exchanges.get(exchange_name.lower())

    def get_enabled_exchanges(self) -> list[str]:
        """Get list of enabled exchange names.

        Returns:
            List of exchange names that are enabled
        """
        return [name for name, config in self.exchanges.items() if config.enabled]

    def is_safe_mode(self) -> bool:
        """Check if application is in safe mode (paper trading).

        Returns:
            True if safe mode is enabled
        """
        return self.general.safe_mode

    def get_simulation_settings(self) -> SimulationSettings:
        """Get simulation settings for safe mode.

        Returns:
            Simulation settings to use when in safe mode
        """
        return self.simulation

    def get_max_position_usd(self) -> float:
        """Get maximum position size in USD.

        Returns:
            Maximum position size from risk settings
        """
        return float(self.risk.global_risk.max_position_usd)

    def get_max_total_exposure_usd(self) -> float:
        """Get maximum total exposure in USD.

        Returns:
            Maximum total exposure from risk settings
        """
        return float(self.risk.global_risk.max_total_exposure_usd)

    def should_halt_trading(self) -> bool:
        """Check if trading should be halted based on safety systems.

        Returns:
            True if any safety system indicates trading should halt
        """
        return self.safety_systems.should_halt_trading()
