"""Strategy Factory for CyberDeltaEngine.

This module implements a factory pattern for creating trading strategies with
enhanced Pydantic validation and configuration management.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.models.funding_strategy_models import StrategyParamsHLPerpBPSpot
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.strategy import Strategy
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy


logger = get_logger(__name__)


class StrategyCreationError(Exception):
    """Raised when strategy creation fails."""

    pass


class StrategyFactory:
    """Factory for creating trading strategies with validated configuration."""

    def __init__(self, config: AppSettings) -> None:
        """Initialize the strategy factory.

        Args:
            config: Application configuration with validated parameters
        """
        self.config = config

    def create_hl_perp_bp_spot_strategy(
        self,
        name: str,
        symbol: str,
        data_handler: DataHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager | None = None,
    ) -> FundingRateArbitrageStrategy:
        """Create a Hyperliquid Perpetual vs Backpack Spot funding arbitrage strategy.

        Args:
            name: Unique name for the strategy instance
            symbol: Trading symbol (e.g., "HYPE")
            data_handler: Data handler for market data access
            portfolio_tracker: Portfolio tracker for position management
            risk_manager: Optional risk manager for position sizing

        Returns:
            Configured FundingRateArbitrageStrategy instance

        Raises:
            StrategyCreationError: If strategy creation fails
        """
        try:
            # Get validated strategy configuration
            strategy_config = self.config.strategies.hl_perp_bp_spot

            # Validate the configuration is enabled
            if not strategy_config.enabled:
                raise StrategyCreationError("HL Perp BP Spot strategy is disabled in configuration")

            # Convert Pydantic model to parameters dict
            params = self._convert_strategy_params_to_dict(strategy_config.params)

            # Create strategy instance
            strategy = FundingRateArbitrageStrategy(
                name=name,
                symbol=symbol,
                data_handler=data_handler,
                portfolio_tracker=portfolio_tracker,
                risk_manager=risk_manager,
                params=params,
            )

            logger.info(
                "strategy_created_via_factory",
                strategy_name=name,
                strategy_type="hl_perp_bp_spot",
                symbol=symbol,
                params=params,
                action="strategy_instantiated",
                message=f"Created {name} strategy for {symbol} via factory",
            )

            return strategy

        except Exception as e:
            error_msg = f"Failed to create HL Perp BP Spot strategy '{name}': {e}"
            logger.error(
                "strategy_creation_failed",
                strategy_name=name,
                strategy_type="hl_perp_bp_spot",
                symbol=symbol,
                error=str(e),
                action="strategy_creation_error",
                message=error_msg,
            )
            raise StrategyCreationError(error_msg) from e

    def create_strategy(
        self,
        strategy_type: str,
        name: str,
        symbol: str,
        data_handler: DataHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager | None = None,
    ) -> Strategy:
        """Create a strategy of the specified type.

        Args:
            strategy_type: Type of strategy to create ("hl_perp_bp_spot")
            name: Unique name for the strategy instance
            symbol: Trading symbol
            data_handler: Data handler for market data access
            portfolio_tracker: Portfolio tracker for position management
            risk_manager: Optional risk manager for position sizing

        Returns:
            Configured Strategy instance

        Raises:
            StrategyCreationError: If strategy type is unknown or creation fails
        """
        if strategy_type == "hl_perp_bp_spot":
            return self.create_hl_perp_bp_spot_strategy(
                name=name,
                symbol=symbol,
                data_handler=data_handler,
                portfolio_tracker=portfolio_tracker,
                risk_manager=risk_manager,
            )
        else:
            raise StrategyCreationError(f"Unknown strategy type: {strategy_type}")

    def _convert_strategy_params_to_dict(
        self, params: StrategyParamsHLPerpBPSpot
    ) -> dict[str, Any]:
        """Convert Pydantic strategy parameters to dictionary.

        Args:
            params: Validated Pydantic parameters model

        Returns:
            Dictionary of parameters suitable for strategy constructor
        """
        # Convert to dict and handle Decimal types
        params_dict = params.model_dump()

        # Convert Decimal values to float for strategy consumption
        converted_params = {}
        for key, value in params_dict.items():
            if hasattr(value, "is_finite"):  # Decimal type
                converted_params[key] = float(value)
            else:
                converted_params[key] = value

        return converted_params

    def get_available_strategy_types(self) -> list[str]:
        """Get list of available strategy types.

        Returns:
            List of supported strategy type names
        """
        return ["hl_perp_bp_spot"]

    def validate_strategy_config(self, strategy_type: str) -> bool:
        """Validate that a strategy type has proper configuration.

        Args:
            strategy_type: Type of strategy to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            if strategy_type == "hl_perp_bp_spot":
                # Configuration is already validated by Pydantic during config loading
                config = self.config.strategies.hl_perp_bp_spot
                return config.enabled
            else:
                return False
        except Exception as e:
            logger.warning(
                "strategy_config_validation_failed",
                strategy_type=strategy_type,
                error=str(e),
                action="config_validation_error",
                message=f"Strategy config validation failed for {strategy_type}: {e}",
            )
            return False
