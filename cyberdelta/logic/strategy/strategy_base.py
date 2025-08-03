"""Base classes and interfaces for trading strategies.

This module provides the foundational classes that all trading strategies
must inherit from, ensuring consistent interfaces and configuration handling.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.models import TradeSignal
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.portfolio.state import PortfolioState

logger = get_logger(__name__)


class BaseStrategy(ABC):
    """Abstract base class for all trading strategies.
    
    This class defines the interface that all strategies must implement
    and provides common functionality for strategy configuration and lifecycle.
    
    Configuration Integration:
    - Receives full AppSettings containing strategy-specific configuration
    - Uses config.strategies for strategy parameters
    - Uses config.general.safe_mode for paper trading awareness
    - Uses config.risk settings for strategy risk awareness
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL strategy parameters from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - Returns typed TradeSignal or None
    - NO assumptions about market conditions
    """
    
    def __init__(self, config: AppSettings):
        """Initialize strategy with configuration.
        
        Args:
            config: Application settings containing all strategy configuration
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Strategy gets full AppSettings, NOT just strategy subset
        - NO hardcoded strategy parameters
        - NO assumptions about strategy configuration structure
        """
        self.config = config
        self.name = self.__class__.__name__
        
        # Cache frequently used settings
        self._safe_mode = config.general.safe_mode
        self._strategy_config = config.strategies
        
        # Strategy-specific configuration will be accessed by subclasses
        # Each strategy is responsible for extracting its own config section
        
        logger.info(
            "strategy_initialized",
            strategy_name=self.name,
            safe_mode=self._safe_mode
        )
    
    async def initialize(self) -> None:
        """Initialize strategy resources and state.
        
        This method is called once when the strategy is first loaded.
        Subclasses can override to perform strategy-specific initialization.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - NO hardcoded initialization parameters
        - Use config for all initialization settings
        - Fail fast if initialization cannot complete
        """
        logger.debug(
            "strategy_initializing",
            strategy_name=self.name
        )
        
        # Subclasses can override for specific initialization
        await self._strategy_initialize()
        
        logger.info(
            "strategy_initialization_completed",
            strategy_name=self.name
        )
    
    @abstractmethod
    async def analyze(
        self, 
        market_data: MarketSnapshot,
        portfolio_state: PortfolioState
    ) -> Optional[TradeSignal]:
        """Analyze market conditions and generate trading signal.
        
        Args:
            market_data: Current market snapshot across all exchanges
            portfolio_state: Current portfolio state across all exchanges
            
        Returns:
            TradeSignal if strategy wants to trade, None otherwise
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed inputs (MarketSnapshot, PortfolioState)
        - Returns typed TradeSignal with Symbol/ExchangeName objects
        - NO string symbols or exchanges in returned signal
        - ALL strategy logic uses config parameters
        - NO hardcoded thresholds or limits
        
        Example Implementation:
        ```python
        async def analyze(self, market_data: MarketSnapshot, portfolio_state: PortfolioState) -> Optional[TradeSignal]:
            # Get strategy-specific config
            momentum_config = self.config.strategies.momentum
            
            # Use Symbol service to get proper Symbol object
            btc_symbol = self._get_strategy_symbol("BTC_USD")
            
            # Get ticker with type safety
            ticker = market_data.get_ticker(ExchangeName.HYPERLIQUID, btc_symbol)
            if not ticker:
                return None
                
            # Use configured threshold, not hardcoded
            if ticker.price_change_24h > momentum_config.price_change_threshold:
                return TradeSignal(
                    symbol=btc_symbol,
                    exchange=ExchangeName.HYPERLIQUID,
                    side=OrderSide.BUY,
                    price=ticker.last_price,
                    confidence=momentum_config.signal_confidence
                )
        ```
        """
        pass
    
    async def cleanup(self) -> None:
        """Cleanup strategy resources.
        
        This method is called when the strategy is being shut down.
        Subclasses can override to perform strategy-specific cleanup.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful cleanup with proper error handling
        - NO assumptions about cleanup order
        """
        logger.debug(
            "strategy_cleanup_starting",
            strategy_name=self.name
        )
        
        # Subclasses can override for specific cleanup
        await self._strategy_cleanup()
        
        logger.info(
            "strategy_cleanup_completed",
            strategy_name=self.name
        )
    
    async def _strategy_initialize(self) -> None:
        """Template method for strategy-specific initialization.
        
        Subclasses can override this method to perform their own initialization
        without having to call super() in the main initialize method.
        """
        pass
    
    async def _strategy_cleanup(self) -> None:
        """Template method for strategy-specific cleanup.
        
        Subclasses can override this method to perform their own cleanup
        without having to call super() in the main cleanup method.
        """
        pass
    
    def get_strategy_name(self) -> str:
        """Get the strategy name.
        
        Returns:
            Strategy class name
        """
        return self.name
    
    def is_safe_mode(self) -> bool:
        """Check if strategy is running in safe mode.
        
        Returns:
            True if safe mode is enabled, False otherwise
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Safe mode setting from config, NOT hardcoded
        - Strategies can use this to adjust behavior
        """
        return self._safe_mode
    
    def _get_strategy_config_section(self, section_name: str) -> object:
        """Get strategy-specific configuration section.
        
        Args:
            section_name: Name of the configuration section
            
        Returns:
            Configuration section object
            
        Raises:
            ValueError: If section not found
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - NO defaults if section missing
        - Fail fast with clear error message
        
        Example:
        ```python
        # In a momentum strategy
        momentum_config = self._get_strategy_config_section("momentum")
        threshold = momentum_config.price_change_threshold
        ```
        """
        if not hasattr(self._strategy_config, section_name):
            raise ValueError(
                f"Strategy '{self.name}' requires config section "
                f"'strategies.{section_name}' which is not configured"
            )
        
        return getattr(self._strategy_config, section_name)
    
    def _validate_strategy_config(self, required_fields: list[str], section_name: str) -> None:
        """Validate that required configuration fields are present.
        
        Args:
            required_fields: List of required field names
            section_name: Configuration section name
            
        Raises:
            ValueError: If any required field is missing
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit validation of required config
        - Fail fast with clear error messages
        - NO silent defaults for critical fields
        
        Example:
        ```python
        # In strategy initialization
        self._validate_strategy_config(
            ["price_change_threshold", "signal_confidence"],
            "momentum"
        )
        ```
        """
        try:
            config_section = self._get_strategy_config_section(section_name)
        except ValueError:
            raise ValueError(
                f"Strategy '{self.name}' requires configuration section "
                f"'strategies.{section_name}'"
            )
        
        missing_fields = []
        for field in required_fields:
            if not hasattr(config_section, field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(
                f"Strategy '{self.name}' missing required config fields "
                f"in 'strategies.{section_name}': {missing_fields}"
            )
        
        logger.debug(
            "strategy_config_validated",
            strategy_name=self.name,
            section=section_name,
            required_fields=required_fields
        )


class StrategyError(Exception):
    """Base exception for strategy-related errors.
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - Explicit error types for different failure modes
    - Clear error messages with context
    """
    pass


class StrategyConfigurationError(StrategyError):
    """Exception raised for strategy configuration errors.
    
    This should be raised when a strategy cannot initialize due to
    missing or invalid configuration.
    """
    pass


class StrategyExecutionError(StrategyError):
    """Exception raised for strategy execution errors.
    
    This should be raised when a strategy encounters an error during
    analysis that prevents signal generation.
    """
    pass


class StrategyValidationError(StrategyError):
    """Exception raised for strategy validation errors.
    
    This should be raised when a strategy detects invalid market data
    or portfolio state that prevents analysis.
    """
    pass