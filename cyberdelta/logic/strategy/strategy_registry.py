"""Strategy registry for dynamic strategy discovery and management.

This module provides the StrategyRegistry class that handles strategy
registration, configuration, and enable/disable management following
the configuration-first principle with no auto-discovery.
"""

from __future__ import annotations

from typing import Dict, List, Type, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.logic.strategy.strategy_base import (
    BaseStrategy, 
    StrategyConfigurationError,
    StrategyError
)
from cyberdelta.logic.strategy.momentum_strategy import MomentumStrategy

logger = get_logger(__name__)


class StrategyRegistry:
    """Registry for strategy registration and configuration management.
    
    This registry manages the lifecycle of trading strategies including:
    - Strategy class registration
    - Strategy instance creation with configuration
    - Enable/disable management based on configuration
    - Strategy validation and initialization
    
    Configuration Usage:
    - Uses config.strategies.enabled_strategies to determine which strategies to load
    - Each strategy gets the full AppSettings for its configuration needs
    - NO auto-discovery - strategies must be explicitly registered and enabled
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - Only loads explicitly enabled strategies from config
    - NO defaults for strategy parameters
    - Fail fast if strategy configuration is invalid
    - NO assumptions about strategy availability
    """
    
    def __init__(self, config: AppSettings):
        """Initialize strategy registry with configuration.
        
        Args:
            config: Application settings containing strategy configuration
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Loads ONLY explicitly enabled strategies from config
        - NO auto-discovery or implicit strategy loading
        - Fail fast if enabled strategy is not available
        """
        self.config = config
        self._strategy_config = config.strategies
        
        # Get enabled strategies from config - NO defaults
        self._enabled_strategies = self._strategy_config.enabled_strategies
        
        # Registry of available strategy classes
        self._available_strategies: Dict[str, Type[BaseStrategy]] = {}
        
        # Registry of active strategy instances
        self._active_strategies: Dict[str, BaseStrategy] = {}
        
        # Initialize with built-in strategies
        self._register_builtin_strategies()
        
        logger.info(
            "strategy_registry_initialized",
            enabled_strategies=self._enabled_strategies,
            available_strategies=list(self._available_strategies.keys())
        )
    
    def _register_builtin_strategies(self) -> None:
        """Register built-in strategy classes.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit registration of each strategy class
        - NO dynamic discovery or reflection
        - Strategy names match configuration exactly
        """
        # Register momentum strategy
        self._available_strategies["momentum"] = MomentumStrategy
        
        logger.debug(
            "builtin_strategies_registered",
            strategies=list(self._available_strategies.keys())
        )
    
    def register_strategy_class(self, name: str, strategy_class: Type[BaseStrategy]) -> None:
        """Register a custom strategy class.
        
        Args:
            name: Strategy name (must match config key)
            strategy_class: Strategy class that extends BaseStrategy
            
        Raises:
            StrategyError: If strategy class is invalid
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit registration only, NO auto-discovery
        - Strategy name must exactly match configuration key
        - Validates strategy class before registration
        """
        if not issubclass(strategy_class, BaseStrategy):
            raise StrategyError(
                f"Strategy class {strategy_class.__name__} must extend BaseStrategy"
            )
        
        if name in self._available_strategies:
            logger.warning(
                "strategy_class_replaced",
                strategy_name=name,
                old_class=self._available_strategies[name].__name__,
                new_class=strategy_class.__name__
            )
        
        self._available_strategies[name] = strategy_class
        
        logger.info(
            "strategy_class_registered",
            strategy_name=name,
            strategy_class=strategy_class.__name__
        )
    
    async def initialize_enabled_strategies(self) -> List[BaseStrategy]:
        """Initialize all enabled strategies from configuration.
        
        Returns:
            List of initialized strategy instances
            
        Raises:
            StrategyConfigurationError: If any enabled strategy cannot be initialized
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Only initializes strategies listed in config.strategies.enabled_strategies
        - Fail fast if any enabled strategy is not available or fails to initialize
        - NO silent skipping of failed strategies
        """
        initialized_strategies = []
        
        for strategy_name in self._enabled_strategies:
            try:
                strategy_instance = await self._initialize_strategy(strategy_name)
                initialized_strategies.append(strategy_instance)
                self._active_strategies[strategy_name] = strategy_instance
                
            except Exception as e:
                # Fail fast - don't continue if any enabled strategy fails
                raise StrategyConfigurationError(
                    f"Failed to initialize enabled strategy '{strategy_name}': {e}"
                ) from e
        
        logger.info(
            "enabled_strategies_initialized",
            strategy_count=len(initialized_strategies),
            strategy_names=[s.get_strategy_name() for s in initialized_strategies]
        )
        
        return initialized_strategies
    
    async def _initialize_strategy(self, strategy_name: str) -> BaseStrategy:
        """Initialize a specific strategy by name.
        
        Args:
            strategy_name: Name of strategy to initialize
            
        Returns:
            Initialized strategy instance
            
        Raises:
            StrategyConfigurationError: If strategy cannot be initialized
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Strategy must be explicitly registered
        - Strategy gets full AppSettings for configuration
        - Fail fast if strategy class not found or initialization fails
        """
        if strategy_name not in self._available_strategies:
            available_names = list(self._available_strategies.keys())
            raise StrategyConfigurationError(
                f"Strategy '{strategy_name}' not available. "
                f"Available strategies: {available_names}"
            )
        
        strategy_class = self._available_strategies[strategy_name]
        
        logger.debug(
            "initializing_strategy",
            strategy_name=strategy_name,
            strategy_class=strategy_class.__name__
        )
        
        try:
            # Create strategy instance with full AppSettings
            strategy_instance = strategy_class(self.config)
            
            # Initialize strategy-specific resources
            await strategy_instance.initialize()
            
            logger.info(
                "strategy_initialized",
                strategy_name=strategy_name,
                strategy_class=strategy_class.__name__,
                safe_mode=strategy_instance.is_safe_mode()
            )
            
            return strategy_instance
            
        except Exception as e:
            logger.error(
                "strategy_initialization_failed",
                strategy_name=strategy_name,
                strategy_class=strategy_class.__name__,
                error=str(e),
                exc_info=True
            )
            raise StrategyConfigurationError(
                f"Strategy '{strategy_name}' initialization failed: {e}"
            ) from e
    
    def get_active_strategies(self) -> List[BaseStrategy]:
        """Get list of currently active strategy instances.
        
        Returns:
            List of active strategy instances
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns only successfully initialized strategies
        - NO assumptions about strategy availability
        """
        return list(self._active_strategies.values())
    
    def get_active_strategy_names(self) -> List[str]:
        """Get list of currently active strategy names.
        
        Returns:
            List of active strategy names
        """
        return list(self._active_strategies.keys())
    
    def get_strategy_by_name(self, strategy_name: str) -> Optional[BaseStrategy]:
        """Get active strategy instance by name.
        
        Args:
            strategy_name: Name of strategy to retrieve
            
        Returns:
            Strategy instance if active, None otherwise
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about strategy existence
        - Returns None rather than raising exception for missing strategy
        """
        return self._active_strategies.get(strategy_name)
    
    async def disable_strategy(self, strategy_name: str) -> bool:
        """Disable and cleanup a specific strategy.
        
        Args:
            strategy_name: Name of strategy to disable
            
        Returns:
            True if strategy was disabled, False if not active
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful cleanup with proper error handling
        - NO assumptions about cleanup order
        """
        if strategy_name not in self._active_strategies:
            logger.warning(
                "strategy_disable_not_active",
                strategy_name=strategy_name
            )
            return False
        
        strategy_instance = self._active_strategies[strategy_name]
        
        try:
            # Cleanup strategy resources
            await strategy_instance.cleanup()
            
            # Remove from active registry
            del self._active_strategies[strategy_name]
            
            logger.info(
                "strategy_disabled",
                strategy_name=strategy_name
            )
            
            return True
            
        except Exception as e:
            logger.error(
                "strategy_disable_error",
                strategy_name=strategy_name,
                error=str(e),
                exc_info=True
            )
            # Still remove from registry even if cleanup failed
            del self._active_strategies[strategy_name]
            return True
    
    async def shutdown_all_strategies(self) -> None:
        """Shutdown and cleanup all active strategies.
        
        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful shutdown with proper error handling
        - Continue cleanup even if individual strategies fail
        - NO assumptions about shutdown order
        """
        strategy_names = list(self._active_strategies.keys())
        
        logger.info(
            "shutting_down_all_strategies",
            strategy_count=len(strategy_names),
            strategy_names=strategy_names
        )
        
        for strategy_name in strategy_names:
            try:
                await self.disable_strategy(strategy_name)
            except Exception as e:
                logger.error(
                    "strategy_shutdown_error",
                    strategy_name=strategy_name,
                    error=str(e),
                    exc_info=True
                )
                # Continue with other strategies
        
        logger.info("all_strategies_shutdown_completed")
    
    def get_registry_status(self) -> Dict[str, object]:
        """Get current registry status and configuration.
        
        Returns:
            Dictionary with registry status information
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns structured status information
        - Configuration context included
        """
        return {
            "enabled_strategies": self._enabled_strategies,
            "available_strategy_classes": list(self._available_strategies.keys()),
            "active_strategies": list(self._active_strategies.keys()),
            "strategy_count": len(self._active_strategies),
            "safe_mode": self.config.general.safe_mode,
            "configuration": {
                "enabled_strategies": self._enabled_strategies
            }
        }
    
    def validate_configuration(self) -> List[str]:
        """Validate strategy configuration without initializing strategies.
        
        Returns:
            List of validation errors (empty if valid)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Validates configuration before attempting initialization
        - Returns explicit error messages
        - NO silent validation failures
        """
        validation_errors = []
        
        # Check that all enabled strategies are available
        for strategy_name in self._enabled_strategies:
            if strategy_name not in self._available_strategies:
                available_names = list(self._available_strategies.keys())
                validation_errors.append(
                    f"Enabled strategy '{strategy_name}' not available. "
                    f"Available: {available_names}"
                )
        
        # Check for duplicate strategy names
        if len(self._enabled_strategies) != len(set(self._enabled_strategies)):
            duplicates = [name for name in self._enabled_strategies 
                         if self._enabled_strategies.count(name) > 1]
            validation_errors.append(
                f"Duplicate strategy names in enabled_strategies: {duplicates}"
            )
        
        # Validate strategy-specific configuration
        for strategy_name in self._enabled_strategies:
            if strategy_name in self._available_strategies:
                strategy_class = self._available_strategies[strategy_name]
                try:
                    # Create temporary instance to validate config
                    # This will raise an exception if config is invalid
                    temp_instance = strategy_class(self.config)
                    logger.debug(
                        "strategy_config_validation_passed",
                        strategy_name=strategy_name
                    )
                except Exception as e:
                    validation_errors.append(
                        f"Strategy '{strategy_name}' configuration invalid: {e}"
                    )
        
        if validation_errors:
            logger.warning(
                "strategy_configuration_validation_failed",
                error_count=len(validation_errors),
                errors=validation_errors
            )
        else:
            logger.info("strategy_configuration_validation_passed")
        
        return validation_errors