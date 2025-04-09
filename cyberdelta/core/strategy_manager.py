import logging
from typing import Dict, List, Optional, Any, Set, Type
from datetime import datetime

from .strategy import Strategy
from .types import TradeSignal, MarketData
from .risk_manager import RiskManager

logger = logging.getLogger(__name__)


class StrategyManager:
    """
    Manages multiple trading strategies, controlling their lifecycle,
    configuration, and execution. Acts as a central coordinator for
    strategy operations including:
    
    - Registration/deregistration of strategies
    - Enabling/disabling strategies
    - Distributing market data to appropriate strategies
    - Collecting and prioritizing trade signals
    - Performance tracking and metrics collection
    """
    
    def __init__(self, risk_manager: Optional[RiskManager] = None):
        """
        Initialize the StrategyManager.
        
        Args:
            risk_manager: Optional risk manager to handle position sizing and risk controls
        """
        self.strategies: Dict[str, Strategy] = {}
        self.enabled_strategies: Set[str] = set()
        self.active_symbols: Set[str] = set()
        self.risk_manager = risk_manager
        self.last_update_time: Optional[datetime] = None
        
        logger.info("StrategyManager initialized")
    
    def register_strategy(self, strategy: Strategy) -> None:
        """
        Register a strategy with the manager.
        
        Args:
            strategy: Strategy instance to register
        """
        if strategy.name in self.strategies:
            logger.warning(f"Strategy '{strategy.name}' already exists, replacing")
            
        self.strategies[strategy.name] = strategy
        self.active_symbols.add(strategy.symbol)
        logger.info(f"Registered strategy '{strategy.name}' for symbol '{strategy.symbol}'")
    
    def unregister_strategy(self, strategy_name: str) -> None:
        """
        Unregister a strategy from the manager.
        
        Args:
            strategy_name: Name of the strategy to unregister
        """
        if strategy_name not in self.strategies:
            logger.warning(f"Strategy '{strategy_name}' not found")
            return
            
        strategy = self.strategies[strategy_name]
        del self.strategies[strategy_name]
        self.enabled_strategies.discard(strategy_name)
        
        # Update active symbols
        self._refresh_active_symbols()
        
        logger.info(f"Unregistered strategy '{strategy_name}'")
    
    def enable_strategy(self, strategy_name: str) -> bool:
        """
        Enable a registered strategy.
        
        Args:
            strategy_name: Name of the strategy to enable
            
        Returns:
            True if successful, False otherwise
        """
        if strategy_name not in self.strategies:
            logger.warning(f"Cannot enable non-existent strategy '{strategy_name}'")
            return False
            
        strategy = self.strategies[strategy_name]
        strategy.enable()
        self.enabled_strategies.add(strategy_name)
        logger.info(f"Enabled strategy '{strategy_name}'")
        return True
    
    def disable_strategy(self, strategy_name: str) -> bool:
        """
        Disable a registered strategy.
        
        Args:
            strategy_name: Name of the strategy to disable
            
        Returns:
            True if successful, False otherwise
        """
        if strategy_name not in self.strategies:
            logger.warning(f"Cannot disable non-existent strategy '{strategy_name}'")
            return False
            
        strategy = self.strategies[strategy_name]
        strategy.disable()
        self.enabled_strategies.discard(strategy_name)
        logger.info(f"Disabled strategy '{strategy_name}'")
        return True
    
    def process_market_data(self, data: MarketData) -> List[TradeSignal]:
        """
        Process market data through all enabled strategies that match the symbol.
        
        Args:
            data: Market data to process
            
        Returns:
            List of trade signals generated from strategies
        """
        signals: List[TradeSignal] = []
        self.last_update_time = datetime.now()
        
        # Only process data for symbols we're actually tracking
        if data.symbol not in self.active_symbols:
            return signals
            
        # Update historical data for all strategies tracking this symbol
        for strategy_name, strategy in self.strategies.items():
            if strategy.symbol == data.symbol:
                strategy.update_historical_data(data)
        
        # Process data through enabled strategies only
        for strategy_name in self.enabled_strategies:
            strategy = self.strategies[strategy_name]
            if strategy.symbol != data.symbol:
                continue
                
            try:
                signal = strategy.process_data(data)
                if signal is not None:
                    # If we have a risk manager, apply position sizing
                    if self.risk_manager is not None:
                        signal = self.risk_manager.size_signal(signal)
                    signals.append(signal)
            except Exception as e:
                logger.error(f"Error processing data in strategy '{strategy_name}': {str(e)}")
                
        return signals
    
    def get_strategies_for_symbol(self, symbol: str) -> List[Strategy]:
        """
        Get all strategies registered for a specific symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List of strategies for the symbol
        """
        return [s for s in self.strategies.values() if s.symbol == symbol]
    
    def get_enabled_strategies(self) -> List[Strategy]:
        """
        Get all currently enabled strategies.
        
        Returns:
            List of enabled strategies
        """
        return [self.strategies[name] for name in self.enabled_strategies]
    
    def get_strategy_performance(self) -> Dict[str, Dict[str, Any]]:
        """
        Get performance metrics for all strategies.
        
        Returns:
            Dictionary of strategy names to performance metrics
        """
        performance = {}
        for name, strategy in self.strategies.items():
            if hasattr(strategy, 'performance_metrics'):
                performance[name] = strategy.performance_metrics
            else:
                performance[name] = {
                    'signals_generated': strategy.signals_generated,
                    'last_signal_time': strategy.last_signal_time
                }
        return performance
    
    def start_all(self) -> None:
        """Start all registered strategies."""
        for name, strategy in self.strategies.items():
            strategy.on_start()
            if strategy.enabled:
                self.enabled_strategies.add(name)
        logger.info(f"Started all strategies ({len(self.strategies)} total)")
    
    def stop_all(self) -> None:
        """Stop all registered strategies."""
        for strategy in self.strategies.values():
            strategy.on_stop()
        self.enabled_strategies.clear()
        logger.info("Stopped all strategies")
    
    def _refresh_active_symbols(self) -> None:
        """Update the set of active symbols based on registered strategies."""
        self.active_symbols = {s.symbol for s in self.strategies.values()} 