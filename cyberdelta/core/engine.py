import logging
import time
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
from datetime import datetime
import uuid

from .strategy import Strategy
from .types import TradeSignal, Position, PositionStatus, SignalType, MarketData

logger = logging.getLogger(__name__)


class Engine:
    """
    Core trading engine responsible for:
    - Managing strategies
    - Processing market data
    - Executing trade signals
    - Tracking positions
    """
    
    def __init__(self, name: str = "CyberDeltaEngine"):
        self.name = name
        self.strategies: Dict[str, Strategy] = {}
        self.positions: Dict[str, Position] = {}
        self.active_positions: Dict[str, Position] = {}
        self.closed_positions: Dict[str, Position] = {}
        self.signal_handlers: List[Callable[[TradeSignal], None]] = []
        self.is_running = False
        self.start_time: Optional[datetime] = None
        self.last_update_time: Optional[datetime] = None
        
        logger.info(f"Engine '{name}' initialized")
        
    def add_strategy(self, strategy: Strategy) -> None:
        """
        Add a strategy to the engine
        
        Args:
            strategy: Strategy instance to add
        """
        if strategy.name in self.strategies:
            logger.warning(f"Strategy '{strategy.name}' already exists, replacing")
            
        self.strategies[strategy.name] = strategy
        logger.info(f"Added strategy '{strategy.name}' for symbol '{strategy.symbol}'")
        
    def remove_strategy(self, strategy_name: str) -> None:
        """
        Remove a strategy from the engine
        
        Args:
            strategy_name: Name of the strategy to remove
        """
        if strategy_name in self.strategies:
            logger.info(f"Removing strategy '{strategy_name}'")
            del self.strategies[strategy_name]
        else:
            logger.warning(f"Strategy '{strategy_name}' not found")
    
    def register_signal_handler(self, handler: Callable[[TradeSignal], None]) -> None:
        """
        Register a handler for trade signals
        
        Args:
            handler: Callable that takes a TradeSignal
        """
        self.signal_handlers.append(handler)
        logger.info(f"Registered signal handler {handler.__name__}")
        
    def process_market_data(self, data: MarketData) -> None:
        """
        Process market data through all strategies for the relevant symbol
        
        Args:
            data: MarketData object containing market information
        """
        if not self.is_running:
            logger.warning("Engine is not running, ignoring market data")
            return
            
        self.last_update_time = datetime.now()
        
        # Process through relevant strategies
        for strategy in self.strategies.values():
            if strategy.symbol == data.symbol and strategy.enabled:
                signal = strategy.process_data(data)
                if signal:
                    self._handle_signal(signal, strategy.name)
                    
        # Update positions with latest data
        for position_id, position in self.active_positions.items():
            if position.symbol == data.symbol:
                self._update_position_status(position_id, data)
    
    def process_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """
        Process a pandas DataFrame of historical/batch market data
        
        Args:
            df: DataFrame with market data (must have timestamp and OHLCV columns)
            symbol: Symbol this data represents
        """
        required_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        missing = [col for col in required_cols if col not in df.columns]
        
        if missing:
            raise ValueError(f"DataFrame missing required columns: {missing}")
            
        for _, row in df.iterrows():
            data = MarketData(
                symbol=symbol,
                timestamp=row['timestamp'] if isinstance(row['timestamp'], datetime) else 
                           pd.to_datetime(row['timestamp']),
                open=float(row['open']),
                high=float(row['high']),
                low=float(row['low']),
                close=float(row['close']),
                volume=float(row['volume']),
                additional_data={k: v for k, v in row.items() 
                                if k not in required_cols}
            )
            self.process_market_data(data)
    
    def start(self) -> None:
        """Start the trading engine and all enabled strategies"""
        if self.is_running:
            logger.warning("Engine is already running")
            return
            
        logger.info(f"Starting engine '{self.name}'")
        self.is_running = True
        self.start_time = datetime.now()
        
        # Start all enabled strategies
        for strategy in self.strategies.values():
            if strategy.enabled:
                strategy.on_start()
    
    def stop(self) -> None:
        """Stop the trading engine and all strategies"""
        if not self.is_running:
            logger.warning("Engine is not running")
            return
            
        logger.info(f"Stopping engine '{self.name}'")
        self.is_running = False
        
        # Stop all strategies
        for strategy in self.strategies.values():
            if strategy.enabled:
                strategy.on_stop()
    
    def _handle_signal(self, signal: TradeSignal, strategy_name: str) -> Optional[str]:
        """
        Handle a trade signal from a strategy
        
        Args:
            signal: The trading signal
            strategy_name: Name of the strategy that generated the signal
            
        Returns:
            Position ID if a new position was created, None otherwise
        """
        logger.info(f"Received {signal.signal_type} signal from {strategy_name} for {signal.symbol}")
        
        # Notify all registered signal handlers
        for handler in self.signal_handlers:
            try:
                handler(signal)
            except Exception as e:
                logger.error(f"Error in signal handler: {e}")
        
        # Process the signal based on type
        if signal.signal_type == SignalType.BUY:
            return self._open_position(signal, strategy_name, SignalType.BUY)
        elif signal.signal_type == SignalType.SELL:
            return self._open_position(signal, strategy_name, SignalType.SELL)
        elif signal.signal_type == SignalType.CLOSE:
            # Find and close positions for this strategy and symbol
            for pos_id, pos in self.active_positions.items():
                if pos.strategy_name == strategy_name and pos.symbol == signal.symbol:
                    self._close_position(pos_id, signal.price, signal.timestamp)
            return None
            
        return None
    
    def _open_position(self, signal: TradeSignal, strategy_name: str, side: SignalType) -> str:
        """
        Open a new position based on a signal
        
        Args:
            signal: The trading signal
            strategy_name: Name of the originating strategy
            side: BUY or SELL
            
        Returns:
            Position ID
        """
        position_id = str(uuid.uuid4())
        
        position = Position(
            id=position_id,
            symbol=signal.symbol,
            strategy_name=strategy_name,
            entry_price=signal.price,
            quantity=signal.quantity,
            side=side,
            status=PositionStatus.OPEN,
            open_time=signal.timestamp,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            metadata=signal.metadata
        )
        
        self.positions[position_id] = position
        self.active_positions[position_id] = position
        
        logger.info(f"Opened {side.name} position {position_id} for {signal.symbol} at {signal.price}")
        return position_id
    
    def _close_position(self, position_id: str, price: float, timestamp: datetime) -> None:
        """
        Close an existing position
        
        Args:
            position_id: ID of the position to close
            price: Closing price
            timestamp: Closing time
        """
        if position_id not in self.active_positions:
            logger.warning(f"Position {position_id} not found or already closed")
            return
            
        position = self.active_positions[position_id]
        position.status = PositionStatus.CLOSED
        position.close_price = price
        position.close_time = timestamp
        
        # Calculate P&L
        if position.side == SignalType.BUY:
            position.pnl = (price - position.entry_price) * position.quantity
        else:  # SELL
            position.pnl = (position.entry_price - price) * position.quantity
            
        # Move to closed positions
        self.closed_positions[position_id] = position
        del self.active_positions[position_id]
        
        logger.info(f"Closed position {position_id} at {price} with PnL: {position.pnl:.2f}")
    
    def _update_position_status(self, position_id: str, data: MarketData) -> None:
        """
        Update position status based on latest market data (check for TP/SL)
        
        Args:
            position_id: Position ID to update
            data: Latest market data
        """
        if position_id not in self.active_positions:
            return
            
        position = self.active_positions[position_id]
        price = data.close
        
        # Check for stop loss hit
        if position.stop_loss and position.side == SignalType.BUY and price <= position.stop_loss:
            logger.info(f"Stop loss triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
        elif position.stop_loss and position.side == SignalType.SELL and price >= position.stop_loss:
            logger.info(f"Stop loss triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
            
        # Check for take profit hit
        elif position.take_profit and position.side == SignalType.BUY and price >= position.take_profit:
            logger.info(f"Take profit triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
        elif position.take_profit and position.side == SignalType.SELL and price <= position.take_profit:
            logger.info(f"Take profit triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
    
    def get_engine_info(self) -> Dict[str, Any]:
        """
        Get information about the engine's current state
        
        Returns:
            Dictionary with engine information
        """
        return {
            "name": self.name,
            "running": self.is_running,
            "start_time": self.start_time,
            "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            "last_update": self.last_update_time,
            "strategy_count": len(self.strategies),
            "active_strategies": sum(1 for s in self.strategies.values() if s.enabled),
            "active_positions": len(self.active_positions),
            "closed_positions": len(self.closed_positions),
            "total_pnl": sum(p.pnl for p in self.closed_positions.values())
        } 