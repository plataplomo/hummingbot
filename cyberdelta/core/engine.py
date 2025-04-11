from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import pandas as pd
import structlog

if TYPE_CHECKING:
    from cyberdelta.core.models import MarketData, OrderSide, Position, SignalType, TradeSignal
from cyberdelta.utils.constants import PositionStatus

from .strategy import Strategy

logger = structlog.get_logger(__name__)


class Engine:
    """
    Core trading engine responsible for:
    - Managing strategies
    - Processing market data
    - Executing trade signals
    - Tracking positions
    """

    def __init__(self, name: str = "CyberDeltaEngine") -> None:
        self.name = name
        self.strategies: dict[str, Strategy] = {}
        self.positions: dict[str, Position] = {}
        self.active_positions: dict[str, Position] = {}
        self.closed_positions: list[Position] = []
        self.signal_handlers: list[Callable[[TradeSignal], None]] = []
        self.is_running = False
        self.start_time: datetime | None = None
        self.last_update_time: datetime | None = None
        self.logger = structlog.get_logger(engine_name=name)

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
        required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            raise ValueError(f"DataFrame missing required columns: {missing}")

        for _, row in df.iterrows():
            # Convert to Decimal safely
            try:
                open_p = Decimal(str(row["open"]))
                high_p = Decimal(str(row["high"]))
                low_p = Decimal(str(row["low"]))
                close_p = Decimal(str(row["close"]))
                volume_p = Decimal(str(row["volume"]))
            except (InvalidOperation, TypeError) as e:
                self.logger.error("Error converting DataFrame row to Decimal", row=row, error=e)
                continue # Skip this row if conversion fails

            data = MarketData(
                symbol=symbol,
                timestamp=row["timestamp"]
                if isinstance(row["timestamp"], datetime)
                else pd.to_datetime(row["timestamp"]).replace(tzinfo=UTC), # Ensure timezone aware
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=volume_p,
                # Removed additional_data argument
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

    def _handle_signal(self, signal: TradeSignal, strategy_name: str) -> str | None:
        """
        Handle a trade signal from a strategy

        Args:
            signal: The trading signal
            strategy_name: Name of the strategy that generated the signal

        Returns:
            Position ID if a new position was created, None otherwise
        """
        logger.info(
            f"Received {signal.signal_type} signal from {strategy_name} for {signal.symbol}"
        )

        # Notify all registered signal handlers
        for handler in self.signal_handlers:
            try:
                handler(signal)
            except Exception as e:
                logger.error(f"Error in signal handler: {e}")

        # Process the signal based on type - Use correct SignalType members
        if signal.signal_type == SignalType.ENTER_LONG:
            # TODO: Determine side correctly based on SignalType?
            # Assuming ENTER_LONG implies BUY side for opening
            return self._open_position(signal, strategy_name, OrderSide.BUY)
        elif signal.signal_type == SignalType.ENTER_SHORT:
            # Assuming ENTER_SHORT implies SELL side for opening
            return self._open_position(signal, strategy_name, OrderSide.SELL)
        elif signal.signal_type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            # Find and close relevant active positions
            position_closed = False
            # Iterate over a copy of keys to allow modification during iteration
            for pos_id in list(self.active_positions.keys()):
                pos = self.active_positions.get(pos_id)
                # Match strategy, symbol, and ensure side matches exit type
                if (
                    pos and pos.strategy_name == strategy_name and pos.symbol == signal.symbol and
                    ((signal.signal_type == SignalType.EXIT_LONG and pos.side == OrderSide.BUY) or
                     (signal.signal_type == SignalType.EXIT_SHORT and pos.side == OrderSide.SELL))
                ):
                    self.logger.info(f"Closing position {pos_id} due to {signal.signal_type} signal")
                    # Use the timestamp from the signal if available
                    close_timestamp = signal.timestamp or datetime.now(UTC)
                    # Price might come from signal or market data - using signal.price for now
                    self._close_position(pos_id, signal.price, close_timestamp)
                    position_closed = True
            if not position_closed:
                 self.logger.warning(
                     f"Received {signal.signal_type} signal from {strategy_name} for {signal.symbol}, "
                     f"but no matching active position found to close."
                 )
            return None # Closing doesn't return a new position ID
        elif signal.signal_type == SignalType.HOLD or signal.signal_type == SignalType.REBALANCE:
             self.logger.debug(f"Ignoring {signal.signal_type} signal type for now.")
             return None
        else:
             self.logger.warning(f"Unhandled signal type: {signal.signal_type}")
             return None

    def _open_position(self, signal: TradeSignal, strategy_name: str, side: OrderSide) -> str:
        """Open a new position based on a signal."""
        position_id = str(uuid.uuid4())
        now_utc = datetime.now(UTC)

        # TODO: Critical - This needs proper integration with RiskManager for sizing
        # and ExecutionHandler for actual order placement and confirmation.
        # Using placeholder values from signal for now, which is incorrect for a live engine.
        entry_price = signal.price or Decimal("0") # Placeholder - Should come from fill
        quantity = signal.quantity or Decimal("1") # Placeholder - Should come from sizing

        if entry_price <= 0 or quantity <= 0:
             self.logger.error(
                 f"Cannot open position {position_id} for {signal.symbol}: Invalid price/quantity from signal",
                 signal=signal
             )
             # Raise an error or return None/empty string to indicate failure
             raise ValueError("Cannot open position with zero or negative price/quantity from signal")

        # Create Position object using ONLY defined fields
        position = Position(
            symbol=signal.symbol,
            side=side, # Use the determined OrderSide
            size=quantity, # Placeholder
            entry_price=entry_price, # Placeholder
            leverage=Decimal("1"), # Placeholder - Should come from config/strategy
            id=position_id,
            status=PositionStatus.OPEN.value, # Use enum value
            timestamp=int(now_utc.timestamp() * 1000),
            strategy_name=strategy_name, # Add strategy_name if it's part of Position model
            # Removed quantity, open_time - redundant or incorrect
            # Removed stop_loss, take_profit, metadata - needs specific handling if Position model supports them
        )

        self.active_positions[position_id] = position
        self.logger.info(f"Opened position {position_id}: {position}")
        return position_id

    def _close_position(self, position_id: str, price: Decimal | None, timestamp: datetime) -> None:
        """Close an active position."""
        if position_id not in self.active_positions:
            self.logger.warning(f"Position {position_id} not found for closing")
            return

        position = self.active_positions.pop(position_id)
        close_price = price or position.mark_price or position.entry_price # Best available close price

        if close_price is None:
            self.logger.error(f"Cannot determine close price for position {position_id}. Cannot calculate PNL.")
            pnl = Decimal("0.0") # Cannot calculate PNL
        else:
            # Ensure Decimal math
            close_price_dec = Decimal(str(close_price))
            entry_price_dec = Decimal(str(position.entry_price))
            size_dec = Decimal(str(position.size))

            if position.side == OrderSide.BUY:
                pnl = (close_price_dec - entry_price_dec) * size_dec
            else: # SELL
                pnl = (entry_price_dec - close_price_dec) * size_dec

        position.status = PositionStatus.CLOSED.value
        position.close_price = close_price_dec if close_price is not None else None # Store close price if known
        position.close_time = timestamp # Store close time
        position.pnl = pnl # Store calculated PNL

        self.closed_positions.append(position) # Add to list of closed positions
        self.logger.info(f"Closed position {position_id}: PNL = {pnl:.2f}")

        # TODO: Update portfolio tracker/realized PNL

    def _update_position_status(self, position_id: str, data: "MarketData") -> None:
        """
        Update the status and PnL of an active position based on new market data.

        Args:
            position_id: The ID of the position to update.
            data: The latest MarketData for the position's symbol.
        """
        if position_id not in self.active_positions:
            return

        position = self.active_positions[position_id]
        price = data.close

        # Check for stop loss hit
        if position.stop_loss and position.side == SignalType.BUY and price <= position.stop_loss:
            logger.info(f"Stop loss triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
        elif (
            position.stop_loss and position.side == SignalType.SELL and price >= position.stop_loss
        ):
            logger.info(f"Stop loss triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)

        # Check for take profit hit
        elif (
            position.take_profit
            and position.side == SignalType.BUY
            and price >= position.take_profit
        ):
            logger.info(f"Take profit triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)
        elif (
            position.take_profit
            and position.side == SignalType.SELL
            and price <= position.take_profit
        ):
            logger.info(f"Take profit triggered for position {position_id} at {price}")
            self._close_position(position_id, price, data.timestamp)

        # Update mark price (example)
        position.mark_price = data.close # Assume close price is the mark price
        # TODO: Proper unrealized PNL calculation requires PortfolioTracker or similar
        # position.unrealized_pnl = self.portfolio_tracker.calculate_unrealized_pnl(position)

        # Placeholder for Stop Loss / Take Profit logic
        # This requires the Position model to actually have stop_loss/take_profit fields
        # and for these to be set when opening.
        # check_price = data.close
        # if position.side == OrderSide.BUY:
        #     if position.stop_loss and check_price <= position.stop_loss:
        #         self.logger.info(f"Stop loss triggered for {position_id}")
        #         # self._close_position(position_id, position.stop_loss, data.timestamp)
        #     elif position.take_profit and check_price >= position.take_profit:
        #         self.logger.info(f"Take profit triggered for {position_id}")
        #         # self._close_position(position_id, position.take_profit, data.timestamp)
        # elif position.side == OrderSide.SELL:
        #     if position.stop_loss and check_price >= position.stop_loss:
        #         self.logger.info(f"Stop loss triggered for {position_id}")
        #         # self._close_position(position_id, position.stop_loss, data.timestamp)
        #     elif position.take_profit and check_price <= position.take_profit:
        #         self.logger.info(f"Take profit triggered for {position_id}")
        #         # self._close_position(position_id, position.take_profit, data.timestamp)
        # Removed references to self.portfolio_tracker for now
        pass # Keep simplified

    def get_engine_info(self) -> dict[str, Any]:
        """
        Get information about the engine's current state

        Returns:
            Dictionary with engine information
        """
        total_pnl_closed = sum(
            p.pnl for p in self.closed_positions if p.pnl is not None
        ) # Sum pnl from list
        return {
            "name": self.name,
            "running": self.is_running,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            "last_update": self.last_update_time.isoformat() if self.last_update_time else None,
            "strategy_count": len(self.strategies),
            "active_strategies": sum(1 for s in self.strategies.values() if s.enabled),
            "active_positions": len(self.active_positions),
            "closed_positions_count": len(self.closed_positions),
            "total_realized_pnl": total_pnl_closed, # Use calculated sum
        }
