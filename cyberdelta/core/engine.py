from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
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

    def __init__(self, name: str = "CyberDeltaEngine"):
        self.name = name
        self.strategies: dict[str, Strategy] = {}
        self.positions: dict[str, "Position"] = {}
        self.active_positions: dict[str, "Position"] = {}
        self.closed_positions: dict[str, "Position"] = {}
        self.signal_handlers: list[Callable[["TradeSignal"], None]] = []
        self.is_running = False
        self.start_time: datetime | None = None
        self.last_update_time: datetime | None = None

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

    def register_signal_handler(self, handler: Callable[["TradeSignal"], None]) -> None:
        """
        Register a handler for trade signals

        Args:
            handler: Callable that takes a TradeSignal
        """
        self.signal_handlers.append(handler)
        logger.info(f"Registered signal handler {handler.__name__}")

    def process_market_data(self, data: "MarketData") -> None:
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
            data = MarketData(
                symbol=symbol,
                timestamp=row["timestamp"]
                if isinstance(row["timestamp"], datetime)
                else pd.to_datetime(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                additional_data={k: v for k, v in row.items() if k not in required_cols},
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

    def _handle_signal(self, signal: "TradeSignal", strategy_name: str) -> str | None:
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

    def _open_position(self, signal: "TradeSignal", strategy_name: str, side: "SignalType") -> str:
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
            metadata=signal.metadata,
        )

        self.positions[position_id] = position
        self.active_positions[position_id] = position

        logger.info(
            f"Opened {side.name} position {position_id} for {signal.symbol} at {signal.price}"
        )
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

    def get_engine_info(self) -> dict[str, Any]:
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
            "total_pnl": sum(p.pnl for p in self.closed_positions.values()),
        }

    def _process_market_data(self, data: dict[str, Any]) -> None:
        """Processes incoming market data (ticker, orderbook, etc.)."""
        # Example: Handle ticker data
        if data.get("type") == "ticker":
            exchange = data.get("exchange")
            symbol = data.get("symbol")
            ticker_data = data.get("data")
            if exchange and symbol and ticker_data:
                try:
                    # Corrected MarketData call: Remove extra kwarg, ensure Decimal
                    market_data = MarketData(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(
                            ticker_data.get("timestamp") / 1000, tz=timezone.utc
                        ),
                        # Corrected: pass Decimals
                        open=Decimal(str(ticker_data.get("open", "0"))),
                        high=Decimal(str(ticker_data.get("high", "0"))),
                        low=Decimal(str(ticker_data.get("low", "0"))),
                        close=Decimal(str(ticker_data.get("close", "0"))),
                        volume=Decimal(str(ticker_data.get("volume", "0"))),
                    )
                    # Update portfolio with latest price
                    self.portfolio_tracker.update_asset_price(symbol, market_data.close)
                    # Notify strategies
                    asyncio.create_task(self.strategy_manager.on_market_data(market_data))
                    # Check stop loss / take profit
                    self._check_positions(symbol, market_data.close)
                except Exception as e:
                    self.logger.error("Error processing ticker data", data=ticker_data, error=e)

        # TODO: Handle other data types (orderbook, trades)

    def _process_signal(self, signal: TradeSignal) -> None:
        """Processes a trading signal generated by a strategy."""
        self.logger.info("Processing signal", signal=signal)
        symbol = signal.symbol
        current_position = self.portfolio_tracker.get_position(symbol)

        # Corrected: Compare with enum members
        if signal.signal_type == SignalType.ENTER_LONG:
            if current_position and current_position.size > Decimal("0"):
                # Corrected: Compare with enum members
                if current_position.side == OrderSide.SELL:
                    self._close_position(symbol, current_position)
                else:
                    self.logger.info("Already long, ignoring signal", symbol=symbol)
                    return
            self._open_position(signal)
        elif signal.signal_type == SignalType.ENTER_SHORT:
            if current_position and current_position.size > Decimal("0"):
                # Corrected: Compare with enum members
                if current_position.side == OrderSide.BUY:
                    self._close_position(symbol, current_position)
                else:
                    self.logger.info("Already short, ignoring signal", symbol=symbol)
                    return
            self._open_position(signal)
        elif signal.signal_type == SignalType.EXIT_LONG:
            # Corrected: Compare with enum members
            if (
                current_position
                and current_position.side == OrderSide.BUY
                and current_position.size > Decimal("0")
            ):
                self._close_position(symbol, current_position)
            else:
                self.logger.info("No long position to exit", symbol=symbol)
        elif signal.signal_type == SignalType.EXIT_SHORT:
            # Corrected: Compare with enum members
            if (
                current_position
                and current_position.side == OrderSide.SELL
                and current_position.size > Decimal("0")
            ):
                self._close_position(symbol, current_position)
            else:
                self.logger.info("No short position to exit", symbol=symbol)
        # elif signal.signal_type == SignalType.HOLD: # Explicitly handle HOLD if needed
        #     pass

    def _open_position(self, signal: TradeSignal) -> None:
        """Opens a new position based on a signal."""
        self.logger.info("Opening position", signal=signal)
        # Determine order parameters (size, price, type)
        # This requires a sizing model based on risk, capital, etc.
        quantity = self.risk_manager.calculate_order_size(signal)  # Example
        if not quantity or quantity <= Decimal("0"):
            self.logger.warning("Calculated order size is zero or invalid", signal=signal)
            return

        # Map SignalType to OrderSide
        order_side = (
            OrderSide.BUY if signal.signal_type == SignalType.ENTER_LONG else OrderSide.SELL
        )
        order_type = OrderType.MARKET  # Example: Use market orders
        price = None  # For market orders

        # Create and submit order via ExecutionHandler
        client_order_id = f"cde-open-{int(time.time() * 1000)}"
        asyncio.create_task(
            self.execution_handler.create_and_submit_order(
                exchange=self.config.get(
                    "trading.default_exchange"
                ),  # Get exchange from config/signal
                symbol=signal.symbol,
                side=order_side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                client_order_id=client_order_id,
                # Add optional params from signal if available
                stop_loss=signal.stop_loss,
                take_profit=signal.take_profit,
                # TODO: Link position creation to successful order execution callback
            )
        )

    def _close_position(
        self, symbol: str, position: Position, close_price: Decimal | None = None
    ) -> None:
        """Closes an existing position."""
        self.logger.info("Closing position", symbol=symbol, position_id=position.id)
        if position.size <= Decimal("0"):
            self.logger.warning("Attempted to close already zero size position", symbol=symbol)
            return

        order_side = OrderSide.SELL if position.side == OrderSide.BUY else OrderSide.BUY
        order_type = OrderType.MARKET  # Use market order to ensure closure

        client_order_id = f"cde-close-{int(time.time() * 1000)}"
        asyncio.create_task(
            self.execution_handler.create_and_submit_order(
                exchange=self.config.get(
                    "trading.default_exchange"
                ),  # Get exchange from config/position
                symbol=symbol,
                side=order_side,
                order_type=order_type,
                quantity=position.size,  # Close full size
                price=None,  # Market order
                client_order_id=client_order_id,
                reduce_only=True,
                # TODO: Update position status/PNL in portfolio on successful execution callback
            )
        )
        # Immediate update in portfolio tracker might be premature
        # position.close_price = close_price if close_price else self.portfolio_tracker.get_current_price(symbol)
        # position.close_time = datetime.now(timezone.utc)
        # position.status = PositionStatus.CLOSED
        # # Simplified PNL calc - should happen on fill
        # if position.entry_price and position.close_price:
        #      pnl = (position.close_price - position.entry_price) * position.size * (-1 if position.side == OrderSide.SELL else 1)
        #      position.realized_pnl = (position.realized_pnl or Decimal("0")) + pnl
        # self.portfolio_tracker.update_position(position)

    def _check_positions(self, symbol: str, current_price: Decimal) -> None:
        """Checks open positions for stop loss or take profit triggers."""
        position = self.portfolio_tracker.get_position(symbol)
        # Corrected: Check size > 0
        if position and position.size > Decimal("0"):
            # Ensure prices are Decimal
            price = (
                current_price if isinstance(current_price, Decimal) else Decimal(str(current_price))
            )
            stop_loss_price = position.stop_loss
            take_profit_price = position.take_profit

            triggered_close = False
            # Corrected: Compare with enum members & check stop_loss not None
            if position.side == OrderSide.BUY:
                if stop_loss_price is not None and price <= stop_loss_price:
                    self.logger.info(
                        f"Stop loss triggered for {symbol} (Long)",
                        price=price,
                        stop=stop_loss_price,
                    )
                    triggered_close = True
                elif take_profit_price is not None and price >= take_profit_price:
                    self.logger.info(
                        f"Take profit triggered for {symbol} (Long)",
                        price=price,
                        take_profit=take_profit_price,
                    )
                    triggered_close = True
            # Corrected: Compare with enum members & check stop_loss not None
            elif position.side == OrderSide.SELL:
                if stop_loss_price is not None and price >= stop_loss_price:
                    self.logger.info(
                        f"Stop loss triggered for {symbol} (Short)",
                        price=price,
                        stop=stop_loss_price,
                    )
                    triggered_close = True
                elif take_profit_price is not None and price <= take_profit_price:
                    self.logger.info(
                        f"Take profit triggered for {symbol} (Short)",
                        price=price,
                        take_profit=take_profit_price,
                    )
                    triggered_close = True

            if triggered_close:
                self._close_position(symbol, position, close_price=price)

    def _calculate_pnl(self, position: Position) -> Decimal | None:
        """Calculates PNL for a position (potentially based on mark price)."""
        # Corrected: Check size > 0
        if position.size <= Decimal("0"):
            return Decimal("0.0")

        # Corrected: Use calculate method, handle None mark_price
        mark_price = position.mark_price or self.portfolio_tracker.get_current_price(
            position.symbol
        )
        if mark_price:
            # Ensure Decimal
            mark_price = mark_price if isinstance(mark_price, Decimal) else Decimal(str(mark_price))
            return position.calculate_unrealized_pnl(mark_price)
        else:
            self.logger.warning(
                "Cannot calculate PNL, missing mark/current price", symbol=position.symbol
            )
            return None
