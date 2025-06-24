"""Strategy Management System.

This module provides the StrategyManager class for coordinating multiple trading strategies,
including lifecycle management, market data distribution, and signal collection.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import structlog

from cyberdelta.config import AppSettings
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy


logger = structlog.get_logger(__name__)


class StrategyManager:
    """Manage multiple trading strategies and control their lifecycle.

    Acts as a central coordinator for strategy operations including:

    - Registration/deregistration of strategies
    - Enabling/disabling strategies
    - Distributing market data to appropriate strategies
    - Collecting and prioritizing trade signals
    - Performance tracking and metrics collection
    """

    def __init__(
        self,
        config: AppSettings,
        execution_handler: ExecutionHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager,
        signal_queue: PrioritySignalQueue,
    ) -> None:
        """Initialize the StrategyManager.

        Args:
            config: Application configuration settings
            execution_handler: Handler for executing trading strategies
            portfolio_tracker: Tracker for managing trading portfolios
            risk_manager: Manager for risk management
            signal_queue: Queue for managing trade signals

        """
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.execution_handler = execution_handler
        self.portfolio_tracker = portfolio_tracker
        self.risk_manager = risk_manager
        self.strategies: dict[str, Strategy] = {}
        self._tasks: list[asyncio.Task[Any]] = []
        self._running = False
        self.enabled_strategies: set[str] = set()
        self.active_symbols: set[str] = set()
        self.last_update_time: datetime | None = None
        self.signal_queue: PrioritySignalQueue = signal_queue

        logger.info("StrategyManager initialized")

    def register_strategy(self, strategy: Strategy) -> None:
        """Register a strategy with the manager.

        Args:
            strategy: Strategy instance to register

        """
        if strategy.name in self.strategies:
            logger.warning(f"Strategy '{strategy.name}' already exists, replacing")

        self.strategies[strategy.name] = strategy
        self.active_symbols.add(strategy.symbol)
        logger.info(f"Registered strategy '{strategy.name}' for symbol '{strategy.symbol}'")

    def unregister_strategy(self, strategy_name: str) -> None:
        """Unregister a strategy from the manager.

        Args:
            strategy_name: Name of the strategy to unregister

        """
        if strategy_name not in self.strategies:
            logger.warning(f"Strategy '{strategy_name}' not found")
            return

        del self.strategies[strategy_name]
        self.enabled_strategies.discard(strategy_name)

        # Update active symbols
        self._refresh_active_symbols()

        logger.info(f"Unregistered strategy '{strategy_name}'")

    def enable_strategy(self, strategy_name: str) -> bool:
        """Enable a registered strategy.

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
        """Disable a registered strategy.

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

    async def process_market_data(self, data: Candle) -> None:
        """Process market data through relevant strategies and handle generated signals.

        Handles:
        - Historical data update (fail-fast on error)
        - Signal generation per strategy
        - Basic signal validation (type and required fields)
        - Logging errors during processing
        - Adding valid signals to the signal queue
        """
        self.last_update_time = datetime.now(UTC)

        if data.symbol not in self.active_symbols:
            return

        # Update historical data for all strategies
        await self._update_historical_data(data)

        # Process data through each enabled strategy
        await self._process_strategies_for_symbol(data)

    async def _update_historical_data(self, data: Candle) -> None:
        """Update historical data for all strategies handling this symbol."""
        strategy_name = ""  # Initialize strategy_name
        try:
            for _strategy_name, strategy in self.strategies.items():
                if strategy.symbol == data.symbol:
                    strategy.update_historical_data(data)
        except Exception as e:
            log_msg = (
                f"Critical error updating historical data for {data.symbol} in {strategy_name}. "
                f"Halting processing."
            )
            logger.error(
                log_msg,
                error=str(e),
                exc_info=True,
            )
            raise

    async def _process_strategies_for_symbol(self, data: Candle) -> None:
        """Process data through each enabled strategy for this symbol."""
        for strategy_name in list(self.enabled_strategies):
            if strategy_name not in self.strategies:
                continue

            strategy = self.strategies[strategy_name]
            if strategy.symbol != data.symbol:
                continue

            await self._process_single_strategy(strategy, data, strategy_name)

    async def _process_single_strategy(
        self, strategy: Strategy, data: Candle, strategy_name: str
    ) -> None:
        """Process data through a single strategy and handle generated signals."""
        try:
            generated_signals = await strategy.process_data(data)
        except Exception as e:
            logger.error(
                f"Error processing data in strategy '{strategy_name}'",
                error=str(e),
                exc_info=True,
            )
            return

        if generated_signals is None:
            return

        signal_list = (
            generated_signals if isinstance(generated_signals, list) else [generated_signals]
        )

        for signal in signal_list:
            await self._process_single_signal(signal, strategy)

    async def _process_single_signal(self, signal: TradeSignal, strategy: Strategy) -> None:
        """Process a single signal through validation and queue addition."""
        # Validate signal
        if not self._validate_signal(signal, strategy):
            return

        # Apply risk management (placeholder for now)
        if not await self._apply_risk_management(signal):
            return

        # Add to signal queue
        await self._add_signal_to_queue(signal)

    def _validate_signal(self, signal: TradeSignal, strategy: Strategy) -> bool:
        """Validate that a signal has all required fields."""
        # Basic Sanity Check:
        if not all(
            [
                getattr(signal, "symbol", None) is not None,
                getattr(signal, "signal_type", None) is not None,
                getattr(signal, "side", None) is not None,
                getattr(signal, "price", None) is not None,
                getattr(signal, "quantity", None) is not None,
            ],
        ):
            # Attempt to get a structured representation if possible
            signal_data_dict = self._get_signal_data_dict(signal)

            logger.warning(
                "Malformed signal received from strategy",
                strategy_name=strategy.name,
                signal_data=signal_data_dict,  # Log the dict
                error_details=("Signal object missing required attributes or not a TradeSignal"),
            )
            return False

        return True

    def _get_signal_data_dict(self, signal: TradeSignal) -> dict[str, Any]:
        """Get a dictionary representation of signal data for logging."""
        if hasattr(signal, "model_dump") and callable(signal.model_dump):
            try:
                return signal.model_dump()
            except Exception as dump_err:
                logger.warning(
                    f"Failed to dump malformed signal data: {dump_err}",
                    signal_object=str(signal),  # Fallback to str()
                )
                return {"error": "Failed to dump signal"}
        else:
            return {"raw_signal": str(signal)}

    async def _apply_risk_management(self, signal: TradeSignal) -> bool:
        """Apply risk management to a signal (placeholder for now)."""
        try:
            # For now, skip risk management sizing since the method doesn't exist
            # TODO: Implement proper risk management integration
            # sized_signal = await self.risk_manager.validate_and_size_trade_signal(signal)
            # if sized_signal is None:
            #     logger.info(
            #         f"Signal rejected by risk manager sizing: {signal.signal_id}",
            #         signal_symbol=signal.symbol,
            #     )
            #     return False
            # signal = sized_signal  # Replace original signal with sized one
            pass  # Placeholder for future risk management integration
            return True

        except Exception as risk_e:
            logger.error(
                "Error during potential (currently bypassed) risk management step",
                signal_id=signal.signal_id if signal else None,
                error=str(risk_e),
                exc_info=True,
            )
            return False

    async def _add_signal_to_queue(self, signal: TradeSignal) -> None:
        """Add a validated signal to the signal queue."""
        try:
            # add_signal is now asynchronous
            await self.signal_queue.add_signal(signal)  # Add await
            logger.debug("Signal added to queue", signal_id=signal.signal_id)
        except Exception as queue_e:  # Use different variable name
            logger.error(
                "Signal queue failed to add signal",
                signal_id=signal.signal_id,
                error=str(queue_e),  # Use queue_e
                exc_info=True,
            )

    async def on_market_data(self, market_data: Candle) -> None:
        """Entry point for market data. Processes data via relevant strategies."""
        await self.process_market_data(market_data)

    def get_strategies_for_symbol(self, symbol: str) -> list[Strategy]:
        """Get a list of enabled strategies for a given symbol.

        Args:
            symbol: Trading symbol to filter strategies

        Returns:
            List of enabled strategies for the symbol

        """
        return [
            strategy
            for name, strategy in self.strategies.items()
            if name in self.enabled_strategies and strategy.symbol == symbol
        ]

    def get_enabled_strategies(self) -> list[Strategy]:
        """Get a list of all currently enabled strategies.

        Returns:
            List of all enabled strategies

        """
        return [
            self.strategies[name] for name in self.enabled_strategies if name in self.strategies
        ]

    def get_strategy_performance(self) -> dict[str, dict[str, Any]]:
        """Retrieve performance metrics for all registered strategies.

        Delegates to each strategy's performance_metrics property.

        Returns:
            Dictionary mapping strategy names to their performance metrics

        """
        performance_data: dict[str, dict[str, Any]] = {}
        for name, strategy in self.strategies.items():
            try:
                # Access the performance_metrics property
                # The isinstance check is removed as the type hint guarantees it's a dict
                performance_data[name] = strategy.performance_metrics
            except Exception as e:
                logger.error(
                    f"Error getting performance metrics from strategy '{name}'",
                    error=str(e),
                )
                performance_data[name] = {"error": "Failed to retrieve metrics"}
        return performance_data

    def start_all(self) -> None:
        """Start all enabled strategies by calling their start_async method."""
        logger.info("Starting all enabled strategies...")
        for name in self.enabled_strategies:
            if name in self.strategies:
                try:
                    # Call the synchronous on_start method
                    self.strategies[name].on_start()  # Renamed from start_async
                    logger.info(f"Strategy '{name}' started.")
                except Exception as e:
                    logger.error(f"Error starting strategy '{name}'", error=str(e), exc_info=True)

    def stop_all(self) -> None:
        """Stop all running strategies by calling their stop_async method."""
        logger.info("Stopping all strategies...")
        for name in list(self.strategies.keys()):
            if name in self.strategies:
                strategy = self.strategies[name]
                try:
                    # Call the synchronous on_stop method
                    strategy.on_stop()  # Renamed from stop_async
                    logger.info(f"Strategy '{name}' stop signal sent.")
                    if name in self.enabled_strategies:
                        strategy.disable()
                        self.enabled_strategies.discard(name)
                        logger.info(f"Strategy '{name}' disabled after stop.")
                except Exception as e:
                    logger.error(f"Error stopping strategy '{name}'", error=str(e), exc_info=True)
        self._tasks.clear()

    def _refresh_active_symbols(self) -> None:
        """Recalculate the set of active symbols based on current strategies."""
        self.active_symbols = {strat.symbol for strat in self.strategies.values() if strat.symbol}
        logger.debug("Active symbols refreshed", active_symbols=self.active_symbols)
