from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy

if TYPE_CHECKING:
    from cyberdelta.utils.config import Config


logger = structlog.get_logger(__name__)


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

    def __init__(
        self,
        config: Config | dict[str, Any],
        execution_handler: ExecutionHandler,
        portfolio_tracker: PortfolioTracker,
        risk_manager: RiskManager,
        signal_queue: PrioritySignalQueue,
    ) -> None:
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

    async def process_market_data(self, data: Candle) -> None:
        """
        Process market data through relevant strategies and handle generated signals.

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

        strategy_name = ""  # Initialize strategy_name
        try:
            for strategy_name, strategy in self.strategies.items():
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

        for strategy_name in list(self.enabled_strategies):
            if strategy_name not in self.strategies:
                continue

            strategy = self.strategies[strategy_name]
            if strategy.symbol != data.symbol:
                continue

            try:
                generated_signals = await strategy.process_data(data)
            except Exception as e:
                logger.error(
                    f"Error processing data in strategy '{strategy_name}'",
                    error=str(e),
                    exc_info=True,
                )
                continue

            if generated_signals is None:
                continue

            signal_list = (
                generated_signals if isinstance(generated_signals, list) else [generated_signals]
            )

            for signal in signal_list:
                # Type validated by Pydantic on Strategy return hint, isinstance check removed.
                # Basic Sanity Check:
                if not all(
                    [
                        getattr(signal, "symbol", None) is not None,
                        getattr(signal, "signal_type", None) is not None,
                        getattr(signal, "side", None) is not None,
                        getattr(signal, "price", None) is not None,
                        getattr(signal, "quantity", None) is not None,
                    ]
                ):
                    logger.error(
                        f"Malformed TradeSignal (missing fields) from strategy '{strategy_name}'",
                        signal_data=signal.model_dump(),
                    )
                    continue

                # Risk Manager Sizing is commented out

                try:
                    # add_signal is synchronous
                    self.signal_queue.add_signal(signal)  # Removed await
                    logger.debug("Signal added to queue", signal_id=signal.signal_id)
                except Exception as e:
                    logger.error(
                        "Signal queue failed to add signal",
                        signal_id=signal.signal_id,
                        error=str(e),
                        exc_info=True,
                    )

    async def on_market_data(self, market_data: Candle) -> None:
        """
        Entry point for market data. Processes data via relevant strategies.
        """
        await self.process_market_data(market_data)

    def get_strategies_for_symbol(self, symbol: str) -> list[Strategy]:
        """Returns a list of enabled strategies for a given symbol."""
        return [
            strategy
            for name, strategy in self.strategies.items()
            if name in self.enabled_strategies and strategy.symbol == symbol
        ]

    def get_enabled_strategies(self) -> list[Strategy]:
        """Returns a list of all currently enabled strategies."""
        return [
            self.strategies[name] for name in self.enabled_strategies if name in self.strategies
        ]

    def get_strategy_performance(self) -> dict[str, dict[str, Any]]:
        """
        Retrieves performance metrics for all registered strategies.
        Delegates to each strategy's performance_metrics property.
        """
        performance_data: dict[str, dict[str, Any]] = {}
        for name, strategy in self.strategies.items():
            try:
                # Access the performance_metrics property
                # The isinstance check is removed as the type hint guarantees it's a dict
                performance_data[name] = strategy.performance_metrics
            except Exception as e:
                logger.error(
                    f"Error getting performance metrics from strategy '{name}'", error=str(e)
                )
                performance_data[name] = {"error": "Failed to retrieve metrics"}
        return performance_data

    def start_all(self) -> None:
        """Starts all enabled strategies by calling their start_async method."""
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
        """Stops all running strategies by calling their stop_async method."""
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
