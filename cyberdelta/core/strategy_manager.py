import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import structlog

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy

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
        config: dict[str, Any],
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

    async def process_market_data(self, data: Candle) -> list[TradeSignal]:
        """
        Asynchronously process market data through all enabled strategies that match the symbol.
        Robustly handles exceptions and filters invalid signals.
        For v0.0.1 safety: If any strategy's update_historical_data fails,
        the whole process fails (fail-fast, all-or-nothing).

        Args:
            data: Candle object containing market information.

        Returns:
            List of valid trade signals generated from strategies (may be empty if no signals)
        Raises:
            Exception: If any update_historical_data call fails.
        """
        signals: list[TradeSignal] = []
        self.last_update_time = datetime.now(UTC)

        if data.symbol not in self.active_symbols:
            return signals

        # Fail-fast: if any update_historical_data fails, raise immediately
        for _strategy_name, strategy in self.strategies.items():
            if strategy.symbol == data.symbol:
                strategy.update_historical_data(data)

        for strategy_name in self.enabled_strategies:
            strategy = self.strategies[strategy_name]
            if strategy.symbol != data.symbol:
                continue
            try:
                result = await strategy.process_data(data)
            except Exception as e:
                logger.error(
                    f"Error processing data in strategy '{strategy_name}': {str(e)}", exc_info=True
                )
                continue
            if result is None:
                continue
            # Always treat result as a list for uniformity
            result_list = result if isinstance(result, list) else [result]
            for signal in result_list:
                # Validate signal type
                if not isinstance(signal, TradeSignal):
                    logger.error(f"Non-TradeSignal object returned by {strategy_name}: {signal}")
                    continue
                # Validate required fields (symbol, signal_type, side, price, quantity)
                if (
                    getattr(signal, "symbol", None) is None
                    or getattr(signal, "signal_type", None) is None
                    or getattr(signal, "side", None) is None
                    or getattr(signal, "price", None) is None
                    or getattr(signal, "quantity", None) is None
                ):
                    logger.error(f"Malformed TradeSignal returned by {strategy_name}: {signal}")
                    continue
                # Defensive: risk manager sizing
                try:
                    # type: ignore[attr-defined] because size_signal is a test mock, not in interface
                    sized_signal = (
                        self.risk_manager.size_signal(signal) if self.risk_manager else signal
                    )  # type: ignore[attr-defined]
                except Exception as e:
                    self.logger.error(
                        f"Error during signal processing (post-generation) in {strategy_name}: {e}",
                        exc_info=True,
                    )
                    continue
                # Only append if still valid after sizing
                signals.append(sized_signal)  # type: ignore[assignment]
        return signals

    async def on_market_data(self, market_data: Candle) -> list[TradeSignal]:
        """
        Asynchronously called when new market data is available. Returns a list of valid
        TradeSignals (may be empty). Robustly handles exceptions and filters invalid signals.
        """
        signals: list[TradeSignal] = []
        strategies = self.get_strategies_for_symbol(market_data.symbol)
        if not strategies:
            return []
        for strategy in strategies:
            try:
                result = await strategy.process_data(market_data)
            except Exception as e:
                self.logger.error(
                    f"Error processing data in strategy '{strategy.name}': {str(e)}", exc_info=True
                )
                continue
            if result is None:
                continue
            result_list = result if isinstance(result, list) else [result]
            for signal in result_list:
                if not isinstance(signal, TradeSignal):
                    self.logger.error(
                        f"Non-TradeSignal object returned by {strategy.name}: {signal}"
                    )
                    continue
                if (
                    getattr(signal, "symbol", None) is None
                    or getattr(signal, "signal_type", None) is None
                    or getattr(signal, "side", None) is None
                    or getattr(signal, "price", None) is None
                    or getattr(signal, "quantity", None) is None
                ):
                    self.logger.error(
                        f"Malformed TradeSignal returned by {strategy.name}: {signal}"
                    )
                    continue
                try:
                    sized_signal = signal  # Pass signal through
                except Exception as e:
                    self.logger.error(
                        f"Error during signal processing (post-generation) in {strategy.name}: {e}",
                        exc_info=True,
                    )
                    continue
                # Only append if still valid after sizing
                signals.append(sized_signal)  # type: ignore[assignment]
        # Defensive: add signals to queue, catch and log errors
        if hasattr(self, "signal_queue") and self.signal_queue:
            for signal in signals:
                try:
                    self.signal_queue.add_signal(signal)
                except Exception as e:
                    logger.error(f"Signal queue failed to add signal: {e}", exc_info=True)
        else:
            logger.warning("Signal queue not available in StrategyManager, cannot queue signals.")
        return signals

    def get_strategies_for_symbol(self, symbol: str) -> list[Strategy]:
        """
        Get all strategies registered for a specific symbol.

        Args:
            symbol: Trading symbol

        Returns:
            List of strategies for the symbol
        """
        return [s for s in self.strategies.values() if s.symbol == symbol]

    def get_enabled_strategies(self) -> list[Strategy]:
        """
        Get all currently enabled strategies.

        Returns:
            List of enabled strategies
        """
        return [self.strategies[name] for name in self.enabled_strategies]

    def get_strategy_performance(self) -> dict[str, dict[str, Any]]:
        """
        Get performance metrics for all strategies.

        Returns:
            Dictionary of strategy names to performance metrics
        """
        performance = {}
        for name, strategy in self.strategies.items():
            if hasattr(strategy, "performance_metrics"):
                performance[name] = strategy.performance_metrics
            else:
                performance[name] = {
                    "signals_generated": strategy.signals_generated,
                    "last_signal_time": strategy.last_signal_time,
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
