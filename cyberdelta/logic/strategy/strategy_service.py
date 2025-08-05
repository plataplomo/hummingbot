"""Strategy service for orchestrating strategy execution and signal generation.

This module provides the StrategyService class that manages the execution
of all registered strategies using validated AppSettings configuration.
"""

from __future__ import annotations

import asyncio
import contextlib

from cyberdelta.application.event_bus import EventBus
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.logic.market.market_service import MarketDataService
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.logic.signal.signal_service import SignalService
from cyberdelta.logic.strategy.strategy_base import BaseStrategy, StrategyError
from cyberdelta.models import Trade, TradeSignal


logger = get_logger(__name__)


class StrategyService:
    """Orchestrates strategy execution and signal generation.

    This service manages the execution loop for all registered strategies,
    provides them with market data and portfolio state, and routes generated
    signals to the signal service for validation.


    Configuration Usage:
    - Uses config.strategies.execution_interval_seconds for loop timing
    - Uses config.strategies.enabled_strategies for strategy filtering
    - Uses config.general.safe_mode for paper trading awareness
    - Uses config.execution.retry_delay_base_sec for error recovery
    - Uses config.execution.retry_backoff_multiplier for backoff


    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - NO assumptions about strategy availability or behavior
    - Fail fast on configuration errors
    """

    def __init__(
        self,
        config: AppSettings,
        market_service: MarketDataService,
        portfolio_service: PortfolioService,
        signal_service: SignalService,
        event_bus: EventBus,
    ) -> None:
        """Initialize strategy service with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            market_service: Market data service for providing market snapshots
            portfolio_service: Portfolio service for providing portfolio state
            signal_service: Signal service for processing generated signals
            event_bus: Event bus for publishing strategy events
        """
        self.config = config
        self._market_service = market_service
        self._portfolio_service = portfolio_service
        self._signal_service = signal_service
        self._event_bus = event_bus
        self._strategies: dict[str, BaseStrategy] = {}
        self._running = False
        self._execution_task: asyncio.Task[None] | None = None
        # Extract strategy configuration - NO hardcoded defaults
        self._strategy_config = config.strategies
        self._execution_interval = self._strategy_config.execution_interval_seconds
        self._enabled_strategies = set(self._strategy_config.enabled_strategies)

        # Error handling settings from execution config
        self._retry_delay = config.execution.retry_delay_base_sec
        self._backoff_multiplier = config.execution.retry_backoff_multiplier

        # Safe mode awareness
        self._safe_mode = config.general.safe_mode

        logger.info(
            "strategy_service_initialized",
            execution_interval_seconds=float(self._execution_interval),
            enabled_strategies=list(self._enabled_strategies),
            safe_mode=self._safe_mode,
            retry_delay_sec=float(self._retry_delay),
        )

    def register_strategy(self, name: str, strategy: BaseStrategy) -> None:
        """Register a trading strategy.

        Args:
            name: Strategy name (must be in enabled_strategies config)
            strategy: Strategy instance that implements BaseStrategy


        Raises:
            ValueError: If strategy name not in enabled list or already registered

        Note:
        - Only explicitly enabled strategies can be registered
        - NO automatic discovery or registration
        - Strategy must be properly initialized BaseStrategy instance
        """
        if name not in self._enabled_strategies:
            msg = f"Strategy '{name}' not in enabled strategies list: {self._enabled_strategies}"
            raise ValueError(msg)

        if name in self._strategies:
            msg = f"Strategy '{name}' is already registered"
            raise ValueError(msg)

        self._strategies[name] = strategy

        logger.info(
            "strategy_registered",
            strategy_name=name,
            strategy_class=strategy.__class__.__name__,
            total_strategies=len(self._strategies),
        )

    def unregister_strategy(self, name: str) -> bool:
        """Unregister a trading strategy.

        Args:
            name: Strategy name to unregister


        Returns:
            True if strategy was unregistered, False if not found


        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit unregistration only
        - Clear logging of operations
        """
        if name in self._strategies:
            strategy = self._strategies.pop(name)

            logger.info(
                "strategy_unregistered",
                strategy_name=name,
                strategy_class=strategy.__class__.__name__,
                remaining_strategies=len(self._strategies),
            )
            return True
        logger.warning("strategy_unregister_not_found", strategy_name=name)
        return False

    async def initialize_strategies(self) -> None:
        """Initialize all registered strategies.

        Raises:
            StrategyError: If any strategy fails to initialize

        Note:
        - Initialization fails fast if any strategy fails
        - NO silent failures or partial initialization
        - All strategies must initialize successfully
        """
        logger.info("strategies_initialization_starting", strategy_count=len(self._strategies))
        for name, strategy in self._strategies.items():
            try:
                logger.debug("strategy_initializing", strategy_name=name)
                await strategy.initialize()

                logger.info("strategy_initialized_successfully", strategy_name=name)
            except Exception as e:
                logger.exception(
                    "strategy_initialization_failed",
                    strategy_name=name,
                    error=str(e),
                )
                msg = f"Failed to initialize strategy '{name}': {e}"
                raise StrategyError(msg) from e

        logger.info("strategies_initialization_completed", initialized_count=len(self._strategies))

    async def cleanup_strategies(self) -> None:
        """Cleanup all registered strategies.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Cleanup continues even if individual strategies fail
        - All cleanup errors are logged but don't stop the process
        - Graceful shutdown sequence
        """
        logger.info("strategies_cleanup_starting", strategy_count=len(self._strategies))
        for name, strategy in self._strategies.items():
            try:
                logger.debug("strategy_cleanup_starting", strategy_name=name)
                await strategy.cleanup()

                logger.info("strategy_cleanup_completed", strategy_name=name)
            except Exception as e:
                logger.exception("strategy_cleanup_failed", strategy_name=name, error=str(e))

        logger.info("strategies_cleanup_completed", cleaned_count=len(self._strategies))

    async def start(self) -> None:
        """Start the strategy execution loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured execution interval
        - NO hardcoded timing or retry logic
        - Proper async task management
        """
        if self._running:
            logger.warning("strategy_service_already_running")
            return

        if not self._strategies:
            logger.warning(
                "strategy_service_start_no_strategies",
                enabled_strategies=list(self._enabled_strategies),
            )
            return

        self._running = True

        # Initialize all strategies first
        await self.initialize_strategies()

        # Start execution loop
        self._execution_task = asyncio.create_task(self._execution_loop())

        logger.info(
            "strategy_service_started",
            strategy_count=len(self._strategies),
            execution_interval=float(self._execution_interval),
        )

    async def stop(self) -> None:
        """Stop the strategy execution loop.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Graceful shutdown with proper task cancellation
        - Strategy cleanup before exit
        - NO assumptions about task state
        """
        if not self._running:
            logger.warning("strategy_service_not_running")
            return

        self._running = False

        # Cancel execution task
        if self._execution_task and not self._execution_task.done():
            self._execution_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._execution_task

        # Cleanup strategies
        await self.cleanup_strategies()

        logger.info("strategy_service_stopped")

    async def _execution_loop(self) -> None:
        """Main execution loop for all strategies.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured execution interval
        - Handles errors per strategy without stopping loop
        - Uses configured retry/backoff for error recovery
        """
        logger.info(
            "strategy_execution_loop_started", interval_seconds=float(self._execution_interval)
        )

        consecutive_errors = 0
        max_consecutive_errors = 5  # Could be configurable

        while self._running:
            try:
                # Get current market data with proper types
                market_snapshot = await self._market_service.get_market_snapshot()

                # Get portfolio state with proper types
                portfolio_state = await self._portfolio_service.get_state()

                # Run each strategy
                signals_generated = 0
                for name, strategy in self._strategies.items():
                    try:
                        # Generate signal with type-safe inputs
                        signal = await strategy.analyze(market_snapshot, portfolio_state)

                        if signal:
                            # Validate signal has required fields
                            self._validate_generated_signal(signal, name)

                            # Send to signal service for validation and routing
                            success = await self._signal_service.process_signal(signal)

                            if success:
                                signals_generated += 1

                                logger.info(
                                    "strategy_signal_generated",
                                    strategy_name=name,
                                    signal_id=signal.signal_id,
                                    symbol=signal.symbol.value,
                                    exchange=(
                                        signal.exchange[0].value
                                        if isinstance(signal.exchange, list)
                                        else signal.exchange.value
                                    ),
                                    side=signal.side.value,
                                    price=float(signal.price) if signal.price else None,
                                )
                            else:
                                logger.warning(
                                    "strategy_signal_rejected",
                                    strategy_name=name,
                                    signal_id=signal.signal_id,
                                )

                    except Exception as e:
                        logger.exception(
                            "strategy_execution_error",
                            strategy_name=name,
                            error=str(e),
                        )
                        # Continue with other strategies - one failure doesn't stop all

                # Reset error counter on successful execution
                consecutive_errors = 0

                logger.debug(
                    "strategy_execution_cycle_completed",
                    signals_generated=signals_generated,
                    strategies_executed=len(self._strategies),
                )

                # Wait for next cycle using configured interval
                await asyncio.sleep(float(self._execution_interval))

            except asyncio.CancelledError:
                logger.info("strategy_execution_loop_cancelled")
                break

            except Exception as e:
                consecutive_errors += 1
                logger.exception(
                    "strategy_execution_loop_error",
                    error=str(e),
                    consecutive_errors=consecutive_errors,
                    max_consecutive_errors=max_consecutive_errors,
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(
                        "strategy_execution_loop_max_errors",
                        consecutive_errors=consecutive_errors,
                        stopping_loop=True,
                    )
                    break

                # Use exponential backoff from config
                backoff_delay = float(self._retry_delay) * (
                    self._backoff_multiplier ** (consecutive_errors - 1)
                )

                logger.info(
                    "strategy_execution_loop_error_backoff",
                    backoff_delay_seconds=backoff_delay,
                    consecutive_errors=consecutive_errors,
                )

                await asyncio.sleep(backoff_delay)

        logger.info("strategy_execution_loop_ended")

    def _validate_generated_signal(self, signal: TradeSignal, strategy_name: str) -> None:
        """Validate signal generated by strategy.

        Args:
            signal: Signal to validate
            strategy_name: Name of strategy that generated signal

        Note:
            Most validation is handled by Pydantic when TradeSignal is created.
            This method just logs successful validation.
        """
        # TradeSignal's Pydantic model ensures all required fields are present and valid
        # The type system and Pydantic handle all validation
        logger.debug(
            "strategy_signal_validated", strategy_name=strategy_name, signal_id=signal.signal_id
        )

    async def handle_trade(self, trade: Trade) -> None:
        """Handle trade execution feedback to strategies.

        Args:
            trade: Trade that was executed


        IMPORTANT: Following CODING_STANDARDS.md:
        - Provides trade feedback to strategies that might need it
        - NO assumptions about which strategies care about trades
        """
        logger.debug(
            "trade_feedback_to_strategies",
            trade_id=trade.id,
            symbol=trade.symbol.value,
            exchange=trade.exchange,
        )

        # Future enhancement: strategies could implement handle_trade() method
        # for trade-based learning or state updates

        # Send trade feedback to all strategies
        for name, strategy in self._strategies.items():
            try:
                await strategy.handle_trade(trade)

                logger.debug(
                    "strategy_trade_feedback_delivered", strategy_name=name, trade_id=trade.id
                )

            except Exception as e:
                logger.exception(
                    "strategy_trade_feedback_error",
                    strategy_name=name,
                    trade_id=trade.id,
                    error=str(e),
                )

    def get_registered_strategies(self) -> list[str]:
        """Get list of registered strategy names.

        Returns:
            List of strategy names currently registered
        """
        return list(self._strategies.keys())

    def get_enabled_strategies(self) -> list[str]:
        """Get list of enabled strategy names from configuration.

        Returns:
            List of strategy names enabled in configuration
        """
        return list(self._enabled_strategies)

    def is_running(self) -> bool:
        """Check if strategy service is currently running.

        Returns:
            True if execution loop is running, False otherwise
        """
        return self._running

    def get_execution_interval(self) -> float:
        """Get the configured execution interval.

        Returns:
            Execution interval in seconds from configuration


        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns configured value, NOT hardcoded default
        """
        return float(self._execution_interval)
