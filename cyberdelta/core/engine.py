"""Core trading engine for strategy management and signal routing."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, cast

import pandas as pd
import structlog


if TYPE_CHECKING:
    from cyberdelta.core.models import TradeSignal
    from cyberdelta.core.models.market.candle import Candle

from .strategy import Strategy


logger = structlog.get_logger(__name__)


class DataFrameProcessingError(ValueError):
    """Error processing DataFrame with required columns."""

    def __init__(self, missing_columns: list[str]) -> None:
        """Initialize with missing column names.

        Args:
            missing_columns: List of column names that are missing from the DataFrame.
        """
        self.missing_columns = missing_columns
        super().__init__(f"DataFrame missing required columns: {missing_columns}")


class EngineConfigurationError(RuntimeError):
    """Error with engine configuration."""


class Engine:
    """Core trading engine responsible for strategy management and signal routing.

    - Managing strategies and their states (enabled/disabled).
    - Routing incoming market data to relevant, enabled strategies.
    - Receiving trade signals from strategies and forwarding them to a configured
      handler (e.g., RiskManager).
    - DOES NOT manage positions, calculate PnL, or execute trades directly.
    """

    def __init__(self, name: str = "CyberDeltaEngine") -> None:
        """Initialize the trading engine with a given name.

        Args:
            name: Name identifier for this engine instance.

        """
        self.name = name
        self.strategies: dict[str, Strategy] = {}
        self.enabled_strategies: set[str] = set()  # Track enabled strategy names
        self.active_symbols: set[str] = set()  # Track symbols monitored by strategies
        # Must be set via set_signal_handler
        self.signal_handler: Callable[[TradeSignal], Awaitable[None]] | None = None
        self.is_running = False
        self.start_time: datetime | None = None
        self.last_data_time: datetime | None = None
        self.logger = structlog.get_logger(engine_name=name)

        logger.info(
            "engine_initialized",
            engine_name=name,
            message=f"Engine '{name}' initialized",
        )

    def add_strategy(self, strategy: Strategy) -> None:
        """Add a strategy instance to the engine. Replaces existing strategy with the same name.

        The strategy is disabled by default upon adding.

        Args:
            strategy: Strategy instance to add.

        """
        if strategy.name in self.strategies:
            logger.warning(
                "strategy_already_exists_replacing",
                strategy_name=strategy.name,
                action="replacing_existing_strategy",
                message=f"Strategy '{strategy.name}' already exists, replacing.",
            )
            # Ensure the old strategy is disabled if replaced
            if strategy.name in self.enabled_strategies:
                self.disable_strategy(strategy.name)

        self.strategies[strategy.name] = strategy
        # Do not automatically enable; require explicit call to enable_strategy
        strategy.disable()
        self._refresh_active_symbols()
        logger.info(
            "strategy_added",
            strategy_name=strategy.name,
            symbol=strategy.symbol,
            initial_state="disabled",
            action="strategy_added",
            message=(
                f"Added strategy '{strategy.name}' for symbol '{strategy.symbol}'. "
                f"Strategy is initially disabled."
            ),
        )

    def remove_strategy(self, strategy_name: str) -> None:
        """Remove a strategy from the engine.

        Args:
            strategy_name: Name of the strategy to remove.

        """
        if strategy_name in self.strategies:
            logger.info(
                "strategy_removing",
                strategy_name=strategy_name,
                action="removing_strategy",
                message=f"Removing strategy '{strategy_name}'",
            )
            # Ensure strategy is disabled before removal
            if strategy_name in self.enabled_strategies:
                self.disable_strategy(strategy_name)  # Also calls strategy.disable()
            del self.strategies[strategy_name]
            self._refresh_active_symbols()
        else:
            logger.warning(
                "strategy_not_found_for_removal",
                strategy_name=strategy_name,
                action="removal_failed",
                message=f"Strategy '{strategy_name}' not found for removal.",
            )

    def enable_strategy(self, strategy_name: str) -> None:
        """Enable a registered strategy to process data and generate signals."""
        if strategy_name not in self.strategies:
            logger.warning(
                "cannot_enable_nonexistent_strategy",
                strategy_name=strategy_name,
                action="enable_failed",
                message=f"Cannot enable non-existent strategy '{strategy_name}'.",
            )
            return
        if strategy_name in self.enabled_strategies:
            logger.debug(
                "strategy_already_enabled",
                strategy_name=strategy_name,
                current_state="enabled",
                action="enable_skipped",
                message=f"Strategy '{strategy_name}' is already enabled.",
            )
            return

        strategy = self.strategies[strategy_name]
        strategy.enable()  # Update the strategy's internal state
        self.enabled_strategies.add(strategy_name)
        # Ensure active symbols reflects enabled state if needed (optional refinement)
        self._refresh_active_symbols()
        logger.info(
            "strategy_enabled",
            strategy_name=strategy_name,
            action="strategy_enabled",
            message=f"Enabled strategy '{strategy_name}'.",
        )

    def disable_strategy(self, strategy_name: str) -> None:
        """Disable a registered strategy."""
        if strategy_name not in self.strategies:
            logger.warning(
                "cannot_disable_nonexistent_strategy",
                strategy_name=strategy_name,
                action="disable_failed",
                message=f"Cannot disable non-existent strategy '{strategy_name}'.",
            )
            return
        if strategy_name not in self.enabled_strategies:
            logger.debug(
                "strategy_already_disabled",
                strategy_name=strategy_name,
                current_state="disabled",
                action="disable_skipped",
                message=f"Strategy '{strategy_name}' is already disabled.",
            )
            return

        strategy = self.strategies[strategy_name]
        strategy.disable()  # Update the strategy's internal state
        self.enabled_strategies.discard(strategy_name)
        # Refreshing symbols might not be strictly needed on disable
        logger.info(
            "strategy_disabled",
            strategy_name=strategy_name,
            action="strategy_disabled",
            message=f"Disabled strategy '{strategy_name}'.",
        )

    def set_signal_handler(self, handler: Callable[[TradeSignal], Awaitable[None]]) -> None:
        """Set the single async handler responsible for processing generated TradeSignals.

        This should typically be the entry point for the RiskManager or a SignalQueue.

        Args:
            handler: The async callable that accepts a TradeSignal.

        """
        self.signal_handler = handler
        # Use getattr for safe name retrieval, fallback to repr
        handler_name = getattr(handler, "__name__", repr(handler))
        logger.info(
            "signal_handler_set",
            handler_name=handler_name,
            action="signal_handler_configured",
            message=f"Signal handler set to: {handler_name}",
        )

    async def process_market_data(self, data: Candle) -> None:
        """Process incoming market data.

        Routes the data to relevant, enabled strategies based on symbol.
        Forwards any generated TradeSignals (list or None) to the configured signal_handler.

        Args:
            data: MarketData object containing market information.

        """
        if not self._validate_engine_state():
            return

        self.last_data_time = datetime.now(UTC)

        if not self._should_process_symbol(data.symbol):
            return

        await self._process_strategies_for_symbol(data)

    def _validate_engine_state(self) -> bool:
        """Validate that the engine is in a state to process market data."""
        if not self.is_running:
            logger.warning("Engine is not running, ignoring market data.")
            return False

        if not self.signal_handler:
            logger.error("Engine has no signal handler configured. Signals cannot be processed.")
            return False

        return True

    def _should_process_symbol(self, symbol: str) -> bool:
        """Check if the symbol should be processed."""
        if symbol not in self.active_symbols:
            logger.debug(
                "no_active_strategy_for_symbol",
                symbol=symbol,
                action="ignoring_data",
                message=f"No active strategy for symbol {symbol}, ignoring data.",
            )
            return False
        return True

    async def _process_strategies_for_symbol(self, data: Candle) -> None:
        """Process data through all enabled strategies for the symbol."""
        for strategy_name in self.enabled_strategies:
            strategy = self.strategies[strategy_name]
            if strategy.symbol == data.symbol:
                await self._process_single_strategy(strategy, data)

    async def _process_single_strategy(self, strategy: Strategy, data: Candle) -> None:
        """Process data through a single strategy."""
        try:
            result_or_coro = strategy.process_data(data)
            # process_data is always async, so this will always be a coroutine
            result = await result_or_coro

            if result is None:
                return

            # Process the result (either single signal or list of signals)
            signals = self._normalize_strategy_result(result)

            if not signals:
                return

            await self._handle_strategy_signals(strategy, signals)

        except Exception as e:
            logger.exception(
                "strategy_data_processing_error",
                strategy_name=strategy.name,
                symbol=data.symbol,
                error=str(e),
            )

    def _normalize_strategy_result(
        self,
        result: TradeSignal | list[TradeSignal],
    ) -> list[TradeSignal]:
        """Normalize strategy result to a list of signals."""
        if isinstance(result, list):
            return result
        return [result]

    async def _handle_strategy_signals(
        self,
        strategy: Strategy,
        signals: list[TradeSignal],
    ) -> None:
        """Handle signals generated by a strategy."""
        for signal in signals:
            # Defensive: check signal type
            if not hasattr(signal, "symbol") or not hasattr(signal, "signal_type"):
                logger.error(
                    "invalid_signal_object",
                    strategy_name=strategy.name,
                    signal=str(signal),
                    message="Invalid signal object returned by strategy",
                )
                continue
            logger.info(
                "strategy_signal_generated",
                strategy_name=strategy.name,
                signal_type=getattr(signal, "signal_type", "UNKNOWN"),
                symbol=getattr(signal, "symbol", "UNKNOWN"),
                message="Strategy generated signal",
            )
            if self.signal_handler is not None:
                await self.signal_handler(signal)

    async def process_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """Process a pandas DataFrame of historical/batch market data.

        Expects columns: timestamp (int/str), open/high/low/close/volume (float/str/Decimal).
        Converts rows to MarketData objects and feeds them to process_market_data.

        Args:
            df: DataFrame with market data (must have timestamp, open, high, low, close, volume).
            symbol: Symbol this data represents.

        Raises:
            ValueError: If DataFrame is missing required columns.
        """
        required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            # Use logger for errors
            logger.error(
                "dataframe_processing_failed",
                symbol=symbol,
                missing_columns=missing,
                message="DataFrame processing failed: Missing required columns",
            )
            raise DataFrameProcessingError(missing)

        logger.info(
            "dataframe_processing_started",
            symbol=symbol,
            rows_count=len(df),
            action="processing_dataframe",
            message=f"Processing DataFrame for {symbol} with {len(df)} rows.",
        )
        # Process each row in the DataFrame
        for idx, row in df.iterrows():
            # Cast to ensure proper typing for pandas operations
            # Note: pandas iterrows returns (index, Series[Unknown]) due to dynamic nature
            idx_typed = cast("int", idx)
            # DEFENSIVE CHECK: Handle pandas Series dynamic typing
            # Pyright=[reportUnknownVariableType] - pandas Series typing is inherently dynamic
            row_typed = row

            # Extract timestamp and convert to datetime
            # DEFENSIVE CHECK: Handle pandas Series.get dynamic return type
            # Pyright=[reportUnknownMemberType, reportUnknownArgumentType] - pandas Series.get has
            # complex overloads
            timestamp_raw: Any = row_typed.get("timestamp")
            if timestamp_raw is None:
                logger.warning(
                    "dataframe_row_missing_timestamp",
                    row_index=idx_typed,
                    symbol=symbol,
                    action="skipping_row",
                    message=f"Row {idx_typed}: Missing timestamp, skipping",
                )
                continue

            # Convert timestamp to datetime
            try:
                # Use pandas to_datetime for robust conversion
                # DEFENSIVE CHECK: Handle pandas to_datetime complex overloads
                # Pyright=[reportUnknownMemberType, reportUnknownArgumentType] - pd.to_datetime has
                # many overloads
                pd_timestamp_result = cast("pd.Timestamp", pd.to_datetime(timestamp_raw, utc=True))
                # Convert to standard datetime if it's a pandas Timestamp
                if hasattr(pd_timestamp_result, "to_pydatetime"):
                    timestamp = pd_timestamp_result.to_pydatetime()
                else:
                    timestamp = cast("datetime", pd_timestamp_result)
            except (ValueError, TypeError, OverflowError) as e:
                logger.warning(
                    "dataframe_row_invalid_timestamp",
                    row_index=idx_typed,
                    symbol=symbol,
                    timestamp_raw=timestamp_raw,
                    error=str(e),
                    action="skipping_row",
                    message=f"Row {idx_typed}: Invalid timestamp {timestamp_raw}, skipping: {e}",
                )
                continue

            # Convert row to dict for Candle creation
            row_dict: dict[str, Any] | None = None
            try:
                # Cast the to_dict result to ensure proper typing
                # DEFENSIVE CHECK: Handle pandas Series.to_dict complex overloads
                # Pyright=[reportUnknownMemberType] - pandas Series.to_dict has complex overloads
                row_dict_result = cast("dict[str, Any]", row_typed.to_dict())
                row_dict = row_dict_result

                # Ensure conversion from string for precision
                open_p = Decimal(str(row_dict["open"]))
                high_p = Decimal(str(row_dict["high"]))
                low_p = Decimal(str(row_dict["low"]))
                close_p = Decimal(str(row_dict["close"]))
                volume_p = Decimal(str(row_dict["volume"]))

                # Create Candle instance
                candle = Candle(
                    symbol=symbol,
                    interval="1m",  # TODO: Use actual interval if available
                    open_time=timestamp,
                    open=open_p,
                    high=high_p,
                    low=low_p,
                    close=close_p,
                    volume=volume_p,
                )

                # Delegate processing to the main method
                await self.process_market_data(candle)
            except (InvalidOperation, TypeError, ValueError) as e:
                logger.exception(
                    "dataframe_row_conversion_error",
                    row_index=idx_typed,
                    symbol=symbol,
                    error=str(e),
                    message="Error converting DataFrame row to MarketData types",
                )
                continue  # Skip this row if conversion fails
        logger.info(
            "dataframe_processing_completed",
            symbol=symbol,
            action="processing_completed",
            message=f"Finished processing DataFrame for {symbol}.",
        )

    def start(self) -> None:
        """Start the trading engine. Calls on_start() for all enabled strategies.

        Raises:
            RuntimeError: If signal handler is not configured before starting.
        """
        if self.is_running:
            logger.warning("Engine is already running.")
            return

        if not self.signal_handler:
            logger.error("Cannot start Engine: Signal handler has not been set.")
            # Prevent starting without a crucial dependency
            engine_config_error_msg = "Engine cannot start without a configured signal handler."
            raise EngineConfigurationError(engine_config_error_msg)

        logger.info(
            "engine_starting",
            engine_name=self.name,
            action="engine_starting",
            message=f"Starting engine '{self.name}'...",
        )
        self.is_running = True
        self.start_time = datetime.now(UTC)  # Use UTC

        # Start only enabled strategies
        enabled_count = 0
        # Iterate copy in case on_start fails/disables
        for strategy_name in list(self.enabled_strategies):
            strategy = self.strategies.get(strategy_name)
            if strategy:  # Should always exist if in enabled_strategies set
                try:
                    logger.debug(
                        "strategy_on_start_called",
                        strategy_name=strategy.name,
                        action="calling_on_start",
                        message=f"Calling on_start for strategy '{strategy.name}'...",
                    )
                    strategy.on_start()
                    enabled_count += 1
                except Exception as e:
                    logger.exception(
                        "strategy_start_error",
                        strategy_name=strategy.name,
                        error=str(e),
                        message="Error calling on_start for strategy, disabling strategy",
                    )
                    self.disable_strategy(strategy_name)  # Disable faulty strategy

        logger.info(
            "engine_started",
            engine_name=self.name,
            enabled_strategies_count=enabled_count,
            action="engine_started",
            message=f"Engine '{self.name}' started with {enabled_count} enabled strategies.",
        )

    def stop(self) -> None:
        """Stop the trading engine. Calls on_stop() for all enabled strategies.

        Ensures all strategies are marked as disabled after stopping.
        """
        if not self.is_running:
            logger.warning("Engine is not running.")
            return

        logger.info(
            "engine_stopping",
            engine_name=self.name,
            action="engine_stopping",
            message=f"Stopping engine '{self.name}'...",
        )
        self.is_running = False

        # Stop all currently enabled strategies first
        stopped_count = 0
        for strategy_name in list(self.enabled_strategies):  # Iterate copy
            strategy = self.strategies.get(strategy_name)
            if strategy:
                try:
                    logger.debug(
                        "strategy_on_stop_called",
                        strategy_name=strategy.name,
                        action="calling_on_stop",
                        message=f"Calling on_stop for strategy '{strategy.name}'...",
                    )
                    strategy.on_stop()
                    stopped_count += 1
                except Exception as e:
                    logger.exception(
                        "strategy_stop_error",
                        strategy_name=strategy.name,
                        error=str(e),
                        message="Error calling on_stop for strategy",
                    )
                # Always disable after stopping, even if on_stop failed
                self.disable_strategy(strategy_name)

        # Ensure any remaining strategies (if any inconsistencies occurred) are disabled
        for strategy_name, strategy in self.strategies.items():
            if strategy.enabled:  # Should not happen if logic is correct, but good safety check
                logger.warning(
                    "strategy_force_disable",
                    strategy_name=strategy_name,
                    message="Strategy was still marked as enabled during stop. Forcibly disabling",
                )
                strategy.disable()
                self.enabled_strategies.discard(strategy_name)

        logger.info(
            "engine_stopped",
            engine_name=self.name,
            stopped_strategies_count=stopped_count,
            action="engine_stopped",
            message=f"Engine '{self.name}' stopped. Called on_stop for {stopped_count} strategies.",
        )

    def _refresh_active_symbols(self) -> None:
        """Update the set of symbols monitored by registered strategies."""
        self.active_symbols = {s.symbol for s in self.strategies.values()}
        logger.debug(
            "active_symbols_refreshed",
            active_symbols=list(self.active_symbols),
            symbols_count=len(self.active_symbols),
            action="symbols_refreshed",
            message=f"Engine active symbols refreshed: {self.active_symbols}",
        )

    def get_engine_info(self) -> dict[str, Any]:
        """Get basic information about the engine's operational state.

        Does NOT include position or P&L information.

        Returns:
            Dictionary with engine state information.

        """
        # Removed PNL calculation - Engine doesn't track closed positions

        uptime_seconds = (
            (datetime.now(UTC) - self.start_time).total_seconds()
            if self.start_time and self.is_running
            else 0
        )

        return {
            "name": self.name,
            "running": self.is_running,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime_seconds": uptime_seconds,
            "last_data_time": self.last_data_time.isoformat() if self.last_data_time else None,
            "total_strategies": len(self.strategies),
            "enabled_strategies": len(self.enabled_strategies),
            # Removed position counts and PNL
        }
