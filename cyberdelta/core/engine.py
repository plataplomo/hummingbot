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


class Engine:
    """Core trading engine responsible for:
    - Managing strategies and their states (enabled/disabled).
    - Routing incoming market data to relevant, enabled strategies.
    - Receiving trade signals from strategies and forwarding them to a configured
      handler (e.g., RiskManager).
    - DOES NOT manage positions, calculate PnL, or execute trades directly.
    """

    def __init__(self, name: str = "CyberDeltaEngine") -> None:
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

        logger.info(f"Engine '{name}' initialized")

    def add_strategy(self, strategy: Strategy) -> None:
        """Add a strategy instance to the engine. Replaces existing strategy with the same name.
        The strategy is disabled by default upon adding.

        Args:
            strategy: Strategy instance to add.

        """
        if strategy.name in self.strategies:
            logger.warning(f"Strategy '{strategy.name}' already exists, replacing.")
            # Ensure the old strategy is disabled if replaced
            if strategy.name in self.enabled_strategies:
                self.disable_strategy(strategy.name)

        self.strategies[strategy.name] = strategy
        # Do not automatically enable; require explicit call to enable_strategy
        strategy.disable()
        self._refresh_active_symbols()
        logger.info(
            f"Added strategy '{strategy.name}' for symbol '{strategy.symbol}'. "
            f"Strategy is initially disabled.",
        )

    def remove_strategy(self, strategy_name: str) -> None:
        """Remove a strategy from the engine.

        Args:
            strategy_name: Name of the strategy to remove.

        """
        if strategy_name in self.strategies:
            logger.info(f"Removing strategy '{strategy_name}'")
            # Ensure strategy is disabled before removal
            if strategy_name in self.enabled_strategies:
                self.disable_strategy(strategy_name)  # Also calls strategy.disable()
            del self.strategies[strategy_name]
            self._refresh_active_symbols()
        else:
            logger.warning(f"Strategy '{strategy_name}' not found for removal.")

    def enable_strategy(self, strategy_name: str) -> None:
        """Enable a registered strategy to process data and generate signals."""
        if strategy_name not in self.strategies:
            logger.warning(f"Cannot enable non-existent strategy '{strategy_name}'.")
            return
        if strategy_name in self.enabled_strategies:
            logger.debug(f"Strategy '{strategy_name}' is already enabled.")
            return

        strategy = self.strategies[strategy_name]
        strategy.enable()  # Update the strategy's internal state
        self.enabled_strategies.add(strategy_name)
        # Ensure active symbols reflects enabled state if needed (optional refinement)
        self._refresh_active_symbols()
        logger.info(f"Enabled strategy '{strategy_name}'.")

    def disable_strategy(self, strategy_name: str) -> None:
        """Disable a registered strategy."""
        if strategy_name not in self.strategies:
            logger.warning(f"Cannot disable non-existent strategy '{strategy_name}'.")
            return
        if strategy_name not in self.enabled_strategies:
            logger.debug(f"Strategy '{strategy_name}' is already disabled.")
            return

        strategy = self.strategies[strategy_name]
        strategy.disable()  # Update the strategy's internal state
        self.enabled_strategies.discard(strategy_name)
        # Refreshing symbols might not be strictly needed on disable
        # self._refresh_active_symbols()
        logger.info(f"Disabled strategy '{strategy_name}'.")

    def set_signal_handler(self, handler: Callable[[TradeSignal], Awaitable[None]]) -> None:
        """Set the single async handler responsible for processing generated TradeSignals.
        This should typically be the entry point for the RiskManager or a SignalQueue.

        Args:
            handler: The async callable that accepts a TradeSignal.

        """
        self.signal_handler = handler
        # Use getattr for safe name retrieval, fallback to repr
        handler_name = getattr(handler, "__name__", repr(handler))
        logger.info(f"Signal handler set to: {handler_name}")

    async def process_market_data(self, data: Candle) -> None:
        """Process incoming market data.
        Routes the data to relevant, enabled strategies based on symbol.
        Forwards any generated TradeSignals (list or None) to the configured signal_handler.

        Args:
            data: MarketData object containing market information.

        """
        if not self.is_running:
            logger.warning("Engine is not running, ignoring market data.")
            return

        if not self.signal_handler:
            logger.error("Engine has no signal handler configured. Signals cannot be processed.")
            return

        self.last_data_time = datetime.now(UTC)

        if data.symbol not in self.active_symbols:
            logger.debug(f"No active strategy for symbol {data.symbol}, ignoring data.")
            return

        for strategy_name in self.enabled_strategies:
            strategy = self.strategies[strategy_name]
            if strategy.symbol == data.symbol:
                try:
                    result_or_coro = strategy.process_data(data)
                    # process_data is always async, so this will always be a coroutine
                    result = await result_or_coro

                    if result is None:
                        continue

                    # Process the result (either single signal or list of signals)
                    if isinstance(result, list):
                        signals = result
                    else:
                        signals = [result]

                    if not signals:
                        continue

                    for signal in signals:
                        # Defensive: check signal type
                        if not hasattr(signal, "symbol") or not hasattr(signal, "signal_type"):
                            logger.error(
                                f"Invalid signal object returned by {strategy.name}: {signal}",
                            )
                            continue
                        logger.info(
                            f"Strategy '{strategy.name}' generated signal: "
                            f"{getattr(signal, 'signal_type', 'UNKNOWN')} for "
                            f"{getattr(signal, 'symbol', 'UNKNOWN')}.",
                        )
                        await self.signal_handler(signal)
                except Exception as e:
                    logger.error(
                        f"Error processing data in strategy '{strategy.name}': {e}",
                        symbol=data.symbol,
                        strategy_name=strategy.name,
                        exc_info=True,
                    )

    async def process_dataframe(self, df: pd.DataFrame, symbol: str) -> None:
        """Process a pandas DataFrame of historical/batch market data.
        Expects columns: timestamp (int/str), open/high/low/close/volume (float/str/Decimal).
        Converts rows to MarketData objects and feeds them to process_market_data.

        Args:
            df: DataFrame with market data (must have timestamp, open, high, low, close, volume).
            symbol: Symbol this data represents.

        """
        required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            # Use logger for errors
            logger.error(
                f"DataFrame processing failed for {symbol}: Missing required columns: {missing}",
            )
            raise ValueError(f"DataFrame missing required columns: {missing}")

        logger.info(f"Processing DataFrame for {symbol} with {len(df)} rows.")
        # Process each row in the DataFrame
        for idx, row in df.iterrows():
            # Cast to ensure proper typing for pandas operations
            # Note: pandas iterrows returns (index, Series[Unknown]) due to dynamic nature
            idx_typed = cast(int, idx)
            row_typed = cast(pd.Series[Any], row)

            # Extract timestamp and convert to datetime
            timestamp_raw = row_typed.get("timestamp")
            if timestamp_raw is None:
                logger.warning(f"Row {idx_typed}: Missing timestamp, skipping")
                continue

            # Convert timestamp to datetime
            try:
                # Use pandas to_datetime for robust conversion
                # Note: pd.to_datetime has complex overloads, cast result for clarity
                pd_timestamp_result = pd.to_datetime(timestamp_raw, utc=True)
                # Convert to standard datetime if it's a pandas Timestamp
                if hasattr(pd_timestamp_result, "to_pydatetime"):
                    timestamp = cast(pd.Timestamp, pd_timestamp_result).to_pydatetime()
                else:
                    timestamp = cast(datetime, pd_timestamp_result)
            except Exception as e:
                logger.warning(f"Row {idx_typed}: Invalid timestamp {timestamp_raw}, skipping: {e}")
                continue

            # Convert row to dict for Candle creation
            row_dict: dict[str, Any] | None = None
            try:
                # Cast the to_dict result to ensure proper typing
                # Note: pandas to_dict has complex overloads, cast for clarity
                row_dict_result = row_typed.to_dict()
                row_dict = cast(dict[str, Any], row_dict_result)

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
                self.logger.error(
                    f"Error converting DataFrame row {idx_typed} for {symbol} to MarketData types",
                    row_data=row_dict if row_dict is not None else {},
                    error=e,
                    exc_info=False,  # Keep log concise for per-row errors
                )
                continue  # Skip this row if conversion fails
        logger.info(f"Finished processing DataFrame for {symbol}.")

    def start(self) -> None:
        """Start the trading engine. Calls on_start() for all enabled strategies."""
        if self.is_running:
            logger.warning("Engine is already running.")
            return

        if not self.signal_handler:
            logger.error("Cannot start Engine: Signal handler has not been set.")
            # Prevent starting without a crucial dependency
            raise RuntimeError("Engine cannot start without a configured signal handler.")

        logger.info(f"Starting engine '{self.name}'...")
        self.is_running = True
        self.start_time = datetime.now(UTC)  # Use UTC

        # Start only enabled strategies
        enabled_count = 0
        # Iterate copy in case on_start fails/disables
        for strategy_name in list(self.enabled_strategies):
            strategy = self.strategies.get(strategy_name)
            if strategy:  # Should always exist if in enabled_strategies set
                try:
                    logger.debug(f"Calling on_start for strategy '{strategy.name}'...")
                    strategy.on_start()
                    enabled_count += 1
                except Exception as e:
                    logger.error(
                        f"Error calling on_start for strategy '{strategy.name}': {e}. "
                        f"Disabling strategy.",
                        exc_info=True,
                    )
                    self.disable_strategy(strategy_name)  # Disable faulty strategy

        logger.info(f"Engine '{self.name}' started with {enabled_count} enabled strategies.")

    def stop(self) -> None:
        """Stop the trading engine. Calls on_stop() for all enabled strategies
        and ensures all strategies are marked as disabled.
        """
        if not self.is_running:
            logger.warning("Engine is not running.")
            return

        logger.info(f"Stopping engine '{self.name}'...")
        self.is_running = False

        # Stop all currently enabled strategies first
        stopped_count = 0
        for strategy_name in list(self.enabled_strategies):  # Iterate copy
            strategy = self.strategies.get(strategy_name)
            if strategy:
                try:
                    logger.debug(f"Calling on_stop for strategy '{strategy.name}'...")
                    strategy.on_stop()
                    stopped_count += 1
                except Exception as e:
                    logger.error(
                        f"Error calling on_stop for strategy '{strategy.name}': {e}",
                        exc_info=True,
                    )
                # Always disable after stopping, even if on_stop failed
                self.disable_strategy(strategy_name)

        # Ensure any remaining strategies (if any inconsistencies occurred) are disabled
        for strategy_name, strategy in self.strategies.items():
            if strategy.enabled:  # Should not happen if logic is correct, but good safety check
                logger.warning(
                    f"Strategy '{strategy_name}' was still marked as enabled during stop. "
                    f"Forcibly disabling.",
                )
                strategy.disable()
                self.enabled_strategies.discard(strategy_name)

        logger.info(f"Engine '{self.name}' stopped. Called on_stop for {stopped_count} strategies.")

    def _refresh_active_symbols(self) -> None:
        """Update the set of symbols monitored by registered strategies."""
        self.active_symbols = {s.symbol for s in self.strategies.values()}
        logger.debug(f"Engine active symbols refreshed: {self.active_symbols}")

    def get_engine_info(self) -> dict[str, Any]:
        """Get basic information about the engine's operational state.
        Does NOT include position or P&L information.

        Returns:
            Dictionary with engine state information.

        """
        # Removed PNL calculation - Engine doesn't track closed positions
        # total_pnl_closed = sum(...)

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
            # "active_positions": len(self.active_positions),
            # "closed_positions_count": len(self.closed_positions),
            # "total_realized_pnl": total_pnl_closed,
        }
