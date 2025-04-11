"""
Dashboard Integration Module for CyberDeltaEngine.

This module provides tools for integrating the real-time dashboard
with the performance tracker and other system components.
"""

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

# from cyberdelta.core.types import TradeOperation, TradeSignal # Remove old imports
from cyberdelta.core.models import TradeSignal  # Import from models
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.strategy import Strategy
from cyberdelta.monitoring.performance_tracker import PerformanceTracker
from cyberdelta.monitoring.real_time_dashboard import (  # Assuming dashboard is here
    launch_dashboard,
)

logger = structlog.get_logger(__name__)


class DashboardIntegration:
    """
    Connects trading system components to the real-time dashboard.

    This class provides an interface for strategies, exchange handlers,
    and other components to send data to the dashboard for visualization.
    """

    def __init__( 
        self,
        output_dir: str | None = None,
        auto_start: bool = True,
        update_interval: int = 5,
        port: int = 8050,
        debug: bool = False,
    ):
        """
        Initialize the dashboard integration.

        Args:
            output_dir: Directory for saving performance data
            auto_start: Whether to automatically start the dashboard
            update_interval: Dashboard update interval in seconds
            port: Port to run the dashboard on
            debug: Enable debug mode for the dashboard
        """
        self.output_dir = output_dir or os.path.join(os.getcwd(), "performance_data")

        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Initialize tracker and dashboard
        self.performance_tracker = PerformanceTracker(output_dir=self.output_dir)
        self.portfolio_tracker = None
        self.dashboard = None
        self.dashboard_thread = None

        # Registered strategies
        self.strategies: dict[str, Strategy] = {}

        # Start dashboard if auto_start is True
        if auto_start:
            self.start_dashboard(update_interval=update_interval, port=port, debug=debug)

    def register_portfolio_tracker(self, portfolio_tracker: PortfolioTracker):
        """
        Register a portfolio tracker with the dashboard.

        Args:
            portfolio_tracker: PortfolioTracker instance
        """
        self.portfolio_tracker = portfolio_tracker

        # Update dashboard if it's already running
        if self.dashboard:
            self.dashboard.portfolio_tracker = portfolio_tracker

    def register_strategy(self, strategy: Strategy):
        """
        Register a strategy with the dashboard.

        Args:
            strategy: Strategy instance
        """
        self.strategies[strategy.name] = strategy
        logger.info(f"Registered strategy {strategy.name} with dashboard")

    def start_dashboard(
        self,
        update_interval: int = 5,
        port: int = 8050,
        debug: bool = False,
        in_thread: bool = True,
    ):
        """
        Start the real-time dashboard.

        Args:
            update_interval: Dashboard update interval in seconds
            port: Port to run the dashboard on
            debug: Enable debug mode for the dashboard
            in_thread: Run dashboard in a separate thread

        Returns:
            Dashboard instance
        """
        if self.dashboard:
            logger.warning("Dashboard already running")
            return self.dashboard

        # Launch dashboard
        self.dashboard, self.dashboard_thread = launch_dashboard(
            performance_tracker=self.performance_tracker,
            portfolio_tracker=self.portfolio_tracker,
            update_interval=update_interval,
            port=port,
            debug=debug,
            in_thread=in_thread,
        )

        logger.info(f"Started dashboard on port {port}")
        return self.dashboard

    def stop_dashboard(self):
        """Stop the dashboard if it's running."""
        if self.dashboard_thread and self.dashboard_thread.is_alive():
            # For explicit termination, we would need to implement a shutdown mechanism
            logger.info(
                "Dashboard is running in a separate thread and will terminate "
                "when the main program ends"
            )

        if self.dashboard:
            logger.info("Dashboard reference removed")
            self.dashboard = None

    def track_return(self, strategy_name: str, timestamp: datetime, return_value: float):
        """
        Track a return for a strategy.

        Args:
            strategy_name: Name of the strategy
            timestamp: Timestamp of the return
            return_value: Return value
        """
        self.performance_tracker.track_return(strategy_name, timestamp, return_value)

    def track_trade(
        self,
        trade_id: str,
        strategy_name: str,
        symbol: str,
        exchange: str,
        direction: str,
        size: float,
        entry_price: float,
        entry_time: datetime,
        exit_price: float | None = None,
        exit_time: datetime | None = None,
        pnl: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track a trade.

        Args:
            trade_id: Unique ID for the trade
            strategy_name: Name of the strategy
            symbol: Symbol traded
            exchange: Exchange used
            direction: Trade direction (LONG/SHORT)
            size: Trade size
            entry_price: Entry price
            entry_time: Entry timestamp
            exit_price: Exit price (optional)
            exit_time: Exit timestamp (optional)
            pnl: Profit/loss (optional)
            metadata: Additional trade metadata (optional)
        """
        self.performance_tracker.track_trade(
            trade_id=trade_id,
            strategy_name=strategy_name,
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            size=size,
            entry_price=entry_price,
            entry_time=entry_time,
            exit_price=exit_price,
            exit_time=exit_time,
            pnl=pnl,
            metadata=metadata,
        )

    def track_trade_exit(
        self,
        trade_id: str,
        exit_price: float,
        exit_time: datetime,
        pnl: float,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Track the exit of a trade.

        Args:
            trade_id: ID of the trade to update
            exit_price: Exit price
            exit_time: Exit timestamp
            pnl: Profit/loss
            metadata: Additional exit metadata (optional)
        """
        self.performance_tracker.track_trade_exit(
            trade_id=trade_id,
            exit_price=exit_price,
            exit_time=exit_time,
            pnl=pnl,
            metadata=metadata,
        )

    def track_signal(self, signal: TradeSignal):
        if not self.dashboard or not signal:
            return

        # Adapt to new TradeSignal definition
        strategy_id = signal.source_strategy or "UnknownStrategy"
        # trades_info = getattr(signal, 'trades', []) # 'trades' attribute doesn't exist

        self.dashboard.add_log(
            f"Signal Received: {strategy_id} - {signal.symbol} - "
            f"{signal.signal_type.name} - Side: {signal.side.name}"
            + (f" @ {signal.price}" if signal.price else "")
            + (f" Qty: {signal.quantity}" if signal.quantity else "")
        )
        # If performance tracking is needed based on signals:
        if self.performance_tracker:
            # Use available attributes
            self.performance_tracker.record_signal(
                timestamp=signal.timestamp or datetime.now(UTC),  # Use signal timestamp or now
                strategy_id=strategy_id,
                symbol=signal.symbol,
                signal_type=signal.signal_type.name,
                side=signal.side.name,
                price=signal.price,
                quantity=signal.quantity,
                # Add other relevant fields if available/needed
            )

        # Update plots or tables if necessary

    def track_signal_from_trade_signal(self, trade_signal: TradeSignal):
        """
        Track a signal from a TradeSignal object.

        Args:
            trade_signal: TradeSignal object
        """
        # Extract signal information
        signal_id = getattr(trade_signal, "signal_id", f"signal-{id(trade_signal)}")
        strategy_name = trade_signal.strategy_name

        # Process all trades in the signal
        for trade in trade_signal.trades:
            symbol = trade.symbol

            # Determine signal type based on operation
            if trade.operation == TradeOperation.ENTER_LONG:
                signal_type = "ENTER_LONG"
            elif trade.operation == TradeOperation.ENTER_SHORT:
                signal_type = "ENTER_SHORT"
            elif trade.operation == TradeOperation.EXIT:
                signal_type = "EXIT"
            else:
                signal_type = str(trade.operation)

            # Extract metadata
            metadata = {
                "size": trade.size,
                "exchange": trade.exchange,
                "target_price": trade.target_price,
            }

            # Add any additional metadata from the signal
            if hasattr(trade_signal, "metadata") and trade_signal.metadata:
                metadata.update(trade_signal.metadata)

            # Track the signal
            self.track_signal(
                signal_id=f"{signal_id}-{symbol}",
                strategy_name=strategy_name,
                symbol=symbol,
                signal_type=signal_type,
                timestamp=trade_signal.timestamp,
                confidence=getattr(trade_signal, "confidence", None),
                metadata=metadata,
            )

    def track_funding_rate(
        self,
        timestamp: datetime,
        exchange: str,
        symbol: str,
        funding_rate: float,
        predicted_rate: float | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Track a funding rate.

        Args:
            timestamp: Funding rate timestamp
            exchange: Exchange
            symbol: Symbol
            funding_rate: Funding rate value
            predicted_rate: Predicted funding rate (optional)
            metadata: Additional metadata (optional)
        """
        self.performance_tracker.track_funding_rate(
            timestamp=timestamp,
            exchange=exchange,
            symbol=symbol,
            funding_rate=funding_rate,
            predicted_rate=predicted_rate,
            metadata=metadata,
        )


# Global dashboard integration instance
_dashboard_integration = None


def get_dashboard_integration(
    output_dir: str | None = None,
    auto_start: bool = True,
    update_interval: int = 5,
    port: int = 8050,
    debug: bool = False,
) -> DashboardIntegration:
    """
    Get the global dashboard integration instance.

    If no instance exists, one will be created.

    Args:
        output_dir: Directory for saving performance data
        auto_start: Whether to automatically start the dashboard
        update_interval: Dashboard update interval in seconds
        port: Port to run the dashboard on
        debug: Enable debug mode for the dashboard

    Returns:
        DashboardIntegration instance
    """
    global _dashboard_integration

    if _dashboard_integration is None:
        _dashboard_integration = DashboardIntegration(
            output_dir=output_dir,
            auto_start=auto_start,
            update_interval=update_interval,
            port=port,
            debug=debug,
        )

    return _dashboard_integration
