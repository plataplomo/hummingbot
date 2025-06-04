"""Dashboard Integration Module for CyberDeltaEngine.

This module provides tools for integrating the real-time dashboard
with the performance tracker and other system components.
"""

import os
import threading
from datetime import UTC, datetime
from decimal import Decimal
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
    """Connects trading system components to the real-time dashboard.

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
    ) -> None:
        """Initialize the dashboard integration.

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
        self.portfolio_tracker: PortfolioTracker | None = None
        self.dashboard: Any = None
        self.dashboard_thread: threading.Thread | None = None

        # Registered strategies
        self.strategies: dict[str, Strategy] = {}

        # Start dashboard if auto_start is True
        if auto_start:
            self.start_dashboard(update_interval=update_interval, port=port, debug=debug)

    def register_portfolio_tracker(self, portfolio_tracker: PortfolioTracker) -> None:
        """Register a portfolio tracker with the dashboard.

        Args:
            portfolio_tracker: PortfolioTracker instance

        """
        self.portfolio_tracker = portfolio_tracker

        # TODO: Need a way to update the running dashboard instance with the new tracker.
        # The current `launch_dashboard` creates a new instance.
        # This might require a more robust dashboard management approach.
        logger.info("Portfolio tracker registered. Restart dashboard for changes to take effect.")

    def register_strategy(self, strategy: Strategy) -> None:
        """Register a strategy with the dashboard.

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
    ) -> None:
        """Start the real-time dashboard.

        Args:
            update_interval: Dashboard update interval in seconds
            port: Port to run the dashboard on
            debug: Enable debug mode for the dashboard
            in_thread: Run dashboard in a separate thread

        Returns:
            Dashboard instance

        """
        if self.portfolio_tracker is None:
            logger.error("Portfolio tracker not registered. Cannot start dashboard.")
            return

        # Launch dashboard
        result = launch_dashboard(
            performance_tracker=self.performance_tracker,
            portfolio_tracker=self.portfolio_tracker,
            port=port,
            debug=debug,
            use_threading=in_thread,
        )
        # launch_dashboard returns either dashboard or (dashboard, thread) tuple
        if isinstance(result, tuple):
            self.dashboard, self.dashboard_thread = result
        else:
            self.dashboard = result

        logger.info(f"Started dashboard on port {port}")

    def stop_dashboard(self) -> None:
        """Stop the dashboard if it's running."""
        if self.dashboard_thread is not None and self.dashboard_thread.is_alive():
            # For explicit termination, we would need to implement a shutdown mechanism
            logger.info(
                "Dashboard is running in a separate thread and will terminate "
                "when the main program ends",
            )

        if self.dashboard:
            logger.info("Dashboard reference removed")
            self.dashboard = None

    def track_return(self, strategy_name: str, timestamp: datetime, return_value: Decimal) -> None:
        """Track a return for a strategy.

        Args:
            strategy_name: Name of the strategy
            timestamp: Timestamp of the return
            return_value: Return value (Decimal)

        """
        self.performance_tracker.track_return(strategy_name, timestamp, float(return_value))

    def track_trade(
        self,
        trade_id: str,
        strategy_name: str,
        symbol: str,
        exchange: str,
        direction: str,
        size: Decimal,
        entry_price: Decimal,
        entry_time: datetime,
        exit_price: Decimal | None = None,
        exit_time: datetime | None = None,
        pnl: Decimal | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Track a trade.

        Args:
            trade_id: Unique ID for the trade
            strategy_name: Name of the strategy
            symbol: Symbol traded
            exchange: Exchange used
            direction: Trade direction (LONG/SHORT)
            size: Trade size (Decimal)
            entry_price: Entry price (Decimal)
            entry_time: Entry timestamp
            exit_price: Exit price (optional, Decimal)
            exit_time: Exit timestamp (optional)
            pnl: Profit/loss (optional, Decimal)
            metadata: Additional trade metadata (optional)

        """
        self.performance_tracker.track_trade(
            trade_id=trade_id,
            strategy_name=strategy_name,
            symbol=symbol,
            exchange=exchange,
            direction=direction,
            size=float(size),
            entry_price=float(entry_price),
            entry_time=entry_time,
            exit_price=float(exit_price) if exit_price is not None else None,
            exit_time=exit_time,
            pnl=float(pnl) if pnl is not None else None,
            metadata=metadata,
        )

    def track_trade_exit(
        self,
        trade_id: str,
        exit_price: Decimal,
        exit_time: datetime,
        pnl: Decimal,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Track the exit of a trade.

        Args:
            trade_id: ID of the trade to update
            exit_price: Exit price (Decimal)
            exit_time: Exit timestamp
            pnl: Profit/loss (Decimal)
            metadata: Additional exit metadata (optional)

        """
        self.performance_tracker.track_trade_exit(
            trade_id=trade_id,
            exit_price=float(exit_price),
            exit_time=exit_time,
            pnl=float(pnl),
            metadata=metadata,
        )

    def track_signal(self, signal: TradeSignal) -> None:
        """Track a signal event.

        Args:
            signal: The TradeSignal object to track.

        """
        if not self.performance_tracker or not signal:
            return

        # Adapt to new TradeSignal definition
        strategy_id = getattr(signal, "source_strategy", "UnknownStrategy") or "UnknownStrategy"
        symbol = getattr(signal, "symbol", "UnknownSymbol")
        signal_type_enum = getattr(signal, "signal_type", None)
        signal_type_name = signal_type_enum.name if signal_type_enum else "UNKNOWN"
        # Note: side, price, and quantity are extracted but not used in current implementation
        # They could be used for more detailed signal tracking in the future

        # Record the signal in the performance tracker
        self.performance_tracker.track_signal(
            signal_id=getattr(signal, "signal_id", f"signal_{datetime.now(UTC).timestamp()}"),
            strategy_name=strategy_id,
            symbol=symbol,
            signal_type=signal_type_name,
            timestamp=getattr(signal, "timestamp", datetime.now(UTC)),
            metadata=getattr(signal, "metadata", None),
        )

    def track_funding_rate(
        self,
        timestamp: datetime,
        exchange: str,
        symbol: str,
        funding_rate: Decimal,
        predicted_rate: Decimal | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Track funding rate data.

        Args:
            timestamp: Funding rate timestamp
            exchange: Exchange
            symbol: Symbol
            funding_rate: Funding rate value (Decimal)
            predicted_rate: Predicted funding rate (optional, Decimal)
            metadata: Additional metadata (optional)

        """
        self.performance_tracker.track_funding_rate(
            timestamp=timestamp,
            exchange=exchange,
            symbol=symbol,
            funding_rate=float(funding_rate),
            predicted_rate=float(predicted_rate) if predicted_rate is not None else None,
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
    """Get the global dashboard integration instance.

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
