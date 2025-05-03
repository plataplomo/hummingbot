from __future__ import annotations  # Enable postponed evaluation

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from cyberdelta.core.models.market.candle import Candle

if TYPE_CHECKING:
    from cyberdelta.core.models import TradeSignal

logger = logging.getLogger(__name__)

_T = TypeVar("_T")  # Define a TypeVar for generic parameter types


class Strategy(ABC):
    """
    Abstract base class for all trading strategies.
    Strategies receive market data and generate trade signals.
    """

    def __init__(self, name: str, symbol: str, params: dict[str, Any] | None = None) -> None:
        """
        Initialize a strategy

        Args:
            name: Unique name for the strategy
            symbol: Trading symbol this strategy operates on
            params: Dictionary of strategy parameters
        """
        self.name = name
        self.symbol = symbol
        self.params = params or {}
        self.enabled = False
        self.last_signal_time: datetime | None = None
        self.signals_generated = 0
        self._historical_data: list[Candle] = []

        logger.info(f"Initialized strategy '{name}' for {symbol}")

    @abstractmethod
    async def process_data(self, data: Candle) -> TradeSignal | list[TradeSignal] | None:
        """
        Asynchronously process new market data and optionally generate one or more trading signals.

        Args:
            data: Candle object containing market information.

        Returns:
            Optional TradeSignal, list of TradeSignals, or None if no trade should be executed
        """
        pass

    def update_historical_data(self, data: Candle, max_bars: int = 1000) -> None:
        """
        Update the strategy's historical data cache

        Args:
            data: New Candle object to add.
            max_bars: Maximum number of data points to keep
        """
        # Only store data for the symbol this strategy is configured for
        if data.symbol != self.symbol:
            return

        self._historical_data.append(data)

        # Trim historical data if it exceeds max_bars
        if len(self._historical_data) > max_bars:
            self._historical_data = self._historical_data[-max_bars:]

    def enable(self) -> None:
        """Enable the strategy"""
        self.enabled = True
        logger.info(f"Enabled strategy '{self.name}'")

    def disable(self) -> None:
        """Disable the strategy"""
        self.enabled = False
        logger.info(f"Disabled strategy '{self.name}'")

    def on_start(self) -> None:
        """Called when the strategy is started"""
        logger.info(f"Strategy '{self.name}' started")

    def on_stop(self) -> None:
        """Called when the strategy is stopped"""
        logger.info(f"Strategy '{self.name}' stopped")

    # Ensure correct indentation for methods within the class
    def get_param(self, name: str, default: Any | None = None) -> Any | None:
        """
        Get a strategy parameter.

        Args:
            name: Parameter name
            default: Default value if parameter doesn't exist

        Returns:
            Parameter value or default
        """
        return self.params.get(name, default)

    def set_param(self, name: str, value: Any) -> None:
        """
        Set a strategy parameter.

        Args:
            name: Parameter name
            value: Parameter value
        """
        self.params[name] = value
        logger.info(f"Strategy '{self.name}' parameter '{name}' set to {value}")

    def get_strategy_info(self) -> dict[str, Any]:
        """
        Get information about the strategy's current state

        Returns:
            Dictionary with strategy information
        """
        return {
            "name": self.name,
            "symbol": self.symbol,
            "enabled": self.enabled,
            "params": self.params,
            "signals_generated": self.signals_generated,
            "last_signal_time": self.last_signal_time,
            "historical_data_points": len(self._historical_data),
        }

    @property
    def performance_metrics(self) -> dict[str, Any]:
        """
        Return default performance metrics for the strategy.
        Subclasses can override to provide richer metrics.
        """
        return {
            "signals_generated": self.signals_generated,
            "last_signal_time": self.last_signal_time,
        }
