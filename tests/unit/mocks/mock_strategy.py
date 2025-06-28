"""Mock strategy implementations for testing.

Provides mock strategy classes and utilities for testing strategy-related functionality.
"""

from __future__ import annotations

import asyncio
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import TradeSignal
from cyberdelta.core.models.market import Candle
from cyberdelta.core.strategy import Strategy


logger = get_logger(__name__)


class MockStrategy(Strategy):
    """A simple mock strategy for testing purposes."""

    def __init__(
        self,
        name: str = "MockStrategy",
        symbol: str = "MOCK/SYMBOL",
        enabled: bool = True,
        applicable_symbols: list[str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the mock strategy."""
        super().__init__(name=name, symbol=symbol, params=params or {})
        self.enabled = enabled
        self.applicable_symbols = applicable_symbols or []
        self.process_data_async_called_with: Any | None = None
        self.start_async_called = False
        self.stop_async_called = False

    async def start_async(self) -> None:
        """Mock start method."""
        self.start_async_called = True
        logger.debug(
            "mock_strategy_started",
            strategy_name=self.name,
            message="Mock strategy started",
        )
        await asyncio.sleep(0)  # Yield control

    async def stop_async(self) -> None:
        """Mock stop method."""
        self.stop_async_called = True
        logger.debug(
            "mock_strategy_stopped",
            strategy_name=self.name,
            message="Mock strategy stopped",
        )
        await asyncio.sleep(0)  # Yield control

    async def process_data(self, data: Candle) -> list[TradeSignal] | None:
        """Mock data processing method.

        Returns:
            None for mock strategy testing
        """
        self.process_data_async_called_with = data
        logger.debug(
            "mock_strategy_processed_data",
            strategy_name=self.name,
            symbol=data.symbol,
            message="Mock strategy processed data",
        )
        return None
