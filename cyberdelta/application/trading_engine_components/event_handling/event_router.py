"""Event router for handling and dispatching events in the trading engine."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import msgspec


if TYPE_CHECKING:
    from cyberdelta.application.trading_engine_components.event_handling.event_processors import (
        EventProcessor,
    )

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.monitoring import RiskSeverity
from cyberdelta.enums.trading import OrderEventType, PositionEventType
from cyberdelta.models.events import (
    MarketData,
    OrderEvent,
    PositionEvent,
    RiskEvent,
    SignalEvent,
)


if TYPE_CHECKING:
    pass


logger = get_logger(__name__)


class EventRouter:
    """Routes events to appropriate processors in the trading engine."""

    def __init__(self, event_processor: EventProcessor) -> None:
        """Initialize event router with event processor.

        Args:
            event_processor: Event processor for handling events
        """
        self._event_processor = event_processor
        self._tasks: list[Any] = []

    async def handle_strategy_signal_event(self, event: msgspec.Struct) -> None:
        """Handle strategy signal generation events.

        Args:
            event: Event to handle
        """
        if isinstance(event, SignalEvent):
            # Convert to TradeSignal and process
            task = asyncio.create_task(self._event_processor.process_strategy_signal_event(event))
            self._tasks.append(task)

    async def handle_order_filled_event(self, event: msgspec.Struct) -> None:
        """Handle order filled events.

        Args:
            event: Event to handle
        """
        if isinstance(event, OrderEvent) and event.event_type == OrderEventType.FILLED:
            task = asyncio.create_task(self._event_processor.process_order_filled_event(event))
            self._tasks.append(task)

    async def handle_market_data_event(self, event: msgspec.Struct) -> None:
        """Handle market data updated events.

        Args:
            event: Event to handle
        """
        if isinstance(event, MarketData):
            task = asyncio.create_task(self._event_processor.process_market_data_event(event))
            self._tasks.append(task)

    async def handle_position_updated_event(self, event: msgspec.Struct) -> None:
        """Handle position updated events.

        Args:
            event: Event to handle
        """
        if isinstance(event, PositionEvent) and event.event_type == PositionEventType.UPDATED:
            task = asyncio.create_task(self._event_processor.process_position_updated_event(event))
            self._tasks.append(task)

    async def handle_risk_limit_event(self, event: msgspec.Struct) -> None:
        """Handle risk limit exceeded events.

        Args:
            event: Event to handle
        """
        if isinstance(event, RiskEvent) and event.severity == RiskSeverity.CRITICAL:
            task = asyncio.create_task(self._event_processor.process_risk_limit_event(event))
            self._tasks.append(task)
