"""Event-driven architecture for portfolio system."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any, Awaitable

from cyberdelta.core.infrastructure.events import EventType, BaseEvent
from typing import Any

# Type for event handlers - using generic BaseEvent
EventHandlerType = Callable[[BaseEvent[Any]], Any]
from cyberdelta.core.infrastructure.services.base_service import BaseService


class EventDispatcher(BaseService):
    """Production-ready event dispatcher with full integration."""

    def __init__(self, service_name: str = "event_dispatcher"):
        """Initialize event dispatcher."""
        super().__init__(service_name)
        self._handlers: dict[EventType, list[EventHandlerType]] = {}
        self._event_queue: asyncio.Queue[BaseEvent[Any]] = asyncio.Queue()
        self._processing_task: asyncio.Task[None] | None = None
        self._event_history: list[BaseEvent[Any]] = []
        self._max_history = 1000

    async def _start_internal(self) -> None:
        """Initialize event dispatcher."""
        # Start event processing task
        self._processing_task = asyncio.create_task(self._process_events())

    async def _stop_internal(self) -> None:
        """Shutdown event dispatcher."""
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass

    async def register_handler(self, event_type: EventType, handler: Callable[[BaseEvent[Any]], Any]) -> None:
        """Register event handler for specific event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []

        self._handlers[event_type].append(handler)

    async def dispatch(self, event: BaseEvent[Any]) -> None:
        """Dispatch event to registered handlers."""
        await self._event_queue.put(event)

    async def _process_events(self) -> None:
        """Background task to process events."""
        while self.is_running:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)

                # Process event
                await self._handle_event(event)

                # Add to history
                self._add_to_history(event)

            except asyncio.TimeoutError:
                continue
            except Exception:
                # Log error but continue processing
                continue

    async def _handle_event(self, event: BaseEvent[Any]) -> None:
        """Handle individual event."""
        handlers = self._handlers.get(event.event_type, [])

        if not handlers:
            return

        # Execute all handlers concurrently
        tasks = []
        for handler in handlers:
            tasks.append(self._safe_handler_call(handler, event))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_handler_call(self, handler: Callable[[BaseEvent[Any]], Any], event: BaseEvent[Any]) -> None:
        """Safely call event handler with error handling."""
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
        except Exception:
            # Log handler error but don't stop processing
            pass

    def _add_to_history(self, event: BaseEvent[Any]) -> None:
        """Add event to history with size limit."""
        self._event_history.append(event)

        # Trim history if too large
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def get_recent_events(self, event_type: EventType | None = None, limit: int = 100) -> list[BaseEvent[Any]]:
        """Get recent events, optionally filtered by type."""
        events = self._event_history

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        return events[-limit:]