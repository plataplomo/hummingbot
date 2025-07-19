"""Event dispatcher implementation for portfolio events."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.events.base.base_event import (
        BasePortfolioEvent,
        EventFilter,
        EventHandler,
    )

logger = get_logger(__name__)

# Constants
MAX_QUEUE_SIZE = 1000


@dataclass
class HandlerRegistration:
    """Registration information for an event handler."""

    registration_id: str
    handler: EventHandler[Any]
    filter: EventFilter | None
    registered_at: float
    invocation_count: int = 0
    last_invoked: float | None = None


class EventDispatcher:
    """Central event dispatcher for portfolio events.

    Manages event distribution to registered handlers with support for
    filtering, async processing, and error handling.
    """

    def __init__(
        self,
        name: str = "PortfolioEventDispatcher",
        max_queue_size: int = 10000,
        processing_timeout: float = 30.0,
        error_retry_limit: int = 3,
    ) -> None:
        """Initialize the event dispatcher.

        Args:
            name: Dispatcher name
            max_queue_size: Maximum event queue size
            processing_timeout: Timeout for processing each event
            error_retry_limit: Maximum retries for failed handlers
        """
        self.name = name
        self.max_queue_size = max_queue_size
        self.processing_timeout = processing_timeout
        self.error_retry_limit = error_retry_limit

        # Handler registry
        self._handlers: dict[str, HandlerRegistration] = {}
        self._handler_lock = asyncio.Lock()

        # Event queue
        self._event_queue: asyncio.Queue[BasePortfolioEvent[Any]] = asyncio.Queue(
            maxsize=max_queue_size
        )

        # Processing state
        self._running = False
        self._processing_task: asyncio.Task[None] | None = None

        # Metrics
        self._metrics: dict[str, Any] = {
            "events_published": 0,
            "events_processed": 0,
            "events_failed": 0,
            "events_dropped": 0,
            "handler_errors": defaultdict(int),
            "processing_times": [],
        }

        logger.info(
            "event_dispatcher_created",
            dispatcher_name=name,
            max_queue_size=max_queue_size,
            processing_timeout=processing_timeout,
        )

    async def start(self) -> None:
        """Start the event dispatcher."""
        if self._running:
            logger.warning("event_dispatcher_already_running", name=self.name)
            return

        self._running = True
        self._processing_task = asyncio.create_task(self._process_events())

        logger.info("event_dispatcher_started", name=self.name)

    async def stop(self) -> None:
        """Stop the event dispatcher."""
        if not self._running:
            logger.warning("event_dispatcher_not_running", name=self.name)
            return

        self._running = False

        # Cancel processing task
        if self._processing_task:
            self._processing_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._processing_task

        # Process remaining events
        remaining_events: list[BasePortfolioEvent[Any]] = []
        while not self._event_queue.empty():
            try:
                event = self._event_queue.get_nowait()
                remaining_events.append(event)
            except asyncio.QueueEmpty:
                break

        if remaining_events:
            logger.warning(
                "event_dispatcher_stopped_with_pending_events",
                name=self.name,
                pending_count=len(remaining_events),
            )

        logger.info("event_dispatcher_stopped", name=self.name)

    async def dispatch(self, event: BasePortfolioEvent[Any]) -> None:
        """Dispatch an event to registered handlers.

        Args:
            event: Event to dispatch
        """
        if not self._running:
            logger.error(
                "event_dispatch_failed_not_running",
                event_type=event.event_type.value,
                event_id=str(event.event_id),
            )
            self._metrics["events_dropped"] += 1
            return

        try:
            # Try to add to queue without blocking
            self._event_queue.put_nowait(event)
            self._metrics["events_published"] += 1

            logger.debug(
                "event_dispatched",
                event_type=event.event_type.value,
                event_id=str(event.event_id),
                queue_size=self._event_queue.qsize(),
            )

        except asyncio.QueueFull:
            logger.exception(
                "event_queue_full",
                event_type=event.event_type.value,
                event_id=str(event.event_id),
                max_size=self.max_queue_size,
            )
            self._metrics["events_dropped"] += 1

    async def register_handler(
        self, handler: EventHandler[Any], event_filter: EventFilter | None = None
    ) -> str:
        """Register an event handler.

        Args:
            handler: Event handler
            event_filter: Optional event filter

        Returns:
            Registration ID
        """
        registration_id = str(uuid4())

        async with self._handler_lock:
            registration = HandlerRegistration(
                registration_id=registration_id,
                handler=handler,
                filter=event_filter,
                registered_at=time.time(),
            )

            self._handlers[registration_id] = registration

            logger.info(
                "event_handler_registered",
                registration_id=registration_id,
                handler_name=handler.get_handler_name(),
                has_filter=event_filter is not None,
            )

        return registration_id

    async def unregister_handler(self, registration_id: str) -> bool:
        """Unregister an event handler.

        Args:
            registration_id: ID from register_handler

        Returns:
            True if unregistered successfully
        """
        async with self._handler_lock:
            if registration_id in self._handlers:
                registration = self._handlers.pop(registration_id)

                logger.info(
                    "event_handler_unregistered",
                    registration_id=registration_id,
                    handler_name=registration.handler.get_handler_name(),
                    invocation_count=registration.invocation_count,
                )

                return True

            logger.warning("event_handler_not_found", registration_id=registration_id)

            return False

    async def _process_events(self) -> None:
        """Process events from the queue."""
        logger.info("event_processing_started", name=self.name)

        while self._running:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)

                # Process event
                await self._handle_event(event)

            except TimeoutError:
                # No events available, continue
                continue
            except asyncio.CancelledError:
                logger.info("event_processing_cancelled", name=self.name)
                break
            except Exception as e:
                logger.exception("event_processing_error", error_type=type(e).__name__)
                self._metrics["events_failed"] += 1

        logger.info("event_processing_stopped", name=self.name)

    async def _handle_event(self, event: BasePortfolioEvent[Any]) -> None:
        """Handle a single event.

        Args:
            event: Event to handle
        """
        start_time = time.time()
        handlers_invoked = 0

        # Get snapshot of handlers to avoid holding lock
        async with self._handler_lock:
            registrations = list(self._handlers.values())

        # Process event with each matching handler
        for registration in registrations:
            # Check filter
            if registration.filter and not registration.filter.should_process(event):
                continue

            # Check if handler can handle this event
            if not registration.handler.can_handle(event):
                continue

            # Invoke handler
            try:
                await asyncio.wait_for(
                    registration.handler.handle(event), timeout=self.processing_timeout
                )

                # Update registration stats
                async with self._handler_lock:
                    if registration.registration_id in self._handlers:
                        registration.invocation_count += 1
                        registration.last_invoked = time.time()

                handlers_invoked += 1

                logger.debug(
                    "event_handler_invoked",
                    event_type=event.event_type.value,
                    event_id=str(event.event_id),
                    handler_name=registration.handler.get_handler_name(),
                )

            except TimeoutError:
                logger.exception(
                    "event_handler_timeout",
                    event_type=event.event_type.value,
                    event_id=str(event.event_id),
                    handler_name=registration.handler.get_handler_name(),
                    timeout=self.processing_timeout,
                )
                self._metrics["handler_errors"][registration.handler.get_handler_name()] += 1

            except Exception as e:
                logger.exception(
                    "event_handler_error",
                    event_type=event.event_type.value,
                    event_id=str(event.event_id),
                    handler_name=registration.handler.get_handler_name(),
                    error_type=type(e).__name__,
                )
                self._metrics["handler_errors"][registration.handler.get_handler_name()] += 1

        # Update metrics
        processing_time = (time.time() - start_time) * 1000  # Convert to ms
        self._metrics["events_processed"] += 1
        self._metrics["processing_times"].append(processing_time)

        # Keep only last MAX_QUEUE_SIZE processing times
        if len(self._metrics["processing_times"]) > MAX_QUEUE_SIZE:
            self._metrics["processing_times"] = self._metrics["processing_times"][-MAX_QUEUE_SIZE:]

        if handlers_invoked > 0:
            logger.debug(
                "event_processed",
                event_type=event.event_type.value,
                event_id=str(event.event_id),
                handlers_invoked=handlers_invoked,
                processing_time_ms=processing_time,
            )
        else:
            logger.debug(
                "event_no_handlers", event_type=event.event_type.value, event_id=str(event.event_id)
            )

    def get_metrics(self) -> dict[str, Any]:
        """Get dispatcher metrics.

        Returns:
            Dictionary with metrics data
        """
        processing_times = self._metrics["processing_times"]

        if processing_times:
            avg_time = sum(processing_times) / len(processing_times)
            max_time = max(processing_times)
            min_time = min(processing_times)
        else:
            avg_time = max_time = min_time = 0.0

        return {
            "dispatcher_name": self.name,
            "is_running": self._running,
            "events_published": self._metrics["events_published"],
            "events_processed": self._metrics["events_processed"],
            "events_failed": self._metrics["events_failed"],
            "events_dropped": self._metrics["events_dropped"],
            "queue_size": self._event_queue.qsize(),
            "handler_count": len(self._handlers),
            "handler_errors": dict(self._metrics["handler_errors"]),
            "processing_time_ms": {
                "average": avg_time,
                "max": max_time,
                "min": min_time,
            },
        }

    def get_handler_info(self) -> list[dict[str, Any]]:
        """Get information about registered handlers.

        Returns:
            List of handler information dictionaries
        """
        return [
            {
                "registration_id": registration.registration_id,
                "handler_name": registration.handler.get_handler_name(),
                "has_filter": registration.filter is not None,
                "registered_at": registration.registered_at,
                "invocation_count": registration.invocation_count,
                "last_invoked": registration.last_invoked,
            }
            for registration in self._handlers.values()
        ]
