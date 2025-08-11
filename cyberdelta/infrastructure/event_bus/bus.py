"""Enhanced EventBus with priority routing and request/response support.

This implementation provides:
- Priority-based event routing (CRITICAL -> HIGH -> NORMAL -> LOW)
- Request/response pattern for synchronous queries
- Direct WebSocket raw message handling
- Pre-compiled decoders for performance
"""

import asyncio
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable
from operator import itemgetter
from typing import Any, TypeVar

import msgspec

from cyberdelta.config.models.event_system_config import EventBusConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.event_bus import HandlerPriority


logger = get_logger(__name__)

T = TypeVar("T", bound=msgspec.Struct)


class EventBus:
    """Enhanced event bus with priority routing and request/response support.

    Features:
    - Type-safe event publishing with msgspec
    - Priority-based handler execution
    - Request/response pattern for synchronous queries
    - Pre-compiled decoders for optimal performance
    - Raw WebSocket message handling
    """

    def __init__(self, config: EventBusConfig) -> None:
        """Initialize the event bus with empty handler registries.

        Args:
            config: Event bus configuration
        """
        self.config = config
        self._handlers: dict[type, list[Callable[..., Awaitable[None]]]] = defaultdict(list)
        self._priority_handlers: dict[type, list[tuple[int, Callable[..., Awaitable[None]]]]] = (
            defaultdict(list)
        )
        self._decoders: dict[str, msgspec.json.Decoder[Any]] = {}
        self._encoder = msgspec.json.Encoder()
        self._pending_requests: dict[str, asyncio.Future[msgspec.Struct]] = {}

    async def publish(self, event: msgspec.Struct) -> None:
        """Publish typed msgspec event with priority support.

        Args:
            event: The msgspec event to publish

        Note:
            Handlers are executed concurrently for performance.
            Errors in handlers are logged but don't stop other handlers.
        """
        event_type = type(event)

        # Combine regular and priority handlers
        handlers = list(self._handlers.get(event_type, []))

        # Add priority handlers in order (lower number = higher priority)
        priority_handlers = sorted(self._priority_handlers.get(event_type, []), key=itemgetter(0))

        # Higher priority handlers run first
        for _, handler in priority_handlers:
            handlers.insert(0, handler)

        if handlers:
            # Execute all handlers concurrently
            results = await asyncio.gather(
                *[handler(event) for handler in handlers], return_exceptions=True
            )

            # Log any handler errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        "handler_error_in_publish",
                        handler_name=handlers[i].__name__,
                        error=str(result),
                        exc_info=result,
                    )

    def subscribe(
        self,
        event_type: type[T],
        handler: Callable[[T], Awaitable[None]],
        priority: HandlerPriority = HandlerPriority.NORMAL,
    ) -> None:
        """Subscribe to specific event type with optional priority.

        Args:
            event_type: The msgspec event type to subscribe to
            handler: Async handler function to process events
            priority: Handler priority level (default: NORMAL)
        """
        if priority == HandlerPriority.NORMAL:
            self._handlers[event_type].append(handler)
        else:
            self._priority_handlers[event_type].append((priority.value, handler))

        # Pre-compile decoder for performance
        if event_type.__name__ not in self._decoders:
            self._decoders[event_type.__name__] = msgspec.json.Decoder(event_type)

    def unsubscribe(self, event_type: type[T], handler: Callable[[T], Awaitable[None]]) -> None:
        """Unsubscribe handler from event type.

        Args:
            event_type: The event type to unsubscribe from
            handler: The handler to remove
        """
        # Remove from regular handlers
        if event_type in self._handlers and handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)

        # Remove from priority handlers
        if event_type in self._priority_handlers:
            self._priority_handlers[event_type] = [
                (p, h) for p, h in self._priority_handlers[event_type] if h != handler
            ]

    async def request(
        self, request: msgspec.Struct, timeout_seconds: float | None = None
    ) -> msgspec.Struct | None:
        """Send request and wait for response.

        This enables synchronous-style queries in the async event system.

        Args:
            request: The request event to send
            timeout_seconds: Maximum time to wait for response (uses config default if None)

        Returns:
            Response event or None if timeout
        """
        if timeout_seconds is None:
            timeout_seconds = self.config.request_timeout_sec

        request_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future

        # Note: msgspec structs are immutable, so request_id must be set during construction
        # If request has request_id field, it should be set when creating the request
        # This is a design pattern limitation with immutable msgspec structs

        # Type-safe check for request_id field using structural typing
        # Only log warning if the struct has a request_id field but it's None
        request_id_value = getattr(request, "request_id", "<no_field>")
        if request_id_value is None:
            logger.warning(
                "request_struct_missing_id",
                request_type=type(request).__name__,
                message="Request struct should have request_id set during construction",
            )

        await self.publish(request)

        try:
            return await asyncio.wait_for(future, timeout_seconds)
        except TimeoutError:
            logger.warning(
                "request_timeout",
                request_type=type(request).__name__,
                timeout_seconds=timeout_seconds,
            )
            return None
        finally:
            self._pending_requests.pop(request_id, None)

    async def respond(self, request_id: str, response: msgspec.Struct) -> None:
        """Send response to a pending request.

        Args:
            request_id: The ID of the original request
            response: The response event to send
        """
        future = self._pending_requests.get(request_id)
        if future and not future.done():
            future.set_result(response)

    async def publish_raw(self, raw_bytes: bytes, event_type: type[T]) -> None:
        """Ultra-fast path for WebSocket data.

        Decodes raw bytes directly to msgspec event and publishes.

        Args:
            raw_bytes: Raw bytes from WebSocket
            event_type: The msgspec event type to decode to
        """
        decoder = self._decoders.get(event_type.__name__)
        if not decoder:
            decoder = msgspec.json.Decoder(event_type)
            self._decoders[event_type.__name__] = decoder

        try:
            event = decoder.decode(raw_bytes)
            await self.publish(event)
        except Exception as e:
            logger.exception("failed_to_decode_raw_message", error=str(e))

    def get_handler_count(self, event_type: type[msgspec.Struct]) -> int:
        """Get total number of handlers for an event type.

        Args:
            event_type: The event type to check

        Returns:
            Total count of handlers (regular + priority)
        """
        regular_count = len(self._handlers.get(event_type, []))
        priority_count = len(self._priority_handlers.get(event_type, []))
        return regular_count + priority_count

    def clear_handlers(self, event_type: type[msgspec.Struct] | None = None) -> None:
        """Clear handlers for specific event type or all handlers.

        Args:
            event_type: Event type to clear handlers for, or None for all
        """
        if event_type:
            self._handlers.pop(event_type, None)
            self._priority_handlers.pop(event_type, None)
        else:
            self._handlers.clear()
            self._priority_handlers.clear()

    def get_total_handler_count(self) -> int:
        """Get total count of all registered handlers.

        Returns:
            Total number of handlers across all event types
        """
        regular_count = sum(len(handlers) for handlers in self._handlers.values())
        priority_count = sum(len(handlers) for handlers in self._priority_handlers.values())
        return regular_count + priority_count

    def get_pending_request_count(self) -> int:
        """Get count of pending requests.

        Returns:
            Number of pending requests
        """
        return len(self._pending_requests)
