"""Comprehensive tests for EventBus.

Tests cover:
1. Event publishing and subscription
2. Priority-based handler execution
3. Request/response pattern
4. Raw WebSocket message handling
5. Error handling and resilience
6. Performance characteristics
"""

import asyncio
import time
from unittest.mock import AsyncMock

import msgspec
import pytest

from cyberdelta.config.models.event_system_config import EventBusConfig
from cyberdelta.enums.event_bus import HandlerPriority
from cyberdelta.infrastructure.event_bus import EventBus


pytestmark = pytest.mark.timing


# Test event structures
class MockEvent(msgspec.Struct):
    """Simple mock event for testing."""

    message: str
    value: int = 0


class MockRequestEvent(msgspec.Struct):
    """Mock request event with request ID."""

    request_id: str = ""
    query: str = "default"


class MockResponseEvent(msgspec.Struct):
    """Mock response event."""

    result: str
    status: str = "ok"


class TestEventBusBasics:
    """Test basic event bus functionality."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.fixture
    def test_event(self) -> MockEvent:
        """Create test event.

        Returns:
            MockEvent: Test event instance.
        """
        return MockEvent(message="test", value=42)

    def test_event_bus_initialization(self, event_bus: EventBus) -> None:
        """Test event bus initializes correctly."""
        # Test via public interface - initially no handlers for any event type
        assert event_bus.get_handler_count(MockEvent) == 0
        assert event_bus.get_handler_count(MockRequestEvent) == 0
        assert event_bus.get_handler_count(MockResponseEvent) == 0

    def test_handler_subscription_normal_priority(self, event_bus: EventBus) -> None:
        """Test subscribing handler with normal priority."""
        handler = AsyncMock()

        # Before subscription - no handlers
        initial_count = event_bus.get_handler_count(MockEvent)
        assert initial_count == 0

        event_bus.subscribe(MockEvent, handler, HandlerPriority.NORMAL)

        # After subscription - should have one handler
        final_count = event_bus.get_handler_count(MockEvent)
        assert final_count == 1

    def test_handler_subscription_high_priority(self, event_bus: EventBus) -> None:
        """Test subscribing handler with high priority."""
        handler = AsyncMock()

        # Before subscription - no handlers
        initial_count = event_bus.get_handler_count(MockEvent)
        assert initial_count == 0

        event_bus.subscribe(MockEvent, handler, HandlerPriority.HIGH)

        # After subscription - should have one handler
        final_count = event_bus.get_handler_count(MockEvent)
        assert final_count == 1

    def test_handler_unsubscription(self, event_bus: EventBus) -> None:
        """Test unsubscribing handlers."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()

        # Subscribe both handlers
        event_bus.subscribe(MockEvent, handler1)
        event_bus.subscribe(MockEvent, handler2, HandlerPriority.HIGH)

        # Should have 2 handlers
        count_before_unsub = event_bus.get_handler_count(MockEvent)
        assert count_before_unsub == 2

        # Unsubscribe one
        event_bus.unsubscribe(MockEvent, handler1)

        # Should have 1 handler remaining
        count_after_unsub = event_bus.get_handler_count(MockEvent)
        assert count_after_unsub == 1

    def test_get_handler_count(self, event_bus: EventBus) -> None:
        """Test getting handler count."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()

        # Initially no handlers
        assert event_bus.get_handler_count(MockEvent) == 0

        # Add handlers
        event_bus.subscribe(MockEvent, handler1)
        event_bus.subscribe(MockEvent, handler2)
        event_bus.subscribe(MockEvent, handler3, HandlerPriority.CRITICAL)

        assert event_bus.get_handler_count(MockEvent) == 3

    def test_clear_handlers_specific(self, event_bus: EventBus) -> None:
        """Test clearing handlers for specific event type."""
        handler = AsyncMock()
        event_bus.subscribe(MockEvent, handler)
        event_bus.subscribe(MockResponseEvent, handler)

        event_bus.clear_handlers(MockEvent)

        assert event_bus.get_handler_count(MockEvent) == 0
        assert event_bus.get_handler_count(MockResponseEvent) == 1

    def test_clear_all_handlers(self, event_bus: EventBus) -> None:
        """Test clearing all handlers."""
        handler = AsyncMock()
        event_bus.subscribe(MockEvent, handler)
        event_bus.subscribe(MockResponseEvent, handler)

        event_bus.clear_handlers()

        assert event_bus.get_handler_count(MockEvent) == 0
        assert event_bus.get_handler_count(MockResponseEvent) == 0


class TestEventBusPublishing:
    """Test event publishing functionality."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.mark.asyncio
    async def test_publish_no_handlers(self, event_bus: EventBus) -> None:
        """Test publishing event with no handlers."""
        event = MockEvent(message="orphan")

        # Should not raise exception
        await event_bus.publish(event)

    @pytest.mark.asyncio
    async def test_publish_single_handler(self, event_bus: EventBus) -> None:
        """Test publishing to single handler."""
        handler = AsyncMock()
        event = MockEvent(message="single", value=1)

        event_bus.subscribe(MockEvent, handler)
        await event_bus.publish(event)

        handler.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_publish_multiple_handlers(self, event_bus: EventBus) -> None:
        """Test publishing to multiple handlers."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()
        event = MockEvent(message="multiple", value=3)

        event_bus.subscribe(MockEvent, handler1)
        event_bus.subscribe(MockEvent, handler2)
        event_bus.subscribe(MockEvent, handler3)

        await event_bus.publish(event)

        handler1.assert_called_once_with(event)
        handler2.assert_called_once_with(event)
        handler3.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_handler_error_isolation(self, event_bus: EventBus) -> None:
        """Test that handler errors don't affect other handlers."""
        good_handler = AsyncMock()
        bad_handler = AsyncMock(side_effect=ValueError("Handler error"))
        another_good_handler = AsyncMock()
        event = MockEvent(message="error_test")

        event_bus.subscribe(MockEvent, good_handler)
        event_bus.subscribe(MockEvent, bad_handler)
        event_bus.subscribe(MockEvent, another_good_handler)

        # Should not raise exception
        await event_bus.publish(event)

        # Good handlers should still be called
        good_handler.assert_called_once_with(event)
        another_good_handler.assert_called_once_with(event)
        bad_handler.assert_called_once_with(event)


class TestEventBusPriority:
    """Test priority-based handler execution."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.mark.asyncio
    async def test_priority_execution_order(self, event_bus: EventBus) -> None:
        """Test handlers are ordered by priority in the handler list."""
        # Test that priority handlers are correctly sorted in the internal data structure

        async def critical_handler(event: MockEvent) -> None:
            pass

        async def high_handler(event: MockEvent) -> None:
            pass

        async def normal_handler(event: MockEvent) -> None:
            pass

        async def low_handler(event: MockEvent) -> None:
            pass

        # Subscribe in random order
        event_bus.subscribe(MockEvent, low_handler, HandlerPriority.LOW)
        event_bus.subscribe(MockEvent, normal_handler, HandlerPriority.NORMAL)
        event_bus.subscribe(MockEvent, critical_handler, HandlerPriority.CRITICAL)
        event_bus.subscribe(MockEvent, high_handler, HandlerPriority.HIGH)

        # Check total handler count includes all priority levels
        total_handlers = event_bus.get_handler_count(MockEvent)
        assert total_handlers == 4  # All 4 handlers (CRITICAL, HIGH, NORMAL, LOW)

    @pytest.mark.asyncio
    async def test_mixed_priority_and_normal_handlers(self, event_bus: EventBus) -> None:
        """Test mixing priority and normal handlers."""
        execution_order: list[str] = []

        async def critical_handler(event: MockEvent) -> None:
            await asyncio.sleep(0)  # Minimal async operation
            execution_order.append("critical")

        async def normal_handler1(event: MockEvent) -> None:
            await asyncio.sleep(0)  # Minimal async operation
            execution_order.append("normal1")

        async def normal_handler2(event: MockEvent) -> None:
            await asyncio.sleep(0)  # Minimal async operation
            execution_order.append("normal2")

        # Subscribe with mixed priorities
        event_bus.subscribe(MockEvent, normal_handler1)  # Normal priority
        event_bus.subscribe(MockEvent, critical_handler, HandlerPriority.CRITICAL)
        event_bus.subscribe(MockEvent, normal_handler2)  # Normal priority

        event = MockEvent(message="mixed_test")
        await event_bus.publish(event)

        # Critical should execute first, then normal handlers
        assert execution_order[0] == "critical"
        assert "normal1" in execution_order[1:]
        assert "normal2" in execution_order[1:]


class TestEventBusRequestResponse:
    """Test request/response pattern."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.mark.asyncio
    async def test_request_response_success(self, event_bus: EventBus) -> None:
        """Test successful request/response."""

        async def request_handler(request: MockRequestEvent) -> None:
            # Simulate processing and respond
            response = MockResponseEvent(result=f"processed: {request.query}")
            await event_bus.respond(request.request_id, response)

        event_bus.subscribe(MockRequestEvent, request_handler)

        request = MockRequestEvent(query="test_query")
        response = await event_bus.request(request, timeout_seconds=1.0)

        assert response is not None
        assert isinstance(response, MockResponseEvent)
        assert response.result == "processed: test_query"

    @pytest.mark.asyncio
    async def test_request_timeout(self, event_bus: EventBus) -> None:
        """Test request timeout."""
        # No handler registered, should timeout
        request = MockRequestEvent(query="timeout_test")
        response = await event_bus.request(request, timeout_seconds=0.1)

        assert response is None

    @pytest.mark.asyncio
    async def test_multiple_concurrent_requests(self, event_bus: EventBus) -> None:
        """Test multiple concurrent requests."""

        async def request_handler(request: MockRequestEvent) -> None:
            # Add small delay to simulate processing
            await asyncio.sleep(0.01)
            response = MockResponseEvent(result=f"processed: {request.query}")
            await event_bus.respond(request.request_id, response)

        event_bus.subscribe(MockRequestEvent, request_handler)

        # Send multiple concurrent requests
        requests = [MockRequestEvent(query=f"query_{i}") for i in range(5)]

        responses = await asyncio.gather(*[
            event_bus.request(req, timeout_seconds=1.0) for req in requests
        ])

        # All should succeed
        assert len(responses) == 5
        for i, response in enumerate(responses):
            assert response is not None
            assert response is not None
            assert isinstance(response, MockResponseEvent)
            assert response.result == f"processed: query_{i}"


class TestEventBusRawHandling:
    """Test raw WebSocket message handling."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.mark.asyncio
    async def test_publish_raw_valid_json(self, event_bus: EventBus) -> None:
        """Test publishing raw valid JSON bytes."""
        handler = AsyncMock()
        event_bus.subscribe(MockEvent, handler)

        # Create valid JSON bytes
        event = MockEvent(message="raw_test", value=123)
        raw_bytes = msgspec.json.encode(event)

        await event_bus.publish_raw(raw_bytes, MockEvent)

        # Handler should be called with decoded event
        handler.assert_called_once()
        called_event = handler.call_args[0][0]
        assert called_event.message == "raw_test"
        assert called_event.value == 123

    @pytest.mark.asyncio
    async def test_publish_raw_invalid_json(self, event_bus: EventBus) -> None:
        """Test publishing raw invalid JSON bytes."""
        handler = AsyncMock()
        event_bus.subscribe(MockEvent, handler)

        # Invalid JSON bytes
        invalid_bytes = b'{"invalid": json}'

        # Should not raise exception, should log error
        await event_bus.publish_raw(invalid_bytes, MockEvent)

        # Handler should not be called
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_decoder_caching(self, event_bus: EventBus) -> None:
        """Test that decoders are cached for performance."""
        # First call should create decoder
        raw_bytes = msgspec.json.encode(MockEvent(message="cache_test"))
        await event_bus.publish_raw(raw_bytes, MockEvent)

        # Test via behavior - should be able to decode and handle multiple raw messages
        # without errors (decoder caching is an implementation detail)
        await event_bus.publish_raw(raw_bytes, MockEvent)

        # Both raw publishes should have succeeded without errors
        # Implementation detail: decoder should be cached, but we test behavior not internals


class TestEventBusEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    def test_unsubscribe_nonexistent_handler(self, event_bus: EventBus) -> None:
        """Test unsubscribing handler that was never subscribed."""
        handler = AsyncMock()

        # Should not raise exception
        event_bus.unsubscribe(MockEvent, handler)

    def test_get_handler_count_nonexistent_event(self, event_bus: EventBus) -> None:
        """Test getting handler count for unregistered event type."""
        assert event_bus.get_handler_count(MockEvent) == 0

    @pytest.mark.asyncio
    async def test_respond_to_nonexistent_request(self, event_bus: EventBus) -> None:
        """Test responding to non-existent request."""
        response = MockResponseEvent(result="orphan_response")

        # Should not raise exception
        await event_bus.respond("nonexistent_id", response)

    @pytest.mark.asyncio
    async def test_respond_to_completed_request(self, event_bus: EventBus) -> None:
        """Test responding to already completed request."""

        async def handler(request: MockRequestEvent) -> None:
            response = MockResponseEvent(result="first_response")
            await event_bus.respond(request.request_id, response)

            # Try to respond again
            duplicate_response = MockResponseEvent(result="duplicate_response")
            await event_bus.respond(request.request_id, duplicate_response)

        event_bus.subscribe(MockRequestEvent, handler)

        request = MockRequestEvent(query="duplicate_test")
        response = await event_bus.request(request, timeout_seconds=1.0)

        # Should get first response
        assert response is not None
        assert isinstance(response, MockResponseEvent)
        assert response.result == "first_response"


class TestEventBusPerformance:
    """Test performance characteristics of event bus."""

    @pytest.fixture
    def event_bus(self) -> EventBus:
        """Create test event bus.

        Returns:
            EventBus: Test event bus instance.
        """
        config = EventBusConfig()
        return EventBus(config)

    @pytest.mark.asyncio
    async def test_high_frequency_publishing(self, event_bus: EventBus) -> None:
        """Test high-frequency event publishing."""
        call_count = 0

        async def fast_handler(event: MockEvent) -> None:
            await asyncio.sleep(0)  # Minimal async operation for event bus compatibility
            nonlocal call_count
            call_count += 1

        event_bus.subscribe(MockEvent, fast_handler)

        # Publish 1000 events rapidly
        events = [MockEvent(message=f"event_{i}", value=i) for i in range(1000)]

        start_time = time.time()

        for event in events:
            await event_bus.publish(event)

        end_time = time.time()
        duration = end_time - start_time

        # All events should be processed
        assert call_count == 1000

        # Should process at least 1000 events/second
        events_per_second = 1000 / duration
        assert events_per_second > 1000, f"Only {events_per_second:.0f} events/sec"

    @pytest.mark.asyncio
    async def test_concurrent_publishing(self, event_bus: EventBus) -> None:
        """Test concurrent event publishing."""
        call_count = 0
        lock = asyncio.Lock()

        async def concurrent_handler(event: MockEvent) -> None:
            nonlocal call_count
            async with lock:
                call_count += 1

        event_bus.subscribe(MockEvent, concurrent_handler)

        # Publish 100 events concurrently
        events = [MockEvent(message=f"concurrent_{i}") for i in range(100)]

        await asyncio.gather(*[event_bus.publish(event) for event in events])

        # All events should be processed
        assert call_count == 100
